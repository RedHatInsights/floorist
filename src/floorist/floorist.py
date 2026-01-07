from datetime import date
from floorist.config import get_config
from os import environ
from sqlalchemy import create_engine
from sqlalchemy import exc as sqlalchemy_exc
from uuid import UUID

import awswrangler as wr
import boto3
import logging
import pandas as pd
import time
import yaml

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

LOG_FMT = "[%(asctime)s] [%(levelname)s] %(message)s"

_RETRYABLE_DB_ERROR_PATTERNS = (
    "SerializationFailure",
    "conflict with recovery",
    "PendingRollbackError",
    "invalid transaction",
)


def _is_retryable_db_error(ex: Exception) -> bool:
    error_str = str(ex)
    return any(pattern in error_str for pattern in _RETRYABLE_DB_ERROR_PATTERNS)


def _configure_loglevel():
    LOGLEVEL = environ.get('LOGLEVEL', 'INFO').upper()
    logging.basicConfig(level=LOGLEVEL, format=LOG_FMT)


def _cleanup_s3_target(dump_count, target):
    logging.info(
        "[Dump #%d] Cleaning up S3 target before retry: %s", dump_count, target
    )
    try:
        wr.s3.delete_objects(target)
        return True
    except Exception as cleanup_ex:
        logging.error(
            "[Dump #%d] Failed to cleanup S3 target before retry: %s",
            dump_count,
            cleanup_ex,
        )
        return False


def _safe_rollback(conn, dump_count):
    try:
        conn.rollback()
        logging.warning(
            "[Dump #%d] Database serialization/recovery conflict detected. "
            "Rolling back transaction.",
            dump_count,
        )
    except Exception as rollback_ex:
        logging.error(
            "[Dump #%d] Error during rollback: %s", dump_count, rollback_ex
        )


def _write_chunks(path, target, row, conn, config, dump_count):
    chunksize = row.get('chunksize', 1000)
    if chunksize == 0:
        chunksize = None
    query = row["query"]

    logging.debug("[Dump #%d] Query: %s", dump_count, query)

    cursor = pd.read_sql(query, conn, chunksize=chunksize)

    # This should allow the parsing of non-streamed results with the same iterative approach below
    if isinstance(cursor, pd.DataFrame):
        cursor = [cursor]

    uuids = {}

    chunk = 1
    for data in cursor:
        if len(uuids) == 0 and len(data) > 0:
            # Detect any columns with UUID
            for column in data:
                if isinstance(data[column][0], UUID):
                    logging.debug("[Dump #%d] UUID column detected: %s", dump_count, column)
                    uuids[column] = "string"

        # Convert any columns with UUID type to string
        data = data.astype(uuids)

        if len(data) > 0:
            wr.s3.to_parquet(data, target,
                             index=False,
                             compression='gzip',
                             dataset=True,
                             mode='append'
                             )
            logging.info("[Dump #%d] Written parquet chunk #%d", dump_count, chunk)
            chunk += 1
        else:
            # Create an empty folder if the returned dataset is empty
            wr._utils.client('s3').put_object(Bucket=config.bucket_name, Body='', Key=path+'/')
            logging.info("[Dump #%d] Empty folder created for empty result", dump_count)

    logging.debug(
        "[Dump #%d] Dumped %s to %s", dump_count, row['query'], row['prefix']
    )


def _dump_with_retry(row, conn, config, dump_count):
    """
    Execute a dump with retry logic.

    Args:
        row: Floorplan row configuration containing 'query', 'prefix', etc.
        conn: Database connection object
        config: Configuration object with bucket_name, etc.
        dump_count: The dump number for logging

    Returns:
        bool: True if dump succeeded, False if dump failed
    """
    try:
        path = f"{row['prefix']}/{date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')}"
        target = f"s3://{config.bucket_name}/{path}"
    except KeyError as ex:
        logging.exception("[Dump #%d] %s", dump_count, ex)
        return False

    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0:
                logging.info(
                    "[Dump #%d] Retry %d of %d (attempt %d total)",
                    dump_count,
                    attempt,
                    MAX_RETRIES - 1,
                    attempt + 1,
                )
                # On retry, clean up the S3 target to avoid duplicate data
                if not _cleanup_s3_target(dump_count, target):
                    logging.error(
                        "[Dump #%d] Cannot retry due to S3 cleanup failure",
                        dump_count,
                    )
                    return False

            _write_chunks(
                path,
                target,
                row,
                conn,
                config,
                dump_count
            )

            # Commit the transaction to release resources and prevent long-running transactions
            conn.commit()
            return True  # Success

        except (
            sqlalchemy_exc.OperationalError,
            sqlalchemy_exc.PendingRollbackError,
        ) as ex:
            _safe_rollback(conn, dump_count)
            if not _is_retryable_db_error(ex):
                logging.exception("[Dump #%d] %s", dump_count, ex)
                break

            if attempt >= MAX_RETRIES - 1:
                logging.exception("[Dump #%d] Retries exhausted %s", dump_count, ex)
                break

            backoff_time = RETRY_DELAY * (2 ** attempt)  # 5s, 10s, 20s
            logging.warning(
                "[Dump #%d] Retrying in %d seconds due to: %s",
                dump_count,
                backoff_time,
                str(ex).split("\n")[0],
            )
            time.sleep(backoff_time)
            continue

        except Exception as ex:
            # Non-retryable exceptions
            logging.exception("[Dump #%d] %s", dump_count, ex)
            break

    return False  # Dump failed


def main():
    _configure_loglevel()
    config = get_config()

    # Compatibility with minio, setting the endpoint URL explicitly if available
    if config.bucket_url:
      wr.config.s3_endpoint_url = config.bucket_url

    boto3.setup_default_session(aws_access_key_id=config.bucket_access_key, aws_secret_access_key=config.bucket_secret_key, region_name=config.bucket_region)

    # Fails if can't connect to S3 or the bucket does not exist
    wr.s3.list_directories(f"s3://{config.bucket_name}")
    logging.info('Successfully connected to the S3 bucket')

    engine = create_engine(f"postgresql://{config.database_username}:{config.database_password}@{config.database_hostname}/{config.database_name}")
    conn = engine.connect().execution_options(stream_results=True)
    logging.info('Successfully connected to the database')

    dump_count = 0
    dumped_count = 0

    with open(config.floorplan_filename, 'r') as stream:
        for row in yaml.safe_load(stream):
            dump_count += 1

            if _dump_with_retry(row, conn, config, dump_count):
                dumped_count += 1

    logging.info('Dumped %d from total of %d', dumped_count, dump_count)

    conn.close()

    if dumped_count != dump_count:
        exit(1)
