from datetime import date
from enum import Enum
from typing import Generator

import botocore.exceptions
from pandas import DataFrame

from floorist.config import get_config, Config
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


class RetryResult(Enum):
    RETRY = "retry"
    FAILURE = "failure"
    EXHAUSTED = "exhausted"


class RetryPolicy:
    def __init__(self, max_retries=3, base_delay=5):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def evaluate(self, ex: Exception, attempt: int) -> RetryResult:
        if not self._is_retryable(ex):
            return RetryResult.FAILURE
        if attempt >= self.max_retries - 1:
            return RetryResult.EXHAUSTED
        return RetryResult.RETRY

    def backoff_delay(self, attempt: int) -> float:
        return self.base_delay * (2 ** attempt)

    @staticmethod
    def _is_retryable(ex: Exception) -> bool:
        error_str = str(ex)
        return any(p in error_str for p in _RETRYABLE_DB_ERROR_PATTERNS)


class DatabaseClient:
    def __init__(self, config: Config):
        self.engine = create_engine(
            f"postgresql://{config.database_username}:{config.database_password}@{config.database_hostname}/{config.database_name}"
        )
        self.conn = self.engine.connect().execution_options(stream_results=True)

    def execute_query(self, query, chunksize) -> Generator[DataFrame, None, None]:
        result = pd.read_sql(query, self.conn, chunksize=chunksize)
        if isinstance(result, DataFrame):
            yield result
        else:
            yield from result

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
        self.engine.dispose()


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

def _safe_rollback(db_client, dump_count):
    try:
        db_client.rollback()
        logging.warning(
            "[Dump #%d] Database serialization/recovery conflict detected. "
            "Rolling back transaction.",
            dump_count,
        )
    except Exception as rollback_ex:
        logging.error(
            "[Dump #%d] Error during rollback: %s", dump_count, rollback_ex
        )


def _write_chunks(path, target, query, chunksize, db_client, config, dump_count):
    logging.debug("[Dump #%d] Query: %s", dump_count, query)
    cursor = db_client.execute_query(query, chunksize)

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
        "[Dump #%d] Dumped %s to %s", dump_count, query, path
    )


def _dump_with_retry(row, db_client, config, dump_count):
    """
    Execute a dump with retry logic.

    Args:
        row: Floorplan row configuration containing 'query', 'prefix', etc.
        db_client: DatabaseClient object
        config: Configuration object with bucket_name, etc.
        dump_count: The dump number for logging

    Returns:
        bool: True if dump succeeded, False if dump failed
    """
    try:
        path = f"{row['prefix']}/{date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')}"
        target = f"s3://{config.bucket_name}/{path}"
        query = row["query"]
        chunksize = row.get("chunksize", 1000) or None
    except KeyError as ex:
        logging.exception("[Dump #%d] %s", dump_count, ex)
        return False

    retry_policy: RetryPolicy = RetryPolicy(MAX_RETRIES, RETRY_DELAY)
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
                query,
                chunksize,
                db_client,
                config,
                dump_count
            )

            # Commit the transaction to release resources and prevent long-running transactions
            db_client.commit()
            return True  # Success

        except (
            sqlalchemy_exc.OperationalError,
            sqlalchemy_exc.PendingRollbackError,
        ) as ex:
            _safe_rollback(db_client, dump_count)

            retry_result = retry_policy.evaluate(ex, attempt)

            if retry_result == RetryResult.FAILURE:
                logging.exception("[Dump #%d] %s", dump_count, ex)
                break

            if retry_result == RetryResult.EXHAUSTED:
                logging.exception("[Dump #%d] Retries exhausted %s", dump_count, ex)
                break

            backoff_time = retry_policy.backoff_delay(attempt)
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
    try:
        wr.s3.list_directories(f"s3://{config.bucket_name}")
    except botocore.exceptions.ClientError as e:
        # On an exception, try again with a trailing slash since the client might not have
        # ListBuckets permission on the bucket name itself, but only on items beneath it.
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in {"AccessDenied"}:
            wr.s3.list_directories(f"s3://{config.bucket_name.rstrip('/')}/")
        else:
            raise

    logging.info('Successfully connected to the S3 bucket')

    db_client: DatabaseClient = DatabaseClient(config)
    logging.info('Successfully connected to the database')

    dump_count = 0
    dumped_count = 0

    with open(config.floorplan_filename, 'r') as stream:
        for row in yaml.safe_load(stream):
            dump_count += 1

            if _dump_with_retry(row, db_client, config, dump_count):
                dumped_count += 1

    logging.info('Dumped %d from total of %d', dumped_count, dump_count)

    db_client.close()

    if dumped_count != dump_count:
        exit(1)
