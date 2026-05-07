from datetime import date
from enum import Enum
from typing import Generator

import botocore.exceptions
from pandas import DataFrame

from floorist.config import get_config, Config
from os import environ
from sqlalchemy import create_engine, event
from sqlalchemy import exc as sqlalchemy_exc

import awswrangler as wr
import boto3
import logging
import pandas as pd
import psycopg2.extensions
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
        return self.base_delay * (2**attempt)

    @staticmethod
    def _is_retryable(ex: Exception) -> bool:
        error_str = str(ex)
        return any(p in error_str for p in _RETRYABLE_DB_ERROR_PATTERNS)


class S3Client:
    def __init__(self, config: Config):
        self.bucket_name = config.bucket_name

        # Compatibility with minio, setting the endpoint URL explicitly if available
        self.bucket_url = config.bucket_url
        if self.bucket_url:
            wr.config.s3_endpoint_url = self.bucket_url

        boto3.setup_default_session(
            aws_access_key_id=config.bucket_access_key,
            aws_secret_access_key=config.bucket_secret_key,
            region_name=config.bucket_region,
        )

    def verify(self):
        # Fails if can't connect to S3 or the bucket does not exist
        try:
            wr.s3.list_directories(f"s3://{self.bucket_name}")
        except botocore.exceptions.ClientError as e:
            # On an exception, try again with a trailing slash since the client might not have
            # ListBuckets permission on the bucket name itself, but only on items beneath it.
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in {"AccessDenied"}:
                wr.s3.list_directories(f"s3://{self.bucket_name.rstrip('/')}/")
            else:
                raise

    def make_path(self, prefix):
        path = f"{prefix}/{date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')}"
        target = f"s3://{self.bucket_name}/{path}"
        return path, target

    def write_parquet(self, data, target, path):
        if len(data) > 0:
            wr.s3.to_parquet(data, target, index=False, compression="gzip", dataset=True, mode="append")
        else:
            wr._utils.client("s3").put_object(Bucket=self.bucket_name, Body="", Key=path + "/")

    def cleanup(self, target):
        wr.s3.delete_objects(target)


class DatabaseClient:
    _uuid_caster = psycopg2.extensions.new_type(
        (2950,),  # PostgreSQL OID for UUID type: select oid from pg_type where typname='uuid'
        "UUID_AS_STRING",
        psycopg2.STRING,
    )

    def __init__(self, config: Config):
        self.engine = create_engine(
            f"postgresql://{config.database_username}:{config.database_password}@{config.database_hostname}/{config.database_name}"
        )
        event.listen(self.engine, "connect", self._register_uuid_caster)
        self.conn = self.engine.connect().execution_options(stream_results=True)

    @staticmethod
    def _register_uuid_caster(dbapi_conn, connection_record):
        # Convert all UUID types to strings automatically.  The s3.write_parquet method will fail if
        # columns remain the UUID type
        psycopg2.extensions.register_type(DatabaseClient._uuid_caster, dbapi_conn)

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


class DumpExecutor:
    def __init__(self, s3_client, db_client, retry_policy):
        self.s3_client = s3_client
        self.db_client = db_client
        self.retry_policy = retry_policy

    def _write_chunks(self, path, target, query, chunksize, dump_count):
        logging.debug("[Dump #%d] Query: %s", dump_count, query)
        cursor = self.db_client.execute_query(query, chunksize)

        chunk = 1
        for data in cursor:
            self.s3_client.write_parquet(data, target, path)
            if len(data) > 0:
                logging.info("[Dump #%d] Written parquet chunk #%d", dump_count, chunk)
                chunk += 1
            else:
                logging.info("[Dump #%d] Empty folder created for empty result", dump_count)

        logging.debug("[Dump #%d] Dumped %s to %s", dump_count, query, path)

    def execute(self, row, dump_count) -> bool:
        """
        Execute a dump with retry logic.

        Args:
            row: Floorplan row configuration containing 'query', 'prefix', etc.
            dump_count: The dump number for logging

        Returns:
            bool: True if dump succeeded, False if dump failed
        """
        try:
            path, target = self.s3_client.make_path(row["prefix"])
            query = row["query"]
            chunksize = row.get("chunksize", 1000) or None
        except KeyError as ex:
            logging.exception("[Dump #%d] %s", dump_count, ex)
            return False

        for attempt in range(self.retry_policy.max_retries):
            try:
                if attempt > 0:
                    logging.info(
                        "[Dump #%d] Retry %d of %d (attempt %d total)",
                        dump_count,
                        attempt,
                        self.retry_policy.max_retries - 1,
                        attempt + 1,
                    )
                    try:
                        self.s3_client.cleanup(target)
                    except Exception:
                        logging.exception("[Dump #%d] S3 cleanup failed, cannot retry", dump_count)
                        return False

                self._write_chunks(path, target, query, chunksize, dump_count)

                # Commit the transaction to release resources and prevent long-running transactions
                self.db_client.commit()
                return True  # Success

            except (
                sqlalchemy_exc.OperationalError,
                sqlalchemy_exc.PendingRollbackError,
            ) as ex:
                logging.warning("[Dump #%d] Database error, rolling back", dump_count)
                try:
                    self.db_client.rollback()
                except Exception as rollback_ex:
                    logging.exception("[Dump #%d] Rollback failed: %s", dump_count, rollback_ex)

                retry_result = self.retry_policy.evaluate(ex, attempt)

                if retry_result == RetryResult.FAILURE:
                    logging.exception("[Dump #%d] %s", dump_count, ex)
                    break

                if retry_result == RetryResult.EXHAUSTED:
                    logging.exception("[Dump #%d] Retries exhausted %s", dump_count, ex)
                    break

                backoff_time = self.retry_policy.backoff_delay(attempt)
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


class Floorist:
    def __init__(self, config):
        self.config = config

        s3_client: S3Client = S3Client(config)
        s3_client.verify()
        logging.info("Successfully connected to the S3 bucket")

        self.db_client: DatabaseClient = DatabaseClient(config)
        logging.info("Successfully connected to the database")

        retry_policy: RetryPolicy = RetryPolicy(MAX_RETRIES, RETRY_DELAY)
        self.executor = DumpExecutor(s3_client, self.db_client, retry_policy)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db_client.close()

    def run(self):
        dump_count = 0
        dumped_count = 0

        with open(self.config.floorplan_filename, "r") as stream:
            for row in yaml.safe_load(stream):
                dump_count += 1

                if self.executor.execute(row, dump_count):
                    dumped_count += 1

        logging.info("Dumped %d from total of %d", dumped_count, dump_count)
        if dumped_count != dump_count:
            exit(1)


def _configure_loglevel():
    LOGLEVEL = environ.get("LOGLEVEL", "INFO").upper()
    logging.basicConfig(level=LOGLEVEL, format=LOG_FMT)


def main():
    _configure_loglevel()
    with Floorist(get_config()) as f:
        f.run()
