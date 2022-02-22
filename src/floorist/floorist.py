from datetime import date
from uuid import uuid4 as uuid

import watchtower
from s3fs import S3FileSystem as s3
from floorist.config import get_config
import os
from boto3.session import Session

import logging
import pandas.io.sql as sqlio
import psycopg2
import yaml


def _get_logger(config):
    logging.basicConfig(level=config.loglevel)
    logger = logging.getLogger(__name__)
    logger.setLevel(config.loglevel)

    if config.cloudwatch_config:
        logger.info("Configuring Cloudwatch logging")
        logger.addHandler(_get_cloudwatch_handler(config.cloudwatch_config))
    else:
        logger.info("Cloudwatch config not found - skipping")

    return logger


def _get_cloudwatch_handler(config):
    aws_access_key_id = config.accessKeyId
    aws_secret_access_key = config.secretAccessKey
    aws_region_name = config.region
    aws_log_group = config.logGroup
    aws_log_stream = os.getenv("AWS_LOG_STREAM", _get_hostname())

    logging.info(f"Configuring watchtower logging (log_group={aws_log_group}, "
                 f"stream_name={aws_log_stream})")
    boto3_session = Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name,
    )
    handler = watchtower.CloudWatchLogHandler(boto3_session=boto3_session,
                                              stream_name=aws_log_stream,
                                              log_group=aws_log_group,
                                              create_log_group=False)

    return handler


def _get_hostname():
    return os.uname().nodename


def main():
    config = get_config()
    logger = _get_logger(config)

    # Fails if can't connect to S3 or the bucket does not exist
    s3(secret=config.bucket_secret_key, key=config.bucket_access_key,
       client_kwargs={'endpoint_url': config.bucket_url}).ls(config.bucket_name)
    logger.debug('Successfully connected to the S3 bucket')

    conn = psycopg2.connect(
        host=config.database_hostname,
        user=config.database_username,
        password=config.database_password,
        database=config.database_name
    )
    logger.debug('Successfully connected to the database')

    dump_count = 0
    dumped_count = 0

    with open(config.floorplan_filename, 'r') as stream:
        # This try block allows us to proceed if a single SQL query fails
        for row in yaml.safe_load(stream):
            dump_count += 1

            try:
                logger.debug(f"Dumping #{dump_count}: {row['query']} to {row['prefix']}")

                data = sqlio.read_sql_query(row['query'], conn)
                target = '/'.join([
                    f"s3://{config.bucket_name}",
                    row['prefix'],
                    date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d'),
                    f"{uuid()}.parquet"
                ])

                data.to_parquet(
                    path=target,
                    compression='gzip',
                    index=False,
                    storage_options={
                        'secret': config.bucket_secret_key,
                        'key': config.bucket_access_key,
                        'client_kwargs': {'endpoint_url': config.bucket_url}
                    }
                )

                logger.debug(f"Dumped #{dumped_count}: {row['query']} to {row['prefix']}")

                dumped_count += 1
            except Exception as ex:
                logger.exception(ex)

    logger.info(f'Dumped {dumped_count} from total of {dump_count}')

    conn.close()

    if dumped_count != dump_count:
        exit(1)
