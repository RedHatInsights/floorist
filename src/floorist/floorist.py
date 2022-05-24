from datetime import date
from floorist.config import get_config
from os import environ
from sqlalchemy import create_engine
from uuid import UUID

import awswrangler as wr
import boto3
import logging
import pandas as pd
import yaml

from floorist.helpers import generate_name, validate_floorplan_entry


def _configure_loglevel():

    LOGLEVEL = environ.get('LOGLEVEL', 'INFO').upper()
    logging.basicConfig(level=LOGLEVEL)

def main():

    _configure_loglevel()
    config = get_config()

    # Compatibility with minio, setting the endpoint URL explicitly if available
    if config.bucket_url:
      wr.config.s3_endpoint_url = config.bucket_url

    boto3.setup_default_session(aws_access_key_id=config.bucket_access_key, aws_secret_access_key=config.bucket_secret_key, region_name=config.bucket_region)

    # Fails if can't connect to S3 or the bucket does not exist
    wr.s3.list_directories(f"s3://{config.bucket_name}")
    logging.debug('Successfully connected to the S3 bucket')

    engine = create_engine(f"postgresql://{config.database_username}:{config.database_password}@{config.database_hostname}/{config.database_name}")
    conn = engine.connect().execution_options(stream_results=True)
    logging.debug('Successfully connected to the database')

    dump_count = 0
    dumped_count = 0

    with open(config.floorplan_filename, 'r') as stream:
        # This try block allows us to proceed if a single SQL query fails
        for row in yaml.safe_load(stream):
            dump_count += 1

            try:

                query = row['query']
                prefix = row['prefix']
                chunksize = row.get('chunksize', 1000)

                logging.debug(f"Dumping #{dump_count}: {query} to {prefix}")

                validate_floorplan_entry(query, prefix)

                cursor = pd.read_sql(query, conn, chunksize=chunksize)
                target = generate_name(config.bucket_name, prefix)

                uuids = {}

                for data in cursor:
                    if len(uuids) == 0 and len(data) > 0:
                        # Detect any columns with UUID
                        for column in data:
                            if isinstance(data[column][0], UUID):
                                uuids[column] = "string"

                    # Convert any columns with UUID type to string
                    data = data.astype(uuids)

                    wr.s3.to_parquet(data, target,
                       index=False,
                       compression='gzip',
                       dataset=True,
                       mode='append'
                    )

                logging.debug(f"Dumped #{dumped_count}: {query} to {prefix}")

                dumped_count += 1
            except Exception as ex:
                logging.exception(ex)

    logging.info(f'Dumped {dumped_count} from total of {dump_count}')

    conn.close()

    if dumped_count != dump_count:
        exit(1)
