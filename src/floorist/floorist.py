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
    logging.info('Successfully connected to the S3 bucket')

    engine = create_engine(f"postgresql://{config.database_username}:{config.database_password}@{config.database_hostname}/{config.database_name}")
    conn = engine.connect().execution_options(stream_results=True)
    logging.info('Successfully connected to the database')

    dump_count = 0
    dumped_count = 0

    with open(config.floorplan_filename, 'r') as stream:
        # This try block allows us to proceed if a single SQL query fails
        for row in yaml.safe_load(stream):
            dump_count += 1
            chunksize = row.get('chunksize', 1000)

            if chunksize == 0:
                chunksize = None

            try:
                logging.info('[Dump #%d] Dumping to %s with chunksize %s', dump_count, row['prefix'], chunksize)
                logging.debug('[Dump #%d] Query: %s', dump_count, row['query'])

                cursor = pd.read_sql(row['query'], conn, chunksize=chunksize)

                # This should allow the parsing of non-streamed results with the same iterative approach below
                if isinstance(cursor, pd.DataFrame):
                    cursor = [cursor]

                path = f"{row['prefix']}/{date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')}"
                target = f"s3://{config.bucket_name}/{path}"

                uuids = {}

                chunk = 1
                for data in cursor:
                    if len(uuids) == 0 and len(data) > 0:
                        # Detect any columns with UUID
                        for column in data:
                            if isinstance(data[column][0], UUID):
                                logging.debug('[Dump #%d] UUID column detected: %s', dump_count, column)
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
                        logging.info('[Dump #%d] Written parquet chunk #%d', dump_count, chunk)
                        chunk += 1
                    else:
                        # Create an empty folder if the returned dataset is empty
                        wr._utils.client('s3').put_object(Bucket=config.bucket_name, Body='', Key=path+'/')
                        logging.info('[Dump #%d] Empty folder created for empty result', dump_count)


                logging.debug('[Dump #%d] Dumped %s to %s', dump_count, row['query'], row['prefix'])

                dumped_count += 1
            except Exception as ex:
                logging.exception('[Dump #%d] %s', dump_count, ex)

    logging.info('Dumped %d from total of %d', dumped_count, dump_count)

    conn.close()

    if dumped_count != dump_count:
        exit(1)
