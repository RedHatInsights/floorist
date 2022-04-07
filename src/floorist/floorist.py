from datetime import date
from uuid import uuid4 as uuid
from s3fs import S3FileSystem as s3
from floorist.config import get_config
from os import environ

import logging
import pandas.io.sql as sqlio
import psycopg2
import yaml


def _configure_loglevel():

  LOGLEVEL = environ.get('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(level=LOGLEVEL)


def main():

  _configure_loglevel()
  config = get_config()

  # Fails if can't connect to S3 or the bucket does not exist
  s3(secret=config.bucket_secret_key, key=config.bucket_access_key,
     client_kwargs={'endpoint_url': config.bucket_url }).ls(config.bucket_name)
  logging.debug('Successfully connected to the S3 bucket')

  conn = psycopg2.connect(
    host=config.database_hostname,
    user=config.database_username,
    password=config.database_password,
    database=config.database_name
  )
  logging.debug('Successfully connected to the database')

  dump_count = 0
  dumped_count = 0

  with open(config.floorplan_filename, 'r') as stream:
    # This try block allows us to proceed if a single SQL query fails
    for row in yaml.safe_load(stream):
      dump_count += 1

      try:
        logging.debug(f"Dumping #{dump_count}: {row['query']} to {row['prefix']}")

        for data in sqlio.read_sql_query(row['query'], conn, chunksize=row.get('chunksize', 1000)):
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
               'key' : config.bucket_access_key,
              'client_kwargs':{'endpoint_url': config.bucket_url }
            }
          )

        logging.debug(f"Dumped #{dumped_count}: {row['query']} to {row['prefix']}")

        dumped_count += 1
      except Exception as ex:
        logging.exception(ex)

  logging.info(f'Dumped {dumped_count} from total of {dump_count}')

  conn.close()

  if dumped_count != dump_count:
    exit(1)
