from datetime import date
from os import environ as env
from uuid import uuid4 as uuid
from s3fs import S3FileSystem as s3

import logging
import pandas.io.sql as sqlio
import psycopg2
import yaml

def main():

  LOGLEVEL = env.get('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(level=LOGLEVEL)

  # Fails if can't connect to S3 or the bucket does not exist
  s3(client_kwargs={'endpoint_url': env.get('AWS_ENDPOINT') }).ls(env['AWS_BUCKET'])
  logging.debug('Successfully connected to the S3 bucket')

  conn = psycopg2.connect(
    host=env['POSTGRES_SERVICE_HOST'],
    user=env['POSTGRESQL_USER'],
    password=env['POSTGRESQL_PASSWORD'],
    database=env['POSTGRESQL_DATABASE']
  )
  logging.debug('Successfully connected to the database')

  dump_count = 0
  dumped_count = 0

  with open(env['FLOORPLAN_FILE'], 'r') as stream:
    # This try block allows us to proceed if a single SQL query fails
    for row in yaml.safe_load(stream):
      dump_count += 1

      try:
        logging.debug(f"Dumping #{dump_count}: {row['query']} to {row['prefix']}")

        data = sqlio.read_sql_query(row['query'], conn)
        target = '/'.join([
          f"s3://{env['AWS_BUCKET']}",
          row['prefix'],
          date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d'),
          f"{uuid()}.parquet"
        ])

        data.to_parquet(
          path=target,
          compression='gzip',
          index=False,
          storage_options={'client_kwargs':{'endpoint_url': env.get('AWS_ENDPOINT') }}
        )

        logging.debug(f"Dumped #{dumped_count}: {row['query']} to {row['prefix']}")

        dumped_count += 1
      except Exception as ex:
        logging.exception(ex)

  logging.info(f'Dumped {dumped_count} from total of {dump_count}')

  conn.close()

  if dumped_count != dump_count:
    exit(1)
