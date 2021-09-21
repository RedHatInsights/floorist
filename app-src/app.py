from datetime import date
from os import environ as env
from uuid import uuid4 as uuid

import pandas.io.sql as sqlio
import psycopg2
import yaml

conn = psycopg2.connect(
  host=env['POSTGRES_SERVICE_HOST'],
  user=env['POSTGRESQL_USER'],
  password=env['POSTGRESQL_PASSWORD'],
  database=env['POSTGRESQL_DATABASE']
)

with open(env['FLOORPLAN_FILE'], 'r') as stream:
  for row in yaml.safe_load(stream):
    data = sqlio.read_sql_query(row['query'], conn)
    target = "/".join([
      f's3://{env["AWS_BUCKET"]}',
      row['prefix'],
      date.today().strftime("year_created=%Y/month_created=%-m/day_created=%-d"),
      f'{uuid()}.parquet'
    ])

    data.to_parquet(
      path=target,
      compression='gzip',
      index=False,
      storage_options={'client_kwargs':{'endpoint_url': env.get('AWS_ENDPOINT') }}
    )
