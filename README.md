# Floorist
[![Build Status](https://app.travis-ci.com/skateman/floorist.svg?branch=master)](https://app.travis-ci.com/skateman/floorist)

Floorist is a simple script to dump SQL queries from a PostgreSQL database into S3 buckets in parquet format.

## Usage

The tool has been designed for use in Kubernetes or OpenShift, but you can also run it locally by using `docker` or `podman`. It can be configured through environment variables and a single floorplan (YAML) file.

```bash
docker build -t floorist .

docker run --rm floorist \
  -e POSTGRES_SERVICE_HOST=localhost \
  -e POSTGRESQL_USER=root \
  -e POSTGRESQL_PASSWORD=123456 \
  -e AWS_ACCESS_KEY_ID=your_key_id \
  -e AWS_SECRET_ACCESS_KEY=your_secret_access_key \
  -e AWS_REGION=us-east-1 \
  -e AWS_BUCKET=floorist-dumps \
  -e FLOORPLAN_FILE=floorplan.yml \
  -v floorplan.yml:floorplan.yml
```

### Environment variables
* `POSTGRES_SERVICE_HOST`
* `POSTGRESQL_USER`
* `POSTGRESQL_PASSWORD`
* `POSTGRESQL_DATABASE`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_REGION`
* `AWS_BUCKET`
* `AWS_ENDPOINT` - not mandatory, for using with minio
* `FLOORPLAN_FILE` - should point to the floorplan (YAML) file

### Floorplan file

The floorplan file simply defines a list of a prefix-query pair. The prefix should be a valid folder path that will be created under the bucket if it does not exist. For the queries it is recommended to assign simpler aliases for dynamically created (joins or aggregates) columns using `AS`.

```yaml
- prefix: dumps/people
  query: >-
    SELECT name, email, birthyear FROM people;
- prefix: dumps/cities
  query: >-
    SELECT name AS city_name, zip, country FROM cities;
```

The example above will create two dumps under the S3 bucket specified in the `AWS_BUCKET` environment variable into the `<prefix>/year_created=<Y>/month_created=<M>/day_created=<D>/<UUID>.parquet` file.

## Testing
For testing the tool, you will need PostgreSQL and minio, there's a Docker Compose file provided in the `test` folder with everything prepared and configured. The configuration for these two services has to be stored in the `test/env.yaml` file, for the Docker Compose setup it's enough to copy the the `test/env.yaml.example` to make it work. However, if you would like to bring your own PostgreSQL server or maybe use a real S3 bucket, you have to edit these values accordingly. The tests can be started via `pytest`.

```bash
pip install -r app-src/requirements.txt
pip install pytest
cp test/env.yaml.example test/env.yaml
docker-compose -f test/docker-compose.yml up -d
pytest
docker-compose -f test/docker-compose.yml down
````

## Contributing
Bug reports and pull requests are welcome, here are some ideas for improvement:
* More fine-grained specification of the output path and filename in the floorplan (e.g. custom timestamps)
* Additional parameters for the parquet exporting (column partitioning, compression, indexing, column types)
* Support for other databases
* Support for different object storage services
* Better tests
* Pylint support

## License
The application is available as open source under the terms of the [Apache License, version 2.0](https://opensource.org/licenses/Apache-2.0).
