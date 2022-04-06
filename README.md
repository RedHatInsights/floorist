# Floorist
[![Build Status](https://ci.ext.devshift.net/buildStatus/icon?job=RedHatInsights-floorist-pr-check "Build status")](https://ci.ext.devshift.net/job/RedHatInsights-floorist-pr-check/)

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

The floorplan file simply defines a list of a prefix-query pair. The prefix should be a valid folder path that will be created under the bucket if it does not exist. For the queries it is recommended to assign simpler aliases for dynamically created (joins or aggregates) columns using `AS`. Optionally you can set a custom `chunksize` for the [query](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html) (default is 1000) that will serve as the maximum number of records in a single parquet file.

```yaml
- prefix: dumps/people
  query: >-
    SELECT name, email, birthyear FROM people;
- prefix: dumps/cities
  query: >-
    SELECT name AS city_name, zip, country FROM cities;
  chunksize: 100
```

The example above will create two dumps under the S3 bucket specified in the `AWS_BUCKET` environment variable into the `<prefix>/year_created=<Y>/month_created=<M>/day_created=<D>/<UUID>.parquet` files.

### Clowder - How to add Floorist to your Clowder template

You only need to add a new job definition on your ClowdApp, and a ConfigMap with the Floorplan definition your app needs.

An example configuring the Floorist job follows, using a secret to host the S3 bucket config, the config map definition, and a floorplan Configmap example for reference.

```yaml
#Clowdapp.yaml

- apiVersion: cloud.redhat.com/v1alpha1
  kind: ClowdApp
  metadata:
    name: "${APP_NAME}"
  spec:
    jobs:
    - name: floorist
      schedule: ${FLOORIST_SCHEDULE}
      suspend: ${FLOORIST_SUSPEND}
      concurrencyPolicy: Forbid
      podSpec:
        image: ${FLOORIST_IMAGE}:${FLOORIST_IMAGE_TAG}
        env:
        - name: AWS_BUCKET
          valueFrom:
            secretKeyRef:
              name: ${FLOORIST_BUCKET_SECRET_NAME}
              key: bucket
        - name: AWS_REGION
          valueFrom:
            secretKeyRef:
              name: ${FLOORIST_BUCKET_SECRET_NAME}
              key: aws_region
        - name: AWS_ENDPOINT
          valueFrom:
            secretKeyRef:
              name: ${FLOORIST_BUCKET_SECRET_NAME}
              key: endpoint
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: ${FLOORIST_BUCKET_SECRET_NAME}
              key: aws_access_key_id
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: ${FLOORIST_BUCKET_SECRET_NAME}
              key: aws_secret_access_key
        - name: FLOORPLAN_FILE
          value: "/tmp/floorplan/floorplan.yaml"
        - name: LOGLEVEL
          value: ${FLOORIST_LOGLEVEL}
        volumeMounts:
        - name: floorplan-volume
          mountPath: "/tmp/floorplan"
        volumes:
          - name: floorplan-volume
            configMap:
              name: floorplan
      resources:
          limits:
            cpu: "${CPU_LIMIT_FLOORIST}"
            memory: "${MEMORY_LIMIT_FLOORIST}"
          requests:
            cpu: "${CPU_REQUEST_FLOORIST}"
            memory: "${MEMORY_REQUEST_FLOORIST}"
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: floorplan
  data:
    floorplan.yaml: |
      - prefix: insights/yout-service-name/hosts-query
        query: >-
          SELECT
            "inventory"."hosts"."id",
              OR "inventory"."hosts"."id" IN (SELECT "test_results"."host_id" FROM "test_results"));
      - prefix: insights/your-service-name/policies-query
        query: >-
          SELECT DISTINCT
            "policies"."id",
            "profiles"."ref_id",
      - prefix: insights/your-service-name/policy_hosts-query
        query: >-
          SELECT "policy_hosts"."host_id", "policy_hosts"."policy_id" FROM "policy_hosts";

parameters:
- name: MEMORY_LIMIT_FLOORIST
  value: 200Mi
- name: MEMORY_REQUEST_FLOORIST
  value: 100Mi
- name: CPU_LIMIT_FLOORIST
  value: 100m
- name: CPU_REQUEST_FLOORIST
  value: 50m
- name: FLOORIST_SCHEDULE
  description: Cronjob schedule definition
  required: true
  value: "0 2 * * *"
- name: FLOORIST_SUSPEND
  description: Disable Floorist cronjob execution
  required: true
  value: "true"
- description: Floorist image name
  name: FLOORIST_IMAGE
  value: quay.io/cloudservices/floorist
- description: Floorist Image tag
  name: FLOORIST_IMAGE_TAG
  required: true
  value: latest
- description: Shared bucket name
  name: FLOORIST_BUCKET_NAME
  required: true
  value: floorist-bucket
```


## Testing

For testing the tool, you will need PostgreSQL and minio, there's a Docker Compose file provided in the `test` folder with everything prepared and configured. The configuration for these two services has to be stored in the `test/env.yaml` file, for the Docker Compose setup it's enough to copy the the `test/env.yaml.example` to make it work. However, if you would like to bring your own PostgreSQL server or maybe use a real S3 bucket, you have to edit these values accordingly. The tests can be started via `pytest`.

There's two ways of running the tests, you can run them locally using `pytest` from your localhost or you can run everything from containers like we do on our CI process.

**Required Python version <= 3.9** (see #18)

### Running tests locally

```bash

# Install with Test dependencies
pip install -e .[test] -r requirements.txt

# Set the environment config file
cp test/env.yaml.example test/env.yaml

# Bring up all the required containers
docker-compose -f test/docker-compose.yml up -d

# Run the tests locally
pytest

# Tear-down
docker-compose -f test/docker-compose.yml down
````

### Running tests from containers

Alternatively, you can also run the same process the CI system runs, locally, by running the `pr_check.sh` script with the `LOCAL_BUILD=true` environment variable set:

```
LOCAL_BUILD=true ./pr_check.sh
```

the **pr_check.sh** script will:

- Build a new image for Floorist using the test dependencies (see [build_deploy.sh](build_deploy.sh) for details)
- Run the tests in a container using the aforementioned image (see [run-tests.sh](run-tests.sh) for details)

please **NOTE** - since the *pr_check.sh* script creates and deletes the containers it uses each time, it has to create a custom *env.yaml* file with the correct container names (i.e., to connect to the right database and the right container with the MinIO bucket), overriding the existing env file in the process (the local tests/env.yaml.example file has the default `localhost` value for when running `pytest`locally, so make sure this file has the correct values between each run if you run it both from the pr_check.sh script and the local `pytest` command)

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
