#!/bin/bash

cd "$APP_ROOT"

DB_CONTAINER_NAME="floorist-db-${IMAGE_TAG}"
MINIO_CONTAINER_NAME="floorist-minio-${IMAGE_TAG}"
MINIO_CLIENT_CONTAINER_NAME="floorist-minio-client-${IMAGE_TAG}"
TEST_CONTAINER_NAME="floorist-test-${IMAGE_TAG}"
NETWORK="floorist-test-${IMAGE_TAG}"

POSTGRES_IMAGE="quay.io/cloudservices/centos-postgresql-12:20210722-70dc4d3"
MINIO_IMAGE="quay.io/minio/minio:RELEASE.2025-06-13T11-33-47Z"
MINIO_CLIENT_IMAGE="quay.io/minio/mc:RELEASE.2025-05-21T01-59-54Z"
MINIO_BUCKET_NAME="floorist"
MINIO_REGION="us-east-1"

DATABASE_USER="floorist"
DATABASE_PASSWORD="floorist"
DATABASE_NAME="floorist"

MINIO_ACCESS_KEY="floorist"
MINIO_SECRET_KEY="floorist"

TESTS_ENV_FILE="$APP_ROOT/tests/env.yaml"

FLOORPLAN_FILE="tests/floorplan_valid.yaml"

create_env_file() {

cat << EOF > "$TESTS_ENV_FILE"
AWS_ENDPOINT: 'http://$MINIO_CONTAINER_NAME:9000'
AWS_ACCESS_KEY_ID: '$MINIO_ACCESS_KEY'
AWS_SECRET_ACCESS_KEY: '$MINIO_SECRET_KEY'
AWS_BUCKET: '$MINIO_BUCKET_NAME'
AWS_REGION: '$MINIO_REGION'
POSTGRES_SERVICE_HOST: '$DB_CONTAINER_NAME'
POSTGRESQL_USER: '$DATABASE_USER'
POSTGRESQL_PASSWORD: '$DATABASE_PASSWORD'
POSTGRESQL_DATABASE: '$DATABASE_NAME'
FLOORPLAN_FILE: '$FLOORPLAN_FILE'
EOF

}

function try_to_use_podman {
  if command -v podman &> /dev/null; then
    declare -g DOCKER="podman"
  else
    declare -g DOCKER="docker"
  fi
}

try_to_use_podman

function teardown_docker {
  ${DOCKER} rm -f "$DB_CONTAINER_NAME" || true
  ${DOCKER} rm -f "$MINIO_CONTAINER_NAME" || true
  ${DOCKER} rm -f "$MINIO_CLIENT_CONTAINER_NAME" || true
  ${DOCKER} rm -f "$TEST_CONTAINER_NAME" || true
  try_to_delete_network || true
}

try_to_delete_network() {

  if ! ${DOCKER} network rm "$NETWORK"; then

    for CONTAINER_ID in "$DB_CONTAINER_NAME" "$MINIO_CONTAINER_NAME" "$MINIO_CONTAINER_NAME" "$TEST_CONTAINER_NAME"; do
      ${DOCKER} rm -f "$CONTAINER_ID"
      ${DOCKER} network disconnect -f "$NETWORK" "$CONTAINER_ID"
    done

    if ! ${DOCKER} network rm "$NETWORK"; then
      echo "failed deleting network '$NETWORK'";
      return 1
    fi
  fi
}

try_to_create_container_network() {

  if ${DOCKER} network inspect "$NETWORK" >/dev/null; then

    if ! try_to_delete_network "$NETWORK"; then
        return 1
    fi
  fi

  if ! ${DOCKER} network create --driver bridge "$NETWORK"; then
    echo "failed to create network $NETWORK"
    return 1
  fi
}

trap "teardown_docker" EXIT SIGINT SIGTERM

try_to_create_container_network || exit 1

DB_CONTAINER_ID=$(${DOCKER} run -d \
  --name "${DB_CONTAINER_NAME}" \
  --network "$NETWORK" \
  --rm \
  -e POSTGRESQL_USER="$DATABASE_USER" \
  -e POSTGRESQL_PASSWORD="$DATABASE_PASSWORD" \
  -e POSTGRESQL_DATABASE="$DATABASE_NAME" \
  -v "${PWD}/enable-extensions.sh:/opt/app-root/src/postgresql-start/enable-extensions.sh:z" \
  "$POSTGRES_IMAGE" || echo "0")

if [[ "$DB_CONTAINER_ID" == "0" ]]; then
  echo "Failed to start DB container"
  exit 1
fi

MINIO_CONTAINER_ID=$(${DOCKER} run -d \
  --name "${MINIO_CONTAINER_NAME}" \
  --network "$NETWORK" \
  --rm \
  -e MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  -e MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  "$MINIO_IMAGE" server /data || echo "0")

if [[ "$MINIO_CONTAINER_ID" == "0" ]]; then
  echo "Failed to start Minio container"
  exit 1
fi

MINIO_CLIENT_COMMAND="""
      until /usr/bin/mc alias set myminio http://$MINIO_CONTAINER_NAME:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY >/dev/null; do sleep 1; done ;
      /usr/bin/mc mb myminio/$MINIO_BUCKET_NAME;
      /usr/bin/mc anonymous set download myminio/$MINIO_BUCKET_NAME;
      exit 0;
"""

MINIO_CLIENT_CONTAINER_ID=$(${DOCKER} run -d \
  --name "${MINIO_CLIENT_CONTAINER_NAME}" \
  --network "$NETWORK" \
  --rm \
  --entrypoint '/bin/sh' \
  "$MINIO_CLIENT_IMAGE" -c "$MINIO_CLIENT_COMMAND" || echo "0")

if [[ "$MINIO_CLIENT_CONTAINER_ID" == "0" ]]; then
  echo "Failed to start Minio client container"
  exit 1
fi

# Do tests
TEST_CONTAINER_ID=$(${DOCKER} run -d \
  --name "${TEST_CONTAINER_NAME}" \
  --network "$NETWORK" \
  --rm \
  -e AWS_ENDPOINT="http://$MINIO_CONTAINER_NAME:9000" \
  -e AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  -e AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  -e AWS_BUCKET="$MINIO_BUCKET_NAME" \
  -e POSTGRES_SERVICE_HOST="$DB_CONTAINER_NAME" \
  -e POSTGRESQL_USER="$DATABASE_USER" \
  -e POSTGRESQL_PASSWORD="$DATABASE_PASSWORD" \
  -e POSTGRESQL_DATABASE="$DATABASE_NAME" \
  -e FLOORPLAN_FILE="$FLOORPLAN_FILE" \
  "$IMAGE_NAME:$IMAGE_TAG" \
  /bin/bash -c 'sleep infinity' || echo "0")

if [[ "$TEST_CONTAINER_ID" == "0" ]]; then
  echo "Failed to start test container"
  exit 1
fi

WORKSPACE=${WORKSPACE:-'.'}
ARTIFACTS_DIR="$WORKSPACE/artifacts"
mkdir -p "$ARTIFACTS_DIR"

create_env_file || exit 1
${DOCKER} cp "$TESTS_ENV_FILE" "$TEST_CONTAINER_ID:/opt/app-root/tests/env.yaml"

# tests
echo '===================================='
echo '===     Running Tests           ===='
echo '===================================='
set +e
${DOCKER} exec "$TEST_CONTAINER_ID" /bin/bash -c "pytest -vvv -s --junitxml=test-report.xml tests"
TEST_RESULT=$?
set -e
# Copy test reports
${DOCKER} cp "$TEST_CONTAINER_ID:/opt/app-root/test-report.xml" "$WORKSPACE/artifacts/junit-test-report.xml"

if [[ $TEST_RESULT -ne 0 ]]; then
  echo '====================================='
  echo '====  ✖ ERROR: TESTS     FAILED  ===='
  echo '====================================='
  exit 1
fi

echo '====================================='
echo '====   ✔ SUCCESS: PASSED TESTS   ===='
echo '====================================='
