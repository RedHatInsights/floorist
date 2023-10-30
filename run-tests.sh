#!/bin/bash

export CICD_BOOTSTRAP_REPO_BRANCH='main'
export CICD_BOOTSTRAP_REPO_ORG='RedHatInsights'
APP_ROOT=${APP_ROOT:-$(pwd)}
CICD_TOOLS_URL="https://raw.githubusercontent.com/${CICD_BOOTSTRAP_REPO_ORG}/cicd-tools/${CICD_BOOTSTRAP_REPO_BRANCH}/src/bootstrap.sh"
# shellcheck source=/dev/null
source <(curl -sSL "$CICD_TOOLS_URL") image_builder

export CICD_IMAGE_BUILDER_IMAGE_NAME='quay.io/cloudservices/floorist'
TAG=$(cicd::image_builder::get_image_tag)

cd "$APP_ROOT"

DB_CONTAINER_NAME="floorist-db-${TAG}"
MINIO_CONTAINER_NAME="floorist-minio-${TAG}"
MINIO_CLIENT_CONTAINER_NAME="floorist-minio-client-${TAG}"
TEST_CONTAINER_NAME="floorist-test-${TAG}"
NETWORK="floorist-test-${TAG}"

POSTGRES_IMAGE="quay.io/cloudservices/centos-postgresql-12"
MINIO_IMAGE="quay.io/cloudservices/minio"
MINIO_CLIENT_IMAGE="quay.io/cloudservices/mc"
MINIO_BUCKET_NAME="floorist"
MINIO_REGION="us-east-1"

DATABASE_USER="floorist"
DATABASE_PASSWORD="floorist"
DATABASE_NAME="floorist"

MINIO_ACCESS_KEY="floorist"
MINIO_SECRET_KEY="floorist"

TESTS_ENV_FILE="${APP_ROOT}/tests/env.yaml"

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

function teardown_docker {
  cicd::container::cmd rm -f "$DB_CONTAINER_NAME" || true
  cicd::container::cmd rm -f "$MINIO_CONTAINER_NAME" || true
  cicd::container::cmd rm -f "$MINIO_CLIENT_CONTAINER_NAME" || true
  cicd::container::cmd rm -f "$TEST_CONTAINER_NAME" || true
  try_to_delete_network || true
}

try_to_delete_network() {

  if ! cicd::container::cmd network rm "$NETWORK"; then

    for CONTAINER_ID in "$DB_CONTAINER_NAME" "$MINIO_CONTAINER_NAME" "$MINIO_CONTAINER_NAME" "$TEST_CONTAINER_NAME"; do
      cicd::container::cmd rm -f "$CONTAINER_ID"
      cicd::container::cmd network disconnect -f "$NETWORK" "$CONTAINER_ID"
    done

    if ! cicd::container::cmd network rm "$NETWORK"; then
      echo "failed deleting network '$NETWORK'";
      return 1
    fi
  fi
}

try_to_create_container_network() {

  if cicd::container::cmd network inspect "$NETWORK" >/dev/null; then

    if ! try_to_delete_network "$NETWORK"; then
        return 1
    fi
  fi

  if ! cicd::container::cmd network create --driver bridge "$NETWORK"; then
    echo "failed to create network $NETWORK"
    return 1
  fi
}

trap "teardown_docker" EXIT SIGINT SIGTERM

try_to_create_container_network || exit 1

DB_CONTAINER_ID=$(cicd::container::cmd run -d \
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

MINIO_CONTAINER_ID=$(cicd::container::cmd run -d \
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
      until /usr/bin/mc config host add myminio http://$MINIO_CONTAINER_NAME:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY >/dev/null; do sleep 1; done ;
      /usr/bin/mc mb myminio/$MINIO_BUCKET_NAME;
      /usr/bin/mc policy set download myminio/$MINIO_BUCKET_NAME;
      exit 0;
"""

MINIO_CLIENT_CONTAINER_ID=$(cicd::container::cmd run -d \
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
TEST_CONTAINER_ID=$(cicd::container::cmd run -d \
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
  "$(cicd::image_builder::get_full_image_name)" \
  /bin/bash -c 'sleep infinity' || echo "0")

if [[ "$TEST_CONTAINER_ID" == "0" ]]; then
  echo "Failed to start test container"
  exit 1
fi

WORKSPACE=${WORKSPACE:-'.'}
ARTIFACTS_DIR="$WORKSPACE/artifacts"
mkdir -p "$ARTIFACTS_DIR"

create_env_file || exit 1
cicd::container::cmd cp "$TESTS_ENV_FILE" "$TEST_CONTAINER_ID:/opt/app-root/tests/env.yaml"

# tests
echo '===================================='
echo '===     Running Tests           ===='
echo '===================================='
set +e
cicd::container::cmd exec "$TEST_CONTAINER_ID" /bin/bash -c "pytest --junitxml=test-report.xml tests"
TEST_RESULT=$?
set -e
# Copy test reports
cicd::container::cmd cp "$TEST_CONTAINER_ID:/opt/app-root/test-report.xml" "$WORKSPACE/artifacts/junit-test-report.xml"

if [[ $TEST_RESULT -ne 0 ]]; then
  echo '====================================='
  echo '====  ✖ ERROR: TESTS     FAILED  ===='
  echo '====================================='
  exit 1
fi

echo '====================================='
echo '====   ✔ SUCCESS: PASSED TESTS   ===='
echo '====================================='
