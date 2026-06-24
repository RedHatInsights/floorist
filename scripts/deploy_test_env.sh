#!/usr/bin/env bash
set -euo pipefail

echo "Started deploy_test_env.sh script"

if ! command -v oc &>/dev/null; then
    echo "ERROR: oc not found in PATH"
    exit 1
fi

# Validate required variables
if [[ -z "${CONTAINER_IMAGE:-}" ]]; then
    echo "ERROR: CONTAINER_IMAGE is not set"
    echo "Usage: CONTAINER_IMAGE=your-image:tag ./scripts/deploy_test_env.sh"
    exit 1
fi

# Optional: load parameters from a param file
# Usage: PARAM_FILE=tests/k8s.env deploy_test_env.sh
PARAM_FILE_ARGS=""
if [[ -n "${PARAM_FILE:-}" ]] && [[ -f "${PARAM_FILE}" ]]; then
    echo "Loading parameters from: ${PARAM_FILE}"
    PARAM_FILE_ARGS="--param-file=${PARAM_FILE}"
fi

# Deploy the infrastructure stack (PostgreSQL, MinIO, secrets, etc.)
echo ""
echo "=== Deploying infrastructure stack ==="
oc process --local -f ./tests/templates/openshift-template.yaml \
    ${PARAM_FILE_ARGS} \
    -o yaml | oc apply -f -

oc get all

echo "Test CONTAINER image: ${CONTAINER_IMAGE}"

sleep 12

export JOB_NAME="${JOB_NAME:-floorist-test-job}"
export TMPDIR="${TMPDIR:-/tmp}"

# Clean up previous test job if it exists (Jobs are immutable)
oc delete job "${JOB_NAME}" --ignore-not-found

# Deploy the test job
echo ""
echo "=== Creating and applying test job ==="
oc process --local -f ./tests/templates/test-job-template.yaml \
    ${PARAM_FILE_ARGS} \
    -p CONTAINER_IMAGE="${CONTAINER_IMAGE}" \
    -p JOB_NAME="${JOB_NAME}" \
    -o yaml | oc apply -f -

sleep 5

echo ""
echo "=== Job Status ==="
oc get jobs

echo ""
echo "=== Job Logs ==="

oc logs -f job/minio-createbucket

echo ""
echo "=== Waiting for test job pod to start ==="
oc wait --for=condition=ready pod -l job-name="${JOB_NAME}" --timeout="${POD_READY_TIMEOUT:-120s}" || true

oc logs -f "job/${JOB_NAME}"

oc delete "job/${JOB_NAME}"
