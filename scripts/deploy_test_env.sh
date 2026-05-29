#!/usr/bin/env bash
set -euo pipefail

echo "Started deploy_test_env.sh script"

oc apply -f ./tests/templates/openshift-template.yaml
oc new-app --template=floorist-stack

oc get all

sleep 300

# echo "Image URL: ${IMAGE_URL}"
# echo "Image Digest: ${IMAGE_DIGEST}"
#
# export POD_NAME="TEST_POD"
# export JOB_NAME="TEST_JOB"
# export CONFIGMAP_NAME="postgresql_cm"
# export TMPDIR="/tmp"
#
#
#
# echo "Creating pod YAML..."
# oc create configmap ${CONFIGMAP_NAME} --from-file=enable-extensions.sh
#
#
# echo "Creating pod YAML..."
# oc process -f templates/test-pod-template.yaml -o yaml \
#     -p IMAGE_URL="${IMAGE_URL}" \
#     -p POD_NAME="${POD_NAME}" \
#     > "${TMPDIR}/${POD_NAME}.yaml"
#
# echo ""
# echo "=== Applying Pod YAML ==="
# cat "${TMPDIR}/${POD_NAME}.yaml"
# oc apply -f "${TMPDIR}/${POD_NAME}"
# sleep 10
# oc get pods
#
#
# oc apply -f tests/templates/service.yaml
# oc get services
#
#
# echo ""
# echo "=== Creating job YAML... ==="
# oc process -f templates/test-job-template.yaml -o yaml \
#     -p IMAGE_URL="${IMAGE_URL}" \
#     -p JOB_NAME="${JOB_NAME}" \
#     > "${TMPDIR}/${JOB_NAME}.yaml"
#
# echo ""
# echo "=== Applying Job YAML ==="
# cat "${TMPDIR}/${JOB_NAME}.yaml"
#
# oc apply -f "${TMPDIR}/${JOB_NAME}.yaml"
#
# echo ""
# echo "Waiting for job to complete..."
# oc wait --for=condition=complete --timeout=60s "job/${JOB_NAME}"
#
# echo ""
# echo "=== Job Status ==="
# oc get jobs
#
# echo ""
# echo "=== Job Logs ==="
# oc logs "job/${JOB_NAME}"
#
# oc delete "pod/${POD_NAME}"
# oc delete "job/${JOB_NAME}"
