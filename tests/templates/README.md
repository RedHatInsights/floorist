# Kubernetes/OpenShift Templates

This directory contains OpenShift Templates for deploying the Floorist test environment.
The templates use `oc process --local` for parameter substitution, which works on both
OpenShift and plain Kubernetes (minikube, kind, etc.) without requiring an OpenShift cluster.

## Files

- **openshift-template.yaml** - Infrastructure stack: PostgreSQL, MinIO, secrets, configmaps, and bucket creation job
- **test-job-template.yaml** - Job to run the Floorist pytest suite against the deployed services

## Deployment

### Quick start (using template defaults)

```bash
export CONTAINER_IMAGE="your-floorist-test-image:tag"
./scripts/deploy_test_env.sh
```

### With custom parameters via env file

```bash
cp tests/k8s.env.example tests/k8s.env
# Edit tests/k8s.env as needed

export CONTAINER_IMAGE="your-floorist-test-image:tag"
PARAM_FILE=tests/k8s.env ./scripts/deploy_test_env.sh
```

### Manual deployment

```bash
# Deploy infrastructure stack with defaults
oc process --local -f tests/templates/openshift-template.yaml -o yaml | kubectl apply -f -

# Or with custom parameters
oc process --local -f tests/templates/openshift-template.yaml \
  -p POSTGRESQL_PASSWORD=mypassword \
  -p MINIO_SECRET_KEY=mysecret \
  -o yaml | kubectl apply -f -

# Or with a param file
oc process --local -f tests/templates/openshift-template.yaml \
  --param-file=tests/k8s.env \
  -o yaml | kubectl apply -f -

# Deploy test job
oc process --local -f tests/templates/test-job-template.yaml \
  -p CONTAINER_IMAGE="your-image:tag" \
  -o yaml | kubectl apply -f -
```

## Requirements

- **oc** CLI (for `oc process --local` template rendering)
- **kubectl** or **oc** (for applying resources to the cluster)

The `oc` binary can be downloaded standalone from
[Red Hat mirror](https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/)
and works without an OpenShift cluster for `--local` processing.

## Services

After deployment, the following services will be available:

- **db** (PostgreSQL) - Port 5432
- **minio** (MinIO) - Port 9000

## Notes

- Default credentials are set to "floorist" for both services - **change these in production environments**
- No persistent volumes are configured - data will be lost if pods are deleted
- The MinIO bucket creation runs as a separate Job that waits for MinIO to be ready
