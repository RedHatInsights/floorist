#!/usr/bin/env bash

# Install bonfire repo/initialize - there are patches applied on bootstrap, required even if not using bonfire
CICD_URL=https://raw.githubusercontent.com/RedHatInsights/bonfire/master/cicd
curl -s $CICD_URL/bootstrap.sh > .cicd_bootstrap.sh && source .cicd_bootstrap.sh
APP_ROOT=${APP_ROOT:-`pwd`}
# --------------------------------------------
# Options that must be configured by app owner
# --------------------------------------------

APP_NAME="floorist"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="floorist"  # name of app-sre "resourceTemplate" in deploy.yaml for this component

cat /etc/redhat-release

BUILD_DEPLOY_BUILD_TARGET="test"
BUILD_DEPLOY_TEMP_IMAGE=true

source "$APP_ROOT/build_deploy.sh" || exit 1

mkdir -p artifacts

source "$APP_ROOT/run-tests.sh"
