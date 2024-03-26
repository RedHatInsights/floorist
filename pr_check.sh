#!/usr/bin/env bash

APP_ROOT=${APP_ROOT:-$(pwd)}
# --------------------------------------------
# Options that must be configured by app owner
# --------------------------------------------

APP_NAME="floorist"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="floorist"  # name of app-sre "resourceTemplate" in deploy.yaml for this component

cat /etc/redhat-release

export BUILD_TARGET="test"

source "$APP_ROOT/build_deploy.sh" || exit 1

mkdir -p artifacts

source "$APP_ROOT/run-tests.sh"
