#!/usr/bin/env bash

set -exv

export CICD_BOOTSTRAP_REPO_BRANCH='main'
export CICD_BOOTSTRAP_REPO_ORG='RedHatInsights'
CICD_TOOLS_URL="https://raw.githubusercontent.com/${CICD_BOOTSTRAP_REPO_ORG}/cicd-tools/${CICD_BOOTSTRAP_REPO_BRANCH}/src/bootstrap.sh"
# shellcheck source=/dev/null
source <(curl -sSL "$CICD_TOOLS_URL") image_builder

export CICD_IMAGE_BUILDER_IMAGE_NAME='quay.io/cloudservices/floorist'

cicd::image_builder::build_and_push --no-cache --target base
