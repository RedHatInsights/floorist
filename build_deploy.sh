#!/usr/bin/env bash

set -exv

source 'deployment/build-deploy-common.sh'

IMAGE_NAME="${IMAGE_NAME:-quay.io/cloudservices/floorist}"
BUILD_DEPLOY_BUILD_TARGET="${BUILD_DEPLOY_BUILD_TARGET:-base}"
BACKWARDS_COMPATIBILITY=false
BUILD_PARAMS="--no-cache"

build_deploy_main || exit 1
