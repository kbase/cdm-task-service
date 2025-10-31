#!/usr/bin/env bash
set -x

###
# list environment variables supplied from the client
###
echo "JOB_ID=$JOB_ID"
echo "CONTAINER_NUMBER=$CONTAINER_NUMBER"
echo "SERVICE_ROOT_URL=$SERVICE_ROOT_URL"
echo "TOKEN_PATH=$TOKEN_PATH"
echo "S3_URL=$S3_URL"
echo "S3_ACCESS_KEY=$S3_ACCESS_KEY"
echo "S3_SECRET_PATH=$S3_SECRET_PATH"
echo "S3_INSECURE=$S3_INSECURE"
echo "S3_ERROR_LOG_PATH=$S3_ERROR_LOG_PATH"
echo "JOB_UPDATE_TIMEOUT_MIN=$JOB_UPDATE_TIMEOUT_MIN"
echo "MOUNT_PREFIX_OVERRIDE=$MOUNT_PREFIX_OVERRIDE"
echo "CODE_ARCHIVE=$CODE_ARCHIVE"

###
# list other env vars
###
echo "PATH=$PATH"
echo "PWD=$PWD"

###
# Put pip and uv on the path, since they're installed for the user only
###
export PATH=$HOME/.local/bin:$PATH

sleep 3600

###
# Extract the archive, install deps, and run the executor
###
tar -xf $CODE_ARCHIVE

echo "Start uv / deps install: $(date)"
pip install --upgrade pip && pip install uv

# TODO CONDOR separate uv deps into external_exec & service deps
uv sync
echo "Complete uv / deps install: $(date)"

echo "Python version: $(uv run python --version)"

export PYTHONPATH=.

uv run python cdmtaskservice/externalexecution/main.py
