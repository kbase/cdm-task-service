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
echo "CODE_ARCHIVE=$CODE_ARCHIVE"

###
# list other env vars
###
echo "PATH=$PATH"
echo "PWD=$PWD"

###
# extract secrets into env vars
###

# Function to trim whitespace
trim() {
  echo "$1" | awk '{$1=$1;print}'
}

# could make the following a function...
# Check if token file path is set
if [ -z "$TOKEN_PATH" ]; then
  echo "Error: TOKEN_PATH is not set." >&2
  exit 1
fi

# Check for file existence and read token
if [ ! -f "$TOKEN_PATH" ]; then
  echo "Error: Token file '$TOKEN_PATH' does not exist." >&2
  exit 1
fi

export TOKEN="$(trim "$(cat "$TOKEN_PATH")")"

if [ -z "$TOKEN" ]; then
  echo "Error: Token file '$TOKEN_PATH' is empty or only whitespace." >&2
  exit 1
fi

# Do the same for the s3 secret
if [ -z "$S3_SECRET_PATH" ]; then
  echo "Error: S3_SECRET_PATH is not set." >&2
  exit 1
fi

if [ ! -f "$S3_SECRET_PATH" ]; then
  echo "Error: S3 secret file '$S3_SECRET_PATH' does not exist." >&2
  exit 1
fi

export S3_SECRET="$(trim "$(cat "$S3_SECRET_PATH")")"

if [ -z "$S3_SECRET" ]; then
  echo "Error: S3 secret file '$S3_SECRET_PATH' is empty or only whitespace." >&2
  exit 1
fi

###
# For some reason setting transfer_output_files to the empty string isn't working. We touch
# an empty file so that job output doesn't get transferred.
###
touch __DUMMY_OUTPUT__

###
# Put pip and uv on the path, since they're installed for the user only
###
export PATH=$HOME/.local/bin:$PATH

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
