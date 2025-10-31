#!/usr/bin/env bash
set -x

ENV_SOURCE_FILE="job_envs.source"

EXPECTED_VARS=(
    JOB_ID
    CONTAINER_NUMBER
    SERVICE_ROOT_URL
    TOKEN_PATH
    S3_URL
    S3_ACCESS_KEY
    S3_SECRET_PATH
    S3_INSECURE
    S3_ERROR_LOG_PATH
    JOB_UPDATE_TIMEOUT_MIN
    MOUNT_PREFIX_OVERRIDE
    CODE_ARCHIVE
)

###
# Make a source-able environment file for debugging purposes and list env vars in the logs
###

{
  echo "# Environment snapshot from Condor job at $(date)"
  echo "# To reproduce: source this file"
  echo
} > "$ENV_SOURCE_FILE"

for var in "${EXPECTED_VARS[@]}"; do
  # only dump if defined
  if [[ -v "$var" ]]; then
    value="${!var}"
    echo "$var=$value"
    safe_value=${value//\'/\'\\\'\'}
    echo "export $var='$safe_value'" >> "$ENV_SOURCE_FILE"
  fi
done

echo "export PYTHONPATH=." >> "$ENV_SOURCE_FILE"
echo "Environment written to $ENV_SOURCE_FILE"

###
# list other env vars
###
echo
echo "PATH=$PATH"
echo "PWD=$PWD"

###
# Put pip and uv on the path, since they're installed for the user only
###
export PATH=$HOME/.local/bin:$PATH

#sleep 3600

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
