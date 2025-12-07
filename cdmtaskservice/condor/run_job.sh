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
    ADDITIONAL_PATH
    GLOBAL_CACHE_DIR
    CODE_ARCHIVE
    REFDATA_HOST_PATH
)

###
# list initial env vars
###
echo "Initial \$PATH=$PATH"
echo "\$PWD=$PWD"
echo "whoami=$(whoami)"

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
    # escape single quotes safely for inclusion in shell file
    safe_value=${value//\'/\'\\\'\'}
    echo "export $var='$safe_value'" >> "$ENV_SOURCE_FILE"
  fi
done

if [[ -n "${ADDITIONAL_PATH:-}" ]]; then
  safe_addl_path=${ADDITIONAL_PATH//\'/\'\\\'\'}
  echo "export PATH='$safe_addl_path':\$PATH" >> "$ENV_SOURCE_FILE"
  export PATH="$safe_addl_path:$PATH"
fi

echo "export PYTHONPATH=." >> "$ENV_SOURCE_FILE"
echo "Environment written to $ENV_SOURCE_FILE"

echo "updated \$PATH=$PATH"

###
# Extract the archive, install deps, and run the executor
###
tar -xf $CODE_ARCHIVE

echo "Start uv / deps install: $(date)"
# Don't use a cache dir for pip - pip caches don't appear to be completely concurrency safe
pip install --no-cache-dir uv

# TODO CONDOR separate uv deps into external_exec & service deps
# UV caches are concurrency safe: https://docs.astral.sh/uv/concepts/cache/#cache-safety
mkdir -p $GLOBAL_CACHE_DIR/uv_cache
export UV_CACHE_DIR=$GLOBAL_CACHE_DIR/uv_cache
export XDG_DATA_HOME=$GLOBAL_CACHE_DIR/uv_data
uv sync
echo "Complete uv / deps install: $(date)"

echo "Python version: $(uv run python --version)"

export PYTHONPATH=.

uv run python cdmtaskservice/externalexecution/main.py
