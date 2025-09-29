#!/usr/bin/env bash
set -x

echo "JOBID=$JOB_ID"
echo "CONTAINER_NUMBER=$CONTAINER_NUMBER"
echo "CODE_ARCHIVE=$CODE_ARCHIVE"
echo "PATH=$PATH"

# For some reason setting transfer_output_files to the empty string isn't working. We touch
# an empty file so that job output doesn't get transferred.
touch __DUMMY_OUTPUT__

# Put pip and uv on the path, since they're installed for the user only
export PATH=$HOME/.local/bin:$PATH

tar -xf $CODE_ARCHIVE

pip install --upgrade pip && pip install uv

# TODO CONDOR separate uv deps into external_exec & service deps
uv sync

export PYTHONPATH=.

# TODO CONDOR actually run the job
uv run python -c "from cdmtaskservice import version; print(version.VERSION)"
