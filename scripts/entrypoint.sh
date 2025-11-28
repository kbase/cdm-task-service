#!/bin/bash

echo "Service mode: $KBCTS_MODE"

# Determine config file and app factory based on mode
if [ "$KBCTS_MODE" = "refdata" ]; then
    CONFIG_FILE="cdmtaskservice_refdata_config.toml"
    APP_FACTORY="cdmtaskservice.app:create_refdata_app"
else
    CONFIG_FILE="cdmtaskservice_config.toml"
    APP_FACTORY="cdmtaskservice.app:create_app"
fi

export KB_DEPLOYMENT_CONFIG=$CONFIG_FILE

jinja $KB_DEPLOYMENT_CONFIG.jinja -X "^KBCTS_" > $KB_DEPLOYMENT_CONFIG

# Use KBCTS_PORT if set, otherwise default to 5000
PORT=${KBCTS_PORT:-5000}

# FastAPI recommends running a single process service per docker container instance as below,
# and scaling via adding more containers. If we need to run multiple processes, use guvicorn as
# a process manager as described in the FastAPI docs
# https://fastapi.tiangolo.com/deployment/docker/#replication-number-of-processes
uvicorn --host 0.0.0.0 --port "$PORT" --factory $APP_FACTORY
