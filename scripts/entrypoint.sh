#!/bin/bash

export KB_DEPLOYMENT_CONFIG=cdmtaskservice_config.toml

jinja $KB_DEPLOYMENT_CONFIG.jinja -X "^KBCTS_" > $KB_DEPLOYMENT_CONFIG

# FastAPI recommends running a single process service per docker container instance as below,
# and scaling via adding more containers. If we need to run multiple processes, use guvicorn as
# a process manager as described in the FastAPI docs
# https://fastapi.tiangolo.com/deployment/docker/#replication-number-of-processes
exec uvicorn   --proxy-headers --host 0.0.0.0 --port 5000 --factory cdmtaskservice.app:create_app
