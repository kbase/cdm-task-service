'''
API for the CDM task service.
'''

import os
import sys

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.routes import SERVICE_NAME, ROUTER_GENERAL
from cdmtaskservice.version import VERSION


# TODO LOGGING - log all write ops w/ username

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_DESCRIPTION = (
    "A service for running arbitrary binaries on remote compute for the KBase CDM"
)

def create_app():
    """
    Create the CDM task service application
    """

    print(f"Server version {VERSION} {GIT_COMMIT}")
    with open(os.environ[_KB_DEPLOYMENT_CONFIG], "rb") as cfgfile:
        cfg = CDMTaskServiceConfig(cfgfile)
    cfg.print_config(sys.stdout)
    sys.stdout.flush()

    app = FastAPI(
        title = SERVICE_NAME,
        description = SERVICE_DESCRIPTION,
        version = VERSION,
        root_path = cfg.service_root_path or "",
        # TODO ERRORS handle service exceptions 
    )
    app.add_middleware(GZipMiddleware)
    app.include_router(ROUTER_GENERAL)

    # TODO APPSTATE handle starup and shudown of app state
    
    return app
