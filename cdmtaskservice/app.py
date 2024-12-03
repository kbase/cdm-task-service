'''
API for the CDM task service.
'''

import logging
import os
import sys

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from http.client import responses
from starlette.exceptions import HTTPException as StarletteHTTPException

from cdmtaskservice import app_state
from cdmtaskservice import errors
from cdmtaskservice.error_mapping import map_error
from cdmtaskservice import models_errors
from cdmtaskservice import routes
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import timestamp


# TODO LOGGING - log all write ops w/ username

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_DESCRIPTION = (
    "A service for running arbitrary binaries on remote compute for the KBase CDM"
)

# httpx is super chatty if the root logger is set to INFO
logging.basicConfig(level=logging.WARNING)
logging.getLogger("cdmtaskservice").setLevel(logging.INFO)


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
        title = routes.SERVICE_NAME,
        description = SERVICE_DESCRIPTION,
        version = VERSION,
        root_path = cfg.service_root_path or "",
        exception_handlers = {
            RequestValidationError: _handle_fastapi_validation_exception,
            StarletteHTTPException: _handle_starlette_exception,
            Exception: _handle_general_exception
        },
        responses = {
            "4XX": {"model": models_errors.ClientError},
            "5XX": {"model": models_errors.ServerError}
        }
    )
    app.add_middleware(GZipMiddleware)
    app.include_router(routes.ROUTER_GENERAL)
    app.include_router(routes.ROUTER_JOBS)
    app.include_router(routes.ROUTER_ADMIN)

    async def build_app_wrapper():
        await app_state.build_app(app, cfg)
    app.add_event_handler("startup", build_app_wrapper)

    async def clean_app_wrapper():
        await app_state.destroy_app_state(app)
    app.add_event_handler("shutdown", clean_app_wrapper)
    
    return app


def _handle_fastapi_validation_exception(r: Request, exc: RequestValidationError):
    return _format_error(
        status.HTTP_400_BAD_REQUEST,
        error_type=errors.ErrorType.REQUEST_VALIDATION_FAILED,
        request_validation_detail=exc.errors()
    )

def _handle_starlette_exception(r: Request, exc: StarletteHTTPException):
    # may need to expand this in the future if we find other error types
    error_type=None
    if exc.status_code == status.HTTP_404_NOT_FOUND:
        error_type = errors.ErrorType.NOT_FOUND
    return _format_error(exc.status_code, message=str(exc.detail), error_type=error_type)


def _handle_general_exception(r: Request, exc: Exception):
    errmap = map_error(exc)
    if errmap.http_code < 500:
        return _format_error(errmap.http_code, str(exc), errmap.err_type)
    else:
        # TODO ERRORHANDLING may want to only return error message for 500s if user is a
        #      service admin, otherwise return a generic message
        if len(exc.args) == 1 and type(exc.args[0]) == str:
            return _format_error(errmap.http_code, exc.args[0])
        else:
            return _format_error(errmap.http_code)
        

def _format_error(
        status_code: int,
        message: str = None,
        error_type: errors.ErrorType = None,
        request_validation_detail = None
        ):
    content = {
        "httpcode": status_code,
        "httpstatus": responses[status_code],
        "time": timestamp()
    }
    if error_type:
        content.update({
            "appcode": error_type.error_code, "apperror": error_type.error_type
        })
    if message:
        content.update({"message": message})
    if request_validation_detail:
        content.update({"request_validation_detail": request_validation_detail})
    return JSONResponse(status_code=status_code, content=jsonable_encoder({"error": content}))
