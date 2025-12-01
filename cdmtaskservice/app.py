'''
API for the CDM task service.
'''

import contextvars
import datetime
import logging
import os
import sys
import uuid

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security.utils import get_authorization_scheme_param
from http.client import responses
from pythonjsonlogger.core import RESERVED_ATTRS
from pythonjsonlogger.json import JsonFormatter
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from cdmtaskservice import app_state
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.error_mapping import map_error
from cdmtaskservice import errors
from cdmtaskservice.exceptions import InvalidAuthHeaderError
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice import logfields
from cdmtaskservice import models_errors
from cdmtaskservice.refserv.config import CDMRefdataServiceConfig
from cdmtaskservice import routes
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import utcdatetime


# TODO FEATURE cleanup files:
#                  * jaws results
#                  * refdata d/l manifest & result files in scratch
#                  * various job files in scratch
#                  * refdata jaws transfer notification files when implemented for lawrencium
#                  * refdata not associated with images, need to keep the refdata mongo doc
#                       around since it's associdated with jobs, and could be used to restage
# TODO FEATURE GPU support
# TODO FEATURE job restarts / recovery (automatic?)
# TODO FEATURE refdata restarts / recovery (automatic?)


_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"
SERVICE_NAME = "CDM Task Service"
REFDATA_SERVICE_NAME = "CDM Refdata Service"
SERVICE_DESCRIPTION = (
    "A service for running arbitrary binaries on remote compute for the KBase CDM"
)
REFDATA_SERVICE_DESCRIPTION = (
    "A CTS companion service for manageing reference data"
)


###
#  Context variables - use these carefully and read the docs before modifying anything
###


logging_extra_var = contextvars.ContextVar("logging_extra_var", default={})
request_id_var = contextvars.ContextVar("request_id_var", default=None)


###
# Logging setup, uses context vars to fill in logs
###


# httpx is super chatty if the root logger is set to INFO
logging.basicConfig(level=logging.WARNING)
# https://stackoverflow.com/a/58777937/643675
logging.Formatter.formatTime = (
    lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(
        record.created, datetime.timezone.utc
    ).astimezone().isoformat(sep="T",timespec="milliseconds"))
rootlogger = logging.getLogger()
# Remove any existing handlers. The list slice prevents list modification while iterating
for handler in rootlogger.handlers[:]:
    rootlogger.removeHandler(handler)

# Not a fan of uvicorn specific stuff here
# Ensure Uvicorn loggers propagate to the root logger
uvicorn_loggers = ["uvicorn", "uvicorn.access", "uvicorn.error"]
for logger_name in uvicorn_loggers:
    logger = logging.getLogger(logger_name)
    logger.propagate = True
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)


class LoggingExtraFilter(logging.Filter):
    """ Insert extra fields into the logs. """
    def filter(self, record):
        for key, value in logging_extra_var.get().items():
            setattr(record, key, value)
        record.request_id = request_id_var.get()
        return True


class CustomJsonFormatter(JsonFormatter):
    """ Remove keys with null values from the logs. """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_log_record(self, log_record):
        return super().process_log_record(
            {k: v for k, v in log_record.items() if v is not None}
        )


handler = logging.StreamHandler()
handler.addFilter(LoggingExtraFilter())
handler.setFormatter(CustomJsonFormatter(
    "{levelname}{name}{message}{asctime}{exc_info}",
    style="{",
    rename_fields={"levelname": "level"},
    reserved_attrs=RESERVED_ATTRS + ["color_message"],
))
rootlogger.addHandler(handler)
logging.getLogger("cdmtaskservice").setLevel(logging.INFO)


###
# Middleware for auth and setting up context vars
###


_SCHEME = "Bearer"
_X_REAL_IP = "X-Real-IP"
_X_FORWARDED_FOR = "X-Forwarded-For"
_USER_AGENT = "User-Agent"


def _safe_strip(s: str):
    return s.strip() if s else None


class _AppMiddleWare(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id_var.set(str(uuid.uuid4()))
        extra_var = {
            logfields.X_FORWARDED_FOR: _safe_strip(request.headers.get(_X_FORWARDED_FOR)),
            logfields.X_REAL_IP: _safe_strip(request.headers.get(_X_REAL_IP)),
            logfields.IP_ADDRESS: _safe_strip(request.client.host),
            logfields.USER_AGENT: _safe_strip(request.headers.get(_USER_AGENT)),
            logfields.URL_PATH: _safe_strip(request.url.path),
            logfields.HTTP_METHOD: _safe_strip(request.method),
        }
        logging_extra_var.set(extra_var)
        user = None
        credentials = None
        authorization = request.headers.get("Authorization")
        if authorization:
            scheme, credentials = get_authorization_scheme_param(authorization)
            if not (scheme and credentials):
                raise InvalidAuthHeaderError(
                    f"Authorization header requires {_SCHEME} scheme followed by token")
            if scheme.lower() != _SCHEME.lower():
                # don't put the received scheme in the error message, might be a token
                raise InvalidAuthHeaderError(f"Authorization header requires {_SCHEME} scheme")
            user = await app_state.get_app_state(request).auth.get_kbase_user(credentials)
        app_state.set_request_user(request, user, credentials)
        extra_var[logfields.USER] = user.user if user else None
        logging_extra_var.set(extra_var)
        return await call_next(request)


###
# App creation
###


def create_app():
    """
    Create the CDM task service application
    """

    logging.getLogger(__name__).info(
        f"Server starting", extra={"version": VERSION, "git_commit": GIT_COMMIT}
    )
    with open(os.environ[_KB_DEPLOYMENT_CONFIG], "rb") as cfgfile:
        cfg = CDMTaskServiceConfig(cfgfile, VERSION)
    cfg.print_config(sys.stdout)  # maybe should redo the service config in json...?
    sys.stdout.flush()

    app = FastAPI(
        title = SERVICE_NAME,
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
    app.add_middleware(_AppMiddleWare)
    app.include_router(routes.ROUTER_GENERAL)
    app.include_router(routes.ROUTER_SITES)
    app.include_router(routes.ROUTER_JOBS)
    app.include_router(routes.ROUTER_IMAGES)
    app.include_router(routes.ROUTER_REFDATA)
    app.include_router(routes.ROUTER_ADMIN)
    app.include_router(routes.ROUTER_CALLBACKS)
    app.include_router(routes.ROUTER_EXTERNAL_EXEC)

    async def build_app_wrapper():
        await app_state.build_app(app, cfg, SERVICE_NAME)
    app.add_event_handler("startup", build_app_wrapper)

    async def clean_app_wrapper():
        await app_state.destroy_app_state(app)
    app.add_event_handler("shutdown", clean_app_wrapper)
    
    return app


def create_refdata_app():
    """
    Create the CDM refdata service application
    """

    logging.getLogger(__name__).info(
        f"Refdata server starting", extra={"version": VERSION, "git_commit": GIT_COMMIT}
    )
    with open(os.environ[_KB_DEPLOYMENT_CONFIG], "rb") as cfgfile:
        cfg = CDMRefdataServiceConfig(cfgfile)
    cfg.print_config(sys.stdout)  # maybe should redo the service config in json...?
    sys.stdout.flush()

    app = FastAPI(
        title = REFDATA_SERVICE_NAME,
        description = REFDATA_SERVICE_DESCRIPTION,
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
    app.add_middleware(_AppMiddleWare)
    app.include_router(routes.ROUTER_GENERAL)

    async def build_app_wrapper():
        await app_state.build_refdata_app(app, cfg, REFDATA_SERVICE_NAME)
    app.add_event_handler("startup", build_app_wrapper)

    async def clean_app_wrapper():
        await app_state.destroy_app_state(app)
    app.add_event_handler("shutdown", clean_app_wrapper)
    
    return app


###
# Eror handling
###


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
        user = app_state.get_request_user(r)
        if not user or not user.is_full_admin():
            # 5XX means something broke in the service, so nothing regular users can do about it
            err = "An unexpected error occurred"
        elif len(exc.args) == 1 and type(exc.args[0]) == str:
            err = exc.args[0]
        else:
            err = str(exc.args)
        return _format_error(errmap.http_code, err)


def _format_error(
    status_code: int,
    message: str = None,
    error_type: errors.ErrorType = None,
    request_validation_detail = None
) -> JSONResponse:
    content = {
        "httpcode": status_code,
        "httpstatus": responses[status_code],
        "time": utcdatetime(),
        "request_id": request_id_var.get(),
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
