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
from fastapi.responses import JSONResponse, Response
from fastapi.security.utils import get_authorization_scheme_param
from http.client import responses
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from cdmtaskservice import app_state
from cdmtaskservice import errors
from cdmtaskservice import kb_auth
from cdmtaskservice import models_errors
from cdmtaskservice import routes
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.error_mapping import map_error
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.exceptions import InvalidAuthHeaderError


# TODO LOGGING - log all write ops w/ username

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_DESCRIPTION = (
    "A service for running arbitrary binaries on remote compute for the KBase CDM"
)

# httpx is super chatty if the root logger is set to INFO
logging.basicConfig(level=logging.WARNING)
logging.getLogger("cdmtaskservice").setLevel(logging.INFO)


_SCHEME = "Bearer"


class _AppMiddleWare(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        # TODO LOGGING / ERRHADNLING set request ID in context var & include in error messages
        # TODO LOGGING set IP / X-Real-IP / X-Forwarded-For in context var
        # TODO LOGGING set user in contextvar
        user = None
        authorization = request.headers.get("Authorization")
        if authorization:
            scheme, credentials = get_authorization_scheme_param(authorization)
            if not (scheme and credentials):
                raise InvalidAuthHeaderError(
                    f"Authorization header requires {_SCHEME} scheme followed by token")
            if scheme.lower() != _SCHEME.lower():
                # don't put the received scheme in the error message, might be a token
                raise InvalidAuthHeaderError(f"Authorization header requires {_SCHEME} scheme")
            user = await app_state.get_app_state(request).auth.get_user(credentials)
        app_state.set_request_user(request, user)
        return await call_next(request)


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
    app.add_middleware(_AppMiddleWare)
    app.include_router(routes.ROUTER_GENERAL)
    app.include_router(routes.ROUTER_JOBS)
    app.include_router(routes.ROUTER_IMAGES)
    app.include_router(routes.ROUTER_REFDATA)
    app.include_router(routes.ROUTER_ADMIN)
    app.include_router(routes.ROUTER_CALLBACKS)

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
        user = app_state.get_request_user(r)
        if not user or user.admin_perm != kb_auth.AdminPermission.FULL:
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
        "time": utcdatetime()
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
