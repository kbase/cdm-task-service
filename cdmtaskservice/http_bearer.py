"""
Alteration of FastAPI's HTTPBearer class to handle the KBase authorization steps.

Also adds an `optional` keyword argument that allows for missing auth. If true and no
authorization information is provided, `None` is returned as the user.
"""

from fastapi.security.http import HTTPBase
from fastapi.openapi.models import HTTPBearer as HTTPBearerModel
from fastapi.requests import Request
from typing import Optional

from cdmtaskservice import app_state
from cdmtaskservice import kb_auth

# Modified from https://github.com/tiangolo/fastapi/blob/e13df8ee79d11ad8e338026d99b1dcdcb2261c9f/fastapi/security/http.py#L100
# Basically the only reason for this class is to get the UI to work with auth.
# Dependent on the middleware in app.py to set the user in the request state.

_SCHEME = "Bearer"


class KBaseHTTPBearer(HTTPBase):
    def __init__(
        self,
        *,
        bearerFormat: Optional[str] = None,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
        # FastAPI uses auto_error, but that allows for malformed headers as well as just
        # no header. Use a different variable name since the behavior is different.
        optional: bool = False,
        # Considered adding an admin auth role here and throwing an exception if the user
        # doesn't have it, but often you want to customize the error message.
        # Easier to handle that in the route method.
    ):
        self.model = HTTPBearerModel(bearerFormat=bearerFormat, description=description)
        self.scheme_name = scheme_name or self.__class__.__name__
        self.optional = optional

    async def __call__(self, request: Request) -> kb_auth.KBaseUser:
        user = app_state.get_request_user(request)
        if not user and not self.optional:
            raise MissingTokenError("Authorization header required")
        return user


class MissingTokenError(kb_auth.AuthenticationError):
    """ An error thrown when a token is expected but not provided. """
