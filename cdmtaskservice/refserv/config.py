"""
A configuration parser for the CDM refdata service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

# TODO CODE this is very similar to the standard config file. Seems like there could be some
#           shared code, but an initial attempt was messy. Retry later

import tomllib
from typing import BinaryIO, TextIO

from cdmtaskservice.config_s3 import S3Config


_SEC_AUTH = "Authentication"
_SEC_S3 = "S3"
_SEC_CTS = "CTS"
_SEC_SERVICE = "Service"

_SECS=[_SEC_AUTH, _SEC_S3, _SEC_CTS, _SEC_SERVICE]


class CDMRefdataServiceConfig:
    """
    The CDM refdata service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the CDM task service
    auth_cts_role: str - the Auth2 custom role that signifies a user is the CTS service.
    s3_url: str - the URL of the S3 instance to use for data storage.
    s3_access_key: str - the S3 access key.
    s3_access_secret: str - the S3 access secret.
    s3_allow_insecure: bool - whether to skip SSL cert validation, leaving the service vulnerable
        to MITM attacks.
    cts_root_url: str - the root URL of the CTS service.
    cts_refdata_token: str - a token that provides the custom role necessary for the CTS to
        identify the user as the refdata service.
    service_root_path: str  | None - if the service is behind a reverse proxy that rewrites the
        service path, the path to the service. The path is required in order for the OpenAPI
        documentation to function.
    service_group: str - the group to which this service belongs.
    """

    def __init__(self, config_file: BinaryIO):
        """
        Create the configuration parser.
        
        config_file - an open file-like object, opened in binary mode, containing the TOML
            config file data.
        version - the software version.
        """
        if not config_file:
            raise ValueError("config_file is required")
        # Since this is service startup and presumably the person starting the server is
        # savvy enough to figure out toml errors, we just throw the errors as is
        config = tomllib.load(config_file)
        # I feel like there ought to be a lib to do this kind of stuff... jsonschema doesn't
        # quite do what I want
        for sec in _SECS:
            _check_missing_section(config, sec)
        # TODO CODE checking URL syntax and returning an immutable class would be nice.
        #           yarl is way too liberal though. yarl + validators maybe?
        self.auth_url = _get_string_required(config, _SEC_AUTH, "url")
        self.auth_full_admin_roles = _get_list_string(config, _SEC_AUTH, "admin_roles_full")
        self.auth_cts_role = _get_string_required(config, _SEC_AUTH, "cts_role")
        self.s3_url = _get_string_required(config, _SEC_S3, "url")
        self.s3_access_key = _get_string_required(config, _SEC_S3, "access_key")
        self.s3_access_secret = _get_string_required(config, _SEC_S3, "access_secret")
        self.s3_allow_insecure = _get_string_optional(config, _SEC_S3, "allow_insecure") == "true"
        # If needed, we could add an S3 region parameter. YAGNI
        # If needed we could add sub sections to support > 1 S3 instance per service. YAGNI
        self.cts_root_url = _get_string_required(config, _SEC_CTS, "cts_root_url")
        self.cts_refdata_token = _get_string_required(config, _SEC_CTS, "cts_refdata_token")
        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")
        self.service_group = _get_string_optional(config, _SEC_SERVICE, "group_id")

    def get_s3_config(self) -> S3Config:
        """ Get the S3 configuration. """
        return S3Config(
            internal_url=self.s3_url,
            access_key=self.s3_access_key,
            access_secret=self.s3_access_secret,
            insecure=self.s3_allow_insecure,
        )

    def print_config(self, output: TextIO):
        """
        Print the configuration to the output argument, censoring secrets.
        """
        output.writelines([line + "\n" for line in [
            "\n*** Service Configuration ***",
            f"Authentication URL: {self.auth_url}",
            f"Authentication full admin roles: {self.auth_full_admin_roles}",
            f"Authentiation CTS role: {self.auth_cts_role}",
            f"S3 URL: {self.s3_url}",
            f"S3 access key: {self.s3_access_key}",
            "S3 access secret: REDACTED FOR YOUR SAFETY AND COMFORT",
            f"S3 allow insecure: {self.s3_allow_insecure}",
            f"CTS root URL: {self.cts_root_url}",
            f"CTS refdata token: REDACTED BY ORDER OF HIS MAJESTY KING OOLEPH OF SWEDEN",
            f"Service root path: {self.service_root_path}",
            f"Service group: {self.service_group}",
            "*** End Service Configuration ***\n"
        ]])

def _check_missing_section(config, section):
    if section not in config:
        raise ValueError(f"Missing section {section}")


# assumes section exists
def _get_string_required(config, section, key) -> str:
    putative = _get_string_optional(config, section, key)
    if not putative:
        raise ValueError(f"Missing value for key {key} in section {section}")
    return putative


# assumes section exists
def _get_string_optional(config, section, key) -> str | None:
    putative = config[section].get(key)
    if putative is None:
        return None
    if type(putative) != str:
        raise ValueError(
            f"Expected string value for key {key} in section {section}, got {putative}")
    if not putative.strip():
        return None
    return putative.strip()


#assumes section exists
def _get_list_string(config, section, key) -> list[str]:
    putative = _get_string_optional(config, section, key)
    if not putative:
        return []
    return [x.strip() for x in putative.split(",") if x.strip()]
