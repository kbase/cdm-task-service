"""
A configuration parser for the CDM task service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

import tomllib
from typing import Optional, BinaryIO, TextIO

_SEC_AUTH = "Authentication"
_SEC_SERVICE = "Service"


class CDMTaskServiceConfig:
    """
    The CDM task service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the CDM task service
    
    service_root_path: str  | None - if the service is behind a reverse proxy that rewrites the
        service path, the path to the service. The path is required in order for the OpenAPI
        documentation to function.
    """

    def __init__(self, config_file: BinaryIO):
        """
        Create the configuration parser.
        config_file - an open file-like object, opened in binary mode, containing the TOML
            config file data.
        """
        if not config_file:
            raise ValueError("config_file is required")
        # Since this is service startup and presumably the person starting the server is
        # savvy enough to figure out toml errors, we just throw the errors as is
        config = tomllib.load(config_file)
        # I feel like there ought to be a lib to do this kind of stuff... jsonschema doesn't
        # quite do what I want
        _check_missing_section(config, _SEC_AUTH)
        _check_missing_section(config, _SEC_SERVICE)
        self.auth_url = _get_string_required(config, _SEC_AUTH, "url")
        self.auth_full_admin_roles = _get_list_string(config, _SEC_AUTH, "admin_roles_full")

        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")

    def print_config(self, output: TextIO):
        """
        Print the configuration to the output argument, censoring secrets.
        """
        output.writelines([
            "\n*** Service Configuration ***\n",
        ])
        # TODO MONGO MINIO settings will go here and above, optionally print mongo user /pwd
        output.writelines([
            f"Authentication URL: {self.auth_url}\n",
            f"Authentication full admin roles: {self.auth_full_admin_roles}\n",
            f"Service root path: {self.service_root_path}\n",
            "*** End Service Configuration ***\n\n"
        ])

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
def _get_string_optional(config, section, key) -> Optional[str]:
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
    return [x.strip() for x in putative.split(",")]
