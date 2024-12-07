"""
A configuration parser for the CDM task service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

import tomllib
from typing import BinaryIO, TextIO

_SEC_AUTH = "Authentication"
_SEC_NERSC = "NERSC"
_SEC_S3 = "S3"
_SEC_MONGODB = "MongoDB"
_SEC_IMAGE = "Images"
_SEC_SERVICE = "Service"

_SECS=[_SEC_AUTH, _SEC_NERSC, _SEC_S3, _SEC_MONGODB, _SEC_IMAGE, _SEC_SERVICE]


class CDMTaskServiceConfig:
    """
    The CDM task service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the CDM task service
    kbase_staff_role: str - the Auth2 custom role indicating a user is a member of KBase staff.
    has_nersc_account_role: str - the Auth2 custom role indicating a user has a NERSC account.
    sfapi_cred_path: str - the path to a NERSC Superfacility API credential file. The file is
        expected to have the client ID as the first line and the client private key in PEM format
        as the remaining lines.
    sfapi_user: str - the user name of the user accociated with the credentials.
    s3_url: str - the url of the S3 instance to use for data storage.
    s3_access_key: str - the S3 access key.
    s3_access_secret: str - the S3 access secret.
    s3_allow_insecure: bool - whether to skip SSL cert validation, leaving the service vulnerable
        to MITM attacks.
    mongo_host: str - the MongoDB host.
    mongo_db: str - the MongoDB database.
    mongo_user: str | None - the MongoDB user name.
    mongo_user: str | None - the MongoDB password.
    mongo_user: bool - whether to set the MongoDB retry writes parameter on.
    crane_path: str - the path to a `crane` executable.
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
        for sec in _SECS:
            _check_missing_section(config, sec)
        self.auth_url = _get_string_required(config, _SEC_AUTH, "url")
        self.auth_full_admin_roles = _get_list_string(config, _SEC_AUTH, "admin_roles_full")
        self.kbase_staff_role = _get_string_required(config, _SEC_AUTH, "kbase_staff_role")
        self.has_nersc_account_role = _get_string_required(
            config, _SEC_AUTH, "has_nersc_account_role"
        )
        self.sfapi_cred_path = _get_string_required(config, _SEC_NERSC, "sfapi_cred_path")
        self.sfapi_user = _get_string_required(config, _SEC_NERSC, "sfapi_user")
        self.s3_url = _get_string_required(config, _SEC_S3, "url")
        self.s3_access_key = _get_string_required(config, _SEC_S3, "access_key")
        self.s3_access_secret = _get_string_required(config, _SEC_S3, "access_secret")
        self.s3_allow_insecure = _get_string_optional(config, _SEC_S3, "allow_insecure") == "true"
        # If needed, we could add an S3 region parameter. YAGNI
        # If needed we could add sub sections to support > 1 S3 instance per service. YAGNI
        self.mongo_host = _get_string_required(config, _SEC_MONGODB, "mongo_host")
        self.mongo_db = _get_string_required(config, _SEC_MONGODB, "mongo_db")
        self.mongo_user = _get_string_optional(config, _SEC_MONGODB, "mongo_user")
        self.mongo_pwd = _get_string_optional(config, _SEC_MONGODB, "mongo_pwd")
        self.mongo_retrywrites = _get_string_optional(
            config, _SEC_MONGODB, "mongo_retrywrites") == "true"
        if bool(self.mongo_user) != bool(self.mongo_pwd):
            raise ValueError("Either both or neither of the mongo user and password must "
                             + "be provided")
        self.crane_path = _get_string_required(config, _SEC_IMAGE, "crane_path")
        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")

    def print_config(self, output: TextIO):
        """
        Print the configuration to the output argument, censoring secrets.
        """
        pwd = "REDACTED FOR YOUR PLEASURE AND ENJOYMENT" if self.mongo_pwd else None
        output.writelines([
            "\n*** Service Configuration ***\n",
            f"Authentication URL: {self.auth_url}\n",
            f"Authentication full admin roles: {self.auth_full_admin_roles}\n",
            f"Authentication KBase staff role: {self.kbase_staff_role}\n",
            f"Authentication has NERSC account role: {self.has_nersc_account_role}\n",
            f"NERSC client credential path: {self.sfapi_cred_path}\n",
            f"NERSC client user: {self.sfapi_user}\n",
            f"S3 url: {self.s3_url}\n",
            f"S3 access key: {self.s3_access_key}\n",
            f"S3 access secret: REDACTED FOR YOUR SAFETY AND COMFORT\n",
            f"S3 allow insecure: {self.s3_allow_insecure}\n",
            f"MongoDB host: {self.mongo_host}\n",
            f"MongoDB database: {self.mongo_db}\n",
            f"MongoDB user: {self.mongo_user}\n",
            f"MongoDB password: {pwd}\n",
            f"MongoDB retry writes: {self.mongo_retrywrites}\n",
            f"crane executable path: {self.crane_path}\n",
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
    return [x.strip() for x in putative.split(",")]
