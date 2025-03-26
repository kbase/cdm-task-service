"""
A configuration parser for the CDM task service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

import tomllib
from typing import BinaryIO, TextIO

_SEC_AUTH = "Authentication"
_SEC_NERSC_JAWS = "NERSC_JAWS"
_SEC_NERSC = "NERSC"
_SEC_JAWS = "JAWS"
_SEC_S3 = "S3"
_SEC_MONGODB = "MongoDB"
_SEC_JOBS = "Jobs"
_SEC_KAFKA = "Kafka"
_SEC_LOGGING = "Logging"
_SEC_IMAGE = "Images"
_SEC_SERVICE = "Service"

_SECS=[
    _SEC_AUTH, _SEC_NERSC_JAWS, _SEC_NERSC, _SEC_JAWS, _SEC_S3, _SEC_MONGODB, _SEC_JOBS,
    _SEC_KAFKA, _SEC_LOGGING, _SEC_IMAGE, _SEC_SERVICE]


class CDMTaskServiceConfig:
    """
    The CDM task service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the CDM task service
    kbase_staff_role: str - the Auth2 custom role indicating a user is a member of KBase staff.
    has_nersc_account_role: str - the Auth2 custom role indicating a user has a NERSC account.
    nersc_jaws_user: str - the user name of the user associated with the NERSC and JAWS
        credentials.
    jaws_refdata_root_dir: str - the JAWS refdata root directory to use for refdata storage
    sfapi_cred_path: str - the path to a NERSC Superfacility API credential file. The file is
        expected to have the client ID as the first line and the client private key in PEM format
        as the remaining lines.
    nersc_remote_code_dir: str - the location at NERSC to upload remote code.
    jaws_url: str - the URL of the JAWS Central service.
    jaws_token: str - the JAWS token used to run jobs.
    jaws_group: str - the JAWS group used to run jobs.
    s3_url: str - the URL of the S3 instance to use for data storage.
    s3_external_url: str - the URL of the S3 instance accessible to external code or services.
    s3_verify_external_url: bool - whether to verify connectivity to the external S3 url at
        service startup.
    s3_access_key: str - the S3 access key.
    s3_access_secret: str - the S3 access secret.
    s3_allow_insecure: bool - whether to skip SSL cert validation, leaving the service vulnerable
        to MITM attacks.
    mongo_host: str - the MongoDB host.
    mongo_db: str - the MongoDB database.
    mongo_user: str | None - the MongoDB user name.
    mongo_user: str | None - the MongoDB password.
    mongo_user: bool - whether to set the MongoDB retry writes parameter on.
    job_max_cpu_hours: float - the maximum number of cpu hours per job.
    kafka_bootstrap_servers: str - the Kafka bootstrap servers in standard format.
    kafka_topic_jobs: str - the Kafka topic where job updates will be pubished.
    container_s3_log_dir: str - where to store container logs in S3.
    crane_path: str - the path to a `crane` executable.
    service_root_url: str - the URL for the service root.
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
        self.jaws_refdata_root_dir = _get_string_required(
            config, _SEC_NERSC_JAWS, "refdata_root_dir"
        )
        self.nersc_jaws_user = _get_string_required(config, _SEC_NERSC_JAWS, "user")
        self.sfapi_cred_path = _get_string_required(config, _SEC_NERSC, "sfapi_cred_path")
        self.nersc_remote_code_dir = _get_string_required(config, _SEC_NERSC, "remote_code_dir")
        self.jaws_url = _get_string_required(config, _SEC_JAWS, "url")
        self.jaws_token = _get_string_required(config, _SEC_JAWS, "token")
        self.jaws_group = _get_string_required(config, _SEC_JAWS, "group")
        self.s3_url = _get_string_required(config, _SEC_S3, "url")
        self.s3_external_url = _get_string_required(config, _SEC_S3, "external_url")
        self.s3_verify_external_url = _get_string_optional(
            config, _SEC_S3, "verify_external_url") != "false"
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
        self.job_max_cpu_hours = _get_float_required(config, _SEC_JOBS, "max_cpu_hours", minimum=1)
        self.kafka_boostrap_servers = _get_string_required(config, _SEC_KAFKA, "bootstrap_servers")
        self.kafka_topic_jobs = _get_string_required(config, _SEC_KAFKA, "topic_jobs")
        self.container_s3_log_dir = _get_string_required(
            config, _SEC_LOGGING, "container_s3_log_dir"
        )
        self.crane_path = _get_string_required(config, _SEC_IMAGE, "crane_path")
        self.service_root_url = _get_string_required(config, _SEC_SERVICE, "root_url")
        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")
        self.service_group = _get_string_optional(config, _SEC_SERVICE, "group_id")

    def print_config(self, output: TextIO):
        """
        Print the configuration to the output argument, censoring secrets.
        """
        pwd = "REDACTED FOR YOUR PLEASURE AND ENJOYMENT" if self.mongo_pwd else None
        output.writelines([line + "\n" for line in [
            "\n*** Service Configuration ***",
            f"Authentication URL: {self.auth_url}",
            f"Authentication full admin roles: {self.auth_full_admin_roles}",
            f"Authentication KBase staff role: {self.kbase_staff_role}",
            f"Authentication has NERSC account role: {self.has_nersc_account_role}",
            f"NERSC / JAWS user: {self.nersc_jaws_user}",
            f"NERSC / JAWS refdata root dir: {self.jaws_refdata_root_dir}",
            f"NERSC client credential path: {self.sfapi_cred_path}",
            f"NERSC remote code dir: {self.nersc_remote_code_dir}",
            f"JAWS Central URL: {self.jaws_url}",
            "JAWS token: REDACTED FOR THE NATIONAL SECURITY OF GONDWANALAND",
            f"JAWS group: {self.jaws_group}",
            f"S3 URL: {self.s3_url}",
            f"S3 external URL: {self.s3_external_url}",
            f"S3 verify external URL: {self.s3_verify_external_url}",
            f"S3 access key: {self.s3_access_key}",
            "S3 access secret: REDACTED FOR YOUR SAFETY AND COMFORT",
            f"S3 allow insecure: {self.s3_allow_insecure}",
            f"MongoDB host: {self.mongo_host}",
            f"MongoDB database: {self.mongo_db}",
            f"MongoDB user: {self.mongo_user}",
            f"MongoDB password: {pwd}",
            f"MongoDB retry writes: {self.mongo_retrywrites}",
            f"Max CPU hours per job: {self.job_max_cpu_hours}",
            f"Kafka bootstrap servers: {self.kafka_boostrap_servers}",
            f"Kafka jobs topic: {self.kafka_topic_jobs}",
            f"Directory in S3 for container logs: {self.container_s3_log_dir}",
            f"crane executable path: {self.crane_path}",
            f"Service root URL: {self.service_root_url}",
            f"Service root path: {self.service_root_path}",
            f"Service group: {self.service_group}",
            "*** End Service Configuration ***\n"
        ]])

def _check_missing_section(config, section):
    if section not in config:
        raise ValueError(f"Missing section {section}")


def _get_float_required(config, section, key, minimum: float = None) -> float:
    putative = config[section].get(key)
    if type(putative) not in {int, float}:
        raise ValueError(
            f"Expected float value for key {key} in section {section}, got {putative}")
    if minimum is not None and putative < minimum:
        raise ValueError(
            f"Expected value >= {minimum} for key {key} in section {section}, got {putative}")
    return putative


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
