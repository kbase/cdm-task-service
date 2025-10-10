"""
A configuration parser for the CDM task service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

import tomllib
from typing import BinaryIO, TextIO

from cdmtaskservice.condor.client import HTCondorWorkerPaths
from cdmtaskservice.jaws.config import JAWSConfig
from cdmtaskservice.nersc.paths import NERSCPaths


_SEC_AUTH = "Authentication"
_SEC_NERSC_JAWS = "NERSC_JAWS"
_SEC_NERSC = "NERSC"
_SEC_JAWS = "JAWS"
_SEC_HTCONDOR = "HTCondor"
_SEC_EXTERNAL_EXEC = "ExternalExecution"
_SEC_S3 = "S3"
_SEC_MONGODB = "MongoDB"
_SEC_JOBS = "Jobs"
_SEC_KAFKA = "Kafka"
_SEC_IMAGE = "Images"
_SEC_SERVICE = "Service"

_SECS=[
    _SEC_AUTH, _SEC_NERSC_JAWS, _SEC_NERSC, _SEC_JAWS, _SEC_HTCONDOR, _SEC_EXTERNAL_EXEC, _SEC_S3,
    _SEC_MONGODB, _SEC_JOBS, _SEC_KAFKA, _SEC_IMAGE, _SEC_SERVICE
]


class CDMTaskServiceConfig:
    """
    The CDM task service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the CDM task service
    kbase_staff_role: str - the Auth2 custom role indicating a user is a member of KBase staff.
    has_nersc_account_role: str - the Auth2 custom role indicating a user has a NERSC account.
    external_executor_role: str - the Auth2 custom role indicating a user is an external job
        executor.
    nersc_jaws_user: str - the user name of the user associated with the NERSC and JAWS
        credentials.
    jaws_refdata_root_dir: str - the JAWS refdata root directory to use for refdata storage.
    jaws_staging_dir_dtn: str - the JAWS staging directory for the `kbase` site on a NERSC DTN.
    jaws_staging_dir_prl: str - the JAWS staging directory for the `kbase` site on the NERSC
        Perlmutter system.
    sfapi_cred_path: str - the path to a NERSC Superfacility API credential file. The file is
        expected to have the client ID as the first line and the client private key in PEM format
        as the remaining lines.
    nersc_remote_code_dir: str - the location at NERSC to upload remote code.
    jaws_url: str - the URL of the JAWS Central service.
    jaws_token: str - the JAWS token used to run jobs.
    jaws_group: str - the JAWS group used to run jobs.
    condor_exe_path: str - the local path to the HTCondor worker executable.
    condor_exe_url_override: str | None - a url, if any, to use for downloading the HTCondor
        executable rather than the default location.
    condor_initialdir: str - the path to use as the condor initialdir.
    condor_clientgroup: str | None - the clientgroup to use for condor submission, if any.
    condor_token_path: str - the path on the condor worker containing a
        KBase token for use when contacting the service.
    condor_s3_access_secret_path: str - the path on the condor worker containing
        the s3 access secret for the S3 instance.
    code_archive_path: str - the local path to the tgz code archive.
    code_archive_url_override: str | None - a url, if any, to use for downloading the 
        tgz code archive rather than the default location.
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
    allowed_s3_paths: list[str] - the list of paths where users are allowed to read and write
        files.
    container_s3_log_dir: str - where to store container logs in S3.
    job_max_cpu_hours: float - the maximum number of cpu hours per job.
    kafka_bootstrap_servers: str - the Kafka bootstrap servers in standard format.
    kafka_topic_jobs: str - the Kafka topic where job updates will be published.
    kafka_startup_unsent_delay_min: int | None - if present, indicates that unsent Kafka job
        updates messages older than the given delay in minutes should be sent.
    crane_path: str - the path to a `crane` executable.
    service_root_url: str - the URL for the service root.
    service_root_path: str  | None - if the service is behind a reverse proxy that rewrites the
        service path, the path to the service. The path is required in order for the OpenAPI
        documentation to function.
    service_group: str - the group to which this service belongs.
    """

    def __init__(self, config_file: BinaryIO, version: str):
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
        self.kbase_staff_role = _get_string_required(config, _SEC_AUTH, "kbase_staff_role")
        self.has_nersc_account_role = _get_string_required(
            config, _SEC_AUTH, "has_nersc_account_role"
        )
        self.external_executor_role = _get_string_required(
            config, _SEC_AUTH, "external_executor_role"
        )
        self.jaws_refdata_root_dir = _get_string_required(
            config, _SEC_NERSC_JAWS, "refdata_root_dir"
        )
        # These typically have $PSCRATCH for the kbjaws kbase site user embedded in it,
        # which I'd prefer not to specify literally in a config. Not sure if there's a better
        # way to deal with it since the service doesn't know anything about the jaws site user.
        # Worry about it later 
        # Also not a fan of having to specify both but so far the other solutions I've considered
        # are even uglier
        self.jaws_staging_dir_dtn = _get_string_required(
            config, _SEC_NERSC_JAWS, "jaws_staging_dir_dtn"
        )
        self.jaws_staging_dir_prl = _get_string_required(
            config, _SEC_NERSC_JAWS, "jaws_staging_dir_perlmutter"
        )
        self.nersc_jaws_user = _get_string_required(config, _SEC_NERSC_JAWS, "user")
        self.sfapi_cred_path = _get_string_required(config, _SEC_NERSC, "sfapi_cred_path")
        self.nersc_remote_code_dir = _get_string_required(config, _SEC_NERSC, "remote_code_dir")
        self.jaws_url = _get_string_required(config, _SEC_JAWS, "url")
        self.jaws_token = _get_string_required(config, _SEC_JAWS, "token")
        self.jaws_group = _get_string_required(config, _SEC_JAWS, "group")
        self.condor_exe_path = _get_string_required(config, _SEC_HTCONDOR, "executable_path")
        self.condor_exe_url_override = _get_string_optional(
            config, _SEC_HTCONDOR, "executable_url_override"
        )
        self.condor_initialdir = _get_string_required(config, _SEC_HTCONDOR, "initialdir")
        self.condor_clientgroup = _get_string_optional(config, _SEC_HTCONDOR, "clientgroup")
        self.condor_token_path = _get_string_required(config, _SEC_HTCONDOR, "token_path")
        self.condor_s3_access_secret_path = _get_string_required(
            config, _SEC_HTCONDOR, "s3_access_secret_path"
        )
        self.code_archive_path = _get_string_required(config, _SEC_EXTERNAL_EXEC, "archive_path")
        self.code_archive_url_override = _get_string_optional(
            config, _SEC_EXTERNAL_EXEC, "archive_url_override"
        )
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
        # may want to specify read only paths as well
        # may want to allow service admins to override
        self.allowed_s3_paths = sorted({
            f"{p.rstrip('/')}/" for p in _get_list_string(config, _SEC_JOBS, "allowed_s3_paths")
        })
        self.container_s3_log_dir = _get_string_required(
            config, _SEC_JOBS, "container_s3_log_dir"
        ).rstrip("/") + "/"
        self.job_max_cpu_hours = _get_float_required(config, _SEC_JOBS, "max_cpu_hours", minimum=1)
        self.kafka_boostrap_servers = _get_string_required(config, _SEC_KAFKA, "bootstrap_servers")
        self.kafka_topic_jobs = _get_string_required(config, _SEC_KAFKA, "topic_jobs")
        startup_delay = _get_int_optional(
            config, _SEC_KAFKA, "check_unsent_messages_on_startup_delay_min"
        )
        startup_delay = None if startup_delay is not None and startup_delay < 1 else startup_delay
        self.kafka_startup_unsent_delay_min = startup_delay
        self.crane_path = _get_string_required(config, _SEC_IMAGE, "crane_path")
        self.service_root_url = _get_string_required(config, _SEC_SERVICE, "root_url")
        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")
        self.service_group = _get_string_optional(config, _SEC_SERVICE, "group_id")
        self._nersc_paths = NERSCPaths(  # fail early if paths fail validation
            f"{self.nersc_remote_code_dir}/{version}",
            self.jaws_refdata_root_dir,
            self.jaws_staging_dir_dtn,
            self.jaws_staging_dir_prl
        )
        self._check_path_overlap()
    
    def _check_path_overlap(self):  # TODO TEST
        if self.allowed_s3_paths:
            # TDOO check paths are valid S3 paths. Bucket required, key not required
            paths = sorted(
                [(self.container_s3_log_dir, True)] + [(p, False) for p in self.allowed_s3_paths],
                key=lambda x: x[0]
            )
            for i, (p1, is_log1) in enumerate(paths):
                for p2, is_log2 in paths[i + 1:]:  # if index is > len - 1 will return []
                    if p2.startswith(p1):
                        name1 = "container_s3_log_dir" if is_log1 else "Allowed path"
                        name2 = "container_s3_log_dir" if is_log2 else "allowed path"
                        raise ValueError(f"{name1} '{p1}' is a prefix of {name2} '{p2}'")

    def get_nersc_paths(self) -> NERSCPaths:
        """
        Get the set of NERSC paths for use with the service.
        """
        return self._nersc_paths
    
    def get_jaws_config(self) -> JAWSConfig:
        """
        Get configuration information for JAWS.
        """
        return JAWSConfig(
            user=self.nersc_jaws_user,
            token=self.jaws_token,
            group=self.jaws_group,
            url=self.jaws_url,
        )
        
    def get_condor_paths(self) -> HTCondorWorkerPaths:
        """
        Get information about environment variables on HTcondor workers for external executors.
        """
        return HTCondorWorkerPaths(
            token_path=self.condor_token_path,
            s3_access_secret_path=self.condor_s3_access_secret_path,
        )

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
            f"Authentication external executor role: {self.external_executor_role}",
            f"NERSC / JAWS user: {self.nersc_jaws_user}",
            f"NERSC / JAWS refdata root dir: {self.jaws_refdata_root_dir}",
            f"NERSC / JAWS DTN staging dir: {self.jaws_staging_dir_dtn}",
            f"NERSC / JAWS Perlmutter staging dir: {self.jaws_staging_dir_prl}",
            f"NERSC client credential path: {self.sfapi_cred_path}",
            f"NERSC remote code dir: {self.nersc_remote_code_dir}",
            f"JAWS Central URL: {self.jaws_url}",
            "JAWS token: REDACTED FOR THE NATIONAL SECURITY OF GONDWANALAND",
            f"JAWS group: {self.jaws_group}",
            f"HTCondor exe path: {self.condor_exe_path}",
            f"HTCondor exe url override: {self.condor_exe_url_override}",
            f"HTCondor initialdir: {self.condor_initialdir}",
            f"HTCondor client group: {self.condor_clientgroup}",
            f"HTCondor token path: {self.condor_token_path}",
            f"HTCondor S3 access secret path: {self.condor_s3_access_secret_path}",
            f"Code archive path: {self.code_archive_path}",
            f"Code archive url override: {self.code_archive_url_override}",
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
            f"Allowed S3 paths: {self.allowed_s3_paths}",
            f"Directory in S3 for container logs: {self.container_s3_log_dir}",
            f"Max CPU hours per job: {self.job_max_cpu_hours}",
            f"Kafka bootstrap servers: {self.kafka_boostrap_servers}",
            f"Kafka jobs topic: {self.kafka_topic_jobs}",
            f"Kafka unsent messages startup send delay: {self.kafka_startup_unsent_delay_min}",
            f"crane executable path: {self.crane_path}",
            f"Service root URL: {self.service_root_url}",
            f"Service root path: {self.service_root_path}",
            f"Service group: {self.service_group}",
            "*** End Service Configuration ***\n"
        ]])

def _check_missing_section(config, section):
    if section not in config:
        raise ValueError(f"Missing section {section}")


def _get_int_optional(config, section, key) -> int | None:
    putative = config[section].get(key)
    if putative is None:
        return None
    if type(putative) != int:
        raise ValueError(
            f"Expected integer value for key {key} in section {section}, got {putative}")
    return putative


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
    return [x.strip() for x in putative.split(",") if x.strip()]
