"""
Generates arguments and environments for job containers.
"""

from dataclasses import dataclass
import os
from pathlib import Path
import shlex

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, check_num as _check_num
from cdmtaskservice.input_file_locations import determine_file_locations
from cdmtaskservice import models


# Note there's a lot of shlex.quote()ing done in this module that is effectively a noop
# since the Job class locks down input strings such that they don't need to be quoted, other than
# the image entrypoint and input files which are out of our control (assuming a service admin
# approves an image with suspect characters for the case of the entrypoint, which shouldn't
# happen). However, we keep the quoting as a secondary safety measure.


@dataclass
class ContainerData:
    
    args: list[str]
    """ The arguments for the container. """
    
    env: dict[str, str]
    """ The environment for the container. """
    
    files: dict[models.S3File, Path]
    """
    The files for the container in an ordered dictionary from the S3 object data to
    the relative container path.
    """


class ArgumentGenerator:
    """
    Generates arguments and environmental variables for a container given a Job.
    """
    
    def __init__(self, job: models.Job, manifest_file_list: list[Path] | None = None):
        """
        Create the argument generator.
        
        job - the job for which arguments will be generated.
        manifest_file_list - required if the job specifies manifest files. A list of file paths
        where the manifest will be present in the container, in container order,
        one per container.
        """
        # If the kbase site ever supports manifest files this will need more thought as it
        # will be run in a context where only one manifest file exists vs. a list
        self._job = _not_falsy(job, "job")
        if not job.job_input.inputs_are_S3File():
            raise ValueError("input files must be S3 files with a checksum")
        self._mfl = manifest_file_list
        param = job.job_input.params.get_file_parameter()
        if param and param.type is models.ParameterType.MANIFEST_FILE and (
            not self._mfl or job.job_input.num_containers != len(self._mfl)
        ):
            raise ValueError(
                "If a manifest file is specified in the job parameters, manifest_file_list "
                + "is required and its length must match the number of containers for the job"
            )
        if self._mfl:
            for m in self._mfl:
                if not models.PATH_REGEX_COMPILED.fullmatch(str(m)):
                    raise ValueError(f"Disallowed manifest path: {m}")

    def get_container_arguments(self, container_number: int) -> ContainerData:
        """
        Get the argument list, environment dictionary, and file information for a container.
        """
        _check_num(container_number, "container_number", minimum=0)
        if container_number >= len(self._job.job_input.input_files):
            raise ValueError(f"container_number must be < {len(self._job.job_input.input_files)}")
        # this is inefficient but probably has zero real impact
        files = self._job.job_input.get_files_per_container()[container_number]
        file_to_rel_path_all = determine_file_locations(self._job.job_input)
        file_to_rel_path = {f: file_to_rel_path_all[f] for f in files}
        cmd = [shlex.quote(c) for c in self._job.image.entrypoint]
        manifest = self._mfl[container_number] if self._mfl else None 
        cmd.extend(_process_pos_args(
            self._job, container_number, files, file_to_rel_path, manifest)
        )
        env = _process_environment(self._job, container_number, files, file_to_rel_path, manifest)
        return ContainerData(cmd, env, file_to_rel_path)


def _process_pos_args(
    job: models.Job,
    container_num: int,
    files: list[models.S3FileWithDataID],
    file_to_rel_path: dict[models.S3FileWithDataID, Path],
    manifest: Path | None,
) -> list[str]:
    cmd = []
    if job.job_input.params.args:
        for p in job.job_input.params.args:
            cmd.extend(_process_parameter(
                p, job, container_num, files, file_to_rel_path, manifest, as_list=True
            ))
    return cmd


def _process_environment(
    job: models.Job,
    container_num: int,
    files: list[models.S3FileWithDataID],
    file_to_rel_path: dict[models.S3FileWithDataID, Path],
    manifest: Path | None,
) -> list[str]:
    env = []
    if job.job_input.params.environment:
        for envkey, enval in job.job_input.params.environment.items():
            enval = _process_parameter(
                enval, job, container_num, files, file_to_rel_path, manifest, no_flag=True
            )
            env.append(f'{envkey}={enval}')
    return env


def _process_parameter(
    param: str | models.Parameter,
    job: models.Job,
    container_num: int,
    files: list[models.S3FileWithDataID],
    file_to_rel_path: dict[models.S3FileWithDataID, Path],
    manifest: Path | None,
    as_list: bool = False,
    no_flag: bool = False,
) -> str | list[str]:
    if isinstance(param, models.Parameter):
        flag = None if no_flag else param.get_flag()
        match param.type:
            case models.ParameterType.INPUT_FILES:
                param = _join_files(
                    files, param.input_files_format, job, file_to_rel_path, flag
                )
            case models.ParameterType.MANIFEST_FILE:  # implies manifest file is not None
                param = _handle_manifest(job, manifest, flag)
            case models.ParameterType.CONTAINTER_NUMBER:
                param = _handle_container_num(param, container_num, flag)
            case _:
                # should be impossible but make code future proof
                raise ValueError(f"Unexpected parameter type: {_}")
    else:
        param = shlex.quote(param)
    return [param] if as_list and not isinstance(param, list) else param


def _handle_container_num(
        p: models.Parameter, container_num: int, flag: str | None
) -> str | list[str]:
    # similar to the function below
    pre = p.container_num_prefix if p.container_num_prefix else ""
    suf = p.container_num_suffix if p.container_num_suffix else ""
    cn = f"{pre}{container_num}{suf}"
    if flag:
        if flag.endswith("="):
            # not sure if this will work, but since Job flags are shell safe it's
            # impossible to test
            param = shlex.quote(flag) + cn
        else:
            param = [shlex.quote(flag), cn]
    else:
        param = shlex.quote(cn)
    return param


def _handle_manifest(job: models.Job, manifest: Path, flag: str | None) -> str | list[str]:
    pth = os.path.join(job.job_input.params.input_mount_point, manifest.name)
    # This is the same as the comma separated list case below... not sure if using a common fn
    # makes sense
    if flag:
        if flag.endswith("="):
            param = shlex.quote(flag + pth)
        else:
            param = [shlex.quote(flag), shlex.quote(pth)]
    else:
        param = shlex.quote(pth)
    return param


# this is a bit on the complex side...
def _join_files(
    files: list[models.S3FileWithDataID],
    format_: models.InputFilesFormat,
    job: models.Job,
    file_to_rel_path: dict[models.S3FileWithDataID, Path],
    flag: str | None,
) -> str | list[str]:
    imp = job.job_input.params.input_mount_point
    match format_:
        case models.InputFilesFormat.COMMA_SEPARATED_LIST:
            fileret = ",".join([_join_path(imp, file_to_rel_path, f) for f in files])
            if flag:
                if flag.endswith("="):
                    fileret = shlex.quote(flag + fileret)
                else:
                    fileret = [shlex.quote(flag), shlex.quote(fileret)]
            else:
                fileret = shlex.quote(fileret)
        case models.InputFilesFormat.SPACE_SEPARATED_LIST:
            if flag:
                if flag.endswith("="):
                    # this case is a little weird
                    fileret = [
                        shlex.quote(flag + _join_path(imp, file_to_rel_path, files[0]))
                    ] + [_join_path(imp, file_to_rel_path, f, quote=True) for f in files[1:]]
                else: 
                    fileret = [shlex.quote(flag)] + [
                        _join_path(imp, file_to_rel_path, f, quote=True) for f in files
                    ]
            else:
                fileret = [_join_path(imp, file_to_rel_path, f, quote=True) for f in files]
        case models.InputFilesFormat.REPEAT_PARAMETER:  # implies flag
            fileret = []
            if flag.endswith("="):
                for f in files:
                    fileret.append(shlex.quote(flag + _join_path(imp, file_to_rel_path, f)))
            else:
                flag = shlex.quote(flag)
                for f in files:
                    fileret.extend((flag, _join_path(imp, file_to_rel_path, f, quote=True)))
        case _:
            # should be impossible but make code future proof
            raise ValueError(f"Unexpected input files format: {_}")
    return fileret


def _join_path(
        imp: str,
        file_to_rel_path: dict[models.S3FileWithDataID, Path],
        f: models.S3FileWithDataID,
        quote: bool = False
) -> str:
    p = os.path.join(imp, file_to_rel_path[f])
    return shlex.quote(p) if quote else p
