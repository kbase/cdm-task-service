'''
A builder of Workflow Definition Language documents for the purposes of running CTS jobs with JAWS.
'''

# TODO TEST automated tests
# TODO TEST parse output with miniwdl or something for syntax checking
# TODO TEST manually test with JAWS

import os
from pathlib import Path
import math
import shlex
from typing import NamedTuple, Any

from cdmtaskservice.models import (
    Job,
    S3File,
    Parameter,
    ParameterType,
    InputFilesFormat,
)
from cdmtaskservice.input_file_locations import determine_file_locations


_WDL_VERSION = "1.0"  # Cromwell, and therefore JAWS, only supports 1.0

_IMAGE_TRANS_CHARS = str.maketrans({".": "_", "-": "_", "/": "_", ":": "_"})
_CONTAINER_NUM = "container_num"


class JawsInput(NamedTuple):
    """ Input to the JGI JAWS job runner. """
    wdl: str
    """ The WDL file as a string. """
    input_json: dict[str, Any]
    """ The input.json file as a dict. """


def generate_wdl(
    job: Job,
    file_mapping: dict[S3File, Path],
    manifest_file_list: list[Path] = None,
) -> JawsInput:
    """
    Generate input for a JAWS run in the form of a WDL file and input.json file contents.
    
    job_input - the input for the job.
    file_mapping - a mapping of the input S3 files to their paths at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
    manifest_file_list - A list of manifest paths at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
        Required if manifest files are specified in the job input.
        The manifest files will be mounted directly into the input mount point for the job,
        regardless of the path, and so must not collide with any other files in the input root.
    """
    # It'd be nice if there were a programmatic WDL writer but I haven't been able to find one
    # This fn is a little long but not too hard to read yet I think
    # If the manifest file name / path changes that'll break the JAWS cache, need to think about that
    # How often will jobs run with identical manifest files though? Maybe MD5 based caching?
    if not job.job_input.inputs_are_S3File():
        raise ValueError("input files must be S3 files with the E-tag")
    job_files = job.job_input.get_files_per_container()
    param = job.job_input.params.get_file_parameter()
    if param and param.type is ParameterType.MANIFEST_FILE and (
        not manifest_file_list or job_files.containers != len(manifest_file_list)
    ):
        raise ValueError(
            "If a manifest file is specified in the job parameters manifest_file_list "
            + "is required and its length must match the number of containers for the job"
        )
    workflow_name = job.image.name.translate(_IMAGE_TRANS_CHARS)
    file_to_rel_path = determine_file_locations(job.job_input)
    input_files = []
    relpaths = []
    environment = []
    cmdlines = []
    mfl = [None] * job_files.containers if not manifest_file_list else manifest_file_list
    for files, manifest in zip(job_files.files, mfl):
        ins = []
        rels = []
        for f in files:
            if f not in file_mapping:
                raise ValueError(f"file_mapping missing {f}")
            ins.append(str(file_mapping[f]))
            rels.append(shlex.quote(str(file_to_rel_path[f])))
        input_files.append(ins)
        relpaths.append(rels)
        cmd = [shlex.quote(c) for c in job.image.entrypoint]
        cmd.extend(_process_flag_args(job, files, file_to_rel_path, manifest))
        cmd.extend(_process_pos_args(job, files, file_to_rel_path, manifest))
        cmdlines.append(cmd)
        environment.append(_process_environment(job, files, file_to_rel_path, manifest))
    input_json = {
        f"{workflow_name}.input_files_list": input_files,
        f"{workflow_name}.file_locs_list": relpaths,
        f"{workflow_name}.environment_list": environment,
        f"{workflow_name}.cmdline_list": cmdlines
    }
    if manifest_file_list:
        input_json[f"{workflow_name}.manifest_list"] = [str(m) for m in manifest_file_list]

    wdl = _generate_wdl(job, workflow_name, bool(manifest_file_list))
    return JawsInput(wdl, input_json)


def _generate_wdl(job: Job, workflow_name: str, manifests: bool):
    # Inserting the job ID into the WDL should not bust the Cromwell cache:
    # https://kbase.slack.com/archives/CGJDCR22D/p1729786486819809
    # https://cromwell.readthedocs.io/en/stable/cromwell_features/CallCaching/
    mani_wf_input = ""
    mani_call_input = ""
    mani_task_input = ""
    if manifests:
        mani_wf_input = """
      Array[File] manifest_list"""
        mani_call_input = """,
        manifest = manifest_list[i]"""
        mani_task_input = """
    File manifest"""

    return f"""
version {_WDL_VERSION}

# CTS_JOB_ID: {job.id}

workflow {workflow_name} {{
  input {{
      Array[Array[File]] input_files_list
      Array[Array[String]] file_locs_list
      Array[Array[String]] environment_list
      Array[Array[String]] cmdline_list{mani_wf_input}
  }}
  scatter (i in range(length(input_files_list))) {{
    call run_container {{
      input:
        {_CONTAINER_NUM} = i,
        input_files = input_files_list[i],
        file_locs = file_locs_list[i],
        environ = environment_list[i],
        cmdline = cmdline_list[i]{mani_call_input}
    }}
  }}
  output {{
    Array[Array[File]] output_files = run_container.output_files
    Array[File] stdouts = run_container.stdout
    Array[File] stderrs = run_container.stderr
  }}
}}

task run_container {{
  input {{
    Int {_CONTAINER_NUM}
    Array[File] input_files
    Array[String] file_locs
    Array[String] environ
    Array[String] cmdline{mani_task_input}
  }}
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./__output__
    
    # link any manifest file into the mount point
    if [[ -n "${{manifest}}" ]]; then
        ln ${{manifest}} ./__input__/$(basename ${{manifest}})
    fi
    
    # link the input files into the mount point
    files=('~{{sep="' '" input_files}}')
    locs=(~{{sep=" " file_locs}})
    for i in ${{!files[@]}}; do
        mkdir -p ./__input__/$(dirname ${{locs[i]}})
        ln ${{files[i]}} ./__input__/${{locs[i]}}
    done
    
    # Set up environment
    job_env=(~{{sep=" " environ}})
    for jenv in ${{job_env[@]}}; do
        export $jenv
    done
      
    # run the command
    ~{{sep=" " cmdline}}
    EC=$?
    echo "Entrypoint exit code: $EC"

    # list the output of the command
    find ./__output__ -type f > ./output_files.txt
    
    exit $EC
  >>>
  
  output {{
    Array[File] output_files = read_lines("output_files.txt")
    File stdout = "stdout"
    File stderr = "stderr"
  }}
  
  runtime {{
    docker: "{job.image.name_with_digest}"
    runtime_minutes: {math.ceil(job.job_input.runtime.total_seconds() / 60)}
    memory: "{job.job_input.memory} B"
    cpu: {job.job_input.cpus}
  }}
}}
"""
    # TODO WDL handle mounting
    # TODO WDL handle refdata


def _process_flag_args(
    job: Job,
    files: list[S3File],
    file_to_rel_path: dict[S3File, Path],
    manifest: Path | None,
) -> list[str]:
    cmd = []
    if job.job_input.params.flag_args:
        for flag, p in job.job_input.params.flag_args.items():
            cmd.extend(_process_parameter(
                p, job, files, file_to_rel_path, manifest, as_list=True, flag=flag
            ))
    return cmd


def _process_pos_args(
    job: Job,
    files: list[S3File],
    file_to_rel_path: dict[S3File, Path],
    manifest: Path | None,
) -> list[str]:
    cmd = []
    if job.job_input.params.positional_args:
        for p in job.job_input.params.positional_args:
            cmd.extend(_process_parameter(p, job, files, file_to_rel_path, manifest, as_list=True))
    return cmd


def _process_environment(
    job: Job,
    files: list[S3File],
    file_to_rel_path: dict[S3File, Path],
    manifest: Path | None,
) -> list[str]:
    env = []
    if job.job_input.params.environment:
        for envkey, enval in job.job_input.params.environment.items():
            enval = _process_parameter(enval, job, files, file_to_rel_path, manifest)
            env.append(f'{envkey}={enval}')
    return env


def _process_parameter(
    param: str | Parameter,
    job: Job,
    files: list[S3File],
    file_to_rel_path: dict[S3File, Path],
    manifest: Path | None,
    as_list: bool = False,  # flag space separated files imply as list
    flag: str = None,
) -> str | list[str]:
    if isinstance(param, Parameter):
        match param.type:
            case ParameterType.INPUT_FILES:
                param = _join_files(files, param.input_files_format, job, file_to_rel_path, flag)
            case ParameterType.MANIFEST_FILE:  # implies manifest file is not None
                param = _handle_manifest(job, manifest, flag)
            case ParameterType.CONTAINTER_NUMBER:
                param = _handle_container_num(flag)
            case _:
                # should be impossible but make code future proof
                raise ValueError(f"Unexpected parameter type: {_}")
    elif flag:
        if flag.endswith("="):
            param = shlex.quote(flag + param)
        else:
            param = [shlex.quote(flag), shlex.quote(param)]
    else:
        shlex.quote(param)
    return [param] if as_list and not isinstance(param, list) else param


def _handle_container_num(flag: str) -> str | list[str]:
    # similar to the function below
    cn = f"${_CONTAINER_NUM}"
    if flag:
        if flag.endswith("="):
            # TODO TEST not sure if this will work
            param = shlex.quote(flag) + cn
        else:
            param = [shlex.quote(flag), cn]
    else:
        param = cn
    return param


def _handle_manifest(job: Job, manifest: Path, flag: str) -> str | list[str]:
    pth = os.path.join(job.job_input.params.input_mount_point, manifest.name)
    # This is the same as the command separated list case below... not sure if using a common fn
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
    files: list[S3File],
    format_: InputFilesFormat,
    job: Job,
    file_to_rel_path: dict[S3File, Path],
    flag: str,
) -> str | list[str]:
    imp = job.job_input.params.input_mount_point
    match format_:
        case InputFilesFormat.COMMA_SEPARATED_LIST:
            fileret = ",".join([_join_path(imp, file_to_rel_path, f) for f in files])
            if flag:
                if flag.endswith("="):
                    fileret = shlex.quote(flag + fileret)
                else:
                    fileret = [shlex.quote(flag), shlex.quote(fileret)]
            else:
                fileret = shlex.quote(fileret)
        case InputFilesFormat.SPACE_SEPARATED_LIST:
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
        case InputFilesFormat.REPEAT_PARAMETER:  # implies flag
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


def _join_path(imp: str, file_to_rel_path: dict[S3File, Path], f: S3File, quote: bool = False
) -> str:
    p = os.path.join(imp, file_to_rel_path[f])
    return shlex.quote(p) if quote else p
