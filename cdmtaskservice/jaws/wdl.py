'''
A builder of Workflow Definition Language documents for the purposes of running CTS jobs with JAWS.
'''

# TODO TEST
# TODO TEST parse output with miniwdl or something for syntax checking
# TODO TEST manually test with JAWS
# TODO MOUNTING add mounting info when available

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


class JawsInput(NamedTuple):
    """ Input to the JGI JAWS job runner. """
    wdl: str
    """ The WDL file as a string. """
    input_json: dict[str, Any]
    """ The input.json file as a dict. """


def generate_wdl(
    job: Job,
    file_mapping: dict[S3File, Path],
) -> JawsInput:
    """
    Generate input for a JAWS run in the form of a WDL file and input.json file contents.
    
    job_input - the input for the job.
    file_mapping - a mapping of the input S3 files to their paths at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
    """
    # It'd be nice if there were a programmatic WDL writer but I haven't been able to find one
    if not job.job_input.inputs_are_S3File():
        raise ValueError("input files must be S3 files with the E-tag")
    workflow_name = job.image.normed_name.split("@")[0].translate(_IMAGE_TRANS_CHARS)
    file_to_rel_path = determine_file_locations(job.job_input)
    input_files = []
    relpaths = []
    environment = []
    cmdlines = []
    for files in job.job_input.get_files_per_container().files:
        ins = []
        rels = []
        for f in files:
            if f not in file_mapping:
                raise ValueError(f"file_mapping missing {f}")
            ins.append(str(file_mapping[f]))
            rels.append(shlex.quote(file_to_rel_path[f]))
        input_files.append(ins)
        relpaths.append(rels)
        cmd = [shlex.quote(c) for c in job.image.entrypoint]
        cmd.extend(_process_flag_args(job, files, file_to_rel_path))
        cmd.extend(_process_pos_args(job, files, file_to_rel_path))
        cmdlines.append(cmd)
        environment.append(_process_environment(job, files, file_to_rel_path))
    input_json = {
        f"{workflow_name}.input_files_list": input_files,
        f"{workflow_name}.file_locs_list": relpaths,
        f"{workflow_name}.environment_list": environment,
        f"{workflow_name}.cmdline_list": cmdlines
    }
    # Inserting the job ID into the WDL should not bust the Cromwell cache:
    # https://kbase.slack.com/archives/CGJDCR22D/p1729786486819809
    # https://cromwell.readthedocs.io/en/stable/cromwell_features/CallCaching/
    wdl = f"""
version {_WDL_VERSION}

# CTS_JOB_ID: {job.id}

workflow {workflow_name} {{
  input {{
      Array[Array[File]] input_files_list
      Array[Array[String]] file_locs_list
      Array[Array[String]] environment_list
      Array[Array[String]] cmdline_list
  }}
  scatter (i in range(length(input_files_list))) {{
    call run_container {{
      input:
        input_files = input_files_list[i],
        file_locs = file_locs_list[i],
        environ = environment_list[i],
        cmdline = cmdline_list[i]
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
    Array[File] input_files
    Array[String] file_locs
    Array[String] environ
    Array[string] cmdline
  }}
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./__output__
  
    # link the input files into the mount points
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
    docker: "{job.image.normed_name}"
    runtime_minutes: {math.ceil(job.job_input.runtime.total_seconds() / 60)}
    memory: "{job.job_input.memory} B"
    cpu: {job.job_input.cpus}
  }}
}}
"""
    # TODO WDL handle pos args
    # TODO WDL handle flag args
    # TODO WDL handle env args
    # TODO WDL handle mounting
    # TODO WDL handle refdata
    # TODO WDL handle file manifests
    # TODO WDL look through the model and design and see what else we're missing
    return JawsInput(wdl, input_json)


def _process_flag_args(job: Job, files: list[S3File], file_to_rel_path: dict[S3File, Path]
) -> list[str]:
    cmd = []
    if job.job_input.params.flag_args:
        for flag, p in job.job_input.params.flag_args.items():
            cmd.extend(_process_parameter(
                p, job, files, file_to_rel_path, as_list=True, flag=flag
            ))
    return cmd


def _process_pos_args(job: Job, files: list[S3File], file_to_rel_path: dict[S3File, Path]
) -> list[str]:
    cmd = []
    if job.job_input.params.positional_args:
        for p in job.job_input.params.positional_args:
            cmd.extend(_process_parameter(p, job, files, file_to_rel_path, as_list=True))
    return cmd


def _process_environment(job: Job, files: list[S3File], file_to_rel_path: dict[S3File, Path]
) -> list[str]:
    env = []
    if job.job_input.params.environment:
        for envkey, enval in job.job_input.params.environment.items():
            enval = _process_parameter(enval, job, files, file_to_rel_path)
            env.append(f'{envkey}={enval}')
    return env


def _process_parameter(
    param: str | Parameter,
    job: Job,
    files: list[S3File],
    file_to_rel_path: dict[S3File, Path],
    as_list: bool = False,  # flag space separated files imply as list
    flag: str = None,
) -> str | list[str]:
    if isinstance(param, Parameter):
        match param.type:
            case ParameterType.INPUT_FILES:
                match param.input_files_format:
                    case InputFilesFormat.COMMA_SEPARATED_LIST:
                        param = _join_files(files, ",", job, file_to_rel_path, flag)
                    case InputFilesFormat.SPACE_SEPARATED_LIST:
                        param = _join_files(files, None, job, file_to_rel_path, flag)
                    # TODO Repeated parameter
                    case _:
                        # should be impossible but make code future proof
                        raise ValueError(f"Unexpected input files format: {_}")
            case ParameterType.MANIFEST_FILE:
                pass # TODO manifest files
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


def _join_files(
    files: list[S3File],
    sep: str,
    job: Job,
    file_to_rel_path: dict[S3File, Path],
    flag: str,
) -> str | list[str]:
    imp = job.job_input.params.input_mount_point
    if sep:
        fileret = sep.join([os.path.join(imp, file_to_rel_path[f]) for f in files])
        if flag:
            if flag.endswith("="):
                fileret = shlex.quote(flag + fileret)
            else:
                fileret = [shlex.quote(flag), shlex.quote(fileret)]
        else:
            fileret = shlex.quote(fileret)
    else:
        if flag:
            if flag.endswith("="):
                # this case is a little weird
                fileret = [
                    shlex.quote(flag + os.path.join(imp, file_to_rel_path[files[0]]))
                ] + [shlex.quote(os.path.join(imp, file_to_rel_path[f])) for f in files[1:]]
            else: 
                fileret = [shlex.quote(flag)] + [
                    shlex.quote(os.path.join(imp, file_to_rel_path[f])) for f in files
                ]
        else:
            fileret = [shlex.quote(os.path.join(imp, file_to_rel_path[f])) for f in files]
    return fileret
