'''
A builder of Workflow Definition Language documents for the purposes of running CTS jobs with JAWS.
'''

# TODO TEST parse output with miniwdl or something for syntax checking
# TODO TEST write manual test for testing with JAWS

from pathlib import Path
import math
import shlex
from typing import NamedTuple, Any

from cdmtaskservice.argument_generator import ArgumentGenerator
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.jaws.constants import OUTPUT_FILES, OUTPUT_DIR, STDOUTS, STDERRS
from cdmtaskservice.models import Job, S3FileWithDataID, PATH_REGEX_COMPILED


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
    file_mapping: dict[S3FileWithDataID, Path],
    manifest_file_list: list[Path] = None,
    relative_refdata_path: Path = None,
) -> JawsInput:
    """
    Generate input for a JAWS run in the form of a WDL file and input.json file contents.
    
    job_input - the input for the job.
    file_mapping - a mapping of the input S3 files to their paths at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file. The JAWS
        site path segments may only contain alphanumerics, period, hyphen, and the underscore 
    manifest_file_list - A list of manifest paths at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
        The path segments may only contain alphanumerics, period, hyphen, and the underscore.
        Required if manifest files are specified in the job input.
        The manifest files will be mounted directly into the input mount point for the job,
        regardless of the path, and so must not collide with any other files in the input root.
    relative_refdata_path - the path to a directory containing reference data for the run at the
        JAWS site, relative to the JAWS refdata root path.
        This will be mounted into the container at the refdata mount point specified in the job.
    """
    # It'd be nice if there were a programmatic WDL writer but I haven't been able to find one
    # If the manifest file name / path changes that'll break the JAWS cache, need to think
    # about that
    # How often will jobs run with identical manifest files though? Maybe MD5 based caching?
    _not_falsy(job, "job")
    _not_falsy(file_mapping, "file_mapping")
    arggen = ArgumentGenerator(job, manifest_file_list=manifest_file_list)
    if relative_refdata_path and not job.get_refdata_mount_point():
        raise ValueError(
            "If a refdata path is supplied, a mount point for the job must be supplied"
        )
    for s3o, localpath in file_mapping.items():
        if not PATH_REGEX_COMPILED.fullmatch(str(localpath)):
            raise ValueError(f"Disallowed local path for S3 object {s3o}: {localpath}")
    workflow_name = job.image.name.translate(_IMAGE_TRANS_CHARS)
    input_files = []
    relpaths = []
    environment = []
    cmdlines = []
    for container_number in range(job.job_input.num_containers):
        ins = []
        rels = []
        arginfo = arggen.get_container_arguments(container_number)
        for f in arginfo.files:
            if f not in file_mapping:
                raise ValueError(f"file_mapping missing {f}")
            ins.append(shlex.quote(str(file_mapping[f])))
            rels.append(shlex.quote(str(arginfo.files[f])))
        input_files.append(ins)
        relpaths.append(rels)
        cmdlines.append(arginfo.args)
        environment.append(arginfo.env)
    input_json = {
        f"{workflow_name}.input_files_list": input_files,
        f"{workflow_name}.file_locs_list": relpaths,
        f"{workflow_name}.environment_list": environment,
        f"{workflow_name}.cmdline_list": cmdlines,
    }
    if manifest_file_list:
        input_json[f"{workflow_name}.manifest_list"] = [str(m) for m in manifest_file_list]

    wdl = _generate_wdl(job, workflow_name, bool(manifest_file_list), relative_refdata_path)
    return JawsInput(wdl, input_json)


def _generate_wdl(job: Job, workflow_name: str, manifests: bool, relative_refdata_path: Path):
    # Inserting the job ID into the WDL should not bust the Cromwell cache:
    # https://kbase.slack.com/archives/CGJDCR22D/p1729786486819809
    # https://cromwell.readthedocs.io/en/stable/cromwell_features/CallCaching/
    
    # hack for shifter compatibility
    image = job.image.name_with_digest.removeprefix("docker.io/")
    mani_wf_input = ""
    mani_call_input = ""
    mani_task_input = ""
    mani_link = ""
    refdata_mount = ""
    if manifests:
        mani_wf_input = """
      Array[File] manifest_list"""
        mani_call_input = """,
        manifest = manifest_list[i]"""
        mani_task_input = """
    File manifest"""
        mani_link = """
    # link any manifest file into the mount point
    if [[ -n "~{manifest}" ]]; then
        ln ~{manifest} ./__input__/$(basename ~{manifest})
    fi"""
    if relative_refdata_path:
        refdata_mount = f'''
    dynamic_refdata: "{relative_refdata_path}:{job.get_refdata_mount_point()}"'''

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
        input_files = input_files_list[i],
        file_locs = file_locs_list[i],
        environ = environment_list[i],
        cmdline = cmdline_list[i]{mani_call_input}
    }}
  }}
  output {{
    Array[Array[File]] {OUTPUT_FILES} = run_container.output_files
    Array[File] {STDOUTS} = run_container.stdout
    Array[File] {STDERRS} = run_container.stderr
  }}
}}

task run_container {{
  input {{
    Array[File] input_files
    Array[String] file_locs
    Array[String] environ
    Array[String] cmdline{mani_task_input}
  }}
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./{OUTPUT_DIR}
    {mani_link}
    
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
    find ./{OUTPUT_DIR} -type f > ./output_files.txt
    
    exit $EC
  >>>
  
  output {{
    Array[File] output_files = read_lines("output_files.txt")
    File stdout = "stdout"
    File stderr = "stderr"
  }}
  
  runtime {{
    docker: "{image}"
    runtime_minutes: {math.ceil(job.job_input.runtime.total_seconds() / 60)}
    memory: "{job.job_input.memory} B"
    cpu: {job.job_input.cpus}{refdata_mount}
    dynamic_input: "__input__:{job.job_input.params.input_mount_point}"
    dynamic_output: "__output__:{job.job_input.params.output_mount_point}"
  }}
}}
"""
