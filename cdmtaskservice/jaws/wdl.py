'''
A builder of Workflow Definition Language documents for the purposes of running CTS jobs with JAWS.
'''

# TODO TEST
# TODO TEST parse output with miniwdl or something for syntax checking
# TODO TEST manually test with JAWS
# TODO MOUNTING add mounting info when available

import math
import shlex
from typing import NamedTuple, Any

from cdmtaskservice.models import Job, S3File
from cdmtaskservice.input_file_locations import determine_file_locations


_WDL_VERSION = "1.0"  # Cromwell, and therefore JAWS, only supports 1.0

_IMAGE_TRANS_CHARS = str.maketrans({".": "_", "-": "_", "/": "_", ":": "_"})


JawsInput = NamedTuple("JawsInput", [("wdl", str), ("input_json", dict[str, Any])])
"""
Input to the JGI JAWS job runner.

wdl - the WDL file as a string.
input_json - the input.json file as a dict.
"""

def generate_wdl(job: Job, file_mapping: dict[S3File, str]) -> JawsInput:
    """
    Generate input for a JAWS run in the form of a WDL file and input.json file contents.
    
    job_input - the input for the job.
    file_mapping - a mapping of the input S3 files to their locations at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
    """
    # It'd be nice if there were a programmatic WDL writer but I haven't been able to find one
    if not job.job_input.inputs_are_S3File():
        raise ValueError("input files must be S3 files with the E-tag")
    workflow_name = job.image.normed_name.split("@")[0].translate(_IMAGE_TRANS_CHARS)
    file_to_rel_path = determine_file_locations(job.job_input)
    ins = []
    relpaths = []
    for f in job.job_input.input_files:
        if f not in file_mapping:
            raise ValueError(f"file_mapping missing {f}")
        ins.append(file_mapping[f])
        relpaths.append(file_to_rel_path[f])
    fpc_tuple = job.job_input.get_files_per_container()
    fpc = fpc_tuple.files_per_container
    input_json = {
        f"{workflow_name}.input_files_list": [
            ins[i:i + fpc] for i in range(0, fpc * fpc_tuple.containers, fpc)
        ],
        f"{workflow_name}.file_locs_list": [
            relpaths[i:i + fpc] for i in range(0, fpc * fpc_tuple.containers, fpc)
        ]
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
  }}
  scatter (i in range(length(input_files_list))) {{
    call run_container {{
      input:
        input_files = input_files_list[i],
        file_locs = file_locs_list[i]
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
    Array[File] file_locs
  }}
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./__output__
  
    # link the input files into the mount points
    files=('~{{sep="' '" input_files}}')
    locs=('~{{sep="' '" file_locs}}') 
    for i in "${{!files[@]}}"; do
        ln "$files[i]" ./__input__/"$locs[i]"
    done
      
    # run the command
    {" ".join([shlex.quote(e) for e in job.image.entrypoint])}
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
