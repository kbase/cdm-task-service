'''
A builder of Workflow Definition Language documents for the purposes of running CTS jobs with JAWS.
'''

# Apparently there aren't any programmatic WDL builders in python? Parsers exist

# TODO TEST
# TODO TEST parse output with miniwdl or something for syntax checking
# TODO TEST manually test with JAWS
# TODO MOUNTING add mounting info when available

import math
import shlex
from typing import NamedTuple, Any

from cdmtaskservice.models import JobInput, S3File
from cdmtaskservice.dockerimages import NormedImageName


_WDL_VERSION = "1.0"  # Cromwell, and therefore JAWS, only supports 1.0

_IMAGE_TRANS_CHARS = str.maketrans({".": "_", "-": "_", "/": "_", ":": "_"})


JawsInput = NamedTuple("JawsInput", [("wdl", str), ("input_json", dict[str, Any])])
"""
Input to the JGI JAWS job runner.

wdl - the WDL file as a string.
input_json - the input.json file as a dict.
"""

def generate_wdl(
    # TODO ARGS this is a big argument list, probably want to rethink
    job_input: JobInput,
    image: NormedImageName,
    entrypoint: list[str],
    file_mapping: dict[S3File, str]
) -> JawsInput:
    """
    Generate input for a JAWS run in the form of a WDL file and input.json file contents.
    
    job_input - the input for the job.
    image - the image to run.
    entrypoint - the entrypoint list for the container
    file_mapping - a mapping of the input S3 files to their locations at the JAWS site.
        These can be absolute paths or relative to the location of the WDL file.
    """
    # It'd be nice if there were a programmatic WDL writer but I haven't been able to find one
    if not job_input.inputs_are_S3File():
        raise ValueError("input files must be S3 files with the E-tag")
    workflow_name = image.normedname.split("@")[0].translate(_IMAGE_TRANS_CHARS)
    ins = []
    for f in job_input.input_files:
        if f not in file_mapping:
            raise ValueError(f"file_mapping missing {f}")
        ins.append(file_mapping[f])
    fpc_tuple = job_input.get_files_per_container()
    fpc = fpc_tuple.files_per_container
    input_json = {f"{workflow_name}.input_files": [
            ins[i:i + fpc] for i in range(0, fpc * fpc_tuple.containers, fpc)
        ]
    }
    wdl = f"""
version {_WDL_VERSION}

workflow {workflow_name} {{
  input {{
      Array[Array[File]] input_files
  }}
  scatter (files in input_files) {{
    call run_container {{
      input:
        input_files = files
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
  }}
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./__output__
  
    # link the input files into the mount points
    for file in '~{{sep="' '" input_files}}'; do
        ln $file ./__input__/$(basename $file)
    done
      
    # run the command
    {" ".join([shlex.quote(e) for e in entrypoint])}
  
    # list the output of the command
    find ./__output__ -type f > ./output_files.txt
  >>>
  
  output {{
    Array[File] output_files = read_lines("output_files.txt")
    File stdout = "stdout"
    File stderr = "stderr"
  }}
  
  runtime {{
    docker: "{image.normedname}"
    runtime_minutes: {math.ceil(job_input.runtime.total_seconds() / 60)}
    memory: "{job_input.memory} B"
    cpu: {job_input.cpus}
  }}
}}
"""
    # TODO WDL handle renaming files to expected names
    # TODO WDL handle pos args
    # TODO WDL handle flag args
    # TODO WDL handle env args
    # TODO WDL handle mounting
    # TODO WDL handle refdata
    # TODO WDL handle file manifests
    # TODO WDL handle input_roots
    # TODO WDL look through the model and design and see what else we're missing
    return JawsInput(wdl, input_json)
