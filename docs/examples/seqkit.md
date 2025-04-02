# SeqKit example

This example uses a docker container for the SeqKit application built from scratch.
The Dockerfile and other related files are available
[here](https://github.com/kbasetest/cdm_seqkit).

## References

https://github.com/shenwei356/seqkit  
https://github.com/kbasetest/cdm_seqkit  

## Nomenclature

* SK - SeqKit
* CTS - CDM task service
* CDM - the KBase Central Data Model
    
## Running the image

Prior to running the image it must be
[registered in the CTS by an admin](../admin_image_setup.md).

The image can be run using the
[OpenAPI UI](https://ci.kbase.us/services/cts/docs#/Jobs/submit_job_jobs__post)
or any software capable of making HTTP requests, like `curl` or Python's `request` library.
Note that the OpenAPI UI renderer may crash your browser for jobs with large number of input
or output files.

Here we show an example using `requests` in an `ipython` terminal.

```python
In [2]: import requests

In [3]: res = requests.post(
   ...:     "https://ci.kbase.us/services/cts/jobs/",
   ...:     headers={"Authorization": f"Bearer {token_ci}"},
   ...:     json={
   ...:         "cluster": "perlmutter-jaws",
   ...:         "image": "ghcr.io/kbasetest/cdm_seqkit:0.1.0",
   ...:         "params": {
   ...:             "input_mount_point": "/in",
   ...:             "output_mount_point": "/out",
   ...:             "args": [
   ...:                 "stats",
   ...:                 "--tabular",
   ...:                 "-o", "/out/stats.txt",
   ...:                 {
   ...:                     "type": "input_files",
   ...:                     "input_files_format": "space_separated_list",
   ...:                 },
   ...:             ],
   ...:         },
   ...:         "input_files":  [
   ...:             "test-bucket/collections_image_test/NONE/CDM/FastGenomics/G
   ...: CA_000008085.1/GCA_000008085.1_ASM808v1_genomic.fna.gz",
   ...:             "test-bucket/collections_image_test/NONE/CDM/FastGenomics/G
   ...: CA_000010565.1/GCA_000010565.1_ASM1056v1_genomic.fna.gz",
   ...:             "test-bucket/collections_image_test/NONE/CDM/FastGenomics/G
   ...: CA_000145985.1/GCA_000145985.1_ASM14598v1_genomic.fna.gz",
   ...:             "test-bucket/collections_image_test/NONE/CDM/FastGenomics/G
   ...: CA_000147015.1/GCA_000147015.1_ASM14701v1_genomic.fna.gz",
   ...:         ],
   ...:         "output_dir": "cts-output/seqkit_test_out",
   ...:         "runtime": "PT5M",
   ...:         "cpus": 1,
   ...:         "num_containers": 1,
   ...:         "memory": "1GB",
   ...:     }
   ...: )

In [4]: res.json()
Out[4]: {'job_id': '8e60bfcd-bbce-4c18-b65b-9f2d95dc804e'}
```

* `token_ci` is your KBase CI environment token. 
* `https://ci.kbase.us/services/cts/jobs/` is the url for the job submission endpoint for
  the CTS in the KBase CI environment.
* Both of the above values will change for different KBase environments.
* The fields in the input JSON are described in the service OpenAPI documentation.
* For the `image` field, it is probably wise to use the SHA256 image digest rather than the
  tag to ensure the correct image is used.
* In `args`:
  * The dictionary means something special - `"type": "input_files"` tells the CTS to
    insert the files into the command line as a `space_separated_list`.
  * In this case we process all the files with one container so that we get a single statistics
    file containing data for all the input files. In some cases it might make more sense to
    split the processing between multiple containers and merge the output later; for instance in
    the case where processing takes a great deal of time and the user wants to parallelize the
    computation.
    * Also note that if multiple containers all write to `/out/stats.txt`, they will
      clobber each other's output.
* `input_files` contains the S3 paths of the files to be provided to the containers. Rather
  than a literal string, each file may also be represented as a dictionary with the file path
  and a CRC64/NVME checksum of the file to ensure the file doesn't change between the last time
  it was checked and job creation. See the OpenAPI documentation for details.
* `output_dir` determines where the SK output files will be placed in S3.
* The job ID can be used to track progress of the job via the service API or OpenAPI UI.
