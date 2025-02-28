# KBase collections example

This example uses the Docker container that is produced by the KBase Collections project.

## References

https://github.com/kbase/collections/tree/develop/src/loaders/compute_tools/checkm2  
https://github.com/chklovski/CheckM2  

## Nomenclature

* C2 - CheckM2
* CTS - CDM task service
* CDM - the KBase Central Data Model
    
## Running the image

Prior to running the image it must be
[registered in the CTS by an admin](../admin_image_setup.md). C2 requires refdata - see the
C2 instructions for how to obtain it.

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
   ...:         "image": "ghcr.io/kbase/collections:checkm2_0.1.6",
   ...:         "params": {
   ...:             "input_mount_point": "/collectionssource",
   ...:             "output_mount_point": "/collectionsdata",
   ...:             "refdata_mount_point": "/reference_data",
   ...:             "environment": {
   ...:                 "DATA_ID_FILE":
   ...:                     {
   ...:                         "type": "manifest_file",
   ...:                         "manifest_file_format": "data_ids",
   ...:                         "manifest_file_header": "genome_id",
   ...:                     },
   ...:                 "ENV": "NONE",
   ...:                 "KBASE_COLLECTION": "CDM",
   ...:                 "SOURCE_VER": "FastGenomics",
   ...:                 "LOAD_VER": "FastGenomics.1",
   ...:                 "ROOT_DIR": "/",
   ...:                 "JOB_ID": {"type": "container_number"},
   ...:                 "THREADS_PER_TOOL_RUN": "4",
   ...:                 "SOURCE_FILE_EXT": ".fna.gz",
   ...:              },
   ...:         },
   ...:         "input_files": [
   ...:             {
   ...:                 'file': 'test-bucket/collections_image_test/NONE/CDM/Fas
   ...: tGenomics/GCA_000008085.1/GCA_000008085.1_ASM808v1_genomic.fna.gz',
   ...:                 'data_id': 'GCA_000008085.1'
   ...:             },
   ...:             {
   ...:                 'file': 'test-bucket/collections_image_test/NONE/CDM/Fas
   ...: tGenomics/GCA_000010565.1/GCA_000010565.1_ASM1056v1_genomic.fna.gz',
   ...:                 'data_id': 'GCA_000010565.1'
   ...:             },
   ...:             {
   ...:                 'file': 'test-bucket/collections_image_test/NONE/CDM/Fas
   ...: tGenomics/GCA_000145985.1/GCA_000145985.1_ASM14598v1_genomic.fna.gz',
   ...:                 'data_id': 'GCA_000145985.1'
   ...:             },
   ...:             {
   ...:                 'file': 'test-bucket/collections_image_test/NONE/CDM/Fas
   ...: tGenomics/GCA_000147015.1/GCA_000147015.1_ASM14701v1_genomic.fna.gz',
   ...:                 'data_id': 'GCA_000147015.1'
   ...:             }
   ...:         ],
   ...:         "input_roots": ["test-bucket/collections_image_test"],
   ...:         "output_dir": "cts-output/collections_image_test_out",
   ...:         "runtime": "PT600S",
   ...:         "cpus": 4,
   ...:         "num_containers": 3,
   ...:         "memory": "100GB",
   ...: })

In [4]: res.json()
Out[4]: {'job_id': '9d47d408-85c3-4e64-9020-38b13ed8dc56'}
```

* `token_ci` is your KBase CI environment token. 
* `https://ci.kbase.us/services/cts/jobs/` is the url for the job submission endpoint for
  the CTS in the KBase CI environment.
* Both of the above values will change for different KBase environments.
* The fields in the input JSON are described in the service OpenAPI documentation.
* The Collections C2 image has very specific requirements for where files are located and how
  those locations are specified in the environment variables. The mount points, environment
  variables, and S3 file paths are set up to meet those requirements. Please see the Collections
  documentation for details, but regarding CTS specific configurations:
    * The reference data mount point, `/reference_data`, is where the image expects
      to find reference data. As such, we do not need to specify it in the input
      arguments or environment.
    * The image expects to be provided with a manifest file that consists of a single
      column of data IDs with the header `genome_id`. The dictionary provided in the
      environment variable `DATA_ID_FILE` along with the data IDs in the `input_files`
      section of the input satisfies this requirement. The dictionary instructs the
      CTS to prepare a manifest file of inputs for each container, splitting the
      input files among the specified 3 containers (note in production hundreds or
      thousands of files per container would be expected).
      It adds the data IDs for the input files for the container to the manifest
      under the `manifest_file_header` header. See the OpenAPI documentation for the
      various types of file handling and formats available in the CTS.
    * The dictionary provided in the `JOB_ID` environment variable instructs the CTS
      to provide the container number to each container in that environment variable.
      The C2 image uses that environment variable to prevent containers from clobbering
      each others' output.
    * As previously mentioned, the C2 image expects a very specific directory structure
      for the inputs. The `input_roots` field instructs the CTS to strip off
      the `test-bucket/collections_image_test` prefix for each file, which the image
      does not expect.
* For the `image` field, it is probably wise to use the SHA256 image digest rather than the
  tag to ensure the correct image is used.
* `input_files` file entries may may also contain CRC64/NVME checksum of the file to ensure
  the file doesn't change between the last time it was checked and job creation.
  See the OpenAPI documentation for details.
* `output_dir` determines where the C2 output files will be placed in S3.
* The job ID can be used to track progress of the job via the service API or OpenAPI UI.
