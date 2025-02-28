# GeNomad example

This example uses the Docker container provided by the GeNomad authors.

Since GeNomad can only process a single file at a time, for production use a custom docker
container should be created that processes more than one file, as creating hundreds or
thousands of Docker containers would create significant overhead.

Also note that the reference data supplied by GeNomad includes softlinks, which are not compatible
with JAWS's reference data transfer system. If the CTS is upgraded to work with other JAWS
sites they will need to be converted to hard links. Additionally, all the reference data files
are inside a directory in the archive file - it would be slighly more user friendly if they
were in the top level directory.

## References

https://github.com/apcamargo/genomad  
https://hub.docker.com/r/antoniopcamargo/genomad  

## Nomenclature

* GN - GeNomad
* CTS - CDM task service
* CDM - the KBase Central Data Model
    
## Running the image

Prior to running the image it must be
[registered in the CTS by an admin](../admin_image_setup.md). GN requires refdata - see the
GN instructions for how to obtain it.

The image can be run using the [OpenAPI UI](https://ci.kbase.us/services/cts/docs#/Jobs/submit_job_jobs__post)
or any software capable of making HTTP requests, like `curl` or Python's `request` library.

Here we show an example using `requests` in an `ipython` terminal.

```python
In [2]: import requests

In [3]: res = requests.post(
   ...:     "https://ci.kbase.us/services/cts/jobs/",
   ...:     headers={"Authorization": f"Bearer {token_ci}"},
   ...:     json={
   ...:         "cluster": "perlmutter-jaws",
   ...:         "image": "docker.io/antoniopcamargo/genomad:1.8.1",
   ...:         "params": {
   ...:             "input_mount_point": "/app",
   ...:             "output_mount_point": "/out",
   ...:             "refdata_mount_point": "/ref_data",
   ...:             "args": [
   ...:                 "end-to-end",
   ...:                 "--cleanup",
   ...:                 {
   ...:                     "type": "input_files",
   ...:                     "input_files_format": "comma_separated_list",
   ...:                 },
   ...:                 "/out",
   ...:                 "/ref_data/genomad_db",
   ...:             ],
   ...:         },
   ...:         "input_files":  [
   ...:             "test-bucket/GCA_000008085.1_ASM808v1_genomic.fna.gz",
   ...:             "test-bucket/GCA_000010565.1_ASM1056v1_genomic.fna.gz",
   ...:             "test-bucket/GCA_000145985.1_ASM14598v1_genomic.fna.gz",
   ...:             "test-bucket/GCA_000147015.1_ASM14701v1_genomic.fna.gz",
   ...:         ],
   ...:         "output_dir": "cts-output/genomad_test_out",
   ...:         "runtime": "PT5M",
   ...:         "cpus": 1,
   ...:         "num_containers": 4,
   ...:         "memory": "30GB",
   ...:     }
   ...: )

In [4]: res.json()
Out[4]: {'job_id': '490f83be-e880-4457-8647-8f9a55c56c54'}
```

* `token_ci` is your KBase CI environment token. 
* `https://ci.kbase.us/services/cts/jobs/` is the url for the job submission endpoint for
  the CTS in the KBase CI environment.
* Both of the above values will change for different KBase environments.
* The fields in the input JSON are described in the service OpenAPI documentation.
* For the `image` field, it is probably wise to use the SHA256 image digest rather than the
  tag to ensure the correct image is used.
* The input mount point is set to `/app` since that is where the GN image expects input
  files to reside.
* The output mount point is arbitrarily set to `/out`, which is then used in the GN command line
  (see `args`)
* In `args`:
  * The string arguments are passed as literals to the GN entrypoint.
  * `--cleanup` is extremely important otherwise GN leaves a large number of temporary files
    in the results to be uploaded to S3.
  * The dictionary means something special - `"type": "input_files"` tells the CTS to
    split the files between containers.
    * In this case since there are 4 containers specified  (in `num_containers`)
      and 4 files provided, each container will only get a single file.
    * This is because the GN image can only process a single file per invocation.
    * As such, specifying the input file format (in this case comma separated) has
      no effect.
    * There are various file processing types and formats - see the OpenAPI
      documentation for a full list.
  * The reference data location is set to `/ref_data/genomad_db`.
    * `/ref_data` is set arbitrarily by `refdata_mount_point`.
    * `genomad_db` is coming from the structure of the GN supplied reference data
      archive, which internally stores all the reference data in that directory.
* `output_dir` determines where the GN output files will be placed in S3.
* The job ID can be used to track progress of the job via the service API or OpenAPI UI.
   