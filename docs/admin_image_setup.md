# Setting up a CTS image as an admin

1. Get the image name and, optionally, refdata location in S3 from the user
   * The S3 location, if provided, must be readable by the CTS
   * If the refdata needs to be unpacked, the user must specify as such
   * The image must:
     * be public
     * not require root
     * have /bin/bash
     * have an entrypoint specified
     * place all output in a specific folder
       * the CTS considers all contents in the specified output folder as app output
     * not write to anywhere except the output folder and the temporary directory
     * not hardlink between input and output directories
     * Windows images are not supported
2. Look over the image and refdata, if applicable
   * check for any potential malicious code / files
   * check that an image run is reproducible
     * e.g. if it's downloading files from elsewhere during the run, it is likely
       not reproducible
3. If refdata is necessary,
   [register it with the service](https://ci.kbase.us/services/cts/docs#/Admin/create_refdata_admin_refdata__refdata_s3_path__post).
4. [Register the image](https://ci.kbase.us/services/cts/docs#/Admin/approve_image_admin_images__image_id__post).
   * If refdata is necessary, include the refdata ID from step 3.
   * It's helpful to include the tag in the image name for documentation purposes
   * It may be useful to include the image digest SHA to ensure the image hasn't changed
    since the image was tested.
