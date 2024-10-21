Adding some notes here around image rules

TODO incorporate into documentation when it exists

* Windows images are not supported
  * TODO add a check to get the platform and error out on windows
    * Can't actually find a windows image to test with that crane config works with
      * microsoft/windows
      * microsoft/windows:ltsc2019
      * mcr.microsoft.com/windows
      * mcr.microsoft.com/windows:ltsc2019
      * microsoft_windows
      * all failed to get config with various errors
* Requires /bin/bash
  * JAWS requirement
* Requires an entrypoint
  * TODO error out if there's no entrypoint.
* All output must be written to the mount point. Anything in the mount point when the job is
  done is considered output
* Do not write to anywhere except the output mount point
  * Shifter mounts FS as readonly
* Hardlinks do not work across mounts, e.g. you cannot hardlink someting from the input monnt
  to the output mount
  * I don't think this will ever be an issue, but if it is we could make a single mount with
    subdirs for input and output