#!/usr/bin/env python3

"""
Script used to check signal handling in canceled HTC jobs. Example usage:

* Shell into the cdm task service container (the local docker file is a copy of the docker file
  in this repo with missing values like tokens, etc. filled in)

```
docker compose -f docker-compose-local.yaml exec -it cdm_task_service bash
```

* Launch and cancel a job (you can make sure the job is running before canceling by checking
  condor_q or for a new signal_test.log file in htcondor, see below):

```
root@b7066d64c6a4:/cts# uv run ipython
In [12]: collector = htcondor2.Collector(htcondor2.param["COLLECTOR_HOST"])
    ...: schedd_ad = collector.locate(htcondor2.DaemonTypes.Schedd)
    ...: schedd = htcondor2.Schedd(schedd_ad)

In [24]: jobres = schedd.submit(htcondor2.Submit({
    ...:     "shell": "/signal_test.sh",
    ...:     "initialdir": "/condor_workdir",
    ...:     "output": "sigtest.out",
    ...:     "error": "sigtest.err",
    ...:     "log": "sigtest.log",
    ...: }))

In [25]: schedd.act(htcondor2.JobAction.Remove, f"ClusterId == {jobres.cluster()
       ⋮ }")
Out[25]: [ TotalError = 0; TotalJobAds = 1; TotalSuccess = 1; TotalNotFound = 0;
TotalBadStatus = 0; TotalChangedAds = 1; TotalAlreadyDone = 0; TotalPermissionDenied = 0 ]
```

* The job submit dictionary can be modified to test running the python script directly,
  using executable vs. shell, etc.
  
* Shell into the htcondor container:

```
docker compose -f docker-compose-local.yaml exec -it htcondor-mini bash
```

* Inspect the logs:

```
[root@56f0ff14c8de /]# cat /condor_workdir/signal_test.log 
PID: 692
Signal 15: SIGTERM
```
"""


import os
import sys
import signal
from time import sleep

LOG_TO = "/condor_workdir/signal_test.log"

def handler(sig, frame):
    print(f"Received signal {sig} ({signal.Signals(sig).name})")
    with open(LOG_TO, "a") as f:
        f.write(f"Signal {sig}: {signal.Signals(sig).name}\n")
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)

pid = os.getpid()
print(f"PID: {pid}")
with open(LOG_TO, "w") as f:
    f.write(f"PID: {pid}\n")

delay = 600
if len(sys.argv) == 2:
    delay = int(sys.argv[1])

sleep(delay)

sys.exit(1)
