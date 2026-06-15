"""
Manual helpers for interacting with the CTS HTCondor client.

Usage (from inside the cdm_task_service container, from /cts):

```
docker compose -f docker-compose-local.yaml exec -it cdm_task_service bash
```

Then, with the config already rendered by the entrypoint:

```
root@...:/cts# uv run ipython
In [1]: from test_manual.condor.helpers import make_condor_client
In [2]: import asyncio
In [3]: client = make_condor_client()
```
"""

import htcondor2

from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.condor.client import CondorClient


def make_condor_client(config_path: str = "./cdmtaskservice_config.toml") -> CondorClient:
    """Create a CondorClient using the rendered container config."""
    with open(config_path, "rb") as f:
        cfg = CDMTaskServiceConfig(f, "manual")
    collector = htcondor2.Collector(htcondor2.param["COLLECTOR_HOST"])
    schedd_ad = collector.locate(htcondor2.DaemonTypes.Schedd)
    schedd = htcondor2.Schedd(schedd_ad)
    return CondorClient(schedd, cfg.get_condor_client_config(), cfg.get_s3_config())
