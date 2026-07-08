"""
Manual helpers for interacting with the CTS MongoDB instance.

Usage (from inside the cdm_task_service container, from /cts):

```
docker compose -f docker-compose-local.yaml exec -it cdm_task_service bash
```

Then, using the same KBCTS_MONGO_* env vars the service itself was started with:

```
root@...:/cts# uv run ipython
In [1]: from test_manual.mongo_helpers import make_mongo_client
In [2]: db = make_mongo_client()
In [3]: db.jobs.find_one()
```
"""

import os

from pymongo import MongoClient
from pymongo.database import Database


def make_mongo_client() -> Database:
    """Create a pymongo client from the container's Mongo env vars and return the CTS database."""
    db_name = os.environ.get("KBCTS_MONGO_DB", "cdmtaskservice")
    client = MongoClient(
        os.environ.get("KBCTS_MONGO_HOST", "mongodb://localhost:27017"),
        authSource=db_name,
        username=os.environ.get("KBCTS_MONGO_USER") or None,
        password=os.environ.get("KBCTS_MONGO_PWD") or None,
        retryWrites=os.environ.get("KBCTS_MONGO_RETRYWRITES", "").lower() == "true",
    )
    return client[db_name]
