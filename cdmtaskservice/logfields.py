"""
Names for fields for logging to keep logs consistent. Separators should be underscores.
"""

# Standard app fields. Don't use these fields elsewhere.
URL_PATH = "path"
HTTP_METHOD = "method"
X_FORWARDED_FOR = "x_forwarded_for"
X_REAL_IP = "x_real_ip"
IP_ADDRESS = "ip"
USER_AGENT = "user_agent"
USER = "user"
REQUEST_ID = "request_id"

# Fields to use in log extra dicts
JOB_ID = "job_id"
TRANS_ID = "trans_id"
REFDATA_ID = "refdata_id"
NERSC_STATUS = "nersc_status"
NERSC_TASK_ID = "nersc_task"
JAWS_RUN_ID = "jaws_id"
NEXT_ACTION_SEC = "next_action_sec"
REMOTE_ERROR = "remote_error"
REMOTE_TRACEBACK = "remote_trace"
FILE = "file"
CLUSTER = "cluster"
PAYLOAD = "payload"
