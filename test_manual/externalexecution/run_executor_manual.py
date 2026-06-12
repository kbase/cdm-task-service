"""
Sets up test data in Minio + MongoDB and runs the CTS external executor locally.
Assumes docker-compose-local.yaml is up (CTS at :5000, Minio at :9000, Mongo at :27017).

Usage:
    PYTHONPATH=. python test_manual/externalexecution/run_executor_manual.py --token MY_TOKEN

The container image will sleep for --sleep seconds. While it is running, send SIGTERM or
Ctrl-C to test that signal handling stops the container and exits cleanly.
"""

import argparse
import asyncio
import datetime
import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
import motor.motor_asyncio

from cdmtaskservice import models, sites
from cdmtaskservice.externalexecution.container_runner import ContainerCreator
from cdmtaskservice.externalexecution.executor import run_executor
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.timestamp import utcdatetime

_IMAGE = "ghcr.io/kbasetest/cts_test_image"
_IMAGE_TAG = "0.1.10-nonroot"
_IMAGE_DIGEST = "sha256:4c65668b647f50dfac587f957d2fe7e6cc717a0491aab34fc10296dd00de0eb2"
_IMAGE_ENTRYPOINT = ["python", "/opt/tester.py"]
_TEST_FILE_CONTENT = b"CTS executor signal-handling test input file\n"
_TEST_FILE_CRC = "cZWeeOsnTSo="  # CRC64NVME of _TEST_FILE_CONTENT
_INPUT_BUCKET = "cts-test-input"
_OUTPUT_BUCKET = "cts-test-output"
_ERROR_LOG_PATH = "cts-test-errors/container_logs"


def _parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--token", default=os.environ.get("KBASE_AUTH_TOKEN"),
                   help="KBase CI token with the external executor role "
                        "(default: $KBASE_AUTH_TOKEN)")
    p.add_argument("--sleep", type=int, default=30,
                   help="Seconds for the container to sleep (default: 30)")
    p.add_argument("--s3-url", default="http://localhost:9000", help="Minio URL")
    p.add_argument("--s3-key", default="miniouser", help="S3 access key")
    p.add_argument("--s3-secret", default="miniopassword", help="S3 access secret")
    p.add_argument("--mongo-url", default="mongodb://localhost:27017")
    p.add_argument("--mongo-db", default="cdmtaskservice")
    p.add_argument("--cts-url", default="http://localhost:5000", help="CTS service URL")
    return p.parse_args()


def _setup_s3(args) -> str:
    """Create buckets and upload the test input file. Returns the s3_path."""
    s3 = boto3.client(
        "s3",
        endpoint_url=args.s3_url,
        aws_access_key_id=args.s3_key,
        aws_secret_access_key=args.s3_secret,
        config=BotoConfig(signature_version="s3v4"),
    )

    for bucket in [_INPUT_BUCKET, _OUTPUT_BUCKET, _ERROR_LOG_PATH.split("/")[0]]:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"  created bucket: {bucket}")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print(f"  bucket exists: {bucket}")
            else:
                raise

    s3_key = "test-input/test_file.txt"
    s3.put_object(Bucket=_INPUT_BUCKET, Key=s3_key, Body=_TEST_FILE_CONTENT)
    s3_path = f"{_INPUT_BUCKET}/{s3_key}"
    print(f"  uploaded test file -> {s3_path}")
    return s3_path


async def _setup_mongo(args, s3_path: str, sleep_secs: int) -> str:
    """Insert a job + subjob into MongoDB. Returns the job_id."""
    client = motor.motor_asyncio.AsyncIOMotorClient(args.mongo_url)
    db = client[args.mongo_db]
    dao = await MongoDAO.create(db)

    job_id = str(uuid.uuid4())
    now = utcdatetime()

    job = models.AdminJobDetails.model_construct(
        id=job_id,
        state=models.JobState.DOWNLOAD_SUBMITTED,
        user="testuser",
        admin_meta={},
        cleaned=False,
        input_file_count=1,
        transition_times=[
            models.AdminJobStateTransition.model_construct(
                state=models.JobState.CREATED,
                time=now,
                trans_id=str(uuid.uuid4()),
                notif_sent=False,
            ),
            models.AdminJobStateTransition.model_construct(
                state=models.JobState.DOWNLOAD_SUBMITTED,
                time=now,
                trans_id=str(uuid.uuid4()),
                notif_sent=False,
            ),
        ],
        image=models.JobImage.model_construct(
            name=_IMAGE,
            digest=_IMAGE_DIGEST,
            tag=_IMAGE_TAG,
            entrypoint=_IMAGE_ENTRYPOINT,
            registered_by="testuser",
            registered_on=now,
        ),
        job_input=models.JobInput.model_construct(
            cluster=sites.Cluster.KBASE,
            image=f"{_IMAGE}@{_IMAGE_DIGEST}",
            params=models.Parameters.model_construct(
                input_mount_point="/input_files",
                output_mount_point="/output_files",
                declobber=False,
                args=["-s", str(sleep_secs)],
                environment=None,
                refdata_mount_point=None,
            ),
            num_containers=1,
            cpus=1,
            memory=10_000_000,
            runtime=datetime.timedelta(seconds=3600),
            output_dir=f"{_OUTPUT_BUCKET}/output/",
            input_files=[
                models.S3FileWithDataID.model_construct(file=s3_path, crc64nvme=_TEST_FILE_CRC)
            ],
            input_roots=None,
        ),
    )

    subjob = models.SubJob.model_construct(
        id=job_id,
        sub_id=0,
        state=models.JobState.DOWNLOAD_SUBMITTED,
        transition_times=[
            models.JobStateTransition.model_construct(
                state=models.JobState.DOWNLOAD_SUBMITTED,
                time=now,
            ),
        ],
    )

    await dao.save_job(job)
    await dao.initialize_subjobs([subjob])
    client.close()
    print(f"  job_id: {job_id}")
    return job_id


async def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    if not args.token:
        sys.exit("error: --token is required or set KBASE_AUTH_TOKEN")

    print("Setting up S3...")
    s3_path = _setup_s3(args)

    print("Setting up MongoDB...")
    job_id = await _setup_mongo(args, s3_path, args.sleep)

    refdata_dir = Path(tempfile.gettempdir()) / "cts-test-refdata"
    refdata_dir.mkdir(exist_ok=True)

    os.environ.update({
        "JOB_ID": job_id,
        "CONTAINER_NUMBER": "0",
        "SERVICE_ROOT_URL": args.cts_url,
        "TOKEN": args.token,
        "S3_URL": args.s3_url,
        "S3_ACCESS_KEY": args.s3_key,
        "S3_SECRET": args.s3_secret,
        "S3_ERROR_LOG_PATH": _ERROR_LOG_PATH,
        "S3_INSECURE": "true",
        "REFDATA_HOST_PATH": str(refdata_dir),
        "JOB_UPDATE_TIMEOUT_MIN": "5",
        "HEARTBEAT_INTERVAL_MIN": "1",
    })

    print(f"\nRunning executor (container will sleep {args.sleep}s)...")
    print("Send SIGTERM or Ctrl-C while sleeping to test signal handling.\n")

    exit_code = await run_executor(sys.stderr, ContainerCreator())
    print(f"\nExecutor exited with code {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
