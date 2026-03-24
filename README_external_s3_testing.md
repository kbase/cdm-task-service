# Testing Against an External S3-Compatible Store

**WARNING** - running the tests will completely wipe any bucket the admin user has access to.
DO NOT run this against an S3 instance that has data you care about

This branch (`dev-test_external`) modifies the S3 test suite so it can run against a real
external S3-compatible object store instead of a locally managed Minio process. This is
useful for validating compatibility with production-like infrastructure such as Ceph
RadosGW.

## How it works

The `MinioController` class (despite the name) no longer starts a local Minio process. It
connects to a pre-existing S3 endpoint whose coordinates are supplied via environment
variables. Similarly, the "bad user" fixture no longer creates a throwaway Minio user; it
reads credentials for a pre-existing IAM user from the environment.

## Prerequisites on the S3 system

Before running the tests you need two things set up on your S3 system:

### 1. Admin user (read/write, bucket creation)

The admin credentials go in `S3_ACCESS_KEY` / `S3_SECRET_KEY`. This user must be able to:

- Create and delete buckets
- Read and write objects
- Call IAM APIs (`put_user_policy`, `delete_user_policy`, `list_user_policies`, etc.)

**Ceph specifics:** See https://docs.ceph.com/en/latest/radosgw/account/#account-management for
how to create an account and root user for CEPH. The root user must be used for tests (note that
anything in the root user's account will be wiped out as noted above.

### 2. Unprivileged "bad user" (no default access)

This user is used to test that access-denial paths work correctly. It must:

- Exist as an IAM user
- Have **no** default permissions on any bucket — the tests apply and remove policies
  themselves during the relevant test cases

Supply its IAM username in `S3_BAD_USER_NAME` and its access key/secret in
`S3_BAD_USER_KEY` / `S3_BAD_USER_SECRET`.

**Ceph specifics:** Create the user and access key using the IAM API, for example with
`aiobotocore`, `boto3`, or with the instructions here:
https://docs.ceph.com/en/latest/radosgw/account/#account-root-example.
Do **not** use `radosgw-admin user create` as that creates users for the standard user system,
not the user account system that supports IAM policies.

## Environment variables

| Variable | Description |
|---|---|
| `S3_ACCESS_KEY` | Access key ID for the admin user |
| `S3_SECRET_KEY` | Secret access key for the admin user |
| `S3_LOCAL_PORT` | Port the S3 endpoint is listening on (used to construct `http://localhost:<port>`) |
| `S3_BAD_USER_KEY` | Access key ID for the unprivileged test user |
| `S3_BAD_USER_SECRET` | Secret access key for the unprivileged test user |
| `S3_BAD_USER_NAME` | IAM username of the unprivileged test user |

Example (bash):

```bash
export S3_ACCESS_KEY=myadminkey
export S3_SECRET_KEY=myadminsecret
export S3_LOCAL_PORT=9010
export S3_BAD_USER_KEY=mybadkey
export S3_BAD_USER_SECRET=mybadsecret
export S3_BAD_USER_NAME=baduser
```

## Running the tests

```bash
PYTHONPATH=. uv run pytest test/s3/s3_client_test.py test/s3/s3_remote_test.py
```

## Known limitations vs. the Minio-based suite

- The endpoint URL is always constructed as `http://localhost:<S3_LOCAL_PORT>`. If your
  S3 endpoint is on a remote host or uses HTTPS you will need to adjust
  `MinioController.host` in `test/controllers/minio.py`.
- The `mc` (Minio client) CLI is no longer used. Any test that previously relied on
  `minio.run_mc(...)` has been rewritten to use the `aiobotocore` IAM or S3 API instead.
- Some tests that assert on specific error message formats (e.g. presigned URL failure
  bodies) may fail if your S3 implementation returns a different XML structure than
  Minio. These are expected test-level failures and do not indicate a bug in the service
  code.  TODO check this
