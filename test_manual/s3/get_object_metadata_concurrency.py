"""
Prints out time to run the metadata method with 1000 paths at various concurrency levels.

From the repo root, run as

PYTHONPATH=.:./test python test_manual/s3/get_object_metadata_concurrency.py
"""

import asyncio
import time

from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths

from controllers.minio import MinioController
from test_common import config


async def main():
    mc = MinioController(
        config.MINIO_EXE_PATH,
        config.MINIO_MC_EXE_PATH,
        "access_key",
        "secret_key",
        config.TEMP_DIR,
    )
    host = f"http://localhost:{mc.port}"
    try:
        await mc.create_bucket("file-bucket")
        paths = []
        objs = []
        print("writing files")
        for i in range(1000):
            i = str(10000 + i)  # keep the file size the same
            objs.append(await mc.upload_file(f"file-bucket/f{i}", i.encode()))
            paths.append(f"file-bucket/f{i}")
        print("done")
        paths = S3Paths(paths)

        s3c = await S3Client.create(host, "access_key", "secret_key")
        concur = (1, 10, 100, 1000)
        times = {}
        for c in reversed(concur):  # reverse to show it's not a warmup effect
            print(f"testing with concurrency of {c}")
            start = time.time()
            await s3c.get_object_meta(paths, concurrency=c)
            times[c] = time.time() - start
    finally:
        mc.destroy(not config.TEMP_DIR_KEEP)

    t0 = times[concur[0]]
    print("Concur\tTime\tImprovement")
    for c in concur:
        t = times[c]
        print(f"{c}\t{t:.5f}\t{t0 / t:.3f}%")


if __name__ == "__main__":
    asyncio.run(main())
