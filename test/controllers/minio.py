from aiobotocore.session import get_session
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Any

from utils import find_free_port

class MinioController:
    # adapted from https://github.com/kbase/java_test_utilities/blob/develop/src/main/java/us/kbase/testutils/controllers/minio/MinioController.java
    
    def __init__(
        self,
        minioexe: Path,
        mcexe: Path,
        access_key: str,
        secret_key: str,
        root_temp_dir: Path,
        mc_alias: str = "minio_controller"
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self._mc = mcexe
        self.mc_alias = mc_alias
        root_temp_dir = root_temp_dir.absolute()
        root_temp_dir.mkdir(parents=True, exist_ok=True)
        tdir = tempfile.mkdtemp(dir=root_temp_dir, prefix="MinioController-")
        self.tempdir = root_temp_dir / tdir
        datadir = self.tempdir / "data"
        datadir.mkdir()
        
        self.port = find_free_port()
        self.host = f"http://localhost:{self.port}"
        
        logfile = self.tempdir / "minio_server.log"
        
        self._logfiledescriptor = open(logfile, "w")
        
        cmd = [
                minioexe,
                "server",
                "--compat",
                "--address", f'localhost:{self.port}',
                # --console-address doesn't seem to work on the 2021-4 Minio distro, maybe too old
                str(datadir)
        ]
        env = dict(os.environ)
        env["MINIO_ACCESS_KEY"] = access_key
        env["MINIO_SECRET_KEY"] = secret_key

        self._proc = subprocess.Popen(
            cmd,
            env=env,
            stderr=subprocess.STDOUT,
            stdout=self._logfiledescriptor,
        )
        time.sleep(0.5)  # wait for server to start up
        self.run_mc("alias", "set", self.mc_alias, self.host, self.access_key, self.secret_key)
    
    def destroy(self, delete_temp_files):
        # I suppose I could turn this into a context manager... meh
        self._proc.terminate()
        time.sleep(0.5)
        self._logfiledescriptor.close()
        if delete_temp_files:
            shutil.rmtree(self.tempdir)
    
    def get_client(self):
        return get_session().create_client(
            "s3",
            endpoint_url=self.host,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    async def clean(self):
        """ Note will only delete the first 1k objects in a bucket """
        async with self.get_client() as client:
            buckets = await client.list_buckets()
            for buk in buckets["Buckets"]:
                bucket = buk["Name"]
                objs = await client.list_objects(Bucket=bucket)
                await client.delete_objects(Bucket=bucket, Delete={
                    "Objects": [{"Key": o["Key"]} for o in objs["Contents"]]
                })
                await client.delete_bucket(Bucket=bucket)

    async def create_bucket(self, bucket):
        async with self.get_client() as client:
            await client.create_bucket(Bucket=bucket)

    async def upload_file(self, path, main_part, num_main_parts=1, last_part=None
    ) -> dict[str, Any]:
        async with self.get_client() as client:
            buk, key = path.split("/", 1)
            if num_main_parts == 1 and not last_part:
                return await client.put_object(
                    Body=io.BytesIO(main_part),
                    Bucket=buk,
                    Key=key,
                    ContentLength=len(main_part)
                )
            else:
                res = await client.create_multipart_upload(Bucket=buk, Key=key)
                uid = res["UploadId"]
                parts = []
                for part_num in range(1, num_main_parts + 1):
                    res = await upload_part(client, main_part, buk, key, uid, part_num)
                    parts.append({"ETag": res["ETag"], "PartNumber": part_num})
                if last_part:
                    res = await upload_part(client, last_part, buk, key, uid, num_main_parts + 1)
                    parts.append({"ETag": res["ETag"], "PartNumber": num_main_parts + 1})
                return await client.complete_multipart_upload(
                    Bucket=buk, Key=key, UploadId=uid, MultipartUpload={"Parts": parts}
                )

    def run_mc(self, *args):
        subprocess.run([self._mc] + list(args)).check_returncode()


async def upload_part(client, part, bucket, key, upload_id, part_num):
    return await client.upload_part(
        Body=io.BytesIO(part),
        Bucket=bucket,
        Key=key,
        ContentLength=len(part),
        UploadId=upload_id,
        PartNumber=part_num
    )


if __name__ == "__main__":
    mc = MinioController(
        shutil.which("minio"),  # must be on the path
        "access_key",
        "secret_key",
        Path("minio_temp_dir")
    )
    print("port: " + str(mc.port))
    inp = input("Type 'd' to delete temp files, anything else to leave them")
    print(f"got [{inp}]")
    mc.destroy(inp == "d")
