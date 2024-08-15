from aiobotocore.session import get_session
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from utils import find_free_port

class MinioController:
    # ported from https://github.com/kbase/java_test_utilities/blob/develop/src/main/java/us/kbase/testutils/controllers/minio/MinioController.java
    
    def __init__(self, minioexe: Path, access_key: str, secret_key: str, root_temp_dir: Path):
        self.access_key = access_key
        self.secret_key = secret_key
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
    
    def destroy(self, delete_temp_files):
        # I suppose I could turn this into a context manager... meh
        self._proc.terminate()
        self._logfiledescriptor.close()
        if delete_temp_files:
            shutil.rmtree(self.tempdir, ignore_errors=True)
    
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
