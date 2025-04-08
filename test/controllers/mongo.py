"""
A controller for MongoDB useful for running tests. Only compatible with Mongo 6.1.0+

Production use is not recommended.
"""

from pathlib import Path
from pymongo.mongo_client import MongoClient
import tempfile
import subprocess
import time
import shutil

from utils import find_free_port
from test_common import config


class MongoController:

    def __init__(self, mongoexe: Path, root_temp_dir: Path):

        # make temp dirs
        root_temp_dir = root_temp_dir.absolute()
        root_temp_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = Path(tempfile.mkdtemp(dir=root_temp_dir, prefix="MongoController-"))
        data_dir = self.temp_dir / "data"
        data_dir.mkdir()

        self.port = find_free_port()

        command = [str(mongoexe), "--port", str(self.port), "--dbpath", str(data_dir)]

        self._outfile = open(self.temp_dir / "mongo.log", "w")

        self._proc = subprocess.Popen(command, stdout=self._outfile, stderr=subprocess.STDOUT)
        time.sleep(1)  # wait for server to start up

        try:
            self.client: MongoClient = MongoClient('localhost', self.port)
            # This line will raise an exception if the server is down
            self.client.server_info()
        except Exception as e:
            raise ValueError("MongoDB server is down") from e

    def destroy(self, delete_temp_files: bool) -> None:
        if self.client:
            self.client.close()
        if self._proc:
            self._proc.terminate()
        if self._outfile:
            self._outfile.close()
        if delete_temp_files and self.temp_dir:
            shutil.rmtree(self.temp_dir)

    def clear_database(self, db_name, drop_indexes=False):
        if drop_indexes:
            self.client.drop_database(db_name)
        else:
            db = self.client[db_name]
            for name in db.list_collection_names():
                if not name.startswith("system."):
                    # don't drop collection since that drops indexes
                    db.get_collection(name).delete_many({})


def main():
    mongoexe = config.MONGO_EXE_PATH
    tempdir = config.TEMP_DIR

    mc = MongoController(mongoexe, tempdir)
    print("port: " + str(mc.port))
    print("temp_dir: " + str(mc.temp_dir))
    mc.client["foo"]["bar"].insert_one({"foo": "bar"})
    mc.clear_database("foo", drop_indexes=True)
    input("press enter to shut down")
    mc.destroy(True)


if __name__ == "__main__":
    main()
