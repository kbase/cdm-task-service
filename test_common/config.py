import configparser
from os import path as ospath
from pathlib import Path


TEST_CFG_LOC = Path(ospath.normpath((Path(__file__) / ".." / ".." / "test.cfg").absolute()))
TEST_CFG_SEC = "cdmtaskservicetest"

_cfg = configparser.ConfigParser()
_cfg.read(TEST_CFG_LOC)

if TEST_CFG_SEC not in _cfg.sections():
    raise ValueError(f"missing {TEST_CFG_SEC} section in test config file {TEST_CFG_LOC}")

TEST_CFG = dict(_cfg[TEST_CFG_SEC])
del _cfg

CRANE_EXE_PATH     = Path(TEST_CFG.get("test.crane.exe"))
MONGO_EXE_PATH     = Path(TEST_CFG.get("test.mongo.exe"))
MINIO_EXE_PATH     = Path(TEST_CFG.get("test.minio.exe"))
MINIO_MC_EXE_PATH  = Path(TEST_CFG.get("test.minio.mc.exe"))
KAFKA_DOCKER_IMAGE = TEST_CFG.get("test.kafka.docker.image")
TEMP_DIR           = Path(TEST_CFG.get("test.temp.dir"))
TEMP_DIR_KEEP      = TEST_CFG.get("test.temp.dir.keep") == "true"

# Don't use these in automated tests, would require putting creds into github which would be
# a huge pain since they expire frequently
SFAPI_CREDS_FILE_PATH = Path(TEST_CFG.get("test.sfapicreds.path"))
SFAPI_CREDS_USER = TEST_CFG.get("test.sfapicreds.user")
