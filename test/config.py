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

CRANE_EXE_PATH = Path(TEST_CFG.get("test.crane.exe"))
MINIO_EXE_PATH = Path(TEST_CFG.get("test.minio.exe"))
TEMP_DIR       = Path(TEST_CFG.get("test.temp.dir"))
TEMP_DIR_KEEP  = TEST_CFG.get("test.temp.dir.keep") == "true"
