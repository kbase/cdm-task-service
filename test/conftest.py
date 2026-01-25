'''
Configure pytest fixtures and helper functions for this directory.
'''

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pytest
import time
import traceback

from controllers.minio import MinioController
from controllers.mongo import MongoController
from controllers.kafka_controller import KafkaController
from test_common import config


MONGO_TEST_DB = "testing"


def assert_exception_correct(got: Exception, expected: Exception, print_traceback=False):
    if print_traceback:
        print("".join(traceback.TracebackException.from_exception(got).format()))
    assert got.args == expected.args
    assert type(got) == type(expected)


def assert_close_to_now_sec(seconds: int):
    t = time.time()
    assert seconds < t + 1
    assert seconds > t - 1


@pytest.fixture(scope="module")
def mongo_serv() -> MongoController:
    mc = MongoController(config.MONGO_EXE_PATH, config.TEMP_DIR)
    
    yield mc
    
    mc.destroy(not config.TEMP_DIR_KEEP)


@pytest.fixture
def mongo(mongo_serv) -> MongoController:
    mongo_serv.clear_database(MONGO_TEST_DB, drop_indexes=True)
    
    yield mongo_serv


@pytest.fixture()
def mondb(mongo) -> AsyncIOMotorDatabase:
    mcli = AsyncIOMotorClient(f"mongodb://localhost:{mongo.port}", tz_aware=True)

    yield mcli[MONGO_TEST_DB]

    mcli.close()
    time.sleep(1)  # causes mongo connect failures after 32 tests otherwise


@pytest.fixture(scope="module")
def minio() -> MinioController:
    mc = MinioController(
        config.MINIO_EXE_PATH,
        config.MINIO_MC_EXE_PATH,
        "access_key",
        "secret_key",
        config.TEMP_DIR,
    )
    
    yield mc
    
    mc.destroy(not config.TEMP_DIR_KEEP)


@pytest.fixture(scope="module")
def minio_unauthed_user(minio) -> (str, str):
    minio.run_mc("admin", "user", "add", minio.mc_alias, "baduser", "badpsswd")
    
    yield ("baduser", "badpsswd")


@pytest.fixture(scope="module")
def kafkacon() -> KafkaController:
    kc = KafkaController(config.KAFKA_DOCKER_IMAGE)
    
    yield kc
    
    kc.destroy(print_logs=False)


@pytest.fixture
def kafka(kafkacon) -> KafkaController:
    kafkacon.delete_all_topics()
    
    yield kafkacon
