'''
Configure pytest fixtures and helper functions for this directory.
'''
import pytest
import traceback

from controllers.minio import MinioController
import config


def assert_exception_correct(got: Exception, expected: Exception, print_traceback=False):
    if print_traceback:
        print("".join(traceback.TracebackException.from_exception(got).format()))
    assert got.args == expected.args
    assert type(got) == type(expected)


@pytest.fixture(scope="module")
def minio():
    mc = MinioController(
        config.MINIO_EXE_PATH,
        config.MINIO_MC_EXE_PATH,
        "access_key",
        "secret_key",
        config.TEMP_DIR,
    )
    host = f"http://localhost:{mc.port}"
    
    yield mc
    
    mc.destroy(not config.TEMP_DIR_KEEP)


@pytest.fixture(scope="module")
def minio_unauthed_user(minio):
    minio.run_mc("admin", "user", "add", minio.mc_alias, "baduser", "badpsswd")
    
    yield ("baduser", "badpsswd")
