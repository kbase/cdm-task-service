'''
Configure pytest fixtures and helper functions for this directory.
'''
import pytest
import traceback

from controllers.minio import MinioController
import config


def assert_exception_correct(got: Exception, expected: Exception):
    err = "".join(traceback.TracebackException.from_exception(got).format())
    assert got.args == expected.args  # add , err to see the traceback
    assert type(got) == type(expected)


@pytest.fixture(scope="module")
def minio():
    mc = MinioController(
        config.MINIO_EXE_PATH,
        "access_key",
        "secret_key",
        config.TEMP_DIR,
    )
    host = f"http://localhost:{mc.port}"
    
    yield mc
    
    mc.destroy(config.TEMP_DIR_KEEP)
