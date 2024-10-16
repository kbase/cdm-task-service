"""
These tests are expected to be run manually.

Note the SFAPI credentials file used as part of the test is copied to a temp file that is deleted
when the tests are complete.

These tests are far from complete, especially around error conditions.

The person running the tests will want to watch the logs to see what's going on -

PYTHONPATH=. pytest --log-cli-level=INFO -rP test_manual/nersc/client_test.py 

.. is useful for this.
"""

# TODO TEST add lots more tests and check logs are printing out the right stuff
# TODO TEST look into testing in automated tests via mocking client methods?

import asyncio
import pytest
import os
import tempfile
import time
from unittest.mock import patch

from cdmtaskservice.nersc import client
from test_common import config


@pytest.mark.asyncio
@patch('cdmtaskservice.nersc.client._CHECK_CREDFILE_SEC', 1)
# adjust to check autoclosing old clients by examining logs
@patch('cdmtaskservice.nersc.client._CHECK_FOR_CLIENT_CLOSE_SEC', 0.05)
@patch('cdmtaskservice.nersc.client._CLIENT_CLOSE_DELAY_SEC', 0.05)
async def test_client_init_and_update_creds():
    tmpdir = config.TEMP_DIR
    credpath = config.SFAPI_CREDS_FILE_PATH
    creduser = config.SFAPI_CREDS_USER
    tmpcreds = tempfile.NamedTemporaryFile(prefix="NERSC_creds_", dir=tmpdir, delete=False)
    tc_path = tmpdir / tmpcreds.name
    try:
        with open(credpath, "rb") as creds:
            tmpcreds.write(creds.read())
        tmpcreds.close()
        cliprov = await client.NERSCSFAPIClientProvider.create(tc_path, creduser)
        client1 = cliprov.get_client()
        assert cliprov.expiration() is None  # TODO CLIEXPIRE fix when possible
        cred_mod_time = os.path.getmtime(tc_path)
        (tc_path).touch(exist_ok=True)
        print(f"Old cred mod time: {cred_mod_time}, new: {os.path.getmtime(tc_path)}")
        await asyncio.sleep(1)
        assert cliprov.get_client() is client1
        await asyncio.sleep(2)  # Wait for new client to be created
        client2 = cliprov.get_client()
        assert client2 is not client1
        
        with open(tc_path, 'wb') as out, open(credpath, "rb") as in_:
            for _ in range(3):
                out.write(in_.readline())
            out.write(b"this should mess things up")
            out.write(in_.read())
        # here there should be logging about failed creds
        await asyncio.sleep(2)
        assert cliprov.get_client() is client2
        with open(tc_path, 'wb') as out, open(credpath, "rb") as in_:
            out.write(in_.read())
        await asyncio.sleep(3)
        # now things should work again
        assert cliprov.get_client() is not client2
        t1 = time.time()
        await asyncio.sleep(0.01)  # increase to force the close task to close the clients
        await cliprov.destroy()
        print(f"time pre destroy: {t1}, post: {time.time()}")
    finally:
        tc_path.unlink()
