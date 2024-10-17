# Most tests are in the test_manual directory to avoid altering files on NERSC as part
# of automated tests.

# TODO TEST add manual tests
# TODO TEST add tests that don't contact nersc like arg checking tests

from cdmtaskservice.nersc import client  # @UnusedImport


def test_noop():
    pass
