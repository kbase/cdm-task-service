'''
Configure pytest fixtures and helper functions for this directory.
'''

import traceback


def assert_exception_correct(got: Exception, expected: Exception):
    err = "".join(traceback.TracebackException.from_exception(got).format())
    assert got.args == expected.args  # add , err to see the traceback
    assert type(got) == type(expected)
