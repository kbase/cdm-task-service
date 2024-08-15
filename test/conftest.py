'''
Configure pytest fixtures and helper functions for this directory.
'''

from contextlib import closing
import socket
import traceback


def assert_exception_correct(got: Exception, expected: Exception):
    err = "".join(traceback.TracebackException.from_exception(got).format())
    assert got.args == expected.args  # add , err to see the traceback
    assert type(got) == type(expected)


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]
