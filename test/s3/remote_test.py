import os
from pathlib import Path

from pytest import raises

from conftest import assert_exception_correct
from cdmtaskservice.s3.remote import calculate_etag

TESTDATA = Path(os.path.normpath((Path(__file__) / ".." / ".." / "testdata")))

def test_calculate_etag():
    testset = [
        (TESTDATA / "random_bytes_1kB", 1024, "b10278db14633f102103c5e9d75c0af0"),
        (TESTDATA / "random_bytes_1kB", 10000, "b10278db14633f102103c5e9d75c0af0"),
        (TESTDATA / "random_bytes_10kB", 1024, "b4b7898bf290001d169572b777efd34f-10"),
        (TESTDATA / "random_bytes_10kB", 5000, "a70a4d1732484e75434df2c08570e1b2-3"),
        (TESTDATA / "random_bytes_10kB", 10240, "3291fbb392f6fad06dbf331dfb74da81"),
        (TESTDATA / "random_bytes_10kB", 100000, "3291fbb392f6fad06dbf331dfb74da81"),
    ]
    for infile, size, etag in testset:
        gottag = calculate_etag(infile, size)
        assert gottag == etag


def test_calculate_etag_fail():
    testset = [
        (None, 1, ValueError("infile must be exist and be a file")),
        (TESTDATA, 1, ValueError("infile must be exist and be a file")),
        (TESTDATA / "empty_file", 1, ValueError("file is empty")),
        (TESTDATA / "random_bytes_1kB", 0, ValueError("partsize must be > 0")),
        (TESTDATA / "random_bytes_1kB", -10000, ValueError("partsize must be > 0")),
    ]
    for infile, size, expected in testset:
        with raises(Exception) as got:
            calculate_etag(infile, size)
        assert_exception_correct(got.value, expected)

