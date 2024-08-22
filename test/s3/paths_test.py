import random
from pytest import raises

from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.exceptions import S3PathError

from conftest import  assert_exception_correct


def test_init_fail_no_path():
    for p in [None, [], tuple()]:
        with raises(Exception) as got:
            S3Paths(p)
        assert_exception_correct(got.value, ValueError("At least one path must be supplied."))


def test_init_fail_bad_path():
    
    charerr = "Bucket name at index x may only contain '-' and lowercase ascii alphanumerics: "
        
    testset = {
        None: "The s3 path at index x cannot be null or a whitespace string",
        "   \t   ": "The s3 path at index x cannot be null or a whitespace string",
         "  /  ": "path '/' at index x must start with the s3 bucket and include a key",
         "foo /  ": "path 'foo /' at index x must start with the s3 bucket and include a key",
        " / bar   ": "path '/ bar' at index x must start with the s3 bucket and include a key",
        "il/foo": "Bucket name at index x must be > 2 and < 64 characters: il",
        ("illegal-bu" * 6) + "cket/foo":
            f"Bucket name at index x must be > 2 and < 64 characters: {'illegal-bu' * 6}cket",
        "illegal.bucket/foo":
            "Bucket at index x has `.` in the name which is unsupported: illegal.bucket",
        "-illegal-bucket/foo":
            "Bucket name at index x cannot start or end with '-': -illegal-bucket",
        "illegal-bucket-/foo":
            "Bucket name at index x cannot start or end with '-': illegal-bucket-",
        "illegal*bucket/foo": charerr + "illegal*bucket",
        "illegal_bucket/foo": charerr + "illegal_bucket",
        "illegal-Bucket/foo": charerr + "illegal-Bucket",
        "illegal-βucket/foo": charerr + "illegal-βucket",
    }
    for k, v in testset.items():
        _init_fail(k, v)


def _init_fail(path: str, expected: str):
    paths = []
    for i in range(random.randrange(10)):
        paths.append(f"foo/bar{i}")
    paths.append(path)
    expected = expected.replace("index x", f"index {len(paths) - 1}")
    with raises(Exception) as got:
        S3Paths(paths)
    assert_exception_correct(got.value, S3PathError(expected))


def test_init():
    s3p = S3Paths(["foo/bar"])
    assert s3p.paths == ("foo/bar",)
    
    s3p = S3Paths(["foo/bar", "baz/bat  \t ", "   thingy-stuff9/𝛙hatever"])
    assert s3p.paths == ("foo/bar", "baz/bat", "thingy-stuff9/𝛙hatever")


def test_split_paths():
    s3p = S3Paths(["foo/bar", "baz/bat  \t ", "   thingy-stuff9/𝛙hatever/baz"])
    
    assert list(s3p.split_paths()) == [
        ["foo", "bar"], ["baz", "bat"], ["thingy-stuff9", "𝛙hatever/baz"]
    ]
    
    
def test_split_paths_w_full_path():
    s3p = S3Paths(["foo/bar", "baz/bat  \t ", "   thingy-stuff9/𝛙hatever/baz"])
    
    assert list(s3p.split_paths(include_full_path=True)) == [
        ["foo", "bar", "foo/bar"],
        ["baz", "bat", "baz/bat"],
        ["thingy-stuff9", "𝛙hatever/baz", "thingy-stuff9/𝛙hatever/baz"]
    ]