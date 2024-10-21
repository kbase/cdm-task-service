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
    longkey = ("a" * 1000) + "𐎦" + ("a" * 21)  # symbol is 4 bytes in utf-8
        
    testset = {
        None: "The s3 path at index x cannot be null or a whitespace string",
        "   \t   ": "The s3 path at index x cannot be null or a whitespace string",
        "  /  ": "Path '  /  ' at index x must start with the s3 bucket and include a key",
        "  /  /  ": "Bucket name at index x cannot be whitespace only",
        "  ////bar  ":
            "Path '  ////bar  ' at index x must start with the s3 bucket and include a key",
        " / bar   ": "Path ' / bar   ' at index x must start with the s3 bucket and include a key",
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
        "illegal bucket/foo": charerr + "illegal bucket",
        "buckit/foo//bar":
            "Path 'buckit/foo//bar' at index x contains illegal character string '//' in the key",
        "buckit///  //bar":
            "Path 'buckit///  //bar' at index x contains illegal character string '//' in the key",
        "buckit/foo/\nbar":
            "Path buckit/foo/\nbar at index x contains a control character "
            + "in the key at position 4",
        "buckit/foo/bar\t":
            "Path buckit/foo/bar\t at index x contains a control character "
            + "in the key at position 7",
        "buckit/" + longkey:
             f"Path 'buckit/{longkey}' at index x's key is longer than 1024 bytes in UTF-8",
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
    assert (len(s3p)) == 1
    
    s3p = S3Paths([
        "foo/bar",
        "  ///  baz////    bat   /",
        "   thingy-stuff9///    /𝛙hatever/ ",
        "  bukkit  /      "])
    assert s3p.paths == (
        "foo/bar",
        "baz/    bat   /",
        "thingy-stuff9/    /𝛙hatever/ ",
        "bukkit/      ")
    assert (len(s3p)) == 4


def test_split_paths():
    s3p = S3Paths(["foo/bar", "baz/bat   ", "   //thingy-stuff9///𝛙hatever/baz"])
    
    assert list(s3p.split_paths()) == [
        ["foo", "bar"], ["baz", "bat   "], ["thingy-stuff9", "𝛙hatever/baz"]
    ]
    
    
def test_split_paths_w_full_path():
    s3p = S3Paths(["foo/bar", "baz/bat   ", "   thingy-stuff9/𝛙hatever/baz"])
    
    assert list(s3p.split_paths(include_full_path=True)) == [
        ["foo", "bar", "foo/bar"],
        ["baz", "bat   ", "baz/bat   "],
        ["thingy-stuff9", "𝛙hatever/baz", "thingy-stuff9/𝛙hatever/baz"]
    ]
