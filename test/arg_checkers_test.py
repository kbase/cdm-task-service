import datetime
from pytest import raises

from cdmtaskservice.arg_checkers import not_falsy, require_string, check_num, verify_aware_datetime


def test_not_falsy():
    for o in ["thing", 1, -8.2, ["a"], {"a": "b"}, object()]:
        assert not_falsy(o, "name") == o


def test_not_falsy_fail():
    for o in [None, "", 0, 0.0, [], {}]:
        with raises(ValueError, match="^r2d2 is required$"):
            not_falsy(o, "r2d2")


def test_require_string():
    assert require_string("  fooo  \t  ", "name") == "fooo"


def test_require_string_fail():
    for o in [None, "", "    \t   "]:
        with raises(ValueError, match="^c3po is required$"):
            require_string(o, "c3po")


def test_check_num():
    for o in [1, 2, 5, 10, 100, 1.000, 1.124153, 2.0, 5.0, 10.0, 16.165155, 100.0]:
        assert check_num(o, "name") == o


def test_check_num_with_min():
    test_set = {
        100: 100,
        -98: -156161,
        894: 893,
        12161662: 12161662,
        781441.141: 781441.141,
        82.1516: 82.1515,
    }
    for k, v in test_set.items():
        assert check_num(k, "name", minimum=v) == k


def test_check_num_fail_None():
    with raises(ValueError, match="^JEJ is required"):
        check_num(None, "JEJ")


def test_check_num_fail_minimum():
    test_set = {
        100: 101,
        -98: 156161,
        894: 895,
        12161662: 12161663,
        0.00000099: 0.000001,
        0.01: 10000000.01
    }
    for k, v in test_set.items():
        with raises(ValueError, match=f"^Luke must be >= {v}$"):
            check_num(k, "Luke", minimum=v)


def test_verify_aware_datetime():
    dts = [
        datetime.datetime(2025, 3, 31, 12, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2025, 3, 31, 12, 0, 0, tzinfo=datetime.timezone(
            datetime.timedelta(hours=5, minutes=30))
        )
    ]
    for dt in dts:
        assert verify_aware_datetime(dt, "whatever") == dt


def test_verify_aware_datetime_fail():
    _fail_verify_aware_datetime(None, "oops", ValueError("oops is required"))
    
    dt = datetime.datetime(2025, 3, 31, 12, 0, 0)
    _fail_verify_aware_datetime(dt, "foo", ValueError("foo must be a timezone aware datetime"))
    
    class NoOffsetTimezone(datetime.tzinfo):
        def utcoffset(self, dt):
            return None
    dt = datetime.datetime(2025, 3, 31, 12, 0, 0, tzinfo=NoOffsetTimezone())
    _fail_verify_aware_datetime(dt, "bar", ValueError("bar must be a timezone aware datetime"))


def _fail_verify_aware_datetime(dt: datetime.datetime, name: str, expected: Exception):
    with raises(type(expected), match=expected.args[0]):
        verify_aware_datetime(dt, name)
