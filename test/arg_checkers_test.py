from pytest import raises

from cdmtaskservice.arg_checkers import not_falsy, require_string, check_int


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


def test_check_int():
    for o in [1, 2, 5, 10, 100]:
        assert check_int(o, "name") == o


def test_check_int_with_min():
    test_set = {
        100: 100,
        -98: -156161,
        894: 893,
        12161662: 12161662,
    }
    for k, v in test_set.items():
        assert check_int(k, "name", minimum=v) == k


def test_check_int_fail_None():
    with raises(ValueError, match="^JEJ is required"):
        check_int(None, "JEJ")


def test_check_int_fail_minimum():
    test_set = {
        100: 101,
        -98: 156161,
        894: 895,
        12161662: 12161663,
    }
    for k, v in test_set.items():
        with raises(ValueError, match=f"^Luke must be >= {v}$"):
            check_int(k, "Luke", minimum=v)
