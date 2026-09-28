import enum
from collections.abc import Callable, Mapping
from typing import Literal, TypeVar, Union

import pytest
from pydantic import BaseModel, ConfigDict

from cdmtaskservice.pydantic_type_walker import DisallowedTypeError, check_annotation


class _Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class _Leaf(BaseModel):
    name: str
    count: int | None


class _NotABaseModel:
    pass


class _Nested(BaseModel):
    leaf: _Leaf
    color: _Color


class _WithDisallowed(BaseModel):
    bad: _Leaf


class _NestedWithDisallowed(BaseModel):
    nested: _WithDisallowed


class _Self(BaseModel):
    child: "_Self | None"


def test_check_annotation_primitives():
    for t in (str, int, float, bool, bytes, type(None)):
        check_annotation(t, "field")


def test_check_annotation_enum():
    check_annotation(_Color, "field")


def test_check_annotation_basemodel():
    check_annotation(_Leaf, "field")


def test_check_annotation_nested_basemodel():
    check_annotation(_Nested, "field")


def test_check_annotation_optional():
    check_annotation(str | None, "field")


def test_check_annotation_union():
    check_annotation(str | int, "field")


def test_check_annotation_typing_union():
    # typing.Union[...] (and Optional[...], which desugars to it) produces a different origin
    # than the `X | Y` syntax - get_origin(Union[str, int]) is `typing.Union`, whereas
    # get_origin(str | int) is `types.UnionType`. Cover both branches of that check.
    check_annotation(Union[str, int], "field")


def test_check_annotation_list():
    check_annotation(list[_Leaf], "field")


def test_check_annotation_dict():
    check_annotation(dict[str, _Leaf], "field")


def test_check_annotation_non_container_generic():
    # Mapping[...] isn't one of the container origins we special-case (dict/list/set/tuple), so
    # it falls through to the generic recurse-without-a-path-suffix branch, same as Literal.
    check_annotation(Mapping[str, _Leaf], "field")


def test_check_annotation_cyclic_reference_does_not_infinite_loop():
    check_annotation(_Self, "field")


def test_check_annotation_fail_non_basemodel_leaf_in_list():
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field\[\] \(.*_NotABaseModel'>\)",
    ):
        check_annotation(list[_NotABaseModel], "field")


def test_check_annotation_fail_non_basemodel_leaf_in_dict():
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field\{\} \(.*_NotABaseModel'>\)",
    ):
        check_annotation(dict[str, _NotABaseModel], "field")


def test_check_annotation_disallowed_type_in_list():
    with pytest.raises(
        DisallowedTypeError, match=r"disallowed type found at field\[\]: .*_Leaf'>"
    ):
        check_annotation(list[_Leaf], "field", disallowed_types=(_Leaf,))


def test_check_annotation_disallowed_type_in_dict():
    with pytest.raises(
        DisallowedTypeError, match=r"disallowed type found at field\{\}\.bad: .*_Leaf'>"
    ):
        check_annotation(dict[str, _WithDisallowed], "field", disallowed_types=(_Leaf,))


def test_check_annotation_disallowed_type_in_dict_key():
    with pytest.raises(
        DisallowedTypeError, match=r"disallowed type found at field\{key\}: .*_Leaf'>"
    ):
        check_annotation(dict[_Leaf, str], "field", disallowed_types=(_Leaf,))


def test_check_annotation_fail_non_type_generic_arg():
    # Callable[[int, str], bool]'s first arg is the list [int, str], not a type - exercises the
    # "not isinstance(annotation, type)" branch, which is rejected like any other invalid leaf.
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field \(\[<class 'int'>, <class 'str'>\]\)",
    ):
        check_annotation(Callable[[int, str], bool], "field")


def test_check_annotation_fail_literal():
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field \(a\)",
    ):
        check_annotation(Literal["a", "b"], "field")


def test_check_annotation_fail_typevar():
    t = TypeVar("t")
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field \(~t\)",
    ):
        check_annotation(t, "field")


def test_check_annotation_fail_non_basemodel_leaf():
    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field \(.*_NotABaseModel'>\)",
    ):
        check_annotation(_NotABaseModel, "field")


def test_check_annotation_fail_nested_non_basemodel_leaf():
    class _Nested2(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

        bad: _NotABaseModel

    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive types: "
            r"field\.bad \(.*_NotABaseModel'>\)",
    ):
        check_annotation(_Nested2, "field")


def test_check_annotation_disallowed_type():
    with pytest.raises(DisallowedTypeError, match=r"disallowed type found at field: .*_Leaf'>"):
        check_annotation(_Leaf, "field", disallowed_types=(_Leaf,))


def test_check_annotation_disallowed_type_nested():
    with pytest.raises(
        DisallowedTypeError, match=r"disallowed type found at field\.bad: .*_Leaf'>"
    ):
        check_annotation(_WithDisallowed, "field", disallowed_types=(_Leaf,))


def test_check_annotation_disallowed_type_deeply_nested():
    with pytest.raises(
        DisallowedTypeError,
        match=r"disallowed type found at field\.nested\.bad: .*_Leaf'>",
    ):
        check_annotation(_NestedWithDisallowed, "field", disallowed_types=(_Leaf,))


def test_check_annotation_disallowed_type_not_present():
    check_annotation(_Nested, "field", disallowed_types=(_WithDisallowed,))


def test_disallowed_type_error_fields():
    e = DisallowedTypeError("field.bad", _Leaf)
    assert e.path == "field.bad"
    assert e.disallowed_type is _Leaf
    assert str(e) == f"disallowed type found at field.bad: {_Leaf}"
