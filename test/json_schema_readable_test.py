import datetime
import enum
from typing import Annotated, Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, RootModel

from cdmtaskservice.json_schema_readable import simplify_schema


def test_simplify_schema_model_docstring_becomes_description():
    class _Leaf(BaseModel):
        """ A leaf node.  """
        x: int

    class _Outer(BaseModel):
        """ The outer model.  """
        leaf: _Leaf

    result = simplify_schema(_Outer.model_json_schema())
    assert result == {
        "type": "object",
        "description": "The outer model.",
        "fields": [
            {
                "type": "object",
                "name": "leaf",
                "required": True,
                "description": "A leaf node.",
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_leaf_with_constraints_and_default():
    class _Leaf(BaseModel):
        name: Annotated[str, Field(
            min_length=1, max_length=10, pattern=r"^[a-z]+$", description="  A name.  "
        )]
        count: Annotated[int, Field(ge=0, le=100, default=5)]

    result = simplify_schema(_Leaf.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "string",
                "name": "name",
                "required": True,
                "description": "A name.",
                "constraints": {
                    "min_length": 1,
                    "max_length": 10,
                    "pattern": "^[a-z]+$",
                },
            },
            {
                "type": "integer",
                "name": "count",
                "required": False,
                "default": 5,
                "constraints": {"minimum": 0, "maximum": 100},
            },
        ],
    }


def test_simplify_schema_leaf_without_explicit_type_defaults_to_any():
    # typing.Any produces a JSON Schema with no "type" key at all - this is the only test that
    # reaches _simplify_leaf's `node.get("type", "any")` fallback rather than an explicit "type".
    class _WithAny(BaseModel):
        val: Any

    result = simplify_schema(_WithAny.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {"type": "any", "name": "val", "required": True},
        ],
    }


def test_simplify_schema_key_order_is_fixed():
    # dict equality ignores key order, so this test checks list(dict.keys()) explicitly. The
    # field also goes through the anyOf-collapse-then-_reorder path (see _simplify_union), which
    # is the one place a node's keys are assembled in a different order than _assemble produces.
    class _WithOrderedKeys(BaseModel):
        maybe: Annotated[
            str | None,
            Field(description="d", default="x", examples=["a"], min_length=1, max_length=5),
        ]

    result = simplify_schema(_WithOrderedKeys.model_json_schema())
    assert list(result.keys()) == ["type", "fields"]
    assert list(result["fields"][0].keys()) == [
        "type", "name", "required", "nullable", "description", "default", "examples",
        "constraints",
    ]


def test_simplify_schema_remaining_constraint_types_are_covered():
    class _WithConstraints(BaseModel):
        bounded: Annotated[int, Field(gt=0, lt=100)]
        tags: Annotated[list[str], Field(min_length=1, max_length=5)]
        uniq: set[str]
        day: datetime.date

    result = simplify_schema(_WithConstraints.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "integer",
                "name": "bounded",
                "required": True,
                "constraints": {"exclusive_minimum": 0, "exclusive_maximum": 100},
            },
            {
                "type": "array",
                "name": "tags",
                "required": True,
                "constraints": {"min_items": 1, "max_items": 5},
                "items": {"type": "string"},
            },
            {
                "type": "array",
                "name": "uniq",
                "required": True,
                "constraints": {"unique_items": True},
                "items": {"type": "string"},
            },
            {
                "type": "string",
                "name": "day",
                "required": True,
                "constraints": {"format": "date"},
            },
        ],
    }


def test_simplify_schema_optional_collapses_to_nullable():
    class _WithOptional(BaseModel):
        maybe: Annotated[str | None, Field(description="Might be missing.")] = None

    result = simplify_schema(_WithOptional.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "string",
                "name": "maybe",
                "required": False,
                "nullable": True,
                "description": "Might be missing.",
                "default": None,
            },
        ],
    }


def test_simplify_schema_optional_with_non_none_default_collapses_to_nullable():
    class _WithOptionalDefault(BaseModel):
        maybe: Annotated[str | None, Field(description="Has a real default.", default="x")]

    result = simplify_schema(_WithOptionalDefault.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "string",
                "name": "maybe",
                "required": False,
                "nullable": True,
                "description": "Has a real default.",
                "default": "x",
            },
        ],
    }


def test_simplify_schema_multi_type_union_becomes_one_of_options():
    class _WithUnion(BaseModel):
        value: str | int

    result = simplify_schema(_WithUnion.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "one of",
                "name": "value",
                "required": True,
                "options": [
                    {"type": "string"},
                    {"type": "integer"},
                ],
            },
        ],
    }


def test_simplify_schema_nullable_multi_type_union_sets_nullable_on_one_of():
    class _WithNullableMultiUnion(BaseModel):
        value: Annotated[str | int | None, Field(description="x")]

    result = simplify_schema(_WithNullableMultiUnion.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "one of",
                "name": "value",
                "required": True,
                "nullable": True,
                "description": "x",
                "options": [
                    {"type": "string"},
                    {"type": "integer"},
                ],
            },
        ],
    }


def test_simplify_schema_discriminated_union_uses_one_of_keyword():
    # a discriminated union renders as the "oneOf" JSON Schema keyword rather than "anyOf" -
    # every other union test uses a plain `X | Y` annotation, which pydantic always renders as
    # "anyOf", so this is the only test that reaches the `node["oneOf"]` branch.
    class _Cat(BaseModel):
        kind: Literal["cat"] = "cat"
        meow: str

    class _Dog(BaseModel):
        kind: Literal["dog"] = "dog"
        bark: str

    class _WithDiscriminatedUnion(BaseModel):
        pet: Annotated[_Cat | _Dog, Field(discriminator="kind")]

    result = simplify_schema(_WithDiscriminatedUnion.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "one of",
                "name": "pet",
                "required": True,
                "options": [
                    {
                        "type": "object",
                        "fields": [
                            {
                                "type": "string",
                                "name": "kind",
                                "required": False,
                                "default": "cat",
                            },
                            {"type": "string", "name": "meow", "required": True},
                        ],
                    },
                    {
                        "type": "object",
                        "fields": [
                            {
                                "type": "string",
                                "name": "kind",
                                "required": False,
                                "default": "dog",
                            },
                            {"type": "string", "name": "bark", "required": True},
                        ],
                    },
                ],
            },
        ],
    }


def test_simplify_schema_optional_model_field_override_takes_precedence_over_inner_model():
    # every other "collapses to nullable" test uses a bare leaf type - this exercises the same
    # collapse-then-_reorder path (see _simplify_union) when the inner simplified node has its
    # own nested "fields" key, to confirm _reorder places it correctly alongside nullable/default.
    # _Leaf has its own conflicting description/default/examples (via json_schema_extra) so all
    # 3 outer-node overrides in _simplify_union are genuinely exercised, not just 1.
    class _Leaf(BaseModel):
        model_config = ConfigDict(json_schema_extra={
            "description": "Leaf's own description.",
            "default": {"x": 0},
            "examples": [{"x": 99}],
        })
        x: int

    class _WithOptionalModel(BaseModel):
        leaf: Annotated[
            _Leaf | None,
            Field(description="Field override.", default={"x": 1}, examples=[{"x": 1}]),
        ] = {"x": 1}

    result = simplify_schema(_WithOptionalModel.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "leaf",
                "required": False,
                "nullable": True,
                "description": "Field override.",
                "default": {"x": 1},
                "examples": [{"x": 1}],
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_optional_model_without_field_override_keeps_inner_model_values():
    # the mirror image of the override test above: when the Optional[...] field itself carries
    # none of description/default/examples, _simplify_union's override loop (see the "if key in
    # node" check) must leave the inner model's own values completely untouched.
    class _Leaf(BaseModel):
        model_config = ConfigDict(json_schema_extra={
            "description": "Leaf's own description.",
            "default": {"x": 0},
            "examples": [{"x": 99}],
        })
        x: int

    class _WithOptionalModel(BaseModel):
        leaf: _Leaf | None

    result = simplify_schema(_WithOptionalModel.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "leaf",
                "required": True,
                "nullable": True,
                "description": "Leaf's own description.",
                "default": {"x": 0},
                "examples": [{"x": 99}],
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_heterogeneous_enum_raises_value_error():
    # a heterogeneous-valued enum (mixed str/int members) omits the "type" key entirely from its
    # JSON Schema, unlike a homogeneous string enum - this is the only test that reaches
    # _simplify_enum's missing-"type" branch.
    class _MixedEnum(enum.Enum):
        A = "a"
        B = 2

    class _WithMixedEnum(BaseModel):
        val: _MixedEnum

    with pytest.raises(
        ValueError, match=r"^Heterogeneous enum type definition found in schema$"
    ):
        simplify_schema(_WithMixedEnum.model_json_schema())


def test_simplify_schema_examples_are_preserved():
    class _WithExamples(BaseModel):
        tagged: Annotated[str, Field(examples=["a", "b"])]

    result = simplify_schema(_WithExamples.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "string",
                "name": "tagged",
                "required": True,
                "examples": ["a", "b"],
            },
        ],
    }


def test_simplify_schema_ref_is_inlined():
    class _Leaf(BaseModel):
        name: Annotated[str, Field(
            min_length=1, max_length=10, pattern=r"^[a-z]+$", description="  A name.  "
        )]
        count: Annotated[int, Field(ge=0, le=100, default=5)]

    class _WithNested(BaseModel):
        leaf: _Leaf

    result = simplify_schema(_WithNested.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "leaf",
                "required": True,
                "fields": [
                    {
                        "type": "string",
                        "name": "name",
                        "required": True,
                        "description": "A name.",
                        "constraints": {
                            "min_length": 1,
                            "max_length": 10,
                            "pattern": "^[a-z]+$",
                        },
                    },
                    {
                        "type": "integer",
                        "name": "count",
                        "required": False,
                        "default": 5,
                        "constraints": {"minimum": 0, "maximum": 100},
                    },
                ],
            },
        ],
    }


def test_simplify_schema_ref_field_level_description_override():
    class _Leaf(BaseModel):
        """ The referenced model's own description. """
        name: str

    class _WithNestedOverride(BaseModel):
        # field-level description on a $ref'd model must take precedence over _Leaf's own
        # docstring-derived description
        leaf: Annotated[_Leaf, Field(description="A specific leaf.")]

    result = simplify_schema(_WithNestedOverride.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "leaf",
                "required": True,
                "description": "A specific leaf.",
                "fields": [
                    {"type": "string", "name": "name", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_ref_chained_through_root_model():
    class _Leaf(BaseModel):
        x: int

    class _Wrapper(RootModel[_Leaf]):
        pass

    class _WithWrapper(BaseModel):
        w: _Wrapper

    result = simplify_schema(_WithWrapper.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "w",
                "required": True,
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_dict_becomes_free_form_object():
    class _Leaf(BaseModel):
        name: str
        count: Annotated[int, Field(default=5)]

    class _WithDict(BaseModel):
        values: dict[str, _Leaf]

    result = simplify_schema(_WithDict.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object (free-form keys)",
                "name": "values",
                "required": True,
                "values": {
                    "type": "object",
                    "fields": [
                        {"type": "string", "name": "name", "required": True},
                        {
                            "type": "integer",
                            "name": "count",
                            "required": False,
                            "default": 5,
                        },
                    ],
                },
            },
        ],
    }


def test_simplify_schema_array_has_items():
    class _Leaf(BaseModel):
        name: str
        count: int

    class _WithList(BaseModel):
        items: list[_Leaf]

    result = simplify_schema(_WithList.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "array",
                "name": "items",
                "required": True,
                "items": {
                    "type": "object",
                    "fields": [
                        {"type": "string", "name": "name", "required": True},
                        {"type": "integer", "name": "count", "required": True},
                    ],
                },
            },
        ],
    }


def test_simplify_schema_array_without_items_schema_raises_value_error():
    # a fixed-length empty tuple has no "items" key at all in its JSON Schema (just
    # minItems/maxItems), unlike list[int] (covered by test_simplify_schema_array_has_items),
    # whose "items" key is always present even when the item type is a leaf.
    class _WithEmptyTuple(BaseModel):
        fixed: tuple[()]

    with pytest.raises(
        ValueError, match=r"^Array type definition found in schema with no items schema$"
    ):
        simplify_schema(_WithEmptyTuple.model_json_schema())


def test_simplify_schema_object_without_properties_or_additional_properties_raises_value_error():
    # a plain pydantic model always emits either a "properties" key (even {} when field-less) or
    # an "additionalProperties" key (for dict-typed fields) - the only way to reach a bare
    # `{"type": "object"}` node with neither key is a manual json_schema_extra override.
    class _WithForcedObjectType(BaseModel):
        val: Annotated[str, Field(json_schema_extra={"type": "object"})]

    with pytest.raises(
        ValueError,
        match=r"^Object type definition found in schema with no properties or "
            r"additionalProperties$",
    ):
        simplify_schema(_WithForcedObjectType.model_json_schema())


def test_simplify_schema_enum_becomes_allowed_values():
    class _Color(enum.Enum):
        RED = "red"
        BLUE = "blue"

    class _WithEnum(BaseModel):
        color: _Color

    result = simplify_schema(_WithEnum.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "string",
                "name": "color",
                "required": True,
                "allowed_values": ["red", "blue"],
            },
        ],
    }


def test_simplify_schema_same_model_reused_at_unrelated_locations_is_not_a_cycle():
    # jsonref inlines $ref targets in place, so two unrelated sibling fields referencing the
    # same model produce the identical dict object at two locations in the resolved tree. That's
    # fine and must not raise - only revisiting a node on the current path down the tree (an
    # actual cycle) should.
    class _Leaf(BaseModel):
        x: int

    class _WithTwoRefs(BaseModel):
        first: _Leaf
        second: _Leaf

    result = simplify_schema(_WithTwoRefs.model_json_schema())
    assert result == {
        "type": "object",
        "fields": [
            {
                "type": "object",
                "name": "first",
                "required": True,
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
            {
                "type": "object",
                "name": "second",
                "required": True,
                "fields": [
                    {"type": "integer", "name": "x", "required": True},
                ],
            },
        ],
    }


def test_simplify_schema_recursive_type_raises_value_error():
    class _Self(BaseModel):
        child: "_Self | None" = None

    _Self.model_rebuild()
    with pytest.raises(ValueError, match=r"^Recursive type definition found in schema$"):
        simplify_schema(_Self.model_json_schema())
