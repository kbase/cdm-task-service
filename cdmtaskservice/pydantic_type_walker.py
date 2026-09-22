"""
A generic helper for recursively validating the shape of a pydantic field type annotation.
"""

from __future__ import annotations

import enum
import types
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

_JSON_PRIMITIVE_TYPES = (str, int, float, bool, bytes, type(None))
_SEQUENCE_ORIGINS = (list, set, frozenset, tuple)


class DisallowedTypeError(ValueError):
    """ Raised by check_annotation when a disallowed type is found in an annotation. """

    def __init__(self, path: str, disallowed_type: type):
        super().__init__(f"disallowed type found at {path}: {disallowed_type}")
        self.path = path
        self.disallowed_type = disallowed_type


def check_annotation(
    annotation: Any, field_name: str, disallowed_types: tuple[type, ...] = ()
) -> None:
    """
    Recursively check a pydantic field type annotation, raising an exception if either:

    * a type in disallowed_types is found anywhere in the annotation's structure - raises
      DisallowedTypeError, or
    * a leaf type is neither a pydantic BaseModel, an enum, nor a JSON primitive type - raises
      ValueError. This guards against non-pydantic classes (e.g. dataclasses) hiding structure
      this function can't see into.

    Unwraps Union / Optional / `X | None` and generic containers (list[...], dict[...], etc.)
    and recurses into nested BaseModel fields.

    annotation - the field's type annotation, e.g. from BaseModel.model_fields[name].annotation.
    field_name - the field's name, used to build the path in a raised exception.
    disallowed_types - types to reject if found anywhere in the annotation's structure, even if
        they are themselves valid BaseModel subclasses.
    """
    _check_annotation(annotation, field_name, disallowed_types, seen=set())


def _check_annotation(
    annotation: Any, path: str, disallowed_types: tuple[type, ...], seen: set[type]
) -> None:
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        if origin is Union or origin is types.UnionType:  # Optional unwraps to this too
            for arg in args:
                _check_annotation(arg, path, disallowed_types, seen)
            return
        if origin is dict:
            key_type, value_type = args
            _check_annotation(key_type, f"{path}{{key}}", disallowed_types, seen)
            _check_annotation(value_type, f"{path}{{}}", disallowed_types, seen)
            return
        if origin in _SEQUENCE_ORIGINS:
            for arg in args:
                _check_annotation(arg, f"{path}[]", disallowed_types, seen)
            return
        # Literal, Callable, and other generics that aren't containers of nested field
        # structure - recurse without altering the path so leaf rejection reports it clearly.
        for arg in args:
            _check_annotation(arg, path, disallowed_types, seen)
        return
    if isinstance(annotation, type):
        if disallowed_types and issubclass(annotation, disallowed_types):
            raise DisallowedTypeError(path, annotation)
        if annotation in _JSON_PRIMITIVE_TYPES or issubclass(annotation, enum.Enum):
            return
        if issubclass(annotation, BaseModel):
            if annotation not in seen:
                seen.add(annotation)
                for fieldname, field in annotation.model_fields.items():
                    _check_annotation(
                        field.annotation, f"{path}.{fieldname}", disallowed_types, seen
                    )
            return
    raise ValueError(
        f"field types must be pydantic BaseModels, enums, or JSON primitive types: "
        f"{path} ({annotation})"
    )
