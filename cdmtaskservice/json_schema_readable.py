"""
Turns a pydantic model's JSON Schema (as produced by BaseModel.model_json_schema()) into a
simplified, self-contained structure that keeps everything an OpenAPI / Swagger UI would show a
user browsing a model, but without the parts that make raw JSON Schema hard to read by eye:

* `$ref` / `$defs` are resolved and inlined, so there's no need to jump around the document to
  see a nested model's shape.
* The `anyOf: [<type>, {"type": "null"}]` pattern produced for an `Optional`/`X | None` field is
  collapsed into a single node for `<type>` with `nullable: true`, rather than a two-branch union.
* Pydantic's auto-generated `title` (e.g. "Crc64Nvme" for a field named `crc64nvme`, or
  "Output Prefix" for `output_prefix`) is dropped wherever it's purely derived from, and so
  redundant with, information already present elsewhere in the output (the field's own `name`).
* Value constraints (length, pattern, numeric bounds, array size, etc.) are pulled out of the
  raw camelCase JSON Schema keywords into a single `constraints` dict with readable snake_case
  names.
* Every remaining field (description, default, examples, enum values as `allowed_values`, and
  nested structure) is kept, in a fixed, predictable order, so nothing an OpenAPI UI would
  normally show is lost.
"""

from typing import Any

import jsonref
from pydantic.alias_generators import to_snake


_CONSTRAINT_KEYS = (
    "minLength", "maxLength", "pattern", "minimum", "maximum", "exclusiveMinimum",
    "exclusiveMaximum", "minItems", "maxItems", "uniqueItems", "format",
)


def simplify_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Simplify a pydantic model's JSON Schema, as returned by BaseModel.model_json_schema(), into
    a self-contained, human-readable structure. See the module docstring for details.
    """
    # merge_props=True keeps field-level overrides (e.g. a description on a $ref'd field) after
    # inlining; proxies=False returns plain dicts rather than lazy JsonRef proxy objects
    resolved = jsonref.replace_refs(schema, merge_props=True, proxies=False)
    return _simplify(resolved, frozenset())


def _simplify(node: dict[str, Any], seen: frozenset[int]) -> dict[str, Any]:
    # jsonref inlines $ref targets in place, so the same dict object can legitimately appear at
    # multiple, unrelated locations (e.g. two sibling fields referencing the same model) - that's
    # fine. A cycle instead revisits the same object *on the current path down the tree*, which
    # is what `seen` (a set of already-visited object ids on this path) tracks.
    if id(node) in seen:
        raise ValueError("Recursive type definition found in schema")
    seen = seen | {id(node)}
    if "anyOf" in node or "oneOf" in node:
        return _simplify_union(node, seen)
    if "enum" in node:
        return _simplify_enum(node)
    if "properties" in node:
        return _simplify_object(node, seen)
    if isinstance(node.get("additionalProperties"), dict):
        return _simplify_dict(node, seen)
    if node.get("type") == "array":
        return _simplify_array(node, seen)
    if node.get("type") == "object" and "additionalProperties" not in node:
        raise ValueError(
            "Object type definition found in schema with no properties or additionalProperties"
        )
    return _simplify_leaf(node)


def _assemble(
    type_: str,
    node: dict[str, Any],
    nullable: bool = False,
    allowed_values: list[Any] | None = None,
    nested: tuple[str, Any] | None = None,
) -> dict[str, Any]:
    """ Build a simplified node's dict in a single, fixed, readable key order. """
    result: dict[str, Any] = {"type": type_}
    if nullable:
        result["nullable"] = True
    if node.get("description") is not None:
        # docstring-derived descriptions often carry incidental leading/trailing whitespace
        result["description"] = node["description"].strip()
    if "default" in node:
        result["default"] = node["default"]
    if node.get("examples") is not None:
        result["examples"] = node["examples"]
    if allowed_values is not None:
        result["allowed_values"] = allowed_values
    constraints = _constraints(node)
    if constraints:
        result["constraints"] = constraints
    if nested is not None:
        result[nested[0]] = nested[1]
    return result


def _constraints(node: dict[str, Any]) -> dict[str, Any]:
    return {to_snake(raw): node[raw] for raw in _CONSTRAINT_KEYS if raw in node}


_KEY_ORDER = (
    "type", "name", "required", "nullable", "description", "default", "examples",
    "allowed_values", "constraints", "fields", "items", "values", "options",
)


def _reorder(node: dict[str, Any]) -> dict[str, Any]:
    """ Rebuild a simplified node's dict in the canonical key order, e.g. after overriding a
    key on an already-assembled node (see _simplify_union). """
    ordered = {k: node[k] for k in _KEY_ORDER if k in node}
    ordered.update({k: v for k, v in node.items() if k not in _KEY_ORDER})
    return ordered


def _simplify_union(node: dict[str, Any], seen: frozenset[int]) -> dict[str, Any]:
    variants = node.get("anyOf") or node["oneOf"]
    nullable = any(v.get("type") == "null" for v in variants)
    non_null = [v for v in variants if v.get("type") != "null"]
    if len(non_null) == 1:
        # the common Optional[X] / X | None case - collapse to a single nullable node. pydantic
        # puts a field-level description/default/examples (e.g. from Field(...)) on this outer
        # anyOf node, not on the inner variant, so they must be copied onto `inner` explicitly -
        # these are the only 3 keys pydantic ever attaches at this level; constraints etc. always
        # live on the inner variant already. Copying overrides any value `inner` already has
        # for these keys (e.g. a $ref'd model's own docstring-derived description).
        inner = _simplify(non_null[0], seen)
        if nullable:
            inner["nullable"] = True
        for key in ("description", "default", "examples"):
            if key in node:
                inner[key] = node[key]
        return _reorder(inner)
    options = [_simplify(v, seen) for v in non_null]
    return _assemble("one of", node, nullable=nullable, nested=("options", options))


def _simplify_enum(node: dict[str, Any]) -> dict[str, Any]:
    if "type" not in node:
        raise ValueError("Heterogeneous enum type definition found in schema")
    return _assemble(node["type"], node, allowed_values=list(node["enum"]))


def _simplify_object(node: dict[str, Any], seen: frozenset[int]) -> dict[str, Any]:
    required = set(node.get("required", []))
    fields = []
    for name, prop in node["properties"].items():
        simplified = _simplify(prop, seen)
        simplified["name"] = name
        simplified["required"] = name in required
        fields.append(_reorder(simplified))
    return _assemble("object", node, nested=("fields", fields))


def _simplify_dict(node: dict[str, Any], seen: frozenset[int]) -> dict[str, Any]:
    values = _simplify(node["additionalProperties"], seen)
    return _assemble("object (free-form keys)", node, nested=("values", values))


def _simplify_array(node: dict[str, Any], seen: frozenset[int]) -> dict[str, Any]:
    if "items" not in node:
        raise ValueError("Array type definition found in schema with no items schema")
    items = _simplify(node["items"], seen)
    return _assemble("array", node, nested=("items", items))


def _simplify_leaf(node: dict[str, Any]) -> dict[str, Any]:
    return _assemble(node.get("type", "any"), node)
