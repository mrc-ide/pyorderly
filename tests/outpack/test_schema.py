import json

import pytest
from jsonschema.exceptions import ValidationError

from pyorderly.outpack.schema import (
    outpack_schema_version,
    read_schema,
    validate,
)


def test_can_read_schema():
    assert isinstance(read_schema("outpack/packet-id.json"), str)


def test_can_validate_simple_schema():
    validate("20230807-152345-0e0662d0", "outpack/packet-id.json")
    with pytest.raises(ValidationError):
        validate("20230807-152345-0e0662d", "outpack/packet-id.json")


def test_can_validate_referenced_schemas():
    p = "example/.outpack/location/local/20230807-152344-ee606dce"
    with open(p) as f:
        loc = json.load(f)
    validate(loc, "outpack/location.json")
    loc["packet"] = "yes"
    with pytest.raises(ValidationError):
        validate(loc, "outpack/location.json")


def test_can_get_schema_version():
    assert outpack_schema_version() == "0.1.1"
