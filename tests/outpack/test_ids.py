import pytest

from pyorderly.outpack.ids import (
    fractional_to_bytes,
    is_outpack_id,
    outpack_id,
    validate_outpack_id,
)


def test_fractional_to_bytes():
    assert fractional_to_bytes(1691686967.895351) == "e535"
    assert fractional_to_bytes(100) == "0000"
    assert fractional_to_bytes(100.0001) == "0006"
    assert fractional_to_bytes(100.001) == "0041"
    assert fractional_to_bytes(100.05) == "0ccc"
    assert fractional_to_bytes(1696872486.999993) == "ffff"


def test_outpack_id_creation():
    x = outpack_id()
    assert len(x) == 24
    assert is_outpack_id(x)


def test_outpack_id_can_be_validated():
    id_ok = "20230810-172859-6b0408e0"
    validate_outpack_id(id_ok)
    assert is_outpack_id(id_ok)
    id_err = "20230810-172859-6b0408e"
    with pytest.raises(Exception, match="Malformed id"):
        validate_outpack_id(id_err)
    assert not is_outpack_id(id_err)
