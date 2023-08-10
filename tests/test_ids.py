import pytest

from outpack.ids import fractional_to_bytes, outpack_id, validate_outpack_id


def test_fractional_to_bytes():
    assert fractional_to_bytes(1691686967.895351) == "e536"
    assert fractional_to_bytes(100) == "0000"
    assert fractional_to_bytes(100.0001) == "0007"
    assert fractional_to_bytes(100.001) == "0042"
    assert fractional_to_bytes(100.05) == "0ccd"


def test_outpack_id_creation():
    x = outpack_id()
    assert len(x) == 24


def test_outpack_id_can_be_validated():
    validate_outpack_id("20230810-172859-6b0408e0")
    with pytest.raises(Exception, match="Malformed id"):
        validate_outpack_id("20230810-172859-6b0408e")
