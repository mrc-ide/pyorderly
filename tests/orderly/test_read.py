import ast
import re

import pytest
from orderly.read import _read_py, orderly_read


def test_read_simple_trivial_parameters():
    assert _read_py(ast.parse("orderly.parameters()")) == {"parameters": {}}
    a = _read_py(ast.parse("orderly.parameters(a=None)"))
    assert a == {"parameters": {"a": None}}
    ab = _read_py(ast.parse("orderly.parameters(a=None, b=1)"))
    assert ab == {"parameters": {"a": None, "b": 1}}


def test_prevent_complex_types_in_parameters():
    msg = "Invalid value for argument 'a' to 'parameters()': len"
    with pytest.raises(Exception, match=re.escape(msg)):
        _read_py(ast.parse("orderly.parameters(a=len)"))


def test_prevent_duplicate_arguments():
    msg = "Duplicate argument 'a' to 'parameters()'"
    with pytest.raises(Exception, match=re.escape(msg)):
        _read_py(ast.parse("orderly.parameters(a=1, a=2, b=2)"))


def test_require_named_arguments():
    msg = "All arguments to 'parameters()' must be named"
    with pytest.raises(Exception, match=re.escape(msg)):
        _read_py(ast.parse("orderly.parameters(1, a=2, b=2)"))


def test_prevent_multiple_calls_to_parameters():
    code = "orderly.parameters(a=1, b=2)\norderly.parameters(c=3)"
    msg = "Duplicate call to 'parameters()' on line 2"
    with pytest.raises(Exception, match=re.escape(msg)):
        _read_py(ast.parse(code))


def test_can_read_orderly_py_with_no_parameters():
    path = "tests/orderly/examples/data/orderly.py"
    assert orderly_read(path) == {"parameters": {}}


def test_can_read_orderly_py_with_parameters():
    path = "tests/orderly/examples/parameters/orderly.py"
    assert orderly_read(path) == {"parameters": {"a": 1, "b": None}}
