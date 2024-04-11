import ast
import re
import sys
from pathlib import Path

import pytest
from orderly.read import _read_py, orderly_read


def test_read_simple_trivial_parameters():
    assert _read_py(ast.parse("orderly.parameters()")) == {"parameters": {}}
    a = _read_py(ast.parse("orderly.parameters(a=None)"))
    assert a == {"parameters": {"a": None}}
    ab = _read_py(ast.parse("orderly.parameters(a=None, b=1)"))
    assert ab == {"parameters": {"a": None, "b": 1}}


def test_skip_over_uninteresting_code():
    code = """
def foo():
    pass
1
parameters(a=1)
print("Hello, World")
foo()
foo().parameters()
"""
    assert _read_py(ast.parse(code)) == {"parameters": {}}


def test_prevent_complex_types_in_parameters():
    msg = "Invalid value for argument 'a' to 'parameters()'"
    with pytest.raises(Exception, match=re.escape(msg)):
        _read_py(ast.parse("orderly.parameters(a=len)"))


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires 3.9 or later")
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


def test_can_read_report_with_no_parameters():
    path = Path("tests/orderly/examples/data/data.py")
    assert orderly_read(path) == {"parameters": {}}


def test_can_read_report_with_parameters():
    path = Path("tests/orderly/examples/parameters/parameters.py")
    assert orderly_read(path) == {"parameters": {"a": 1, "b": None}}


def test_throw_nice_error_with_kwargs():
    code = """
pars = {
  "x": 0,
  "y": False
}
orderly.parameters(**pars)
"""
    msg = re.escape("Passing parameters as **kwargs is not supported")
    with pytest.raises(Exception, match=msg):
        _read_py(ast.parse(code))
