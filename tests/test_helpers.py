import helpers
import pytest

from outpack.helpers import _plan_copy_files, _validate_files


def test_can_clean_file_input():
    assert _validate_files("x") == {"x": "x"}
    assert _validate_files(["x"]) == {"x": "x"}
    assert _validate_files(["x", "y"]) == {"x": "x", "y": "y"}
    assert _validate_files({"x": "x", "y": "y"}) == {"x": "x", "y": "y"}


def test_can_create_plan(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    id = helpers.create_random_packet(root)
    result = _plan_copy_files(root, id, {"here.txt": "data.txt"})
    assert result.id == id
    assert result.name == "data"
    assert result.files == {"here.txt": "data.txt"}


def test_can_error_if_plan_impossible(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    id = helpers.create_random_packet(root)
    with pytest.raises(Exception, match="does not contain the requested path"):
        _plan_copy_files(root, id, {"here.txt": "other.txt"})
    with pytest.raises(Exception, match="Directories not yet supported"):
        _plan_copy_files(root, id, {"here.txt": "data/"})
