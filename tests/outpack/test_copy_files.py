import re

import pytest

from pyorderly.outpack.copy_files import (
    _plan_copy_files,
    _validate_files,
    copy_files,
)
from pyorderly.outpack.location_pull import outpack_location_pull_metadata
from pyorderly.outpack.search_options import SearchOptions

from .. import helpers


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
    assert len(result.files) == 1
    assert result.files["here.txt"].path == "data.txt"


def test_can_error_if_plan_impossible(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    id = helpers.create_random_packet(root)
    with pytest.raises(Exception, match="does not contain the requested path"):
        _plan_copy_files(root, id, {"here.txt": "other.txt"})
    with pytest.raises(Exception, match="Directories not yet supported"):
        _plan_copy_files(root, id, {"here.txt": "data/"})


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_copy_files_from_remote(tmp_path, use_file_store):
    root = helpers.create_temporary_roots(
        tmp_path, add_location=True, use_file_store=use_file_store
    )
    id = helpers.create_random_packet(root["src"])
    outpack_location_pull_metadata("src", root=root["dst"])

    output = tmp_path / "output"
    output.mkdir()

    msg = f"File `data.txt` from packet {id} is not available locally"
    with pytest.raises(Exception, match=re.escape(msg)):
        copy_files(
            id,
            {"here.txt": "data.txt"},
            output,
            options=SearchOptions(allow_remote=False),
            root=root["dst"],
        )

    copy_files(
        id,
        {"here.txt": "data.txt"},
        output,
        options=SearchOptions(allow_remote=True),
        root=root["dst"],
    )
