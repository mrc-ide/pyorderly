import shutil

import helpers
import pytest
from orderly.run import _validate_src_directory, orderly_run

from outpack.init import outpack_init
from outpack.metadata import read_metadata_core
from outpack.root import root_open

import helpers


## We're going to need a small test helper module here at some point,
## unfortunately pytest makes that totally unobvious how we do it, but
## we'll get there. For now inline the code as we use it.
def test_can_run_simple_example(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "data"
    path_src.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        "tests/orderly/examples/data/orderly.py", path_src / "orderly.py"
    )
    res = orderly_run("data", root=path)
    path_res = path / "archive" / "data" / res
    assert path_res.exists()
    assert not (path / "draft" / "data" / res).exists()
    # TODO: need a nicer way of doing this, one that would be part of
    # the public API.
    meta = root_open(tmp_path, False).index.metadata(res)
    assert meta.id == res
    assert meta.name == "data"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 2
    assert {el.path for el in meta.files} == {"orderly.py", "result.txt"}
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [{"path": "orderly.py", "role": "orderly"}],
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_failed_reports_are_not_saved(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "data"
    path_src.mkdir(parents=True, exist_ok=True)
    with open(path_src / "orderly.py", "w") as f:
        f.write("raise Exception('Some error')")
    with pytest.raises(Exception, match="Running orderly report failed!"):
        orderly_run("data", root=path)
    assert not (tmp_path / "archive" / "data").exists()
    assert len(root_open(tmp_path, False).index.unpacked()) == 0

    assert (tmp_path / "draft" / "data").exists()
    contents = list((tmp_path / "draft" / "data").iterdir())
    assert len(contents) == 1
    assert contents[0].joinpath("outpack.json").exists()


def test_validate_report_src_directory(tmp_path):
    path = outpack_init(tmp_path)
    root = root_open(path, False)
    path_src = path / "src"
    path_src.mkdir()

    x = path_src / "x"
    with pytest.raises(Exception, match="The path '.+/x' does not exist"):
        _validate_src_directory("x", root)
    x.mkdir()
    with pytest.raises(
        Exception,
        match="The path '.+/x' exists but does not contain 'orderly.py'",
    ):
        _validate_src_directory("x", root)
    y = path_src / "y"
    with open(y, "w"):
        pass
    with pytest.raises(
        Exception, match="The path '.+/y' exists but is not a directory"
    ):
        _validate_src_directory("y", root)
    # Finally, the happy path
    with open(x / "orderly.py", "w"):
        pass
    assert _validate_src_directory("x", root) == x


def test_can_run_example_with_resource(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "resource"
    path_src.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree("tests/orderly/examples/resource", path_src)
    res = orderly_run("resource", root=path)

    meta = root_open(tmp_path, False).index.metadata(res)
    assert meta.id == res
    assert meta.name == "resource"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 3
    assert {el.path for el in meta.files} == {
        "orderly.py",
        "result.txt",
        "numbers.txt",
    }
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [
                {"path": "orderly.py", "role": "orderly"},
                {"path": "numbers.txt", "role": "resource"},
            ],
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_can_run_example_with_artefact(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "artefact"
    path_src.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        "tests/orderly/examples/artefact/orderly.py", path_src / "orderly.py"
    )
    res = orderly_run("artefact", root=path)
    path_res = path / "archive" / "artefact" / res
    assert path_res.exists()
    assert not (path / "draft" / "artefact" / res).exists()
    # TODO: need a nicer way of doing this, one that would be part of
    # the public API.
    meta = root_open(tmp_path, False).index.metadata(res)
    assert meta.id == res
    assert meta.name == "artefact"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 2
    assert {el.path for el in meta.files} == {"orderly.py", "result.txt"}
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [{"path": "orderly.py", "role": "orderly"}],
            "artefacts": [{"name": "Random numbers", "files": ["result.txt"]}],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_can_error_if_artefacts_not_produced(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "resource"
    path_src.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree("tests/orderly/examples/resource", path_src)
    orderly_run("resource", root=path)
    with open(path_src / "orderly.py", "a") as f:
        f.write("orderly.artefact('something', 'a')\n")
    with pytest.raises(
        Exception, match="Script did not produce the expected artefacts: 'a'"
    ):
        orderly_run("resource", root=path)
    assert (tmp_path / "draft" / "resource").exists()
    contents = list((tmp_path / "draft" / "resource").iterdir())
    assert len(contents) == 1
    assert contents[0].joinpath("outpack.json").exists()
    meta = read_metadata_core(contents[0].joinpath("outpack.json"))
    assert meta.name == "resource"
    assert meta.custom == {}
    with open(path_src / "orderly.py", "a") as f:
        f.write("orderly.artefact('something else', ['c', 'b'])\n")
    with pytest.raises(
        Exception,
        match="Script did not produce the expected artefacts: 'a', 'b', 'c'",
    ):
        orderly_run("resource", root=path)


def test_can_run_with_description(tmp_path):
    helpers.create_orderly_root(tmp_path, ["description"])
    id = orderly_run("description", root=tmp_path)
    meta = root_open(tmp_path, False).index.metadata(id)
    assert meta.custom["orderly"]["description"] == {
        "display": "Some report",
        "long": None,
        "custom": None,
    }


def test_can_run_simple_dependency(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["data", "depends"], root)
    id1 = orderly_run("data", root=tmp_path)
    id2 = orderly_run("depends", root=tmp_path)
    meta = root.index.metadata(id2)
    assert len(meta.depends) == 1
    assert meta.depends[0].packet == id1
    assert meta.depends[0].query == "latest()"
    assert len(meta.depends[0].files)
    assert meta.depends[0].files[0].here == "input.txt"
    assert meta.depends[0].files[0].there == "result.txt"
