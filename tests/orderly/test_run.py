import multiprocessing

import pytest
from orderly.run import (
    _validate_parameters,
    _validate_src_directory,
    orderly_run,
)

from .. import helpers


## We're going to need a small test helper module here at some point,
## unfortunately pytest makes that totally unobvious how we do it, but
## we'll get there. For now inline the code as we use it.
def test_can_run_simple_example(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("data", root)

    id = orderly_run("data", root=root)
    path_res = tmp_path / "archive" / "data" / id
    assert path_res.exists()
    assert not (tmp_path / "draft" / "data" / id).exists()

    # TODO: need a nicer way of doing this, one that would be part of
    # the public API.
    meta = root.index.metadata(id)
    assert meta.id == id
    assert meta.name == "data"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 2
    assert {el.path for el in meta.files} == {"data.py", "result.txt"}
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [{"path": "data.py", "role": "orderly"}],
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_failed_reports_are_not_saved(tmp_path):
    root = helpers.create_temporary_root(tmp_path)

    with pytest.raises(Exception, match="Running orderly report failed!") as e:
        code = "raise Exception('Some error')"
        helpers.run_snippet("data", code, root)

    assert str(e.value.__cause__) == "Some error"

    assert not (tmp_path / "archive" / "data").exists()
    assert len(root.index.unpacked()) == 0

    assert (tmp_path / "draft" / "data").exists()
    contents = list((tmp_path / "draft" / "data").iterdir())
    assert len(contents) == 1
    assert contents[0].joinpath("outpack.json").exists()


def test_validate_report_src_directory(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    path_src = tmp_path / "src"
    path_src.mkdir()

    x = path_src / "x"
    with pytest.raises(Exception, match="The path '.+/x' does not exist"):
        _validate_src_directory("x", root)

    x.mkdir()
    with pytest.raises(
        Exception,
        match="The path '.+/x' exists but does not contain 'x.py'",
    ):
        _validate_src_directory("x", root)

    y = path_src / "y"
    y.touch()
    with pytest.raises(
        Exception, match="The path '.+/y' exists but is not a directory"
    ):
        _validate_src_directory("y", root)

    # Finally, the happy path
    (x / "x.py").touch()
    assert _validate_src_directory("x", root) == (x, "x.py")


def test_can_run_example_with_resource(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("resource", root)

    id = orderly_run("resource", root=root)

    meta = root.index.metadata(id)
    assert meta.id == id
    assert meta.name == "resource"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 3
    assert {el.path for el in meta.files} == {
        "resource.py",
        "result.txt",
        "numbers.txt",
    }
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [
                {"path": "resource.py", "role": "orderly"},
                {"path": "numbers.txt", "role": "resource"},
            ],
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_error_if_script_is_modified(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    code = """
with open("report.py", "w") as f:
    f.write("new text")
"""

    with pytest.raises(
        Exception, match="File was changed after being added: report.py"
    ):
        helpers.run_snippet("report", code, root)


def test_error_if_resource_is_modified(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    code = """
import orderly
orderly.resource("data.txt")
with open("data.txt", "w") as f:
    f.write("new data")
"""

    helpers.write_file(tmp_path / "src" / "report" / "data.txt", "old data")

    with pytest.raises(
        Exception, match="File was changed after being added: data.txt"
    ):
        helpers.run_snippet("report", code, root)


def test_can_run_example_with_artefact(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("artefact", root)

    id = orderly_run("artefact", root=root)
    path_res = tmp_path / "archive" / "artefact" / id
    assert path_res.exists()
    assert not (tmp_path / "draft" / "artefact" / id).exists()
    # TODO: need a nicer way of doing this, one that would be part of
    # the public API.
    meta = root.index.metadata(id)
    assert meta.id == id
    assert meta.name == "artefact"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 2
    assert {el.path for el in meta.files} == {"artefact.py", "result.txt"}
    assert meta.depends == []
    custom = {
        "orderly": {
            "role": [{"path": "artefact.py", "role": "orderly"}],
            "artefacts": [{"name": "Random numbers", "files": ["result.txt"]}],
            "description": {"display": None, "long": None, "custom": None},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_can_error_if_artefacts_not_produced(tmp_path):
    root = helpers.create_temporary_root(tmp_path)

    code = """
import orderly
orderly.artefact("something", "a")
"""
    with pytest.raises(
        Exception, match="Script did not produce the expected artefacts: 'a'"
    ):
        helpers.run_snippet("report", code, root)

    code = """
import orderly
orderly.artefact("something", "a")
orderly.artefact("something", ["c", "b"])
"""
    with pytest.raises(
        Exception,
        match="Script did not produce the expected artefacts: 'a', 'b', 'c'",
    ):
        helpers.run_snippet("report", code, root)


def test_can_run_with_description(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("description", root)

    id = orderly_run("description", root=tmp_path)
    meta = root.index.metadata(id)
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
    assert meta.depends[0].query == "latest"
    assert len(meta.depends[0].files)
    assert meta.depends[0].files[0].here == "input.txt"
    assert meta.depends[0].files[0].there == "result.txt"


def test_can_run_with_parameters(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)
    id = orderly_run("parameters", parameters={"b": 2}, root=tmp_path)
    with open(tmp_path / "archive" / "parameters" / id / "result.txt") as f:
        result = f.read()
    assert result == "a: 1\nb: 2\n"
    meta = root.index.metadata(id)
    assert meta.parameters == {"a": 1, "b": 2}


def test_can_validate_parameters():
    assert _validate_parameters({}, {}) == {}
    assert _validate_parameters(None, {}) == {}
    assert _validate_parameters({"a": 10}, {"a": 1}) == {"a": 10}
    assert _validate_parameters({"a": 10}, {"a": None}) == {"a": 10}
    assert _validate_parameters({}, {"a": 1}) == {"a": 1}
    assert _validate_parameters(None, {"a": 1}) == {"a": 1}
    with pytest.raises(Exception, match="Parameters given, but none declared"):
        _validate_parameters({"a": 1}, {})
    with pytest.raises(Exception, match="Missing parameters: a"):
        _validate_parameters({}, {"a": None})
    with pytest.raises(Exception, match="Missing parameters: ., .$"):
        _validate_parameters({}, {"a": None, "b": None})
    with pytest.raises(Exception, match="Missing parameters: b$"):
        _validate_parameters({}, {"a": 1, "b": None})
    with pytest.raises(Exception, match="Unknown parameters: b$"):
        _validate_parameters({"b": 1}, {"a": 1})
    err = "Expected parameter a to be a simple value"
    with pytest.raises(Exception, match=err):
        _validate_parameters({"a": str}, {"a": 1})


@pytest.mark.parametrize("method", multiprocessing.get_all_start_methods())
def test_can_run_multiprocessing(tmp_path, method):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("mp", root)
    id = orderly_run("mp", root=root, parameters={"method": method})

    meta = root.index.metadata(id)
    assert meta.custom["orderly"]["artefacts"] == [
        {"name": "Squared numbers", "files": ["result.txt"]}
    ]
