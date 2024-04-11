import multiprocessing
import os

import pytest
from orderly.run import (
    _validate_parameters,
    _validate_src_directory,
    orderly_run,
)
from pytest_unordered import unordered

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
            "shared": {},
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
            "shared": {},
        }
    }
    assert meta.custom == custom
    assert meta.git is None


def test_can_use_implicit_resource_directory(tmp_path):
    root = helpers.create_temporary_root(tmp_path)

    report = tmp_path / "src" / "report"
    helpers.write_file(report / "data" / "numbers.txt", "1\n2\n3\n")

    code = """
with open("data/numbers.txt") as f:
    return str(sum(map(int, f.readlines())))
"""
    id, result = helpers.run_snippet("report", code, root)
    assert int(result) == 6

    meta = root.index.metadata(id)
    assert {f.path for f in meta.files} == {
        "report.py",
        os.path.join("data", "numbers.txt"),
    }
    assert meta.custom["orderly"]["role"] == [
        {"path": "report.py", "role": "orderly"},
    ]


def test_can_use_explicit_resource_directory(tmp_path):
    root = helpers.create_temporary_root(tmp_path)

    report = tmp_path / "src" / "report"
    helpers.write_file(report / "data" / "numbers.txt", "1\n2\n3\n")

    code = """
import orderly
orderly.resource("data")
with open("data/numbers.txt") as f:
    return str(sum(map(int, f.readlines())))
"""
    id, result = helpers.run_snippet("report", code, root)
    assert int(result) == 6

    meta = root.index.metadata(id)
    assert {f.path for f in meta.files} == {
        "report.py",
        os.path.join("data", "numbers.txt"),
    }
    assert meta.custom["orderly"]["role"] == [
        {"path": "report.py", "role": "orderly"},
        {"path": os.path.join("data", "numbers.txt"), "role": "resource"},
    ]


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
            "shared": {},
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


def test_can_use_shared_resources(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("shared", root)
    helpers.copy_shared_resources("numbers.txt", root)

    id = orderly_run("shared", root=tmp_path)
    meta = root.index.metadata(id)

    assert {el.path for el in meta.files} == {
        "shared.py",
        "result.txt",
        "shared_data.txt",
    }
    assert meta.custom == {
        "orderly": {
            "role": [
                {"path": "shared.py", "role": "orderly"},
                {"path": "shared_data.txt", "role": "shared"},
            ],
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
            "shared": {
                "shared_data.txt": "numbers.txt",
            },
        }
    }


def test_can_use_shared_resources_directory(tmp_path):
    # TODO(mrc-5241): This test uses os.path.join to form nested paths, which
    # on windows will use a backslash as the path separator. This matches the
    # implementation, but we probably want to revisit this at some point, and
    # normalize all paths in the metadata to use posix-style forward slashes.
    from os.path import join

    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("shared_dir", root)
    helpers.copy_shared_resources("data", root)

    id = orderly_run("shared_dir", root=tmp_path)
    meta = root.index.metadata(id)

    assert {el.path for el in meta.files} == {
        "shared_dir.py",
        "result.txt",
        join("shared_data", "numbers.txt"),
        join("shared_data", "weights.txt"),
    }
    assert meta.custom == {
        "orderly": {
            "role": unordered(
                [
                    {"path": "shared_dir.py", "role": "orderly"},
                    {
                        "path": join("shared_data", "weights.txt"),
                        "role": "shared",
                    },
                    {
                        "path": join("shared_data", "numbers.txt"),
                        "role": "shared",
                    },
                ]
            ),
            "artefacts": [],
            "description": {"display": None, "long": None, "custom": None},
            "shared": {
                join("shared_data", "numbers.txt"): join("data", "numbers.txt"),
                join("shared_data", "weights.txt"): join("data", "weights.txt"),
            },
        }
    }


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


def test_pycache_is_not_copied_from_source(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.touch_files(
        tmp_path / "src" / "report" / "foo.txt",
        tmp_path / "src" / "report" / "data" / "bar.txt",
        tmp_path / "src" / "report" / "__pycache__" / "baz.txt",
    )

    code = """
import glob
return glob.glob("**/*", recursive=True)
"""
    id, files = helpers.run_snippet("report", code, root)

    assert set(files) == {
        "report.py",
        "foo.txt",
        "data",
        os.path.join("data", "bar.txt"),
    }


def test_packet_files_excludes_pycache(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    code = """
import os
os.mkdir("data")
os.mkdir("__pycache__")
with open("foo.txt", "w"): pass
with open("data/bar.txt", "w"): pass
with open("__pycache__/baz.txt", "w"): pass
"""

    id, _ = helpers.run_snippet("report", code, root)

    meta = root.index.metadata(id)
    assert {f.path for f in meta.files} == {
        "report.py",
        "foo.txt",
        os.path.join("data", "bar.txt"),
    }

    packet = tmp_path / "archive" / "report" / id
    assert (packet / "data").exists()
    assert not (packet / "__pycache__").exists()
