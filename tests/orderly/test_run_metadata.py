import os

import pytest

import pyorderly
from pyorderly.current import ActiveOrderlyContext
from pyorderly.outpack.packet import Packet
from pyorderly.outpack.util import transient_working_directory
from pyorderly.run import orderly_run

from .. import helpers


def test_resource_requires_that_files_exist_with_no_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        with pytest.raises(Exception, match="File does not exist:"):
            pyorderly.resource("a")

    (src / "a").touch()
    with transient_working_directory(src):
        res = pyorderly.resource("a")
    assert res == ["a"]


def test_resource_requires_relative_paths(tmp_path):
    (tmp_path / "a").touch()
    with pytest.raises(Exception, match="to be a relative path"):
        pyorderly.resource(str(tmp_path / "a"))


def test_resource_expands_lists_with_no_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "report"
    helpers.touch_files(src / "a" / "x", src / "a" / "y")

    with transient_working_directory(src):
        res = pyorderly.resource("a")
    assert set(res) == {os.path.join("a", "x"), os.path.join("a", "y")}


def test_resource_requires_file_exists_with_packet(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    helpers.touch_files(src / "a")

    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src) as active:
        with transient_working_directory(src):
            res = pyorderly.resource(["a"])

    assert active.resources == res


def test_shared_resource_can_copy_single_name(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_shared_resources("numbers.txt", root)

    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = pyorderly.shared_resource("numbers.txt")

    assert (src / "numbers.txt").exists()
    assert res == {"numbers.txt": "numbers.txt"}


def test_shared_resource_can_copy_multiple_names(tmp_path):
    from os.path import join

    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_shared_resources(["numbers.txt", "data"], root)

    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = pyorderly.shared_resource(["numbers.txt", "data"])

    assert (src / "numbers.txt").exists()
    assert (src / "data" / "weights.txt").exists()
    assert (src / "data" / "numbers.txt").exists()
    assert res == {
        "numbers.txt": "numbers.txt",
        join("data", "numbers.txt"): join("data", "numbers.txt"),
        join("data", "weights.txt"): join("data", "weights.txt"),
    }


def test_shared_resource_can_rename_files_when_copying(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_shared_resources(["numbers.txt", "data"], root)

    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = pyorderly.shared_resource(
            {
                "foo.txt": "numbers.txt",
                "bar.txt": os.path.join("data", "weights.txt"),
            }
        )

    assert (src / "foo.txt").exists()
    assert (src / "bar.txt").exists()

    assert not (src / "numbers.txt").exists()
    assert not (src / "data").exists()

    assert res == {
        "foo.txt": "numbers.txt",
        "bar.txt": os.path.join("data", "weights.txt"),
    }


def test_shared_resource_requires_relative_paths(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_shared_resources(["numbers.txt"], root)

    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)

    with transient_working_directory(src):
        with pytest.raises(Exception, match="to be a relative path"):
            pyorderly.shared_resource(str(tmp_path / "shared" / "numbers.txt"))


def test_artefact_is_allowed_without_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = pyorderly.artefact("a", "b")
    assert res == ["b"]


def test_can_add_description(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src"
    src.mkdir()
    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src) as active:
        pyorderly.description(
            long="long description",
            display="display description",
            custom={"a": 1, "b": "foo"},
        )
    assert active.description.long == "long description"
    assert active.description.display == "display description"
    assert active.description.custom == {"a": 1, "b": "foo"}


def test_cant_add_description_twice(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src):
        pyorderly.description(long="long description")
        with pytest.raises(Exception, match="Only one call to 'description'"):
            pyorderly.description(display="display description")


def test_can_run_description_without_packet_with_no_effect(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = pyorderly.description(
            long="long description",
            display="display description",
            custom={"a": 1, "b": "foo"},
        )
    assert res is None


def test_can_use_dependency(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["data", "depends"], root)
    id1 = orderly_run("data", root=tmp_path)
    src = tmp_path / "draft" / "depends" / "some-id"
    src.mkdir(parents=True)
    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src):
        with transient_working_directory(src):
            files = {"input.txt": "result.txt"}
            result = pyorderly.dependency(None, "latest", files)

    assert result.id == id1
    assert result.name == "data"
    assert len(result.files) == 1
    assert result.files["input.txt"].path == "result.txt"
    assert src.joinpath("input.txt").exists()


def test_dependency_must_have_empty_name(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["data", "depends"], root)
    orderly_run("data", root=tmp_path)
    src = tmp_path / "src" / "depends"
    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src):
        with transient_working_directory(src):
            with pytest.raises(Exception, match="'name' must be None"):
                pyorderly.dependency("data", "latest", {})


def test_can_use_dependency_without_packet(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["data", "depends"], root)
    id1 = orderly_run("data", root=tmp_path)
    src = tmp_path / "src" / "depends"
    with transient_working_directory(src):
        files = {"input.txt": "result.txt"}
        result = pyorderly.dependency(None, "latest", files)

    assert result.id == id1
    assert result.name == "data"
    assert len(result.files) == 1
    assert result.files["input.txt"].path == "result.txt"
    assert src.joinpath("input.txt").exists()


def test_can_use_parameters(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)

    p = Packet(root, src, "tmp", parameters={"x": 1, "y": "foo"})
    with ActiveOrderlyContext(p, src):
        with transient_working_directory(src):
            params = pyorderly.parameters(x=None, y=None)
            assert params == pyorderly.Parameters(x=1, y="foo")


def test_can_use_parameters_without_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)

    with transient_working_directory(src):
        p = pyorderly.parameters(x=1, y="foo")
        assert p == pyorderly.Parameters(x=1, y="foo")

        with pytest.raises(
            Exception, match="No value was specified for parameter x."
        ):
            pyorderly.parameters(x=None, y="foo")

        with pytest.raises(
            Exception, match="No value was specified for parameters x, y."
        ):
            pyorderly.parameters(x=None, y=None)
