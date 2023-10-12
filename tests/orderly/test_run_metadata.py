import sys

import helpers
import orderly
import pytest
from orderly.current import ActiveOrderlyContext
from orderly.run import orderly_run

from outpack.packet import Packet
from outpack.util import transient_working_directory


def test_resource_requires_that_files_exist_with_no_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        with pytest.raises(Exception, match="File does not exist:"):
            orderly.resource("a")
    with open(src / "a", "w"):
        pass
    with transient_working_directory(src):
        res = orderly.resource("a")
    assert res == ["a"]


def test_resource_requires_relative_paths(tmp_path):
    with transient_working_directory(tmp_path):
        with pytest.raises(Exception, match="File does not exist:"):
            orderly.resource("a")
    with open(tmp_path / "a", "w"):
        pass
    with pytest.raises(Exception, match="to be a relative path"):
        orderly.resource(str(tmp_path / "a"))


def test_resource_expands_lists_with_no_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    sub = src / "a"
    sub.mkdir()
    with open(sub / "x", "w"):
        pass
    with open(sub / "y", "w"):
        pass

    with transient_working_directory(src):
        res = orderly.resource("a")
    expected = {"windows": ["a\\x", "a\\y"], "unix": ["a/x", "a/y"]}
    platform = "windows" if sys.platform.startswith("win") else "unix"
    assert sorted(res) == expected[platform]


def test_resource_requires_file_exists_with_packet(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with open(src / "a", "w"):
        pass

    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src) as active:
        with transient_working_directory(src):
            res = orderly.resource(["a"])

    assert active.resources == res


def test_artefact_is_allowed_without_packet(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = orderly.artefact("a", "b")
    assert res == ["b"]


def test_can_add_description(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src"
    src.mkdir()
    p = Packet(root, src, "tmp")
    with ActiveOrderlyContext(p, src) as active:
        orderly.description(
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
        orderly.description(long="long description")
        with pytest.raises(Exception, match="Only one call to 'description'"):
            orderly.description(display="display description")


def test_can_run_description_without_packet_with_no_effect(tmp_path):
    helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    with transient_working_directory(src):
        res = orderly.description(
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
            res = orderly.dependency(None, "latest", files)
    assert res.id == id1
    assert res.name == "data"
    assert res.files == files
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
                orderly.dependency("data", "latest", {})


def test_can_use_dependency_without_packet(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["data", "depends"], root)
    id1 = orderly_run("data", root=tmp_path)
    src = tmp_path / "src" / "depends"
    with transient_working_directory(src):
        files = {"input.txt": "result.txt"}
        res = orderly.dependency(None, "latest", files)
    assert res.id == id1
    assert res.name == "data"
    assert res.files == files
    assert src.joinpath("input.txt").exists()
