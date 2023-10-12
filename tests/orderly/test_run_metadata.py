import helpers
import orderly
import pytest
from orderly.current import ActiveOrderlyContext

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
    assert sorted(res) == ["a/x", "a/y"]


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
