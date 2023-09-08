import orderly
import pytest

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.util import transient_working_directory
from orderly.current import ActiveOrderlyPacket, get_active_packet


def test_resource_requires_that_files_exist_with_no_packet(tmp_path):
    with transient_working_directory(tmp_path):
        with pytest.raises(Exception, match="File does not exist:"):
            orderly.resource("a")
    with open(tmp_path / "a", "w"):
        pass
    with transient_working_directory(tmp_path):
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
    sub = tmp_path / "a"
    sub.mkdir()
    with open(sub / "x", "w") as f:
        pass
    with open(sub / "y", "w") as f:
        pass

    with transient_working_directory(tmp_path):
        res = orderly.resource("a")
    assert sorted(res) == ["a/x", "a/y"]

def test_resource_requires_file_exists_with_packet(tmp_path):
    root = tmp_path / "root"
    outpack_init(root)
    src = tmp_path / "src"
    src.mkdir()
    with open(src / "a", "w") as f:
        pass

    p = Packet(root, src, "tmp")
    # TODO: ActiveOrderlyPacket should do this directory shift I think
    with ActiveOrderlyPacket(p, src) as active:
        with transient_working_directory(src):
            res = orderly.resource(["a"])

    assert active.resources == res
