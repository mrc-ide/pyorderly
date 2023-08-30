import orderly
import pytest

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.util import transient_working_directory
from orderly.current import ActivePacket


def test_resource_requires_that_files_exist_with_no_packet(tmp_path):
    path = tmp_path / "a"
    with transient_working_directory(tmp_path):
        with pytest.raises(Exception, match="File does not exist:"):
            orderly.resource(path)
    with open(path, "w"):
        pass
    with transient_working_directory(tmp_path):
        orderly.resource(path)


# def test_resource_expands_lists(tmp_path):
#     root = tmp_path / "root"
#     outpack_init(root)
#     src = tmp_path / "src"
#     src.mkdir()
#     with open(src / "a", "w") as f:
#         pass
#     with open(src / "b", "w") as f:
#         pass
#     p = Packet(root, src, "tmp")
#     with ActivePacket(p, src):
#         with transient_working_directory(src):
#             orderly.resource(["a"])
#             breakpoint()
#             pass
