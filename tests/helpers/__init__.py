import os
import random
from tempfile import TemporaryDirectory

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.root import root_open


def create_random_packet(root, name="data", parameters=None,
                         packet_id=None):
    d = [f"{random.random()}\n" for _ in range(10)]  # noqa: S311
    with TemporaryDirectory() as src:
        path_data = os.path.join(src, "data.txt")
        with open(path_data, "w") as f:
            f.writelines(d)
        p = Packet(root, src, name, id=packet_id, parameters=parameters)
        p.end()
        return p.id


def create_temporary_root(path, *, use_file_store=False):
    outpack_init(path, use_file_store=use_file_store)
    return root_open(path, False)
