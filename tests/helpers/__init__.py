import random
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.root import root_open


@contextmanager
def create_packet(root, name, *, packet_id=None, parameters=None):
    """
    Create an Outpack packet.

    This function should be used as a context manager in a `with` block. The
    packet can be populated in the block's body. The packet gets completed and
    added to the repository when the context manager is exited.
    """
    with TemporaryDirectory() as src:
        p = Packet(root, src, name, id=packet_id, parameters=parameters)
        try:
            yield p
        except BaseException:
            p.end(insert=False)
        else:
            p.end(insert=True)


def create_random_packet(root, name="data", *, parameters=None, packet_id=None):
    d = [f"{random.random()}\n" for _ in range(10)]  # noqa: S311

    with create_packet(
        root, name=name, parameters=parameters, packet_id=packet_id
    ) as p:
        path_data = p.path / "data.txt"
        with open(path_data, "w") as f:
            f.writelines(d)

    return p.id


def create_temporary_root(path, **kwargs):
    outpack_init(path, **kwargs)
    return root_open(path, False)


def copy_examples(names, root):
    if isinstance(names, str):
        names = [names]
    for nm in names:
        path_src = root.path / "src" / nm
        path_src.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(Path("tests/orderly/examples") / nm, path_src)
