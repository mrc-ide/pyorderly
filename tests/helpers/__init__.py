import os
import random
import shutil
import string
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from outpack.ids import outpack_id
from outpack.init import outpack_init
from outpack.metadata import MetadataCore, PacketDepends
from outpack.packet import Packet
from outpack.root import root_open
from outpack.schema import outpack_schema_version
from outpack.util import run_script


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


## Create a chain of packets a, b, c, ... that depend on each other
def create_random_packet_chain(root, length, base=None):
    ids = {}
    with TemporaryDirectory() as src:
        for i in range(length):
            name = chr(i + ord("a"))
            packet_id = outpack_id()
            ids[name] = packet_id
            packet_path = Path(src) / name / packet_id
            os.makedirs(packet_path)

            packet = Packet(
                root, packet_path, name, id=packet_id, locate=False
            )

            if i == 0 and base is None:
                with open(packet_path / "data.txt", "w") as f:
                    f.write("0")
            else:
                lines = ["with open('input.txt', 'r') as f:",
                         "    data = f.read()",
                         "with open('data.txt', 'w') as f:",
                         f"    f.write(data + '{i}')"]
                with open(packet_path / "orderly.py", "w") as f:
                    f.write('\n'.join(lines) + '\n')

                if i > 0:
                    id_use = ids[chr(i - 1 + ord("a"))]
                else:
                    id_use = base

                if id_use is not None:
                    packet.use_dependency(id_use, {"input.txt": "data.txt"})

                run_script(packet_path, "orderly.py", None)

            packet.end(insert=True)

    return ids


def create_temporary_root(path, **kwargs):
    outpack_init(path, **kwargs)
    return root_open(path, locate=False)


def create_temporary_roots(path, location_names=None, **kwargs):
    if location_names is None:
        location_names = ["src", "dst"]
    root = {}
    for name in location_names:
        root[name] = create_temporary_root(path / name, **kwargs)
    return root


def copy_examples(names, root):
    if isinstance(names, str):
        names = [names]
    for nm in names:
        path_src = root.path / "src" / nm
        path_src.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(Path("tests/orderly/examples") / nm, path_src)


def create_metadata_depends(id: str, depends: List[str] = None):
    if depends is None:
        depends = []
    dependencies = [PacketDepends(dependency_id, "", [])
                    for dependency_id in depends]
    return {id: MetadataCore(
        outpack_schema_version(),
        id,
        "name_" + random_characters(4),
        {},
        {},
        [],
        dependencies,
        None,
        None
    )}


def random_characters(n):
    return ''.join(random.choice(string.ascii_letters + string.digits)
                   for _ in range(n))
