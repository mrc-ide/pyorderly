import os
import random
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.root import root_open


def create_random_packet(root, name="data", parameters=None, packet_id=None):
    d = [f"{random.random()}\n" for _ in range(10)]  # noqa: S311
    with TemporaryDirectory() as src:
        path_data = os.path.join(src, "data.txt")
        with open(path_data, "w") as f:
            f.writelines(d)
        p = Packet(root, src, name, id=packet_id, parameters=parameters)
        p.end()
        return p.id


def create_temporary_root(path, **kwargs):
    outpack_init(path, **kwargs)
    return root_open(path, False)


def create_orderly_root(path, examples):
    outpack_init(path)
    if isinstance(examples, str):
        examples = [examples]
    for ex in examples:
        path_src = path / "src" / ex
        path_src.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(f"tests/orderly/examples/{ex}", path_src)


def copy_examples(names, root):
    if isinstance(names, str):
        names = [names]
    for nm in names:
        path_src = root.path / "src" / nm
        path_src.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(Path("tests/orderly/examples") / nm, path_src)
