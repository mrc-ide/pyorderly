import random
import shutil
import string
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional

from orderly.run import orderly_run

from outpack.init import outpack_init
from outpack.metadata import MetadataCore, PacketDepends
from outpack.packet import Packet
from outpack.root import root_open
from outpack.schema import outpack_schema_version


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
        except BaseException as e:
            print("Error in packet creation: ", e)
            p.end(insert=False)
        else:
            p.end(insert=True)


def create_random_packet(root, name="data", *, parameters=None, packet_id=None):
    d = [f"{random.random()}\n" for _ in range(10)]

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
    for i in range(length):
        name = chr(i + ord("a"))
        with create_packet(root, name) as p:
            if base is not None:
                p.use_dependency(base, {"input.txt": "data.txt"})

            d = [f"{random.random()}\n" for _ in range(10)]
            with open(p.path / "data.txt", "w") as f:
                f.writelines(d)

            ids[name] = p.id
            base = p.id

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


def create_metadata_depends(id: str, depends: Optional[List[str]] = None):
    if depends is None:
        depends = []
    dependencies = [
        PacketDepends(dependency_id, "", []) for dependency_id in depends
    ]
    return {
        id: MetadataCore(
            outpack_schema_version(),
            id,
            "name_" + random_characters(4),
            {},
            {},
            [],
            dependencies,
            None,
            None,
        )
    }


def random_characters(n):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(n)
    )


# Like Rs rep function, useful for setting up test values
def rep(x, each):
    ret = []
    if isinstance(each, int):
        each = [each] * len(x)
    if len(x) != len(each):
        msg = (
            "Repeats must be int or same length as the thing you want to repeat"
        )
        raise Exception(msg)
    for item, times in zip(x, each):
        ret.extend([item] * times)

    return ret


def touch_files(*files):
    """
    Create empty files at the given paths.

    If necessary, parent directories are created for each file.
    """
    for f in files:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.touch()


def write_file(path, text):
    """
    Write text to a path.

    If necessary, parent directories are created for each file.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def run_snippet(name, code, root, **kwargs):
    """Run a snippet of Python code as an Orderly report."""
    write_file(root.path / "src" / name / f"{name}.py", code)
    return orderly_run(name, root=root, **kwargs)
