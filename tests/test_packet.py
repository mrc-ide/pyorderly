import pytest

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.root import Root


def test_can_add_simple_packet(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)

    src.mkdir(parents=True, exist_ok=True)
    with src.joinpath("a").open("w") as f:
        f.write("hello")

    p = Packet(root, src, "data")
    p.end()

    assert type(p.id) == str
    assert p.name == "data"
    assert p.parameters == {}
    assert p.depends == []
    assert len(p.files) == 1
    assert p.files[0].path == "a"
    assert list(p.time.keys()) == ["begin", "end"]
    assert p.git is None

    r = Root(root)
    assert r.index.unpacked() == [p.id]
    assert r.index.metadata(p.id) == p.metadata
    assert (root / "archive" / "data" / p.id / "a").exists()


def test_can_add_packet_to_store(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root, use_file_store=True, path_archive=None)

    src.mkdir(parents=True, exist_ok=True)
    with src.joinpath("a").open("w") as f:
        f.write("hello")
    with src.joinpath("b").open("w") as f:
        f.write("goodbye")

    p = Packet(root, src, "data")
    p.end()

    assert len(p.files) == 2

    r = Root(root)
    assert len(r.files.ls()) == 2
    assert sorted([str(h) for h in r.files.ls()]) == sorted(
        [f.hash for f in p.files]
    )


def test_cant_end_packet_twice(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root, use_file_store=True, path_archive=None)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    p.end()
    with pytest.raises(Exception, match="Packet '.+' already ended"):
        p.end()


def test_can_cancel_packet(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)

    src.mkdir(parents=True, exist_ok=True)
    with src.joinpath("a").open("w") as f:
        f.write("hello")

    p = Packet(root, src, "data")
    p.end(insert=False)

    r = Root(root)
    assert len(r.index.unpacked()) == 0
    assert src.joinpath("outpack.json").exists()


def test_can_insert_a_packet_into_existing_root(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)

    src.mkdir(parents=True, exist_ok=True)
    with src.joinpath("a").open("w") as f:
        f.write("hello")

    p1 = Packet(root, src, "data")
    p1.end()
    p2 = Packet(root, src, "data")
    p2.end()

    r = Root(root)
    assert r.index.unpacked() == [p1.id, p2.id]
