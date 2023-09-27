import helpers
import pytest

from outpack.init import outpack_init
from outpack.packet import Packet
from outpack.root import root_open


def test_can_add_simple_packet(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)

    src.mkdir(parents=True, exist_ok=True)
    with src.joinpath("a").open("w") as f:
        f.write("hello")

    p = Packet(root, src, "data")
    p.end()

    assert isinstance(p.id, str)
    assert p.name == "data"
    assert p.parameters == {}
    assert p.depends == []
    assert len(p.files) == 1
    assert p.files[0].path == "a"
    assert list(p.time.keys()) == ["start", "end"]
    assert p.git is None

    r = root_open(root, False)
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

    r = root_open(root, False)
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

    r = root_open(root, False)
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

    r = root_open(root, False)
    assert r.index.unpacked() == [p1.id, p2.id]


def test_can_add_custom_metadata(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    d = {"a": 1, "b": 2}
    assert list(p.custom.keys()) == []
    p.add_custom_metadata("key", d)
    assert list(p.custom.keys()) == ["key"]
    assert p.custom["key"] == d
    p.end()
    r = root_open(root, False)
    assert p.metadata.custom == {"key": d}
    assert r.index.metadata(p.id) == p.metadata


def test_error_raised_if_same_custom_data_readded(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    d = {"a": 1, "b": 2}
    p.add_custom_metadata("myapp", d)
    s = "metadata for 'myapp' has already been added for this packet"
    with pytest.raises(Exception, match=s):
        p.add_custom_metadata("myapp", d)


def test_can_mark_files_as_immutable(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p1 = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p1.mark_file_immutable("data.csv")
    p1.end()
    r = root_open(root, False)
    assert r.index.unpacked() == [p1.id]
    assert len(p1.metadata.files) == 1
    assert p1.metadata.files[0].path == "data.csv"


def test_can_validate_immutable_files_on_end(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p.mark_file_immutable("data.csv")
    p.end()
    assert len(p.metadata.files) == 1


def test_can_detect_deletion_of_immutable_file(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p1 = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p1.mark_file_immutable("data.csv")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n5,6\n")
    with pytest.raises(
        Exception,
        match="File was changed after being added: data.csv",
    ):
        p1.end()


def test_readding_files_rehashes_them(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p.mark_file_immutable("data.csv")
    src.joinpath("data.csv").unlink()
    with pytest.raises(Exception, match="File was deleted after being added"):
        p.end()


def test_can_detect_modification_of_immutable_file(tmp_path):
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p1 = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p1.mark_file_immutable("data.csv")
    p1.mark_file_immutable("data.csv")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n5,6\n")
    with pytest.raises(Exception, match="Hash of .+ does not match"):
        p1.mark_file_immutable("data.csv")
    p = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p.mark_file_immutable("data.csv")
    with open(src / "data.csv", "a") as f:
        f.write("5,6\n")
    with pytest.raises(Exception, match="File was changed after being added"):
        p.end()


def test_can_detect_modification_of_immutable_file_if_readded(tmp_path):
    """Test that it is the _first_ addition of the hash that matters."""
    root = tmp_path / "root"
    src = tmp_path / "src"
    outpack_init(root)
    src.mkdir(parents=True, exist_ok=True)
    p = Packet(root, src, "data")
    with open(src / "data.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    p.mark_file_immutable("data.csv")
    with open(src / "data.csv", "a") as f:
        f.write("5,6\n")
    with pytest.raises(Exception, match="Hash of .+ does not match"):
        p.mark_file_immutable("data.csv")


def test_helper(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    packet_id = helpers.create_random_packet(root)
    assert isinstance(packet_id, str)
    meta = root_open(tmp_path, False).index.metadata(packet_id)
    assert meta.name == "data"
