import os
import shutil

import pytest

from outpack.hash import hash_string, hash_file
from outpack.location_path import OutpackLocationPath
from outpack.test_util import create_temporary_root
from outpack.util import read_string


def test_can_construct_location_path_object(tmp_path):
    root = create_temporary_root(tmp_path)
    loc = OutpackLocationPath(root.path)
    dat = loc.list()
    assert dat is None


def test_location_path_requires_existing_directory(tmp_path):
    with pytest.raises(Exception) as e:
        OutpackLocationPath(tmp_path / "file")
    assert e.match("Expected 'path' to be an existing directory")


def test_location_path_requires_exact_root(tmp_path):
    root = create_temporary_root(tmp_path)
    subdir = root.path / "subdir"
    os.makedirs(subdir)
    with pytest.raises(Exception) as e:
        OutpackLocationPath(subdir)
    assert e.match("Did not find existing outpack root in")

    OutpackLocationPath(root.path)


def test_location_path_returns_list_of_packet_ids(tmp_path):
    root = create_temporary_root(tmp_path)
    shutil.copytree("example", tmp_path, dirs_exist_ok=True)
    path = root.path
    loc = OutpackLocationPath(path)

    dat = loc.list()
    packet_ids = os.listdir(tmp_path / ".outpack" / "location" / "local")
    assert all(packet_id in dat.keys() for packet_id in packet_ids)
    for packet_id, packet_location in dat.items():
        hash = read_string(path / ".outpack" / "metadata" / packet_id)
        assert packet_location.hash == str(hash_string(hash, "sha256"))


def test_location_path_can_return_metadata(tmp_path):
    root = create_temporary_root(tmp_path)
    shutil.copytree("example", tmp_path, dirs_exist_ok=True)
    path = root.path
    loc = OutpackLocationPath(path)

    local_packets = os.listdir(tmp_path / ".outpack" / "location" / "local")
    metadata_path = tmp_path / ".outpack" / "metadata"
    str = [
        read_string(metadata_path / packet_id) for packet_id in local_packets
    ]

    assert loc.metadata(local_packets[1])[local_packets[1]] == str[1]
    for idx, i in enumerate(loc.metadata(local_packets).values()):
        assert i == str[idx]
    first = [local_packets[0], local_packets[0]]
    # Only returns one item even though we pass 2 in, dicts cannot
    # have the same key multiple times
    assert loc.metadata(first) == {local_packets[0]: str[0]}


def test_requesting_nonexistent_metadata_errors(tmp_path):
    root = create_temporary_root(tmp_path)
    shutil.copytree("example", tmp_path, dirs_exist_ok=True)
    path = root.path
    loc = OutpackLocationPath(path)

    errs = ["20220317-125935-ee5fd50e", "20220317-130038-48ffb8ba"]
    with pytest.raises(Exception) as e:
        loc.metadata(errs[0])
    assert e.match("Some packet ids not found: '20220317-125935-ee5fd50e'")

    with pytest.raises(Exception) as e:
        loc.metadata(errs)
    assert e.match(
        "Some packet ids not found: '20220317-125935-ee5fd50e', "
        "'20220317-130038-48ffb8ba'"
    )

    local_packets = os.listdir(tmp_path / ".outpack" / "location" / "local")
    with pytest.raises(Exception) as e:
        loc.metadata([local_packets[0], errs[0], local_packets[1]])
    assert e.match("Some packet ids not found: '20220317-125935-ee5fd50e'")


def test_can_locate_files_from_store(tmp_path):
    root = create_temporary_root(tmp_path, use_file_store=True)
    path = root.path
    # TODO: Support file store in testing


def test_sensible_error_if_file_not_found_in_store(tmp_path):
    root = create_temporary_root(tmp_path, use_file_store=True)
    # TODO: Support file store in testing


def test_can_find_file_from_archive(tmp_path):
    root = create_temporary_root(tmp_path)
    shutil.copytree("example", tmp_path, dirs_exist_ok=True)
    path = root.path
    loc = OutpackLocationPath(path)

    idx = root.index.data()
    files = next(iter(idx.metadata.values())).files
    print(files)

    h = list(filter(lambda file: file.path == "result.txt", files))[0].hash
    dest = tmp_path / "dest"

    res = loc.fetch_file(h, dest)
    assert res == dest
    assert hash_file(dest) == h
