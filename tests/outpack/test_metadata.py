import sys

import pytest

from pyorderly.outpack.metadata import (
    PacketDependsPath,
    PacketFile,
    read_metadata_core,
    read_packet_location,
)


def test_can_read_metadata():
    d = read_metadata_core("example/.outpack/metadata/20230807-152344-ee606dce")
    assert d.id == "20230807-152344-ee606dce"
    assert d.name == "data"
    assert d.parameters == {"a": 1, "b": "x"}
    assert d.time == {"start": 1691421824.9377, "end": 1691421825.0305}
    assert len(d.files) == 3
    assert d.files[0] == PacketFile(
        path="data.csv",
        size=21.0,
        hash="sha256:2a85eb5a027c8d2255e672d1592cc38c82cc0b08279b545a573ceccce9eb27cd",
    )
    assert d.files[1] == PacketFile(
        path="log.json",
        size=896.0,
        hash="sha256:cfb4d4931dcbaef0e0edb2f77b8ed75a15e8b98eb4ce935909fe3c0f277440c9",
    )
    assert d.files[2] == PacketFile(
        path="orderly.R",
        size=104.0,
        hash="sha256:6ee430041e1b83b72bac79a2e548a0450117dc763cc6ca71aca3bd2dbda6f520",
    )
    assert len(d.depends) == 0
    assert d.git.sha == "407c7343fdfc1ff4d949b6719bd6977b96cf4fe6"
    assert d.git.branch == "config"
    assert d.git.url == ["git@github.com:reside-ic/outpack-py.git"]


def test_can_read_metadata_with_depends():
    d = read_metadata_core("example/.outpack/metadata/20230814-163026-ac5900c0")
    assert len(d.depends) == 1
    assert d.depends[0].packet == "20230807-152345-0e0662d0"
    assert (
        d.depends[0].query == 'latest(parameter:a == this:x && name == "data")'
    )
    assert d.depends[0].files == [
        PacketDependsPath(here="incoming.csv", there="data.csv")
    ]


def test_can_read_location():
    d = read_packet_location(
        "example/.outpack/location/local/20230814-163026-ac5900c0"
    )
    assert d.packet == "20230814-163026-ac5900c0"
    assert d.time == 1692030626.7001
    assert (
        d.hash
        == "sha256:94809b0a23e2e1986304f112726ff20401000d51026f8fa85c7501ecd340b323"
    )


def test_can_create_packet_file_metadata_from_file():
    directory = "example/archive/data/20230807-152344-ee606dce"
    path = "data.csv"
    res = PacketFile.from_file(directory, path, "sha256")
    # Due to file endings, windows file is 2 bytes longer on git clone, with different hash.
    expected_hash = {
        "windows": "sha256:d8a04fe36644edecc2581a43b6eb4d9fd7adb6702d80dde83129b43433cd93c4",
        "unix": "sha256:2a85eb5a027c8d2255e672d1592cc38c82cc0b08279b545a573ceccce9eb27cd",
    }
    expected_size = {"windows": 23, "unix": 21}
    platform = "windows" if sys.platform.startswith("win") else "unix"
    assert res == PacketFile(
        path, expected_size[platform], expected_hash[platform]
    )


def test_can_get_file_hash_from_metadata():
    d = read_metadata_core("example/.outpack/metadata/20230807-152344-ee606dce")
    expected = "sha256:2a85eb5a027c8d2255e672d1592cc38c82cc0b08279b545a573ceccce9eb27cd"
    assert d.file_hash("data.csv") == expected


def test_can_error_if_file_not_found_in_metadata():
    d = read_metadata_core("example/.outpack/metadata/20230807-152344-ee606dce")
    with pytest.raises(Exception, match="Packet .+ does not contain file 'f'"):
        d.file_hash("f")
