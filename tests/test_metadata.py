from outpack.metadata import (
    PacketDependsPath,
    PacketFile,
    read_metadata_core,
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
