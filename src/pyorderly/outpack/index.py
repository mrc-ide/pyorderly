import pathlib
from dataclasses import dataclass

from pyorderly.outpack.metadata import (
    MetadataCore,
    PacketLocation,
    read_metadata_core,
    read_packet_location,
)


@dataclass
class IndexData:
    metadata: dict[str, MetadataCore]
    location: dict[str, dict[str, PacketLocation]]
    unpacked: list[str]

    @staticmethod
    def new():
        return IndexData({}, {}, [])


class Index:
    def __init__(self, path):
        self._path = pathlib.Path(path)
        self.data = IndexData.new()

    def rebuild(self):
        self.data = _index_update(self._path, IndexData.new())
        return self

    def refresh(self):
        self.data = _index_update(self._path, self.data)
        return self

    def all_metadata(self) -> dict[str, MetadataCore]:
        return self.refresh().data.metadata

    def metadata(self, id) -> MetadataCore:
        if id in self.data.metadata:
            return self.data.metadata[id]
        return self.refresh().data.metadata[id]

    def all_locations(self) -> dict[str, dict[str, PacketLocation]]:
        return self.refresh().data.location

    def location(self, name) -> dict[str, PacketLocation]:
        return self.refresh().data.location.get(name, {})

    def packets_in_location(self, name) -> list[str]:
        return list(self.location(name).keys())

    def unpacked(self) -> list[str]:
        return self.refresh().data.unpacked


def _index_update(path_root, data):
    data.metadata = _read_metadata(path_root, data.metadata)
    data.location = _read_locations(path_root, data.location)
    data.unpacked = sorted(data.location["local"].keys())
    return data


def _read_metadata(path_root, data):
    path = path_root / ".outpack" / "metadata"
    for p in path.iterdir():
        if p.name not in data:
            data[p.name] = read_metadata_core(p)
    return data


def _read_locations(path_root, data) -> dict[str, dict[str, PacketLocation]]:
    path = path_root / ".outpack" / "location"
    for loc in path.iterdir():
        if loc.name not in data:
            data[loc.name] = {}
        d = data[loc.name]
        for p in loc.iterdir():
            if p.name not in d:
                d[p.name] = read_packet_location(p)
    return data
