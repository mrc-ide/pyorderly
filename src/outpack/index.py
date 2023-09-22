import pathlib
from dataclasses import dataclass
from typing import List

from outpack.metadata import read_metadata_core, read_packet_location


@dataclass
class IndexData:
    metadata: dict
    location: dict
    unpacked: List[str]

    @staticmethod
    def new():
        return IndexData({}, {}, [])


class Index:
    def __init__(self, path):
        self._path = pathlib.Path(path)
        self._data = IndexData.new()

    def rebuild(self):
        self._data = _index_update(self._path, IndexData.new())
        return self

    def refresh(self):
        self._data = _index_update(self._path, self._data)
        return self

    def metadata(self, id):
        if id in self._data.metadata:
            return self._data.metadata[id]
        return self.refresh()._data.metadata[id]

    def location(self, name):
        return self.refresh()._data.location[name]

    def unpacked(self):
        return self.refresh()._data.unpacked

    def data(self):
        return self.refresh()._data


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


def _read_locations(path_root, data):
    path = path_root / ".outpack" / "location"
    for loc in path.iterdir():
        if loc.name not in data:
            data[loc.name] = {}
        d = data[loc.name]
        for p in loc.iterdir():
            if p.name not in d:
                d[p.name] = read_packet_location(p)
    return data
