from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from pathlib import Path

from dataclasses_json import dataclass_json

from outpack.hash import hash_file
from outpack.ids import validate_outpack_id
from outpack.tools import GitInfo, git_info


@dataclass_json()
@dataclass
class PacketFile:
    path: str
    size: float
    hash: str  # noqa: A003

    @staticmethod
    def from_file(directory, path, hash_algorithm):
        f = Path(directory).joinpath(path)
        s = f.stat().st_size
        h = str(hash_file(f, hash_algorithm))
        return PacketFile(path, s, h)


@dataclass_json()
@dataclass
class PacketDependsPath:
    here: str
    there: str


@dataclass_json()
@dataclass
class PacketDepends:
    packet: str
    query: str
    files: List[PacketDependsPath]


@dataclass_json()
@dataclass
class MetadataCore:
    id: str  # noqa: A003
    name: str
    parameters: Dict[str, Union[bool, int, float, str]]
    time: Dict[str, float]
    files: List[PacketFile]
    depends: List[PacketDepends]
    git: Optional[GitInfo]


@dataclass_json()
@dataclass
class PacketLocation:
    packet: str
    time: float
    hash: str  # noqa: A003


def read_metadata_core(path):
    with open(path) as f:
        return MetadataCore.from_json(f.read().strip())


def read_packet_location(path):
    with open(path) as f:
        return PacketLocation.from_json(f.read().strip())