from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from dataclasses_json import dataclass_json


@dataclass_json()
@dataclass
class PacketFile:
    path: str
    size: float
    hash: str  # noqa: A003


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


@dataclass_json
@dataclass
class GitInfo:
    sha: str
    branch: str
    url: List[str]


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


def read_metadata_core(path):
    with open(path) as f:
        s = f.read()
    return MetadataCore.from_json(s.strip())
