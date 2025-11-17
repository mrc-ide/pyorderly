from pathlib import Path
from typing import TypeAlias

from pydantic import Field

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.tools import GitInfo
from pyorderly.outpack.util import StrictModel


class PacketFile(StrictModel):
    path: str
    size: int
    hash: str

    @staticmethod
    def from_file(directory, path, hash_algorithm):
        f = Path(directory).joinpath(path)
        s = f.stat().st_size
        h = str(hash_file(f, hash_algorithm))
        return PacketFile(path=path, size=s, hash=h)


class PacketFileWithLocation(PacketFile):
    location: str
    packet_id: str

    @staticmethod
    def from_packet_file(file: PacketFile, location: str, packet_id: str):
        return PacketFileWithLocation(
            path=file.path,
            size=file.size,
            hash=file.hash,
            location=location,
            packet_id=packet_id,
        )


class PacketDependsPath(StrictModel):
    here: str
    there: str


class PacketDepends(StrictModel):
    packet: str
    query: str
    files: list[PacketDependsPath]

    @staticmethod
    def files_from_dict(files):
        return [{"here": h, "there": t} for h, t, in files.items()]


Parameters: TypeAlias = dict[str, bool | int | float | str]


class MetadataCore(StrictModel):
    schema_version: str
    id: str
    name: str
    parameters: Parameters = Field(default_factory=dict)
    time: dict[str, float] = Field(default_factory=dict)
    files: list[PacketFile] = Field(default_factory=list)
    depends: list[PacketDepends] = Field(default_factory=list)
    git: GitInfo | None = Field(default=None)
    custom: dict | None = Field(default=None)

    def file_hash(self, name):
        for x in self.files:
            if x.path == name:
                return x.hash
        msg = f"Packet {self.id} does not contain file '{name}'"
        raise Exception(msg)


class PacketLocation(StrictModel):
    packet: str
    time: float
    hash: str


def read_metadata_core(path) -> MetadataCore:
    with open(path) as f:
        return MetadataCore.model_validate_json(f.read())


def read_packet_location(path) -> PacketLocation:
    with open(path) as f:
        return PacketLocation.model_validate_json(f.read())
