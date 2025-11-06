from pathlib import Path
from typing import TypeAlias

from pydantic import BaseModel

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.tools import GitInfo


class PacketFile(BaseModel):
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


class PacketDependsPath(BaseModel):
    here: str
    there: str


class PacketDepends(BaseModel):
    packet: str
    query: str
    files: list[PacketDependsPath]

    @staticmethod
    def files_from_dict(files):
        return [{"here": h, "there": t} for h, t, in files.items()]


Parameters: TypeAlias = dict[str, bool | int | float | str]


class MetadataCore(BaseModel):
    schema_version: str
    id: str
    name: str
    parameters: Parameters = {}
    time: dict[str, float] = {}
    files: list[PacketFile] = []
    depends: list[PacketDepends] = []
    git: GitInfo | None = None
    custom: dict | None = None

    def file_hash(self, name):
        for x in self.files:
            if x.path == name:
                return x.hash
        msg = f"Packet {self.id} does not contain file '{name}'"
        raise Exception(msg)


class PacketLocation(BaseModel):
    packet: str
    time: float
    hash: str


def read_metadata_core(path) -> MetadataCore:
    with open(path) as f:
        return MetadataCore.model_validate_json(f.read())


def read_packet_location(path) -> PacketLocation:
    with open(path) as f:
        return PacketLocation.model_validate_json(f.read())
