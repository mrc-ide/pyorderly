import builtins
from abc import abstractmethod
from contextlib import AbstractContextManager

from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation


class LocationDriver(AbstractContextManager):
    """
    A location implementation.

    The driver object is treated as a context manager and is entered and exited
    before and after its methods are called.
    """

    @abstractmethod
    def list(self) -> dict[str, PacketLocation]: ...

    @abstractmethod
    def metadata(self, packet_ids: builtins.list[str]) -> dict[str, str]: ...

    @abstractmethod
    def fetch_file(
        self, packet: MetadataCore, file: PacketFile, dest: str
    ) -> None: ...
