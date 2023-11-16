import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from outpack.packet import Packet
from outpack.root import OutpackRoot, root_open


class OrderlyCustomMetadata:
    def __init__(self):
        self.resources = []
        self.artefacts = []
        self.description = None


@dataclass
class OrderlyContext:
    # Indicates if a packet is currently running
    is_active: bool
    # The active packet, if one is running
    packet: Optional[Packet]
    # the path to the packet running directory. This is a pathlib Path object
    path: Path
    # the path to the packet source
    path_src: Path
    # The outpack root for the currently running packet
    root: OutpackRoot
    # Parameters used to run the packet
    parameters: dict
    # The name of the packet
    name: str
    # The id of the packet, only non-None if active
    id: Optional[str]
    # Special orderly custom metadata
    orderly: OrderlyCustomMetadata
    # Execution environment; currently this is globals() but we'll
    # tidy this up later
    envir: dict

    @staticmethod
    def from_packet(packet, path_src):
        return OrderlyContext(
            is_active=True,
            packet=packet,
            path=packet.path,
            path_src=Path(path_src),
            root=packet.root,
            parameters=packet.parameters,
            name=packet.name,
            id=packet.id,
            orderly=OrderlyCustomMetadata(),
            envir=globals(),
        )

    @staticmethod
    def interactive():
        path = Path(os.getcwd())
        return OrderlyContext(
            is_active=False,
            packet=None,
            path=path,
            path_src=path,
            root=detect_orderly_interactive_root(path),
            parameters={},
            name=path.name,
            id=None,
            orderly=OrderlyCustomMetadata(),
            envir=globals(),
        )


class ActiveOrderlyContext:
    _context = None
    _our_context = None

    def __init__(self, packet, path_src):
        self._our_context = OrderlyContext.from_packet(packet, path_src)

    def __enter__(self):
        ActiveOrderlyContext._context = self._our_context
        return self._our_context.orderly

    def __exit__(self, exc_type, exc_value, exc_tb):
        ActiveOrderlyContext._context = None

    @staticmethod
    def current():
        return ActiveOrderlyContext._context


def get_active_context():
    return ActiveOrderlyContext.current() or OrderlyContext.interactive()


def detect_orderly_interactive_root(path):
    path = Path(path)
    root = path.parent.parent
    ok = root.joinpath("src").is_dir() and root.joinpath(".outpack").exists()
    if not ok:
        msg = f"Failed to detect orderly path at {path}"
        raise Exception(msg)
    return root_open(root, locate=False)
