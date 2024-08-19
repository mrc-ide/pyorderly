import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyorderly.outpack.packet import Packet
from pyorderly.outpack.root import OutpackRoot, root_open
from pyorderly.outpack.search_options import SearchOptions


class OrderlyCustomMetadata:
    def __init__(self):
        self.resources = []
        self.shared_resources = {}
        self.artefacts = []
        self.description = None


@dataclass
class OrderlyContext:
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
    # Special orderly custom metadata
    orderly: OrderlyCustomMetadata
    # Options used when searching for dependencies
    search_options: Optional[SearchOptions]

    @staticmethod
    def from_packet(packet, path_src, search_options=None):
        return OrderlyContext(
            packet=packet,
            path=packet.path,
            path_src=Path(path_src),
            root=packet.root,
            parameters=packet.parameters,
            name=packet.name,
            orderly=OrderlyCustomMetadata(),
            search_options=search_options,
        )

    @staticmethod
    def interactive():
        path = Path(os.getcwd())
        return OrderlyContext(
            packet=None,
            path=path,
            path_src=path,
            root=detect_orderly_interactive_root(path),
            parameters={},
            name=path.name,
            orderly=OrderlyCustomMetadata(),
            # TODO: expose a way of configuring this
            search_options=None,
        )

    @property
    def is_active(self):
        return self.packet is not None


class ActiveOrderlyContext:
    _context = None
    _our_context = None

    def __init__(self, *args, **kwargs):
        self._our_context = OrderlyContext.from_packet(*args, **kwargs)

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
