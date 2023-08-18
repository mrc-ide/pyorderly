from pathlib import Path
import shutil
import time

from outpack.ids import outpack_id
from outpack.hash import hash_string
from outpack.metadata import PacketFile, MetadataCore, PacketLocation
from outpack.root import Root
from outpack.schema import validate
from outpack.tools import git_info
from outpack.util import all_normal_files


class Packet:
    def __init__(self, root, path, name, parameters=None):
        self.root = Root(root)
        self.path = Path(path)
        self.id = outpack_id()
        self.name = name
        self.parameters = parameters or {}
        self.depends = []
        self.files = []
        self.time = {"begin": time.time()}
        self.git = git_info(self.path)
        self.metadata = None

    def end(self, *, insert=True):
        if self.metadata:
            msg = f"Packet '{id}' already ended"
            raise Exception(msg)
        self.time["end"] = time.time()
        hash_algorithm = self.root.config.core.hash_algorithm
        self.files = [PacketFile.from_file(self.path, f, hash_algorithm)
                      for f in all_normal_files(self.path)]
        self.metadata = self._build_metadata()
        if insert:
            _insert(self.root, self.path, self.metadata)
        else:
            _cancel(self.root, self.path, self.metadata)

    def _build_metadata(self):
        return MetadataCore(self.id, self.name, self.parameters,
                            self.time, self.files, self.depends,
                            self.git)


def _insert(root, path, meta):
    # check that we have not already inserted this packet; in R we
    # look to see if it's unpacked but actually the issue is if it is
    # present as metadata at all.
    if root.config.core.use_file_store:
        store = root.files
        for p in meta.files:
            root.files.put(path / p.path, p.hash)

    if root.config.core.path_archive:
        dest = root.path / "archive" / meta.name / meta.id
        for p in meta.files:
            p_dest = dest / p.path
            p_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path / p.path, p_dest)

    json = meta.to_json(separators=(",", ":"))
    hash_meta = hash_string(json, root.config.core.hash_algorithm)
    path_meta = root.path / ".outpack" / "metadata" / meta.id
    path_meta.parent.mkdir(parents=True, exist_ok=True)
    with open(path_meta, "w") as f:
        f.write(json)

    mark_known(root, meta.id, "local", hash_meta, time.time())


def _cancel(root, path, meta):
    with path.joinpath("outpack.json").open("w") as f:
        f.write(meta.to_json())


def mark_known(root, packet_id, location, hash, time):
    dat = PacketLocation(packet_id, time, hash)
    dest = root.path / ".outpack" / "location" / location / packet_id
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        f.write(dat.to_json(separators=(",", ":")))
