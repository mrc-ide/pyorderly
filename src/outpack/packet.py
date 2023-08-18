from pathlib import Path
import shutil
import time

from outpack.ids import outpack_id
from outpack.hash import hash_data
from outpack.metadata import PacketFile, MetadataCore, PacketLocation
from outpack.schema import validate


class Packet:
    def __init__(self, root, path, name, parameters=None):
        self.root = root
        self.path = Path(path)
        self.id = outpack_id()
        self.name = name
        self.parameters = parameters or {}
        self.depends = []
        self.files = []
        self.time = {"begin": time.time()}
        self.git = git_info(self.path)

    def _metadata():
        return MetadataCore(self.id, self.name, self.parameters,
                            self.time, self.files, self.depends,
                            self.git)        

    def end(self, *, insert=True):
        if self.metadata:
            msg = f"Packet {id} already ended"
            raise Exception(msg)
        self.time["end"] = time.time()
        # TODO: need to be careful here, only want regular
        # files. Later expand to remove many.
        files = list(self.path.iterdir())
        algorithm = self.root.config.core.hash_algorithm
        self.files = [PacketFile.from_file(path, f, algorithm) for f in files]
        meta = self._metadata()
        if insert:
            _insert(self.root, self.path, meta)
        else:
            _cancel(self.root, self.path, meta)


def packet_start(root, path, name, parameters=None):
    return Packet(root, path, name, parameters)
                 

def create(root, path, id, name, parameters, time, files, depends):
    meta = create_metadata(path, id, name, parameters, time, files, depends,
                           hash_algorithm)
    insert(path, meta, root)


def _insert(root, path, meta):
    # check that we have not already inserted this packet; in R we
    # look to see if it's unpacked but actually the issue is if it is
    # present as metadata at all.
    if root.config.core.use_file_store:
        store = root.store
        for p in meta.files:
            store.put(path / p.path, p.hash)

    if root.config.core.path_archive:
        dest = root.path / "archive" / meta.name / meta.id
        dest.mkdir()
        for p in meta.files:
            p_dest = dest / p.path
            p_dest.parent.mkdir(exist_ok=True)
            shutil.copy(path / p.path, p_dest)

    time <- Sys.time()
    json = meta.to_json()
    hash_meta <- hash_data(json, root.config.core.hash_algorithm)
    path_meta = root.path / "metadata" / meta.id
    with open(path_meta, "w") as f:
        f.write(json)

    mark_known(root, meta.id, "local", hash_meta, time.time())


def _cancel(root, path, meta):
    pass


def mark_known(root, packet_id, location, hash, time):
    dat = PacketLocation(id, time, hash)
    dest = root.path / ".outpack" / "location" / location / packet_id
    dest.parent().mkdir(exists_ok=True)
    with open(dest, "w") as f:
        f.write(dat.to_json())

