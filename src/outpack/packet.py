import shutil
import time
from pathlib import Path

from outpack.hash import hash_file, hash_parse, hash_string, hash_validate_file
from outpack.helpers import copy_files
from outpack.ids import outpack_id, validate_outpack_id
from outpack.metadata import (
    MetadataCore,
    PacketDepends,
    PacketFile,
    PacketLocation,
)
from outpack.root import root_open
from outpack.schema import outpack_schema_version, validate
from outpack.search import search
from outpack.search_query import as_query
from outpack.tools import git_info
from outpack.util import all_normal_files


# TODO: most of these fields should be private.
class Packet:
    def __init__(
        self, root, path, name, *, parameters=None, id=None, locate=True
    ):
        self.root = root_open(root, locate)
        self.path = Path(path)
        if id is None:
            self.id = outpack_id()
        else:
            validate_outpack_id(id)
            self.id = id
        self.name = name
        self.parameters = parameters or {}
        self.depends = []
        self.files = []
        self.time = {"start": time.time()}
        self.git = git_info(self.path)
        self.custom = {}
        self.metadata = None
        self.immutable = {}

    def use_dependency(self, query, files, search_options=None):
        query = as_query(query)
        # check query.is_single - can't be done until query expanded...
        id = search(query, options=search_options, root=self.root)
        if not id:
            msg = f"Failed to find packet for query {query}"
            raise Exception(msg)

        result = copy_files(id, files, self.path, root=self.root)
        for f in result.files.keys():
            self.mark_file_immutable(f)
        d = PacketDepends(id, str(query), PacketDepends.files_from_dict(files))
        self.depends.append(d)
        return result

    def mark_file_immutable(self, path):
        path_full = self.path / path
        if path in self.immutable:
            hash_validate_file(path_full, self.immutable[path])
        else:
            hash_algorithm = self.root.config.core.hash_algorithm
            self.immutable[path] = hash_file(path_full, hash_algorithm)

    def add_custom_metadata(self, key, value):
        if key in self.custom:
            msg = f"metadata for '{key}' has already been added for this packet"
            raise Exception(msg)
        self.custom[key] = value

    def end(self, *, insert=True):
        if self.metadata:
            msg = f"Packet '{id}' already ended"
            raise Exception(msg)
        self.time["end"] = time.time()
        hash_algorithm = self.root.config.core.hash_algorithm
        self.files = [
            PacketFile.from_file(self.path, f, hash_algorithm)
            for f in all_normal_files(self.path)
        ]
        _check_immutable_files(self.files, self.immutable)
        self.metadata = self._build_metadata()
        validate(self.metadata.to_dict(), "outpack/metadata.json")
        if insert:
            _insert(self.root, self.path, self.metadata)
        else:
            _cancel(self.root, self.path, self.metadata)

    def _build_metadata(self):
        return MetadataCore(
            outpack_schema_version(),
            self.id,
            self.name,
            self.parameters,
            self.time,
            self.files,
            self.depends,
            self.git,
            self.custom,
        )


def _insert(root, path, meta):
    # check that we have not already inserted this packet; in R we
    # look to see if it's unpacked but actually the issue is if it is
    # present as metadata at all.
    if root.config.core.use_file_store:
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


def _cancel(_root, path, meta):
    with path.joinpath("outpack.json").open("w") as f:
        f.write(meta.to_json())


def _check_immutable_files(files, immutable):
    if not immutable:
        return
    found = {x.path: x.hash for x in files}
    for p, h in immutable.items():
        if p not in found:
            msg = f"File was deleted after being added: {p}"
            raise Exception(msg)
        if hash_parse(found[p]) != h:
            msg = f"File was changed after being added: {p}"
            raise Exception(msg)


def mark_known(root, packet_id, location, hash, time):
    dat = PacketLocation(packet_id, time, str(hash))
    validate(dat.to_dict(), "outpack/location.json")
    dest = root.path / ".outpack" / "location" / location / packet_id
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        f.write(dat.to_json(separators=(",", ":")))
