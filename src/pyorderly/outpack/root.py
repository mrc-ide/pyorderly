import os
import shutil
from errno import ENOENT
from pathlib import Path
from typing import Optional, Union

from pyorderly.outpack.config import read_config
from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.hash import hash_file, hash_parse
from pyorderly.outpack.index import Index
from pyorderly.outpack.metadata import PacketLocation
from pyorderly.outpack.schema import validate
from pyorderly.outpack.util import find_file_descend


class OutpackRoot:
    files: Optional[FileStore] = None

    def __init__(self, path):
        self.path = Path(path)
        self.config = read_config(path)
        if self.config.core.use_file_store:
            self.files = FileStore(self.path / ".outpack" / "files")
        self.index = Index(path)

    def export_file(self, id, there, here, dest):
        meta = self.index.metadata(id)
        hash = meta.file_hash(there)
        dest = Path(dest)
        here_full = dest / here
        if self.config.core.use_file_store:
            try:
                self.files.get(hash, here_full, overwrite=False)
            except FileNotFoundError as e:
                e.filename = there
                raise
        else:
            # consider starting from the case most likely to contain
            # this hash, since we already know that it's 'id' unless
            # it's corrupt - this is what the R version does (though
            # it only does that).
            src = find_file_by_hash(self, hash)
            if not src:
                msg = f"File not found in archive, or corrupt: {there}"
                raise FileNotFoundError(ENOENT, msg, there)
            here_full.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, here_full)
        return here


def root_open(
    path: Union[OutpackRoot, str, os.PathLike, None], *, locate: bool = False
) -> OutpackRoot:
    if isinstance(path, OutpackRoot):
        return path

    if path is None:
        path = Path.cwd()
    else:
        path = Path(path).absolute()

    if not path.is_dir():
        msg = "Expected 'path' to be an existing directory"
        raise Exception(msg)
    if locate:
        path_outpack = find_file_descend(".outpack", path)
        has_outpack = path_outpack is not None
        pass
    else:
        has_outpack = path.joinpath(".outpack").is_dir()
        path_outpack = path
    if not has_outpack:
        msg = f"Did not find existing outpack root in '{path}'"
        raise Exception(msg)
    return OutpackRoot(path_outpack)


def find_file_by_hash(root, hash):
    path_archive = root.path / root.config.core.path_archive
    hash_parsed = hash_parse(hash)
    for id in root.index.unpacked():
        meta = root.index.metadata(id)
        for f in meta.files:
            if f.hash == hash:
                path = path_archive / meta.name / meta.id / f.path
                if hash_file(path, hash_parsed.algorithm) == hash_parsed:
                    return path
                else:
                    msg = (
                        f"Rejecting file from archive '{f.path}' "
                        f"in '{meta.name}/{meta.id}'"
                    )
                    print(msg)
    return None


def mark_known(root, packet_id, location, hash, time):
    dat = PacketLocation(packet_id, time, str(hash))
    validate(dat.to_dict(), "outpack/location.json")
    dest = root.path / ".outpack" / "location" / location / packet_id
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        f.write(dat.to_json(separators=(",", ":")))
