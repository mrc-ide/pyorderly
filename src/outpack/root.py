import os
import warnings
from pathlib import Path

from outpack.config import read_config
from outpack.filestore import FileStore
from outpack.index import Index
from outpack.util import find_file_descend
from outpack.hash import hash_parse, hash_file


class OutpackRoot:
    files = None

    def __init__(self, path):
        self.path = Path(path)
        self.config = read_config(path)
        if self.config.core.use_file_store:
            self.files = FileStore(self.path / "files")
        self.index = Index(path)


def root_open(path, locate):
    if isinstance(path, OutpackRoot):
        return path
    if path is None:
        path = os.getcwd()
    path = Path(path)
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
    index = root.index.data()

    path_archive = root.path / root.config.core.path_archive
    algorithm = hash_parse(hash)

    for packet_id in index.unpacked():
        meta = index.metadata(packet_id)
        files = list(filter(lambda file: file.hash == hash, meta.files))
        for file in files:
            path = path_archive / meta.name / packet_id / file.path
            if path.exists() and hash_file(path, algorithm) == hash:
                return path
            rejected = [file.path for file in files]

            rejected_msg = "', '".join(rejected)
            msg = (f"Rejecting file from archive '{rejected_msg}' in "
                   f"'{meta.name} /{packet_id}'")
            warnings.warn(msg)
