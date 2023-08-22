import os
from pathlib import Path

from outpack.config import read_config
from outpack.filestore import FileStore
from outpack.index import Index
from outpack.util import find_file_descend


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
