import itertools
import shutil
from collections.abc import Iterable
from errno import ENOENT
from pathlib import Path
from typing import Optional

from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.hash import Hash, hash_file, hash_parse
from pyorderly.outpack.index import Index
from pyorderly.outpack.metadata import MetadataCore


class Archive:
    def __init__(self, path: Path, index: Index):
        self._path = Path(path)
        self._index = index

    def _try_find_file_in_packet(self, id: str, hash: Hash):
        meta = self._index.metadata(id)
        for f in meta.files:
            if f.hash == str(hash):
                path = self._path / meta.name / meta.id / f.path
                if hash_file(path, hash.algorithm) == hash:
                    return path
                else:
                    msg = (
                        f"Rejecting file from archive '{f.path}' "
                        f"in '{meta.name}/{meta.id}'"
                    )
                    print(msg)

        return None

    def try_find_file(
        self, hash: str, *, candidates: Iterable[str] = ()
    ) -> Optional[Path]:
        hash_parsed = hash_parse(hash)
        packets = set(self._index.unpacked()).difference(candidates)
        for id in itertools.chain(candidates, packets):
            result = self._try_find_file_in_packet(id, hash_parsed)
            if result is not None:
                return result
        return None

    def find_file(self, hash: str, *, candidates: Iterable[str] = ()) -> Path:
        path = self.try_find_file(hash, candidates=candidates)
        if path is None:
            msg = "File not found in archive, or corrupt"
            raise FileNotFoundError(ENOENT, msg, hash)
        else:
            return path

    def import_packet(self, meta: MetadataCore, path: Path) -> Path:
        dest = self._path / meta.name / meta.id
        for f in meta.files:
            f_dest = dest / f.path
            f_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path / f.path, f_dest)
        return dest

    def import_packet_from_store(
        self, meta: MetadataCore, store: FileStore
    ) -> Path:
        dest = self._path / meta.name / meta.id
        for f in meta.files:
            store.get(f.hash, dest / f.path, overwrite=True)
        return dest
