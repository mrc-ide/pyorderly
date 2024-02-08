import os
import os.path
import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path

from outpack.hash import Hash, hash_parse, hash_validate_file


class FileStore:
    def __init__(self, path):
        self._path = Path(path)
        os.makedirs(path, exist_ok=True)

    def filename(self, hash):
        dat = hash_parse(hash)
        return self._path / dat.algorithm / dat.value[:2] / dat.value[2:]

    def get(self, hash, dst, *, overwrite=False):
        src = self.filename(hash)
        if not os.path.exists(src):
            msg = f"Hash '{hash}' not found in store"
            raise Exception(msg)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if not overwrite and os.path.exists(dst):
            msg = f"Failed to copy '{src}' to '{dst}', file already exists"
            raise Exception(msg)
        shutil.copyfile(src, dst)

    def exists(self, hash):
        return os.path.exists(self.filename(hash))

    def put(self, src, hash, *, move=False):
        hash_validate_file(src, hash)
        dst = self.filename(hash)
        if not os.path.exists(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if move:
                shutil.move(src, dst)
            else:
                shutil.copyfile(src, dst)
            os.chmod(dst, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
        return hash

    def ls(self):
        # Lots of ways of pulling this off with higer order functions
        # (os.walk, Path.glob etc), but this is probably clearest.
        ret = []
        for algorithm in os.listdir(self._path):
            path_alg = self._path / algorithm
            for prefix in os.listdir(path_alg):
                path_prefix = os.path.join(path_alg, prefix)
                for suffix in os.listdir(path_prefix):
                    ret.append(Hash(algorithm, prefix + suffix))
        return ret

    def destroy(self) -> None:
        def onerror(func, path, _exc_info):
            """
            Error handler for ``shutil.rmtree``.

            If the error is due to an access error (read only file)
            it attempts to add write permission and then retries.

            If the error is for another reason it re-raises the error.
            We manually remove write permission in ``put`` above so this
            is expected

            Usage : ``shutil.rmtree(path, onerror=onerror)``
            """
            if not os.access(path, os.W_OK):
                os.chmod(path, stat.S_IWUSR)
                func(path)
            else:
                raise

        shutil.rmtree(self._path, onerror=onerror)

    @contextmanager
    def tmp(self):
        # On a newer version of tempfile we could use `delete_on_close = True`
        path = self._path / "tmp"
        path.mkdir(exist_ok=True)
        f = tempfile.NamedTemporaryFile(dir=path, delete=False)
        try:
            yield f.name
        finally:
            try:
                os.unlink(f.name)
            except OSError:
                pass
