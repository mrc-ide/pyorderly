import os
import os.path
import shutil
import stat

from outpack.hash import Hash, hash_parse, hash_validate


class FileStore:
    def __init__(self, path):
        self._path = path
        os.makedirs(path, exist_ok=True)

    def filename(self, hash):
        dat = hash_parse(hash)
        return os.path.join(
            self._path, dat.algorithm, dat.value[:2], dat.value[2:]
        )

    def get(self, hash, dst):
        src = self.filename(hash)
        if not os.path.exists(src):
            msg = f"Hash '{hash}' not found in store"
            raise Exception(msg)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)

    def exists(self, hash):
        return os.path.exists(self.filename(hash))

    def put(self, src, hash):
        hash_validate(src, hash)
        dst = self.filename(hash)
        if not os.path.exists(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(src, dst)
            os.chmod(dst, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
        return hash

    def ls(self):
        # Lots of ways of pulling this off with higer order functions
        # (os.walk, Path.glob etc), but this is probably clearest.
        ret = []
        for algorithm in os.listdir(self._path):
            path_alg = os.path.join(self._path, algorithm)
            for prefix in os.listdir(path_alg):
                path_prefix = os.path.join(path_alg, prefix)
                for suffix in os.listdir(path_prefix):
                    ret.append(Hash(algorithm, prefix + suffix))
        return ret
