import random

import pytest

from outpack.digest import Digest, digest_file
from outpack.filestore import FileStore


def test_can_create_store(tmp_path):
    p = str(tmp_path / "store")
    s = FileStore(p)
    assert s.ls() == []


def test_can_store_files(tmp_path):
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    letters = [chr(i + ord("a")) for i in range(10)]
    for i in range(10):
        with open(tmp_path / "tmp" / letters[i], "wb") as f:
            f.write(random.randbytes(10))

    s = FileStore(str(tmp_path / "store"))
    p = tmp_path / "tmp" / "a"
    h = digest_file(p, "md5")
    assert not s.exists(h)
    assert s.put(p, h) == h
    assert s.exists(h)
    assert s.ls() == [h]

    dest = tmp_path / "dest"
    s.get(h, dest)
    assert dest.exists()
    assert digest_file(dest, "md5") == h

    for i in range(10):
        p = tmp_path / "tmp" / letters[i]
        s.put(p, digest_file(p, "md5"))

    assert len(s.ls()) == 10


def test_if_digest_not_found(tmp_path):
    p = str(tmp_path / "store")
    s = FileStore(p)
    h = Digest("md5", "7c4d97e580abb6c2ffb8b1872907d84b")
    dest = tmp_path / "dest"
    with pytest.raises(Exception) as e:
        s.get(h, dest)
    assert e.match("Digest 'md5:7c4d97e.+' not found in store")
