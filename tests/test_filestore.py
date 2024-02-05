import os
import random

import pytest

from outpack.filestore import FileStore
from outpack.hash import Hash, hash_file


def randstr(n):
    return "".join([str(random.randint(0, 9)) for _ in range(n)])  #  noqa: S311


def test_can_create_store(tmp_path):
    p = str(tmp_path / "store")
    s = FileStore(p)
    assert s.ls() == []


def test_can_store_files(tmp_path):
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    letters = [chr(i + ord("a")) for i in range(10)]
    for i in range(10):
        with open(tmp_path / "tmp" / letters[i], "w") as f:
            f.write(randstr(10))

    s = FileStore(str(tmp_path / "store"))
    p = tmp_path / "tmp" / "a"
    h = hash_file(p, "md5")
    assert not s.exists(h)
    assert s.put(p, h) == h
    assert s.exists(h)
    assert s.ls() == [h]

    dest = tmp_path / "dest"
    s.get(h, dest, False)
    assert dest.exists()
    assert hash_file(dest, "md5") == h

    for i in range(10):
        p = tmp_path / "tmp" / letters[i]
        s.put(p, hash_file(p, "md5"))

    assert len(s.ls()) == 10


def test_get_files_fails_if_overwrite_false(tmp_path):
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    letters = [chr(i + ord("a")) for i in range(10)]
    for i in range(10):
        with open(tmp_path / "tmp" / letters[i], "w") as f:
            f.write(randstr(10))

    store = FileStore(str(tmp_path / "store"))
    p = tmp_path / "tmp" / "a"
    h = hash_file(p, "md5")
    assert not store.exists(h)
    assert store.put(p, h) == h
    assert store.exists(h)

    dest = tmp_path / "dest"
    assert not dest.exists()
    store.get(h, dest, False)
    assert dest.exists()
    assert hash_file(dest, "md5") == h

    store.get(h, dest, True)
    assert dest.exists()
    assert hash_file(dest, "md5") == h

    with pytest.raises(Exception) as e:
        store.get(h, dest, False)
    assert e.match(r"Failed to copy '.+' to '.+', file already exists")


def test_if_hash_not_found(tmp_path):
    p = str(tmp_path / "store")
    s = FileStore(p)
    h = Hash("md5", "7c4d97e580abb6c2ffb8b1872907d84b")
    dest = tmp_path / "dest"
    with pytest.raises(Exception) as e:
        s.get(h, dest, False)
    assert e.match("Hash 'md5:7c4d97e.+' not found in store")


def test_can_create_filename_within_the_store(tmp_path):
    path = str(tmp_path / "store")
    store = FileStore(path)
    temp_file = store.tmp()
    assert os.path.dirname(temp_file) == str(store._path / "tmp")
    assert not os.path.exists(temp_file)
    assert os.path.exists(os.path.dirname(temp_file))
