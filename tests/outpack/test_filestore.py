import os
import platform
import random

import pytest

from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.hash import Hash, hash_file


def randstr(n):
    return "".join([str(random.randint(0, 9)) for _ in range(n)])


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
    path_a = tmp_path / "tmp" / "a"
    hash_a = hash_file(path_a, "md5")
    assert not s.exists(hash_a)
    assert s.put(path_a, hash_a, move=True) == hash_a
    assert s.exists(hash_a)
    assert not path_a.exists()
    assert s.ls() == [hash_a]

    path_b = tmp_path / "tmp" / "b"
    hash_b = hash_file(path_b, "md5")
    assert not s.exists(hash_b)
    assert s.put(path_b, hash_b) == hash_b
    assert s.exists(hash_b)
    assert path_b.exists()
    assert all(h in s.ls() for h in [hash_a, hash_b])

    dest = tmp_path / "dest"
    s.get(hash_a, dest, overwrite=False)
    assert dest.exists()
    assert hash_file(dest, "md5") == hash_a

    for i in range(9):
        p = tmp_path / "tmp" / letters[i + 1]
        s.put(p, hash_file(p, "md5"))

    assert len(s.ls()) == 10


@pytest.mark.skipif(
    platform.system() != "Windows",
    reason="destroy onerror handler only invoked on Windows \
so only run this test on Windows",
)
def test_destroy_store_raises_error(tmp_path, mocker):
    store_path = tmp_path / "store"

    mocker.patch("os.access", return_value=True)

    store = FileStore(str(store_path))
    assert store_path.exists()
    file_path = tmp_path / "a"
    with open(file_path, "w") as f:
        f.write(randstr(10))
    file_hash = hash_file(file_path, "md5")
    assert store.put(file_path, file_hash, move=False) == file_hash
    assert store.ls() == [file_hash]

    # Error raised from anything other than file permission issue
    with pytest.raises(Exception, match="Access is denied"):
        store.destroy()


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
    store.get(h, dest, overwrite=False)
    assert dest.exists()
    assert hash_file(dest, "md5") == h

    store.get(h, dest, overwrite=True)
    assert dest.exists()
    assert hash_file(dest, "md5") == h

    with pytest.raises(Exception) as e:
        store.get(h, dest, overwrite=False)
    assert e.match(r"Failed to copy '.+' to '.+', file already exists")


def test_if_hash_not_found(tmp_path):
    p = str(tmp_path / "store")
    s = FileStore(p)
    h = Hash("md5", "7c4d97e580abb6c2ffb8b1872907d84b")
    dest = tmp_path / "dest"
    with pytest.raises(Exception) as e:
        s.get(h, dest, overwrite=False)
    assert e.match("Hash 'md5:7c4d97e.+' not found in store")


def test_can_create_filename_within_the_store(tmp_path):
    path = str(tmp_path / "store")
    store = FileStore(path)
    with store.tmp() as temp_file:
        assert os.path.dirname(temp_file) == str(store._path / "tmp")
        assert os.path.exists(temp_file)
        assert os.path.exists(os.path.dirname(temp_file))
