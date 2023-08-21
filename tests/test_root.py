from outpack.config import read_config
from outpack.filestore import FileStore
from outpack.index import Index
from outpack.init import outpack_init
from outpack.root import root_open


def test_can_open_existing_root():
    r = root_open("example", False)
    assert r.config == read_config("example")
    assert isinstance(r.files, FileStore)
    assert isinstance(r.index, Index)


def test_can_open_root_with_no_store(tmp_path):
    outpack_init(tmp_path)
    r = root_open(tmp_path, False)
    assert r.config == read_config(tmp_path)
    assert r.files is None
    assert isinstance(r.index, Index)


def test_can_open_root_from_subdir():
    r = root_open("example", False)
    assert root_open("example/src", True).path == r.path
    assert root_open("example/src/data", True).path == r.path
