from outpack.config import read_config
from outpack.filestore import FileStore
from outpack.init import outpack_init
from outpack.index import Index
from outpack.root import Root


def test_can_open_existing_root():
    r = Root("example")
    assert r.config == read_config("example")
    assert isinstance(r.files, FileStore)
    assert isinstance(r.index, Index)


def test_can_open_root_with_no_store(tmp_path):
    outpack_init(tmp_path)
    r = Root(tmp_path)
    assert r.config == read_config(tmp_path)
    assert r.files is None
    assert isinstance(r.index, Index)
