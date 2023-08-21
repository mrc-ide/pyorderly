from pathlib import Path
import pytest

from outpack.config import read_config
from outpack.filestore import FileStore
from outpack.index import Index
from outpack.init import outpack_init
from outpack.root import root_open
from outpack.util import transient_working_directory


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


def test_can_open_root_from_wd():
    p = Path("example").resolve()
    with transient_working_directory("example"):
        assert root_open(None, True).path == p
        assert root_open(None, False).path == p


def test_can_open_root_from_subdir():
    p = Path("example").resolve()
    with transient_working_directory("example/src/data"):
        assert root_open(None, True).path == p
        with pytest.raises(Exception, match="Did not find existing outpack"):
            assert root_open(None, False).path == p
