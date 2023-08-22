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


def test_can_open_root_from_a_subdir():
    r = root_open("example", False)
    assert root_open("example/src", True).path == r.path
    assert root_open("example/src/data", True).path == r.path


def test_can_open_root_from_wd():
    p = Path("example").resolve()
    with transient_working_directory("example"):
        assert root_open(None, True).path == p
        assert root_open(None, False).path == p


def test_can_error_if_recursion_to_find_root_fails():
    p = Path("example").resolve()
    with transient_working_directory("example/src/data"):
        assert root_open(None, True).path == p
        with pytest.raises(Exception, match="Did not find existing outpack"):
            assert root_open(None, False).path == p


def test_roots_are_handed_back():
    r = root_open("example", False)
    assert root_open(r, True) == r
    assert root_open(r, False) == r


def test_paths_must_be_directories(tmp_path):
    p = tmp_path / "p"
    with p.open("w"):
        pass
    with pytest.raises(Exception, match="to be an existing directory"):
        assert root_open(p, False)
