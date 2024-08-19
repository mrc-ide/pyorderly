import pytest

from pyorderly.outpack.config import read_config
from pyorderly.outpack.init import outpack_init


def test_can_create_new_repo(tmp_path):
    path = outpack_init(tmp_path)
    assert path.exists()
    assert path.is_dir()
    cfg = read_config(path)
    assert cfg.core.hash_algorithm == "sha256"
    assert cfg.core.path_archive == "archive"
    assert not cfg.core.use_file_store
    assert not cfg.core.require_complete_tree
    assert len(cfg.location) == 1
    assert list(cfg.location.keys()) == ["local"]


def test_fail_to_create_with_error_if_path_exists(tmp_path):
    root = tmp_path / "root"
    root.touch()
    with pytest.raises(
        Exception, match="Path '.+' already exists but is not a directory"
    ):
        outpack_init(root)


def test_allow_reinitialising_with_same_options(tmp_path):
    path1 = outpack_init(
        tmp_path,
        path_archive=None,
        use_file_store=True,
        require_complete_tree=True,
    )
    path2 = outpack_init(
        tmp_path,
        path_archive=None,
        use_file_store=True,
        require_complete_tree=True,
    )
    assert path1 == path2


def test_require_same_options_if_reinitialising(tmp_path):
    outpack_init(tmp_path)
    with pytest.raises(Exception) as e:
        outpack_init(
            tmp_path,
            path_archive=None,
            use_file_store=True,
            require_complete_tree=True,
        )
    assert "Trying to change configuration when re-initialising:" in str(e)
    assert "* 'path_archive' was archive but None requested" in str(e)
    assert "* 'require_complete_tree' was False but True requested" in str(e)
    assert "* 'use_file_store' was False but True requested" in str(e)
