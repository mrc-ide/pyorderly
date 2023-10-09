import pytest

from outpack.util import transient_working_directory
from outpack.root import OutpackRoot, root_open
from orderly.current import detect_orderly_interactive_root

import helpers


def test_can_open_root_interactively(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    res = detect_orderly_interactive_root(src)
    assert isinstance(res, type(root))
    assert res.path == root.path
    assert res.config == root.config


def test_can_error_if_interactive_directory_incorrect(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    src = tmp_path / "src" / "x"
    src.mkdir(parents=True)
    child = src / "a"
    child.mkdir()
    with pytest.raises(Exception, match="Failed to detect orderly path at"):
        detect_orderly_interactive_root(src.parent)
    with pytest.raises(Exception, match="Failed to detect orderly path at"):
        detect_orderly_interactive_root(child)
