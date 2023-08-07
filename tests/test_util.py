from outpack.util import find_file_descend


def test_find_descend(tmp_path):
    (tmp_path / "a" / "b" / "c" / "d").mkdir(parents=True)
    (tmp_path / "a" / "b" / ".foo").mkdir(parents=True)
    assert find_file_descend(".foo", tmp_path / "a/b/c/d") == str(tmp_path / "a/b")
    assert find_file_descend(".foo", tmp_path / "a") is None
