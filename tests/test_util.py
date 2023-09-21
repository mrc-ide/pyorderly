import datetime

import pytest

from outpack.util import (
    assert_file_exists,
    expand_dirs,
    find_file_descend,
    iso_time_str,
    match_value,
    num_to_time,
    time_to_num,
)


def test_find_descend(tmp_path):
    (tmp_path / "a" / "b" / "c" / "d").mkdir(parents=True)
    (tmp_path / "a" / "b" / ".foo").mkdir(parents=True)
    assert find_file_descend(".foo", tmp_path / "a/b/c/d") == str(
        tmp_path / "a/b"
    )
    assert find_file_descend(".foo", tmp_path / "a") is None


def test_convert_iso_time():
    t = 1691686967.895351
    assert iso_time_str(t) == "20230810-170247"


def test_convert_num_to_time():
    t = 1691686967.895351
    x = datetime.datetime(
        2023, 8, 10, 17, 2, 47, 895351, tzinfo=datetime.timezone.utc
    )
    assert time_to_num(x) == t
    assert num_to_time(t) == x


def test_can_test_for_files_existing(tmp_path):
    with open(tmp_path / "a", "w"):
        pass
    with open(tmp_path / "b", "w"):
        pass
    with open(tmp_path / "c", "w"):
        pass
    assert_file_exists(["a", "b", "c"], workdir=tmp_path)
    assert_file_exists(["a"], workdir=tmp_path)
    assert_file_exists("a", workdir=tmp_path)

    with pytest.raises(Exception, match="File does not exist: x"):
        assert_file_exists(["a", "b", "x", "c"], workdir=tmp_path)
    with pytest.raises(Exception, match="File does not exist: x"):
        assert_file_exists(["x"], workdir=tmp_path)
    with pytest.raises(Exception, match="File does not exist: x"):
        assert_file_exists("x", workdir=tmp_path)


def test_can_expand_paths(tmp_path):
    sub_a = tmp_path / "a"
    sub_a.mkdir()
    sub_b = tmp_path / "b"
    sub_b.mkdir()
    with open(sub_a / "x", "w"):
        pass
    with open(sub_a / "y", "w"):
        pass
    assert expand_dirs([], workdir=tmp_path) == []
    assert set(expand_dirs(["a"], workdir=tmp_path)) == {"a/x", "a/y"}
    assert set(expand_dirs(["a", "b"], workdir=tmp_path)) == {"a/x", "a/y"}
    with open(sub_b / "x", "w"):
        pass
    assert set(expand_dirs(["a", "b"], workdir=tmp_path)) == {
        "a/x",
        "a/y",
        "b/x",
    }


def test_match_value():
    assert match_value("this", ["that", "this"], "name") is None
    with pytest.raises(Exception) as e:
        match_value("this", ["foo", "bar"], "name")
    assert e.match("name must be one of 'foo', 'bar'")
    with pytest.raises(Exception) as e:
        match_value("this", ["foo"], "name")
    assert e.match("name must be one of 'foo'")
