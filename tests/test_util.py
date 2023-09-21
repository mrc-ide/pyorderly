import datetime

import pytest

from outpack.util import (
    find_file_descend,
    iso_time_str,
    match_value,
    num_to_time,
    time_to_num,
    read_string
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


def test_match_value():
    assert match_value("this", ["that", "this"], "name") is None
    with pytest.raises(Exception) as e:
        match_value("this", ["foo", "bar"], "name")
    assert e.match("name must be one of 'foo', 'bar'")
    with pytest.raises(Exception) as e:
        match_value("this", ["foo"], "name")
    assert e.match("name must be one of 'foo'")


def test_read_string(tmp_path):
    lines = ["   this is my first   line\t ", " this is the second  "]
    path = tmp_path / "file"
    with open(path, "w") as f:
        f.writelines(lines)

    assert read_string(path) == "this is my first   line\t  this is the second"