import datetime
import sys

import pytest

from outpack.util import (
    assert_file_exists,
    expand_dirs,
    find_file_descend,
    iso_time_str,
    match_value,
    num_to_time,
    read_string,
    run_script,
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
    expected_ax_ay = {"windows": {"a\\x", "a\\y"}, "unix": {"a/x", "a/y"}}
    platform = "windows" if sys.platform.startswith("win") else "unix"
    assert expand_dirs([], workdir=tmp_path) == []
    assert set(expand_dirs(["a"], workdir=tmp_path)) == expected_ax_ay[platform]
    assert (
        set(expand_dirs(["a", "b"], workdir=tmp_path))
        == expected_ax_ay[platform]
    )
    with open(sub_b / "x", "w"):
        pass
    expected_ax_ay_bx = {
        "windows": {"a\\x", "a\\y", "b\\x"},
        "unix": {"a/x", "a/y", "b/x"},
    }
    assert (
        set(expand_dirs(["a", "b"], workdir=tmp_path))
        == expected_ax_ay_bx[platform]
    )


def test_match_value():
    assert match_value("this", ["that", "this"], "name") is None
    with pytest.raises(Exception) as e:
        match_value("this", ["foo", "bar"], "name")
    assert e.match("name must be one of 'foo', 'bar'")
    with pytest.raises(Exception) as e:
        match_value("this", ["foo"], "name")
    assert e.match("name must be one of 'foo'")


def test_read_string(tmp_path):
    lines = ["  this is my first  line\t ", " this is the second  "]
    path = tmp_path / "file"
    with open(path, "w") as f:
        f.writelines(lines)

    assert read_string(path) == "  this is my first  line\t  this is the second"


def test_can_inject_data_into_run(tmp_path):
    lines = ["with open('result.txt', 'w') as f:\n  f.write(str(a))\n"]
    path = tmp_path / "script.py"
    with open(path, "w") as f:
        f.writelines(lines)
    run_script(tmp_path, "script.py", {"a": "hello"})
    assert tmp_path.joinpath("result.txt").exists()
    with open(tmp_path.joinpath("result.txt")) as f:
        assert f.read() == "hello"
