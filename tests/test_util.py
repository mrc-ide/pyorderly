import datetime
import os

import pytest

from outpack.util import (
    all_normal_files,
    assert_file_exists,
    expand_dirs,
    find_file_descend,
    format_list,
    iso_time_str,
    match_value,
    num_to_time,
    partition,
    pl,
    read_string,
    run_script,
    time_to_num,
)

from . import helpers


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


def test_all_normal_files_recurses(tmp_path):
    helpers.touch_files(
        tmp_path / "foo.txt",
        tmp_path / "a" / "bar.txt",
        tmp_path / "a" / "b" / "baz.txt",
        tmp_path / "a" / "b" / "c" / "quux.txt",
    )
    expected = {
        "foo.txt",
        os.path.join("a", "bar.txt"),
        os.path.join("a", "b", "baz.txt"),
        os.path.join("a", "b", "c", "quux.txt"),
    }
    assert set(all_normal_files(tmp_path)) == expected


def test_all_normal_files_excludes_pycache(tmp_path):
    helpers.touch_files(
        tmp_path / "a.txt",
        tmp_path / "__pycache__" / "b.txt",
        tmp_path / "__pycache__" / "a" / "c.txt",
        tmp_path / "a" / "d.txt",
        tmp_path / "a" / "__pycache__" / "e.txt",
    )

    expected = {
        "a.txt",
        os.path.join("a", "d.txt"),
    }
    assert set(all_normal_files(tmp_path)) == expected


def test_can_expand_paths(tmp_path):
    helpers.touch_files(tmp_path / "a" / "x", tmp_path / "a" / "y")
    (tmp_path / "b").mkdir()

    expected_ax_ay = {os.path.join("a", "x"), os.path.join("a", "y")}

    assert expand_dirs([], workdir=tmp_path) == []
    assert set(expand_dirs(["a"], workdir=tmp_path)) == expected_ax_ay
    assert set(expand_dirs(["a", "b"], workdir=tmp_path)) == expected_ax_ay

    helpers.touch_files(tmp_path / "b" / "x")

    expected_ax_ay_bx = {
        os.path.join("a", "x"),
        os.path.join("a", "y"),
        os.path.join("b", "x"),
    }
    assert set(expand_dirs(["a", "b"], workdir=tmp_path)) == expected_ax_ay_bx


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


def test_can_format_list():
    assert format_list(["one", "two"]) == "'one', 'two'"
    assert format_list(["one"]) == "'one'"
    assert format_list({"one", "two"}) in ("'one', 'two'", "'two', 'one'")
    assert format_list({"one", "one"}) == "'one'"  # noqa:B033


def test_can_pluralise():
    assert pl([], "item") == "items"
    assert pl(["one"], "item") == "item"
    assert pl(["one", "two"], "item") == "items"
    assert pl({"Inky"}, "octopus", "octopodes") == "octopus"
    assert pl({"Inky", "Tentacool"}, "octopus", "octopodes") == "octopodes"
    assert pl(2, "item") == "items"
    assert pl(1, "item") == "item"


def test_can_partition():
    test_list = ["one", "two", "three", "four", "five"]
    true_list, false_list = partition(lambda x: "e" in x, test_list)
    assert true_list == ["one", "three", "five"]
    assert false_list == ["two", "four"]

    true_list, false_list = partition(lambda x: "z" in x, test_list)
    assert true_list == []
    assert false_list == test_list
