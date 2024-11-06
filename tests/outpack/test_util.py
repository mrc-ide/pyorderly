import datetime
import os
import re

import pytest

from pyorderly.outpack.util import (
    all_normal_files,
    as_posix_path,
    assert_file_exists,
    assert_relative_path,
    expand_dirs,
    find_file_descend,
    format_list,
    iso_time_str,
    match_value,
    num_to_time,
    openable_temporary_file,
    partition,
    pl,
    read_string,
    time_to_num,
)

from .. import helpers


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


def test_can_test_for_relative_path():
    assert_relative_path("foo.txt", "file")
    assert_relative_path("dir/foo.txt", "file")

    pattern = r"Expected file path '[/\\]foo.txt' to be a relative path"
    with pytest.raises(Exception, match=pattern):
        assert_relative_path("/foo.txt", "file")

    pattern = r"Path '..[/\\]foo.txt' must not contain '..' component"
    with pytest.raises(Exception, match=pattern):
        assert_relative_path("../foo.txt", "file")

    pattern = r"Path 'aa[/\\]..[/\\]foo.txt' must not contain '..' component"
    with pytest.raises(Exception, match=pattern):
        assert_relative_path("aa/../foo.txt", "file")


@pytest.mark.skipif(os.name != "nt", reason="Windows-specific test")
def test_can_test_for_relative_path_windows():
    msg = r"Expected file path 'C:\aa\foo.txt' to be a relative path"
    with pytest.raises(Exception, match=re.escape(msg)):
        assert_relative_path(r"C:\aa\foo.txt", "file")

    msg = r"Expected file path 'C:foo.txt' to be a relative path"
    with pytest.raises(Exception, match=re.escape(msg)):
        assert_relative_path(r"C:foo.txt", "file")

    msg = r"Expected file path '\aa\foo.txt' to be a relative path"
    with pytest.raises(Exception, match=re.escape(msg)):
        assert_relative_path(r"\aa\foo.txt", "file")


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


def test_openable_temporary_file():
    with openable_temporary_file(mode="w") as f1:
        f1.write("Hello")
        f1.close()

        with open(f1.name) as f2:
            assert f2.read() == "Hello"
    assert not os.path.exists(f1.name)

    with openable_temporary_file(mode="r") as f1:
        with open(f1.name, "w") as f2:
            f2.write("Hello")

        assert f1.read() == "Hello"
    assert not os.path.exists(f1.name)


def test_as_posix_path():
    from os.path import join

    input = join("foo", "bar", "baz")
    assert as_posix_path(input) == "foo/bar/baz"

    input = [join("hello", "world"), join("foo", "bar", "baz")]
    assert as_posix_path(input) == ["hello/world", "foo/bar/baz"]

    input = {
        join("here", "aaa"): join("there", "bbb"),
        join("foo", "bar"): join("baz", "qux"),
    }
    assert as_posix_path(input) == {
        "here/aaa": "there/bbb",
        "foo/bar": "baz/qux",
    }
