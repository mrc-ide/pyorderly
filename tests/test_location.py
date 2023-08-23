import pytest

from outpack.location import (
    orderly_location_add,
    orderly_location_list,
    orderly_location_remove,
    orderly_location_rename,
)
from outpack.test_util import create_temporary_root


def test_no_locations_except_local_by_default(tmp_path):
    root = create_temporary_root(tmp_path)
    locations = orderly_location_list(root)
    assert locations == ["local"]


def test_can_add_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    orderly_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    locations = orderly_location_list(root["a"])
    assert set(locations) == {"local", "b"}

    orderly_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )
    locations = orderly_location_list(root["a"])
    assert set(locations) == {"local", "b", "c"}


def test_cant_add_location_with_reserved_name(tmp_path):
    root = create_temporary_root(tmp_path)
    upstream = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        orderly_location_add(
            "local", "path", {"path": str(upstream.path)}, root=root
        )
    assert e.match("Cannot add a location with reserved name 'local'")


def test_cant_add_location_with_existing_name(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    orderly_location_add(
        "upstream", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    with pytest.raises(Exception) as e:
        orderly_location_add(
            "upstream", "path", {"path": str(root["c"].path)}, root=root["a"]
        )
    assert e.match("A location with name 'upstream' already exists")
    locations = orderly_location_list(root=root["a"])
    assert set(locations) == {"local", "upstream"}


def test_locations_must_be_paths(tmp_path):
    root = create_temporary_root(tmp_path / "a")
    locations = orderly_location_list(root=root)
    assert set(locations) == {"local"}

    other = tmp_path / "other"
    with pytest.raises(Exception) as e:
        orderly_location_add("other", "path", {"path": str(other)}, root=root)

    assert e.match("Expected 'path' to be an existing directory")

    other.mkdir()
    with pytest.raises(Exception) as e:
        orderly_location_add("other", "path", {"path": str(other)}, root=root)

    assert e.match("Did not find existing outpack root in .*")


def test_can_rename_a_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    orderly_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )

    locations = orderly_location_list(root=root["a"])
    assert set(locations) == {"local", "b"}

    orderly_location_rename("b", "c", root=root["a"])

    locations = orderly_location_list(root=root["a"])
    assert set(locations) == {"local", "c"}


def test_cant_rename_a_location_using_existent_name(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    orderly_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    orderly_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )

    with pytest.raises(Exception) as e:
        orderly_location_rename("b", "c", root["a"])

    assert e.match("A location with name 'c' already exists")

    with pytest.raises(Exception) as e:
        orderly_location_rename("b", "local", root["a"])

    assert e.match("A location with name 'local' already exists")


def test_cant_rename_a_non_existent_location(tmp_path):
    root = create_temporary_root(tmp_path)
    assert set(orderly_location_list(root=root)) == {"local"}

    with pytest.raises(Exception) as e:
        orderly_location_rename("a", "b", root)

    assert e.match("No location with name 'a' exists")


def test_cant_rename_default_locations(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        orderly_location_rename("local", "desktop", root)

    assert e.match("Cannot rename default location 'local'")


def test_can_remove_a_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    orderly_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    orderly_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )

    locations = orderly_location_list(root=root["a"])
    assert set(locations) == {"local", "b", "c"}

    # Remove a location without packets
    orderly_location_remove("c", root=root["a"])

    locations = orderly_location_list(root=root["a"])
    assert set(locations) == {"local", "b"}

    ## TODO:
    ## Test removing a location with a packet
    ## Test packets marked as orphan


def test_cant_remove_default_locations(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        orderly_location_remove("local", root)

    assert e.match("Cannot remove default location 'local'")


def test_cant_remove_non_existent_location(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        orderly_location_remove("b", root)

    assert e.match("No location with name 'b' exists")
