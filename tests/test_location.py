import pytest

from outpack.location import (
    location_resolve_valid,
    outpack_location_add,
    outpack_location_list,
    outpack_location_remove,
    outpack_location_rename,
)

from .helpers import create_temporary_root


def test_no_locations_except_local_by_default(tmp_path):
    root = create_temporary_root(tmp_path)
    locations = outpack_location_list(root)
    assert locations == ["local"]


def test_can_add_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    locations = outpack_location_list(root["a"])
    assert set(locations) == {"local", "b"}

    outpack_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )
    locations = outpack_location_list(root["a"])
    assert set(locations) == {"local", "b", "c"}


def test_cant_add_location_with_reserved_name(tmp_path):
    root = create_temporary_root(tmp_path)
    upstream = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_add(
            "local", "path", {"path": str(upstream.path)}, root=root
        )
    assert e.match("Cannot add a location with reserved name 'local'")


def test_cant_add_location_with_existing_name(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    outpack_location_add(
        "upstream", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    with pytest.raises(Exception) as e:
        outpack_location_add(
            "upstream", "path", {"path": str(root["c"].path)}, root=root["a"]
        )
    assert e.match("A location with name 'upstream' already exists")
    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "upstream"}


def test_locations_must_be_paths(tmp_path):
    root = create_temporary_root(tmp_path / "a")
    locations = outpack_location_list(root=root)
    assert set(locations) == {"local"}

    other = tmp_path / "other"
    with pytest.raises(Exception) as e:
        outpack_location_add("other", "path", {"path": str(other)}, root=root)

    assert e.match("Expected 'path' to be an existing directory")

    other.mkdir()
    with pytest.raises(Exception) as e:
        outpack_location_add("other", "path", {"path": str(other)}, root=root)

    assert e.match("Did not find existing outpack root in .*")


def test_can_rename_a_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "b"}

    outpack_location_rename("b", "c", root=root["a"])

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "c"}


def test_cant_rename_a_location_using_existent_name(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    outpack_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )

    with pytest.raises(Exception) as e:
        outpack_location_rename("b", "c", root["a"])

    assert e.match("A location with name 'c' already exists")

    with pytest.raises(Exception) as e:
        outpack_location_rename("b", "local", root["a"])

    assert e.match("A location with name 'local' already exists")


def test_cant_rename_a_non_existent_location(tmp_path):
    root = create_temporary_root(tmp_path)
    assert set(outpack_location_list(root=root)) == {"local"}

    with pytest.raises(Exception) as e:
        outpack_location_rename("a", "b", root)

    assert e.match("No location with name 'a' exists")


def test_cant_rename_default_locations(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_rename("local", "desktop", root)

    assert e.match("Cannot rename default location 'local'")


def test_can_remove_a_location(tmp_path):
    root = {}
    for name in ["a", "b", "c"]:
        root[name] = create_temporary_root(tmp_path / name)

    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["a"]
    )
    outpack_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["a"]
    )

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "b", "c"}

    # Remove a location without packets
    outpack_location_remove("c", root=root["a"])

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "b"}

    ## TODO:
    ## Test removing a location with a packet
    ## Test packets marked as orphan mrc-4601


def test_cant_remove_default_locations(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_remove("local", root)

    assert e.match("Cannot remove default location 'local'")


def test_cant_remove_non_existent_location(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_remove("b", root)

    assert e.match("No location with name 'b' exists")


def test_validate_arguments_to_path_locations(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_add("other", "path", {"root": "mypath"}, root=root)

    assert e.match("Fields missing from args: 'path'")


def test_cant_add_wip_location_type(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_add(
            "other", "http", {"url": "https://example.com"}, root=root
        )

    assert e.match("Cannot add a location with type 'http' yet.")

    with pytest.raises(Exception) as e:
        outpack_location_add(
            "other", "custom", {"driver": "mydriver"}, root=root
        )

    assert e.match("Cannot add a location with type 'custom' yet.")


def test_can_resolve_locations(tmp_path):
    root = {}
    for name in ["dst", "a", "b", "c", "d"]:
        root[name] = create_temporary_root(tmp_path / name)
        if not name == "dst":
            outpack_location_add(
                name, "path", {"path": str(root[name].path)}, root=root["dst"]
            )

    locations = location_resolve_valid(
        None,
        root["dst"],
        include_local=False,
        include_orphan=False,
        allow_no_locations=False,
    )
    assert locations == ["a", "b", "c", "d"]
    locations = location_resolve_valid(
        None,
        root["dst"],
        include_local=True,
        include_orphan=False,
        allow_no_locations=False,
    )
    assert locations == ["local", "a", "b", "c", "d"]
    locations = location_resolve_valid(
        None,
        root["dst"],
        include_local=True,
        include_orphan=True,
        allow_no_locations=False,
    )
    assert locations == ["local", "a", "b", "c", "d"]
    locations = location_resolve_valid(
        ["a", "b", "local", "d"],
        root["dst"],
        include_local=False,
        include_orphan=False,
        allow_no_locations=False,
    )
    assert locations == ["a", "b", "d"]
    locations = location_resolve_valid(
        ["a", "b", "local", "d"],
        root["dst"],
        include_local=True,
        include_orphan=False,
        allow_no_locations=False,
    )
    assert locations == ["a", "b", "local", "d"]

    with pytest.raises(Exception) as e:
        location_resolve_valid(
            True,
            root["dst"],
            include_local=True,
            include_orphan=False,
            allow_no_locations=False,
        )

    assert e.match(
        "Invalid input for 'location'; expected None or a list of strings"
    )

    with pytest.raises(Exception) as e:
        location_resolve_valid(
            "other",
            root["dst"],
            include_local=True,
            include_orphan=False,
            allow_no_locations=False,
        )

    assert e.match("Unknown location: 'other'")

    with pytest.raises(Exception) as e:
        location_resolve_valid(
            ["a", "b", "f", "g"],
            root["dst"],
            include_local=True,
            include_orphan=False,
            allow_no_locations=False,
        )

    assert e.match("Unknown location: '[fg]', '[fg]'")
