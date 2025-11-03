import pytest

from pyorderly.outpack.location import (
    _find_all_dependencies,
    location_resolve_valid,
    outpack_location_add,
    outpack_location_add_path,
    outpack_location_list,
    outpack_location_remove,
    outpack_location_rename,
)

from ..helpers import (
    create_metadata_depends,
    create_temporary_root,
    create_temporary_roots,
)


def test_no_locations_except_local_by_default(tmp_path):
    root = create_temporary_root(tmp_path)
    locations = outpack_location_list(root)
    assert locations == ["local"]


def test_can_add_location(tmp_path):
    root = create_temporary_roots(tmp_path, ["a", "b", "c"])

    outpack_location_add_path("b", root["b"], root=root["a"])
    locations = outpack_location_list(root["a"])
    assert set(locations) == {"local", "b"}

    outpack_location_add_path("c", root["c"], root=root["a"])
    locations = outpack_location_list(root["a"])
    assert set(locations) == {"local", "b", "c"}


def test_cant_add_location_with_reserved_name(tmp_path):
    root = create_temporary_root(tmp_path)
    upstream = create_temporary_root(tmp_path)

    with pytest.raises(Exception) as e:
        outpack_location_add_path("local", upstream, root=root)
    assert e.match("Cannot add a location with reserved name 'local'")


def test_cant_add_location_with_existing_name(tmp_path):
    root = create_temporary_roots(tmp_path, ["a", "b", "c"])

    outpack_location_add_path("upstream", root["b"], root=root["a"])
    with pytest.raises(Exception) as e:
        outpack_location_add_path("upstream", root["c"], root=root["a"])
    assert e.match("A location with name 'upstream' already exists")
    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "upstream"}


def test_locations_must_be_paths(tmp_path):
    root = create_temporary_root(tmp_path / "a")
    locations = outpack_location_list(root=root)
    assert set(locations) == {"local"}

    other = tmp_path / "other"
    with pytest.raises(Exception) as e:
        outpack_location_add_path("other", other, root=root)

    assert e.match("Expected 'path' to be an existing directory")

    other.mkdir()
    with pytest.raises(Exception) as e:
        outpack_location_add_path("other", other, root=root)

    assert e.match("Did not find existing outpack root in .*")


def test_can_rename_a_location(tmp_path):
    root = create_temporary_roots(tmp_path, ["a", "b", "c"])

    outpack_location_add_path("b", root["b"], root=root["a"])

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "b"}

    outpack_location_rename("b", "c", root=root["a"])

    locations = outpack_location_list(root=root["a"])
    assert set(locations) == {"local", "c"}


def test_cant_rename_a_location_using_existent_name(tmp_path):
    root = create_temporary_roots(tmp_path, ["a", "b", "c"])

    outpack_location_add_path("b", root["b"], root=root["a"])
    outpack_location_add_path("c", root["c"], root=root["a"])

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
    root = create_temporary_roots(tmp_path, ["a", "b", "c"])

    outpack_location_add_path("b", root["b"], root=root["a"])
    outpack_location_add_path("c", root["c"], root=root["a"])

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
            "other", "custom", {"driver": "mydriver"}, root=root
        )

    assert e.match("Cannot add a location with type 'custom' yet.")


def test_can_resolve_locations(tmp_path):
    root = create_temporary_roots(tmp_path, ["dst", "a", "b", "c", "d"])
    for name in ["a", "b", "c", "d"]:
        outpack_location_add_path(name, root[name], root=root["dst"])

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


def test_resolve_location_makes_copy(tmp_path):
    root = create_temporary_roots(tmp_path, add_location=True)

    candidates = ["src", "local"]
    result = location_resolve_valid(
        candidates,
        root["dst"],
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )

    assert result == ["src"]
    assert candidates == ["src", "local"]


def test_can_resolve_dependencies_where_there_are_none():
    metadata = create_metadata_depends("a")
    deps = _find_all_dependencies(["a"], metadata)
    assert deps == ["a"]

    metadata = {
        **create_metadata_depends("a"),
        **create_metadata_depends("b", ["a"]),
    }
    deps = _find_all_dependencies(["a"], metadata)
    assert deps == ["a"]


def test_can_find_dependencies():
    metadata = {
        **create_metadata_depends("a"),
        **create_metadata_depends("b"),
        **create_metadata_depends("c"),
        **create_metadata_depends("d", ["a", "b"]),
        **create_metadata_depends("e", ["b", "c"]),
        **create_metadata_depends("f", ["a", "c"]),
        **create_metadata_depends("g", ["a", "f", "c"]),
        **create_metadata_depends("h", ["a", "b", "c"]),
        **create_metadata_depends("i", ["f"]),
        **create_metadata_depends("j", ["i", "e", "a"]),
    }

    assert _find_all_dependencies(["a"], metadata) == ["a"]
    assert _find_all_dependencies(["b"], metadata) == ["b"]
    assert _find_all_dependencies(["c"], metadata) == ["c"]

    assert _find_all_dependencies(["d"], metadata) == ["a", "b", "d"]
    assert _find_all_dependencies(["e"], metadata) == ["b", "c", "e"]
    assert _find_all_dependencies(["f"], metadata) == ["a", "c", "f"]

    assert _find_all_dependencies(["g"], metadata) == ["a", "c", "f", "g"]
    assert _find_all_dependencies(["h"], metadata) == ["a", "b", "c", "h"]
    assert _find_all_dependencies(["i"], metadata) == ["a", "c", "f", "i"]
    assert _find_all_dependencies(["j"], metadata) == [
        "a",
        "b",
        "c",
        "e",
        "f",
        "i",
        "j",
    ]


def test_can_find_multiple_dependencies_at_once():
    metadata = {
        **create_metadata_depends("a"),
        **create_metadata_depends("b"),
        **create_metadata_depends("c"),
        **create_metadata_depends("d", ["a", "b"]),
        **create_metadata_depends("e", ["b", "c"]),
        **create_metadata_depends("f", ["a", "c"]),
        **create_metadata_depends("g", ["a", "f", "c"]),
        **create_metadata_depends("h", ["a", "b", "c"]),
        **create_metadata_depends("i", ["f"]),
        **create_metadata_depends("j", ["i", "e", "a"]),
    }

    assert _find_all_dependencies([], metadata) == []
    assert _find_all_dependencies(["c", "b", "a"], metadata) == ["a", "b", "c"]
    assert _find_all_dependencies(["d", "e", "f"], metadata) == [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
    ]


def test_can_ignore_missing_dependencies():
    metadata = {
        **create_metadata_depends("a", ["b", "c"]),
        **create_metadata_depends("b", ["d"]),
        **create_metadata_depends("d"),
    }

    with pytest.raises(Exception, match="Unknown packet c"):
        _find_all_dependencies(["a"], metadata)

    assert _find_all_dependencies(
        ["a"], metadata, allow_missing_packets=True
    ) == ["a", "b", "c", "d"]
