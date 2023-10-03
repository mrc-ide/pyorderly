import json

import pytest
from helpers import create_random_packet, create_temporary_root

from outpack.ids import outpack_id
from outpack.location import (
    location_resolve_valid,
    outpack_location_add,
    outpack_location_list,
    outpack_location_pull_metadata,
    outpack_location_remove,
    outpack_location_rename,
)
from outpack.util import read_string


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


def test_can_pull_metadata_from_a_file_base_location(tmp_path):
    root_upstream = create_temporary_root(
        tmp_path / "upstream", use_file_store=True
    )

    ids = [create_random_packet(root_upstream) for _ in range(3)]
    root_downstream = create_temporary_root(
        tmp_path / "downstream", use_file_store=True
    )

    outpack_location_add(
        "upstream",
        "path",
        {"path": str(root_upstream.path)},
        root=root_downstream,
    )
    assert outpack_location_list(root_downstream) == ["local", "upstream"]

    outpack_location_pull_metadata("upstream", root=root_downstream)

    metadata = root_downstream.index.all_metadata()
    assert len(metadata) == 3
    assert set(metadata.keys()) == set(ids)
    assert metadata == root_upstream.index.all_metadata()

    packets = root_downstream.index.location("upstream")
    assert len(packets) == 3
    assert set(packets.keys()) == set(ids)


def test_can_pull_empty_metadata(tmp_path):
    root_upstream = create_temporary_root(
        tmp_path / "upstream", use_file_store=True
    )
    root_downstream = create_temporary_root(
        tmp_path / "downstream", use_file_store=True
    )

    outpack_location_add(
        "upstream",
        "path",
        {"path": str(root_upstream.path)},
        root=root_downstream,
    )
    outpack_location_pull_metadata("upstream", root=root_downstream)

    index = root_downstream.index.data
    assert len(index.metadata) == 0


def test_can_pull_metadata_from_subset_of_locations(tmp_path):
    root = {"a": create_temporary_root(tmp_path / "a", use_file_store=True)}

    location_names = ["x", "y", "z"]
    for name in location_names:
        root[name] = create_temporary_root(tmp_path / name, use_file_store=True)
        outpack_location_add(
            name, "path", {"path": str(root[name].path)}, root=root["a"]
        )

    assert outpack_location_list(root["a"]) == ["local", "x", "y", "z"]

    ids = {}
    for name in location_names:
        ids[name] = [create_random_packet(root[name]) for _ in range(3)]

    outpack_location_pull_metadata(["x", "y"], root=root["a"])
    index = root["a"].index

    assert set(index.all_metadata()) == set(ids["x"] + ids["y"])
    locations = index.all_locations()
    assert set(locations.keys()) == {"local", "x", "y"}
    assert len(locations["local"]) == 0
    assert len(locations["x"]) == 3
    assert len(locations["y"]) == 3

    x_metadata = root["x"].index.all_metadata().keys()
    y_metadata = root["y"].index.all_metadata().keys()
    for packet_id in index.all_metadata().keys():
        if packet_id in ids["x"]:
            assert packet_id in x_metadata
        else:
            assert packet_id in y_metadata

    outpack_location_pull_metadata(root=root["a"])
    index = root["a"].index

    assert set(index.all_metadata()) == set(ids["x"] + ids["y"] + ids["z"])
    locations = index.all_locations()
    assert set(locations.keys()) == {"local", "x", "y", "z"}
    assert len(locations["local"]) == 0
    assert len(locations["x"]) == 3
    assert len(locations["y"]) == 3
    assert len(locations["z"]) == 3
    z_metadata = root["z"].index.all_metadata().keys()
    for packet_id in index.all_metadata().keys():
        if packet_id in ids["x"]:
            assert packet_id in x_metadata
        elif packet_id in ids["y"]:
            assert packet_id in y_metadata
        else:
            assert packet_id in z_metadata


def test_cant_pull_metadata_from_an_unknown_location(tmp_path):
    root = create_temporary_root(tmp_path)
    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata("upstream", root=root)

    assert e.match("Unknown location: 'upstream'")


def test_noop_to_pull_metadata_from_no_locations(tmp_path):
    root = create_temporary_root(tmp_path)
    outpack_location_pull_metadata("local", root=root)
    outpack_location_pull_metadata(root=root)


def test_handle_metadata_where_hash_does_not_match_reported(tmp_path):
    here = create_temporary_root(tmp_path / "here")
    there = create_temporary_root(tmp_path / "there")
    outpack_location_add("server", "path", {"path": str(there.path)}, root=here)
    packet_id = create_random_packet(there)

    path_metadata = there.path / ".outpack" / "metadata" / packet_id
    parsed = json.loads(read_string(path_metadata))
    with open(path_metadata, "w") as f:
        f.write(json.dumps(parsed, indent=4))

    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata(root=here)

    assert e.match(
        f"Hash of metadata for '{packet_id}' from 'server' does not match:"
    )
    assert e.match("This is bad news")
    assert e.match("remove this location")


def test_handle_metadata_where_two_locations_differ_in_hash_for_same_id(
    tmp_path,
):
    root = {}

    for name in ["a", "b", "us"]:
        root[name] = create_temporary_root(tmp_path / name)

    packet_id = outpack_id()
    create_random_packet(root["a"], packet_id=packet_id)
    create_random_packet(root["b"], packet_id=packet_id)

    outpack_location_add(
        "a", "path", {"path": str(root["a"].path)}, root=root["us"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["us"]
    )

    outpack_location_pull_metadata(location="a", root=root["us"])

    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata(location="b", root=root["us"])

    assert e.match(
        "We have been offered metadata from 'b' that has a different"
    )
    assert e.match(f"Conflicts for: '{packet_id}'")
    assert e.match("please let us know")
    assert e.match("remove this location")


def test_can_pull_metadata_through_chain_of_locations(tmp_path):
    root = {}
    for name in ["a", "b", "c", "d"]:
        root[name] = create_temporary_root(tmp_path / name)

    # More interesting topology, with a chain of locations, but d also
    # knowing directly about an earlier location
    # > a -> b -> c -> d
    # >       `-------/
    outpack_location_add(
        "a", "path", {"path": str(root["a"].path)}, root=root["b"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["c"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["d"]
    )
    outpack_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["d"]
    )

    # Create a packet and make sure it's in both b and c
    create_random_packet(root["a"])
    outpack_location_pull_metadata(root=root["b"])
    # TODO: complete test once orderly_location_pull_packet


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


def test_informative_error_when_no_locations_configured(tmp_path):
    root = create_temporary_root(tmp_path)

    locations = location_resolve_valid(
        None,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )
    assert locations == []

    with pytest.raises(Exception) as e:
        location_resolve_valid(
            None,
            root,
            include_local=False,
            include_orphan=False,
            allow_no_locations=False,
        )

    assert e.match("No suitable location found")
