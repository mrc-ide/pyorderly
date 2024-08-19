from pathlib import Path

import pytest

from pyorderly.outpack.location import outpack_location_add_path
from pyorderly.outpack.location_pull import outpack_location_pull_metadata
from pyorderly.outpack.search import Query, search, search_unique
from pyorderly.outpack.search_options import SearchOptions

from ..helpers import (
    create_random_packet,
    create_temporary_root,
    create_temporary_roots,
)


def test_can_do_simple_search(tmp_path):
    root = create_temporary_root(tmp_path)
    id = create_random_packet(root, "data")

    assert search("latest", root=root) == {id}
    assert search(f"'{id}'", root=root) == {id}
    assert search("latest(name == 'data')", root=root) == {id}


def test_can_search_by_parameters(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, parameters={"a": 1})
    id2 = create_random_packet(root, parameters={"a": 2})

    assert search("parameter:a == 1", root=root) == {id1}
    assert search("parameter:a == 2", root=root) == {id2}


def test_can_return_multiple_results(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = {create_random_packet(root, "data") for i in range(10)}

    assert search("name == 'data'", root=root) == ids


def test_can_return_no_results_on_miss(tmp_path):
    root = create_temporary_root(tmp_path)
    assert len(search("latest", root=root)) == 0
    assert len(search("name == 'data'", root=root)) == 0


def test_single_errors_on_multiple_results(tmp_path):
    root = create_temporary_root(tmp_path)
    create_random_packet(root, "data")
    create_random_packet(root, "data")

    with pytest.raises(
        ValueError, match="Query found 2 packets, but expected exactly one"
    ):
        search("single(name == 'data')", root=root)


def test_single_errors_on_no_results(tmp_path):
    root = create_temporary_root(tmp_path)
    with pytest.raises(
        ValueError, match="Query found 0 packets, but expected exactly one"
    ):
        search("single(name == 'nonexistent')", root=root)


def test_single_valued():
    assert Query.parse("latest").is_single_valued()
    assert Query.parse("latest(name == 'data')").is_single_valued()
    assert Query.parse("single(name == 'data')").is_single_valued()
    assert Query.parse("'20230807-152345-1ad02157'").is_single_valued()
    assert Query.parse("id == '20230807-152345-1ad02157'").is_single_valued()
    assert Query.parse("'20230807-152345-1ad02157' == id").is_single_valued()

    assert not Query.parse("name == 'data'").is_single_valued()
    assert not Query.parse(
        "id != '20230807-152345-1ad02157'"
    ).is_single_valued()
    assert not Query.parse(
        "(id == '20230807-152345-1ad02157') || (id == '20230814-163026-ac5900c0')"
    ).is_single_valued()


def test_search_unique_succeeds(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, "data")
    id2 = create_random_packet(root, "other")

    assert search_unique(f"'{id1}'", root=root) == id1
    assert search_unique(f"id == '{id1}'", root=root) == id1
    assert search_unique("latest", root=root) == id2
    assert search_unique("latest(name == 'data')", root=root) == id1


def test_search_unique_fails_on_miss(tmp_path):
    root = create_temporary_root(tmp_path)
    create_random_packet(root, "data", parameters={"a": 1})

    with pytest.raises(Exception, match="Failed to find packet"):
        search_unique("latest(name == 'other')", root=root)

    with pytest.raises(Exception, match="Failed to find packet"):
        search_unique("latest(parameter:a == 2)", root=root)


def test_search_unique_fails_on_non_single_values(tmp_path):
    root = create_temporary_root(tmp_path)
    id = create_random_packet(root)

    with pytest.raises(
        Exception, match="Query is not guaranteed to return a single packet."
    ):
        search_unique("name == 'data'", root=root)

    with pytest.raises(
        Exception, match="Query is not guaranteed to return a single packet."
    ):
        search_unique(f"id != '{id}'", root=root)


def test_search_with_this_parameter(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, name="foo")
    id2 = create_random_packet(root, name="bar")

    assert search("name == this:x", root=root, this={"x": "foo"}) == {id1}
    assert search("name == this:x", root=root, this={"x": "bar"}) == {id2}
    assert search("name == this:x", root=root, this={"x": "baz"}) == set()


def test_search_with_missing_this_parameter(tmp_path):
    root = create_temporary_root(tmp_path)
    create_random_packet(root)

    with pytest.raises(
        Exception, match="this parameters are not supported in this context"
    ):
        search("name == this:x", root=root)

    with pytest.raises(Exception, match="Parameter `x` was not found"):
        search("name == this:x", root=root, this={"y": 1})


def test_search_with_missing_parameter(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, parameters={"x": 1})
    id2 = create_random_packet(root, parameters={"y": 1})

    # Missing parameters are just silently ignored
    assert search("parameter:x == 1", root=root) == {id1}
    assert search("parameter:y == 1", root=root) == {id2}


def test_search_with_negation(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, name="data", parameters={"x": 1})
    id2 = create_random_packet(root, name="data", parameters={"x": 2})
    id3 = create_random_packet(root, name="other")

    assert search("!(name == 'other')", root=root) == {id1, id2}
    assert search("!(name == 'data')", root=root) == {id3}

    # These two have slightly semantics when the x parameter is undefined.
    # Former matches even when x is missing, latter does not.
    assert search("!(parameter:x == 1)", root=root) == {id2, id3}
    assert search("parameter:x != 1", root=root) == {id2}


def test_search_boolean(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, name="data", parameters={"x": 1})
    id2 = create_random_packet(root, name="data", parameters={"x": 2})
    id3 = create_random_packet(root, name="other", parameters={"x": 1})
    id4 = create_random_packet(root, name="other", parameters={"x": 2})

    assert search("name == 'data' && parameter:x == 1", root=root) == {id1}
    assert search("name == 'data' || parameter:x == 1", root=root) == {
        id1,
        id2,
        id3,
    }
    assert search("parameter:x == 1 || parameter:x == 2", root=root) == {
        id1,
        id2,
        id3,
        id4,
    }


def test_search_comparison(tmp_path):
    root = create_temporary_root(tmp_path)
    id1 = create_random_packet(root, parameters={"number": 1})
    id2 = create_random_packet(root, parameters={"number": 2})
    id3 = create_random_packet(root, parameters={"number": 3})
    id4 = create_random_packet(root, parameters={"number": 4})

    assert search("parameter:number == 1", root=root) == {id1}
    assert search("parameter:number != 1", root=root) == {id2, id3, id4}
    assert search("parameter:number > 2", root=root) == {id3, id4}
    assert search("parameter:number >= 2", root=root) == {id2, id3, id4}
    assert search("parameter:number < 3", root=root) == {id1, id2}
    assert search("parameter:number <= 3", root=root) == {id1, id2, id3}


def test_search_accepts_str_or_path_root(tmp_path):
    root = create_temporary_root(tmp_path)
    id = create_random_packet(root, "data")

    assert isinstance(root.path, Path)

    assert search("latest(name == 'data')", root=root.path) == {id}
    assert search("latest(name == 'data')", root=str(root.path)) == {id}
    assert search_unique("latest(name == 'data')", root=root.path) == id
    assert search_unique("latest(name == 'data')", root=str(root.path)) == id


def test_search_environment_not_supported(tmp_path):
    root = create_temporary_root(tmp_path)
    create_random_packet(root)

    with pytest.raises(
        NotImplementedError, match="environment lookup is not supported"
    ):
        search("name == environment:x", root=root)


def test_search_returns_remote_packets(tmp_path):
    root = create_temporary_roots(tmp_path, add_location=True)
    id = create_random_packet(root["src"])
    outpack_location_pull_metadata(root=root["dst"])

    def do_search(**kwargs):
        options = SearchOptions(**kwargs)
        return search("name == 'data'", root=root["dst"], options=options)

    assert do_search() == set()
    assert do_search(allow_remote=True) == {id}


def test_search_can_pull_automatically(tmp_path):
    root = create_temporary_roots(tmp_path, add_location=True)
    id = create_random_packet(root["src"])

    def do_search(**kwargs):
        options = SearchOptions(**kwargs)
        return search("name == 'data'", root=root["dst"], options=options)

    assert do_search(allow_remote=True) == set()
    assert do_search(allow_remote=True, pull_metadata=True) == {id}

    # At this point the packet has been pulled as is available even without the
    # flag.
    assert do_search(allow_remote=True) == {id}


def test_search_can_filter_by_location(tmp_path):
    root = create_temporary_roots(tmp_path, names=["x", "y", "dst"])
    x_id = create_random_packet(root["x"])
    y_id = create_random_packet(root["y"])
    local_id = create_random_packet(root["dst"])

    outpack_location_add_path("x", root["x"], root=root["dst"])
    outpack_location_add_path("y", root["y"], root=root["dst"])
    outpack_location_pull_metadata(root=root["dst"])

    def do_search(**kwargs):
        options = SearchOptions(**kwargs)
        return search("name == 'data'", root=root["dst"], options=options)

    assert do_search(allow_remote=True, location=["local"]) == {local_id}
    assert do_search(allow_remote=True, location=["x"]) == {x_id}
    assert do_search(allow_remote=True, location=["y"]) == {y_id}
    assert do_search(allow_remote=True, location=["x", "local"]) == {
        local_id,
        x_id,
    }

    with pytest.raises(Exception, match="Unknown location: 'nonexistent'"):
        do_search(allow_remote=True, location=["x", "nonexistent"])


def test_search_can_pull_and_filter(tmp_path):
    root = create_temporary_roots(tmp_path, names=["x", "y", "dst"])
    outpack_location_add_path("x", root["x"], root=root["dst"])
    outpack_location_add_path("y", root["y"], root=root["dst"])

    x_id = create_random_packet(root["x"])
    create_random_packet(root["y"])
    local_id = create_random_packet(root["dst"])

    options = SearchOptions(
        allow_remote=True, pull_metadata=True, location=["x", "local"]
    )
    assert search("name == 'data'", root=root["dst"], options=options) == {
        local_id,
        x_id,
    }
