import pytest

from outpack.search import Query, search, search_unique
from outpack.search_options import SearchOptions

from .helpers import create_random_packet, create_temporary_root


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


def test_can_not_pass_remote_options(tmp_path):
    root = create_temporary_root(tmp_path)

    with pytest.raises(NotImplementedError, match="Can't use 'allow_remote'"):
        options = SearchOptions(allow_remote=True)
        search("latest", root=root, options=options)
