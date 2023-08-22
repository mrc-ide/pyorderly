import pytest

from outpack.init import outpack_init
from outpack.search import query_eval, search
from outpack.search_options import SearchOptions
from outpack.search_query import QueryComponent


def test_can_do_simple_search():
    assert search("latest", root="example") == "20230814-163026-ac5900c0"
    assert (
        search("20230807-152345-0e0662d0", root="example")
        == "20230807-152345-0e0662d0"
    )
    assert search("20230807-152345-0e0662bb", root="example") is None


def test_can_return_no_results_on_miss(tmp_path):
    root = tmp_path / "root"
    outpack_init(root)

    assert search("latest", root=root) is None
    assert search("20230807-152345-0e0662d0", root=root) is None


def test_can_not_pass_remote_options():
    with pytest.raises(NotImplementedError, match="Can't use 'allow_remote'"):
        options = SearchOptions(allow_remote=True)
        search("latest", root="example", options=options)


def test_guard_aginst_new_expessions():
    expr = QueryComponent("somethingelse", "expr", [])
    with pytest.raises(NotImplementedError, match="Unhandled expression"):
        query_eval(expr, None)
