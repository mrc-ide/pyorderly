import pytest

from outpack.search_query import Query, QueryComponent, query_parse


def test_can_parse_simple_latest_query():
    expr = query_parse("latest")
    assert isinstance(expr, Query)
    assert expr.is_single
    assert expr.parameters == []
    assert expr.expr == QueryComponent("latest", "latest()", [])


def test_can_parse_simple_id_query():
    x = "20230810-172859-6b0408e0"
    expr = query_parse(x)
    assert isinstance(expr, Query)
    assert expr.is_single
    assert expr.parameters == []
    assert expr.expr == QueryComponent("id", x, [x])


def test_can_not_parse_interesting_query():
    with pytest.raises(Exception, match="Unhandled query expression"):
        query_parse("latest(parameter:x == this:y)")


def test_can_convert_query_to_string():
    x = "20230810-172859-6b0408e0"
    assert str(query_parse(x)) == x
    assert str(query_parse("latest()")) == "latest()"
    assert str(query_parse("latest")) == "latest()"
