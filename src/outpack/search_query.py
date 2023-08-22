from dataclasses import dataclass
from typing import List

from outpack.ids import is_outpack_id


class Query:
    def __init__(self, expr):
        self.expr = expr
        self.is_single = True
        self.parameters = []


@dataclass
class QueryComponent:
    kind: str
    expr: str
    args: List[str]

    def __init__(self, kind, expr, args):
        self.kind = kind
        self.expr = expr
        self.args = args


def query_parse_latest(expr):
    return QueryComponent("latest", expr, [])


def query_parse_id(expr):
    return QueryComponent("id", expr, [expr])


def query_parse(expr):
    if expr == "latest":
        expr = "latest()"
    expr = query_parse_expr(expr)
    return Query(expr)


def query_parse_expr(expr):
    if expr == "latest()":
        return query_parse_latest(expr)
    elif is_outpack_id(expr):
        return query_parse_id(expr)
    else:
        msg = f"Unhandled query expression '{expr}'"
        raise Exception(msg)
