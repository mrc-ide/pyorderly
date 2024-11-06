import os
from dataclasses import dataclass
from itertools import chain
from typing import Any, Optional, Union

import outpack_query_parser as parser

from pyorderly.outpack.ids import is_outpack_id
from pyorderly.outpack.location import location_resolve_valid
from pyorderly.outpack.location_pull import outpack_location_pull_metadata
from pyorderly.outpack.metadata import MetadataCore, Parameters
from pyorderly.outpack.root import OutpackRoot, root_open
from pyorderly.outpack.search_options import SearchOptions


@dataclass
class Query:
    text: str
    node: Any

    def __str__(self):
        return self.text

    @classmethod
    def parse(cls, text):
        return cls(text, parser.parse_query(text))

    def is_single_valued(self):
        """
        Return true if the query is guaranteed to produce at most a single result.

        This could be either an expression wrapped in a `single(...)` or
        `latest(...)` call, or it is an ID lookup of the form `id == ...`.
        """
        if isinstance(self.node, (parser.Single, parser.Latest)):
            return True
        elif isinstance(self.node, parser.Test):
            return self.node.operator == parser.TestOperator.Equal and (
                isinstance(self.node.lhs, parser.LookupId)
                or isinstance(self.node.rhs, parser.LookupId)
            )
        else:
            return False


class QueryEnv:
    def __init__(
        self,
        root: OutpackRoot,
        options: SearchOptions,
        this: Optional[Parameters],
    ):
        self.index = QueryIndex(root, options)
        self.this = this


class QueryIndex:
    root: OutpackRoot
    index: dict[str, MetadataCore]
    options: SearchOptions

    def __init__(self, root, options):
        self.root = root

        locations = location_resolve_valid(
            options.location,
            self.root,
            include_local=True,
            include_orphan=True,
            allow_no_locations=False,
        )

        ids = set(
            chain.from_iterable(
                root.index.location(name).keys() for name in locations
            )
        )

        if not options.allow_remote:
            ids.intersection_update(root.index.unpacked())

        self.index = {i: root.index.metadata(i) for i in ids}
        self.options = options


def as_query(query: Union[Query, str]) -> Query:
    if isinstance(query, Query):
        return query
    else:
        if is_outpack_id(query):
            query = f"'{query}'"
        return Query.parse(query)


def search(
    query: Union[Query, str],
    *,
    root: Union[OutpackRoot, str, os.PathLike],
    options: Optional[SearchOptions] = None,
    this: Optional[Parameters] = None,
) -> set[str]:
    """
    Search an outpack repository for all packets that match the given query.

    This may return zero or more packet IDs.
    """
    if options is None:
        options = SearchOptions()

    root = root_open(root)
    query = as_query(query)

    if options.pull_metadata:
        outpack_location_pull_metadata(location=options.location, root=root)

    env = QueryEnv(root, options, this)

    return eval_query(query.node, env)


def search_unique(
    query: Union[Query, str],
    *,
    root: Union[OutpackRoot, str, os.PathLike],
    options: Optional[SearchOptions] = None,
    this: Optional[Parameters] = None,
):
    """
    Search an outpack repository for a packet that matches the given query.

    Returns a single packet ID. Throws an exception if no packet is found or if
    the query is not syntactically guaranteed to find at most one.
    """
    query = as_query(query)

    if not query.is_single_valued():
        msg = "Query is not guaranteed to return a single packet. Try wrapping it in `latest` or `single`."
        raise Exception(msg)

    results = search(query, options=options, root=root, this=this)
    if len(results) > 1:  # pragma: no cover
        msg = "Query unexpectedly returned more than one result"
        raise AssertionError(msg)

    if not results:
        msg = f"Failed to find packet for query {query}"
        raise Exception(msg)

    return next(iter(results))


def eval_test_value(
    node, env: QueryEnv, metadata: MetadataCore
) -> Optional[Union[bool, int, float, str]]:
    if isinstance(node, parser.Literal):
        return node.value
    elif isinstance(node, parser.LookupId):
        return metadata.id
    elif isinstance(node, parser.LookupName):
        return metadata.name
    elif isinstance(node, parser.LookupParameter):
        return metadata.parameters.get(node.name)
    elif isinstance(node, parser.LookupThis):
        if env.this is None:
            msg = "this parameters are not supported in this context"
            raise Exception(msg)
        else:
            value = env.this.get(node.name)
            if value is None:
                msg = f"Parameter `{node.name}` was not found in current packet"
                raise Exception(msg)

            return value
    elif isinstance(node, parser.LookupEnvironment):
        msg = "environment lookup is not supported yet"
        raise NotImplementedError(msg)
    else:  # pragma: no cover
        msg = f"Unhandled test value: {node}"
        raise NotImplementedError(msg)


def eval_latest(node: parser.Latest, env: QueryEnv) -> set[str]:
    if node.inner:
        candidates = eval_query(node.inner, env)
    else:
        candidates = set(env.index.index.keys())

    if candidates:
        return {max(candidates)}
    else:
        return set()


def eval_single(node: parser.Single, env: QueryEnv) -> set[str]:
    candidates = eval_query(node.inner, env)
    if len(candidates) != 1:
        msg = f"Query found {len(candidates)} packets, but expected exactly one"
        raise ValueError(msg)
    return candidates


def eval_test_one(
    node: parser.Test, env: QueryEnv, metadata: MetadataCore
) -> bool:
    # mypy isn't very good at inferring type narrowing from the is_numerical
    # condition. Use `Any` to silence it.
    lhs: Any = eval_test_value(node.lhs, env, metadata)
    rhs: Any = eval_test_value(node.rhs, env, metadata)

    # We treat missing or ill-typed values as soft-failures. They evaluate to
    # False but don't cause errors.
    is_valid = lhs is not None and rhs is not None
    is_numerical = isinstance(lhs, (float, int)) and isinstance(
        rhs, (float, int)
    )

    if node.operator == parser.TestOperator.Equal:
        return is_valid and (lhs == rhs)
    elif node.operator == parser.TestOperator.NotEqual:
        return is_valid and (lhs != rhs)
    elif node.operator == parser.TestOperator.LessThan:
        return is_numerical and (lhs < rhs)
    elif node.operator == parser.TestOperator.LessThanOrEqual:
        return is_numerical and (lhs <= rhs)
    elif node.operator == parser.TestOperator.GreaterThan:
        return is_numerical and (lhs > rhs)
    elif node.operator == parser.TestOperator.GreaterThanOrEqual:
        return is_numerical and (lhs >= rhs)
    else:  # pragma: no cover
        msg = f"Unhandled test operator: {node.operator}"
        raise NotImplementedError(msg)


def eval_test(node: parser.Test, env: QueryEnv) -> set[str]:
    return {
        packet_id
        for (packet_id, metadata) in env.index.index.items()
        if eval_test_one(node, env, metadata)
    }


def eval_boolean(node: parser.BooleanExpr, env: QueryEnv) -> set[str]:
    lhs = eval_query(node.lhs, env)
    rhs = eval_query(node.rhs, env)

    if node.operator == parser.BooleanOperator.And:
        return lhs & rhs
    elif node.operator == parser.BooleanOperator.Or:
        return lhs | rhs
    else:  # pragma: no cover
        msg = f"Unhandled boolean operator: {node.operator}"
        raise NotImplementedError(msg)


def eval_negation(node: parser.Negation, env: QueryEnv) -> set[str]:
    complement = eval_query(node.inner, env)
    return set(env.index.index.keys()).difference(complement)


def eval_query(node, env: QueryEnv) -> set[str]:
    if isinstance(node, parser.Latest):
        return eval_latest(node, env)
    elif isinstance(node, parser.Single):
        return eval_single(node, env)
    elif isinstance(node, parser.Test):
        return eval_test(node, env)
    elif isinstance(node, parser.BooleanExpr):
        return eval_boolean(node, env)
    elif isinstance(node, parser.Negation):
        return eval_negation(node, env)
    elif isinstance(node, parser.Brackets):
        return eval_query(node.inner, env)
    else:  # pragma: no cover
        msg = f"Unhandled query expression: {node}"
        raise NotImplementedError(msg)
