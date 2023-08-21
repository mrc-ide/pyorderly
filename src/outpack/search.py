from outpack.root import Root
from outpack.search_options import SearchOptions
from outpack.search_query import query_parse


class QueryEnv:
    def __init__(self, root, options):
        self.index = QueryIndex(root, options)


class QueryIndex:
    root = None
    index = None
    options = None
    _seen = None

    def __init__(self, root, options):
        self.root = root
        if options.allow_remote:
            msg = "Can't use 'allow_remote' in search yet"
            raise NotImplementedError(msg)
        ids = root.index.unpacked()
        self.index = {i: root.index.metadata(i) for i in ids}
        self.options = options
        self._seen = {}


def search(expr, *, options=None, root=None):
    if options is None:
        options = SearchOptions()
    root = Root(root)
    query = query_parse(expr)
    env = QueryEnv(root, options)
    return query_eval(query.expr, env)


def query_eval(expr, env):
    if expr.kind == "latest":
        return query_eval_latest(expr, env)
    elif expr.kind == "id":
        return query_eval_id(expr, env)
    else:
        msg = "Unhandled expression [outpack bug - please report]"
        raise NotImplementedError(msg)


def query_eval_latest(query, env):
    # Assertion here as a reminder we'll need to expand this.
    assert len(query.args) == 0  # noqa: S101
    candidates = env.index.index.keys()
    if len(candidates) == 0:
        return None
    return max(candidates)


def query_eval_id(query, env):
    packet_id = query.args[0]
    if packet_id not in env.index.index:
        return None
    return packet_id
