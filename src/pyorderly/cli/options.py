import functools

import click

from pyorderly.outpack.search_options import SearchOptions


class NumericalParamType(click.ParamType):
    """
    A type for numerical parameters.

    It automatically deduces whether the parameter should be interpreted as an
    int or a float.
    """

    name = "number"

    def convert(self, value, param, ctx):
        if isinstance(value, float):
            return value

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            self.fail(f"{value!r} is not a valid numerical value", param, ctx)


def with_search_options(f):
    @click.option(
        "--pull-metadata",
        is_flag=True,
        help="Synchronize metadata from remote locations.",
    )
    @click.option(
        "--allow-remote",
        is_flag=True,
        help="Search for packets in remote locations.",
    )
    @click.option(
        "--location",
        multiple=True,
        help=(
            "Locations in which packets are searched. This option may be"
            " specified multiple times to allow multiple locations. If none are"
            " specified, all configured locations are searched."
        ),
        metavar="NAME",
    )
    @click.pass_context
    @functools.wraps(f)
    def inner(ctx, allow_remote, pull_metadata, location, *args, **kwargs):
        options = SearchOptions(
            allow_remote=allow_remote,
            pull_metadata=pull_metadata,
            location=list(location) if location else None,
        )
        return ctx.invoke(f, *args, **kwargs, search_options=options)

    return inner
