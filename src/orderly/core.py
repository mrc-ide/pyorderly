from dataclasses import dataclass
from typing import Dict, List, Union

from dataclasses_json import dataclass_json

from orderly.current import get_active_context
from outpack import util
from outpack.helpers import copy_files
from outpack.search import search_unique


@dataclass_json()
@dataclass
class Artefact:
    name: str
    files: List[str]


@dataclass_json()
@dataclass
class Description:
    display: str
    long: str
    custom: Dict[str, Union[str, int, bool]]

    @staticmethod
    def empty():
        return Description(None, None, None)


def parameters(**kwargs):  # noqa: ARG001
    """Declare parameters used in a report.

    Parameters
    ----------
    kwargs :
      Keyword mappings of parameter names to default values (or to None
      if no default is given)

    Returns
    -------
    Nothing, this function hs no effect at all!
    """
    pass


def resource(files):
    """Declare that a file, or group of files, are an orderly resource.

    By explicitly declaring files as resources, orderly will mark the
    files as immutable inputs and validate that your analysis does not
    modify them with 'orderly.run'.

    Parameters
    ----------
    files : str|Path or [str|Path]

    Returns
    -------
    Nothing, this is called for its side effects within a running packet

    """
    files = util.relative_path_array(files, "resource")
    util.assert_file_exists(files)
    ctx = get_active_context()
    src = ctx.path if ctx.is_active else None
    files_expanded = util.expand_dirs(files, workdir=src)
    if ctx.is_active:
        # TODO: If strict mode, copy expanded files into the working dir
        for f in files_expanded:
            ctx.packet.mark_file_immutable(f)
        ctx.orderly.resources += files_expanded
    return files_expanded


def artefact(name, files):
    """Declare an artefact.

    By doing this you turn on a number of orderly features:

    (1) Files that are artefacts will not be copied from the src
    directory into the draft directory unless they are also listed as
    a resource by 'orderly.resource'

    (2) If your script fails to produce these files, then
    `orderly.run` will fail, guaranteeing that your task really does
    produce the things you need it do.

    (3) Within the final metadata, your artefacts will have additional
    metadata; the description that you provide and a grouping.

    Parameters
    ----------
    description : str
        The name of the artefact

    files : str or [str]
        The file, or array of files, that make up this artefact. These
        are relative paths.

    Returns
    -------
    Nothing, this is called for its side effects within a running packet
    """
    files = util.relative_path_array(files, "artefact")
    ctx = get_active_context()
    if ctx.is_active:
        ctx.orderly.artefacts.append(Artefact(name, files))
    return files


def description(*, display=None, long=None, custom=None):
    """Describe the current report.

    Parameters
    ----------
    display : str
      A friendly name for the report; this will be displayed in some
      locations of the web interfaces, packit.

    long : str
      A longer description, perhaps a sentence or two.

    custom : dict
      Any additional metadata. Must be a dictionary with string keys
      and string, number or boolean values.

    Returns
    -------
    Nothing, this is called for its side effects within a running packet
    """
    ctx = get_active_context()
    if ctx.is_active:
        _prevent_multiple_calls(ctx.orderly.description, "description")
        ctx.orderly.description = Description(display, long, custom)


def dependency(name, query, files):
    """Declare a dependency on another packet.

    Parameters
    ----------
    name: str | None
      The name of the packet to depend on, or None

    query: str
      A search query for packets, as a string. For example, "latest",
      "latest(parameter:x == 'value')" or "20230807-152344-ee606dce"

    files: str | [str] | dict[str, str]
      Files to use from the dependent packet

    Returns
    -------
    Data on the resolved dependency; this is an `orderly.helpers.Plan` object,
    which contains elements `id`, `name` and `files`
    """
    ctx = get_active_context()
    if name is not None:
        # Later, we need to combine 'query' and 'name' if given.
        msg = "'name' must be None for now, we'll fix this later"
        raise Exception(msg)

    if ctx.is_active:
        # TODO: search options here from need to come through from
        # orderly_run via the context, it's not passed through from
        # run yet, or present in the context, because search is barely
        # supported.
        result = ctx.packet.use_dependency(query, files)
    else:
        # TODO: get options from the interactive search options, once
        # it does anything.
        id = search_unique(query, root=ctx.root)
        result = copy_files(id, files, ctx.path, root=ctx.root)

    # TODO: print about this, once we decide what that looks like generally
    return result


def _prevent_multiple_calls(obj, what):
    if obj:
        msg = f"Only one call to '{what}' is allowed"
        raise Exception(msg)
