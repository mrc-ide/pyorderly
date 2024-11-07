import os.path
import shutil
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Union

from dataclasses_json import dataclass_json

from pyorderly.current import get_active_context
from pyorderly.outpack import util
from pyorderly.outpack.copy_files import copy_files
from pyorderly.outpack.search import search_unique
from pyorderly.outpack.util import pl


@dataclass_json()
@dataclass
class Artefact:
    name: str
    files: list[str]


@dataclass_json()
@dataclass
class Description:
    display: str
    long: str
    custom: dict[str, Union[str, int, bool]]

    @staticmethod
    def empty():
        return Description(None, None, None)


class Parameters(SimpleNamespace):
    """
    A container for parameters used in a report.

    An instance of this class is returned by the `orderly.parameters` function.
    Individual parameters can be accessed as fields of the object.

    Example:

        >>> params = orderly.parameters(p=1)
        >>> params.p
        1
    """

    pass


def parameters(**kwargs) -> Parameters:
    """Declare parameters used in a report.

    Parameters
    ----------
    kwargs :
      Keyword mappings of parameter names to default values (or to None
      if no default is given)

    Returns
    -------
    The parameters that were passed to the report. If running outside of an
    orderly context, this returns kwargs unmodified.
    """
    ctx = get_active_context()
    if ctx.is_active:
        # We don't need to apply defaults from kwargs as the packet runner
        # already did so.
        return Parameters(**ctx.parameters)
    else:
        missing = [k for k, v in kwargs.items() if v is None]
        if missing:
            msg = f"No value was specified for {pl(missing, 'parameter')} {', '.join(missing)}."
            raise Exception(msg)

        return Parameters(**kwargs)


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
        files_expanded_posix = util.as_posix_path(files_expanded)

        # TODO: If strict mode, copy expanded files into the working dir
        for f in files_expanded_posix:
            ctx.packet.mark_file_immutable(f)
        ctx.orderly.resources += files_expanded_posix

    return files_expanded


def shared_resource(
    files: Union[str, list[str], dict[str, str]]
) -> dict[str, str]:
    """Copy shared resources into a packet directory.

    You can use this to share common resources (data or code) between multiple
    packets. Additional metadata will be added to keep track of where the files
    came from. Using this function requires the shared resources directory
    `shared/` exists at the orderly root; an error will be
    raised if this is not configured when we attempt to fetch files.

    Parameters
    ----------
    files: str | [str] | dict[str, str]
        The shared resources to copy. If a dictionary is provided, the keys will
        be the destination file while the value is the filename within the
        shared resource directory.

    Returns
    -------
        A dictionary of files that were copied. If a directory was copied, this
        includes all of its individual contents. Do not rely on the ordering
        where directory expansion was performed, as it may be platform
        dependent.
    """
    ctx = get_active_context()

    files = util.relative_path_mapping(files, "shared resource")
    result = _copy_shared_resources(ctx.root.path, ctx.path, files)

    if ctx.is_active:
        result_posix = util.as_posix_path(result)
        for f in result_posix.keys():
            ctx.packet.mark_file_immutable(f)
        ctx.orderly.shared_resources.update(result_posix)

    return result


def _copy_shared_resources(
    root: Path, packet: Path, files: dict[str, str]
) -> dict[str, str]:
    shared_path = root / "shared"
    if not shared_path.exists():
        msg = "The shared resources directory 'shared' does not exist at orderly's root"
        raise Exception(msg)

    result = {}
    for here, there in files.items():
        src = shared_path / there
        dst = packet / here

        if not src.exists():
            msg = f"Shared resource file '{there}' does not exist. Looked within directory '{shared_path}'"
            raise Exception(msg)
        elif src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
            copied = {
                os.path.join(here, f): os.path.join(there, f)
                for f in util.all_normal_files(src)
            }
            result.update(copied)
        else:
            shutil.copyfile(src, dst)
            result[here] = there

    return result


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
        result = ctx.packet.use_dependency(query, files, ctx.search_options)
    else:
        id = search_unique(query, root=ctx.root, options=ctx.search_options)
        result = copy_files(
            id,
            files,
            ctx.path,
            root=ctx.root,
            options=ctx.search_options,
        )

    # TODO: print about this, once we decide what that looks like generally
    return result


def _prevent_multiple_calls(obj, what):
    if obj:
        msg = f"Only one call to '{what}' is allowed"
        raise Exception(msg)
