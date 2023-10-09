from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from orderly.current import get_active_context
from outpack import util


@dataclass_json()
@dataclass
class Artefact:
    name: str = None
    files: List[str] = None


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
