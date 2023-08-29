from pathlib import PosixPath

from orderly.current import get_active_packet
from outpack import util


def resource(files):
    """Declare that a file, or group of files, are an orderly resource. By
    explicitly declaring files as resources, orderly will mark the
    files as immutable iputs and validate that your analysis does not
    modify them with 'orderly.run'.

    Parameters
    ----------
    files : str|Path or [str|Path]

    Returns
    -------
    Nothing, this is called for its side effects within a running packet
    """
    if not isinstance(files, list):
        files = [files]
    p = get_active_packet()
    if p is None:
        util.assert_file_exists(files)
    else:
        src = p.src
        p.artefacts.append(Artefact(name, files))
        util.assert_file_exists(files, workdir=src)
        files_expanded = util.expand_dirs(files, workdir=src)
        # TODO: If strict mode, copy expanded files into the working dir
        for f in files_expanded:
            p.mark_file_immutable(f)
        p.custom["orderly"]["resources"] += files_expanded


def artefact(name, files):
    """Declare an artefact. By doing this you turn on a number of orderly
    features.

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
    if isinstance(files, str):
        files = [files]
    p = get_active_packet()
    if p is None:
        pass
    else:
        p.orderly.artefacts.append(Artefact(name, files))
