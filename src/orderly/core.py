import os

from orderly.current import get_active_packet
from outpack import util


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
    if not isinstance(files, list):
        files = [files]
    for f in files:
        if os.path.isabs(f):
            msg = f"Expected resource path '{f}' to be a relative path"
            raise Exception(msg)
    util.assert_file_exists(files)
    p = get_active_packet()
    src = p.packet.path if p else None
    files_expanded = util.expand_dirs(files, workdir=src)
    if p:
        # TODO: If strict mode, copy expanded files into the working dir
        for f in files_expanded:
            p.packet.mark_file_immutable(f)
        p.orderly.resources += files_expanded
    return files_expanded
