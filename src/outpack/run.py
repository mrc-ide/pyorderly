import shutil

from outpack.packet import Packet
from outpack.root import root_open
from outpack.ids import outpack_id
from outpack.util import run_script


# This is wildly simpler than what we need in general, and won't
# support (for example) dependencies, queries, or anything else that
# calls back out from the orderly report and wants to interact with
# the packet. We'll get that all added back in later.

def orderly_run(name, *, root=None, locate=True):
    root = root_open(root, locate)

    path_src = _validate_src_directory(name, root)

    packet_id = outpack_id()
    path_dest = root.path / "draft" / name / packet_id
    path_dest.mkdir(parents=True)

    inputs_info = _copy_resources_implicit(path_src, path_dest)

    packet = Packet(root, path_dest, name, id=packet_id, locate=False)
    # TODO: add custom orderly state here
    # TODO: mark the outpack.py file as immutable
    # TODO: mark this packet as "current" within our package so we can access it later (e.g., when doing depenencies)

    try:
        run_script(path_src, "orderly.py")
    except Exception as e:
        _orderly_cleanup_failure(packet)
        success = False
        error = e
        raise # or raise something else?

    _orderly_cleanup_success(packet)
    return packet_id


def _validate_src_directory(name, root):
    path = root.path / "src" / name
    if not path.joinpath("orderly.py").exists():
        msg = f"Did not find orderly report '{name}"
        if path.is_dir():
            detail = f"The path 'src/{name}' exists but does not contain 'orderly.py'"
        elif path.exists():
            detail = f"The path 'src/{name}' exists but is not a directory"
        else:
            detail = f"The path 'src/{name}' does not exist"
        msg = f"{msg}\n* {detail}"
        raise Exception(msg)
    return path


def _copy_resources_implicit(src, dest):
    info = {}
    for p in src.iterdir():
        p_rel = p.relative_to(src)
        p_dest = dest.joinpath(p_rel)
        p_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, p_dest)
        info[p_rel] = p.stat()
    return info


def _orderly_cleanup_success(packet):
    # check artefacts -- but we don't have any artefacts yet
    # check files (either strict or relaxed) -- but we don't do this either
    # add custom metadata metadata -- nothing to add here yet
    packet.end(insert=True)
    shutil.rmtree(packet.path)


def _orderly_cleanup_failure(packet):
    # add metadata
    packet.end(insert=False)
