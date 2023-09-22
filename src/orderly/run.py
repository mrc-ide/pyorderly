import shutil

from orderly.current import ActiveOrderlyPacket
from outpack.ids import outpack_id
from outpack.packet import Packet
from outpack.root import root_open
from outpack.util import run_script


def orderly_run(name, *, root=None, locate=True):
    root = root_open(root, locate)

    path_src = _validate_src_directory(name, root)

    packet_id = outpack_id()
    path_dest = root.path / "draft" / name / packet_id
    path_dest.mkdir(parents=True)

    _copy_resources_implicit(path_src, path_dest)

    packet = Packet(root, path_dest, name, id=packet_id, locate=False)
    try:
        with ActiveOrderlyPacket(packet, path_src) as orderly:
            packet.mark_file_immutable("orderly.py")
            run_script(path_dest, "orderly.py")
    except Exception as error:
        _orderly_cleanup_failure(packet)
        # This is pretty barebones for now; we will need to do some
        # work to make sure that we retain enough contextual errors
        # for the user to see that the report failed, and that it
        # failed *because* something else failed.
        msg = "Running orderly report failed!"
        raise Exception(msg) from error

    try:
        _orderly_cleanup_success(packet, orderly)
    except Exception:
        _orderly_cleanup_failure(packet)
        raise
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


def _orderly_cleanup_success(packet, orderly):
    missing = set()
    for artefact in orderly.artefacts:
        for path in artefact.files:
            if not packet.path.joinpath(path).exists():
                missing.add(path)
    if missing:
        missing = ", ".join(f"'{x}'" for x in sorted(missing))
        msg = f"Script did not produce the expected artefacts: {missing}"
        raise Exception(msg)
    # check files (either strict or relaxed) -- but we can't do that yet
    packet.add_custom_metadata("orderly", _custom_metadata(orderly))
    packet.end(insert=True)
    shutil.rmtree(packet.path)


def _custom_metadata(orderly):
    role = [{"path": "orderly.py", "role": "orderly"}]
    for p in orderly.resources:
        role.append({"path": p, "role": "resource"})

    return {"role": role, "artefacts": orderly.artefacts}


def _orderly_cleanup_failure(packet):
    packet.end(insert=False)
