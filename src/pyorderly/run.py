import runpy
import shutil
from pathlib import Path

from pyorderly.core import Description
from pyorderly.current import ActiveOrderlyContext, OrderlyCustomMetadata
from pyorderly.outpack.ids import outpack_id
from pyorderly.outpack.metadata import MetadataCore
from pyorderly.outpack.packet import Packet, insert_packet
from pyorderly.outpack.root import root_open
from pyorderly.outpack.sandbox import run_in_sandbox
from pyorderly.outpack.util import all_normal_files
from pyorderly.read import orderly_read


def orderly_run(
    name, *, parameters=None, search_options=None, root=None, locate=True
):
    root = root_open(root, locate=locate)

    path_src, entrypoint = _validate_src_directory(name, root)

    dat = orderly_read(path_src / entrypoint)
    parameters = _validate_parameters(parameters, dat["parameters"])

    packet_id = outpack_id()
    path_dest = root.path / "draft" / name / packet_id
    path_dest.mkdir(parents=True)

    _copy_resources_implicit(path_src, path_dest)

    metadata = run_in_sandbox(
        _packet_builder,
        args=(
            root.path,
            packet_id,
            name,
            path_dest,
            path_src,
            entrypoint,
            parameters,
            search_options,
        ),
        cwd=path_dest,
    )

    insert_packet(root, path_dest, metadata)

    # This is intentionally not in a `try-finally` block. If creating the
    # packet fails and an exception was raised, we want to keep the packet in
    # the drafts directory for the user to examine.
    shutil.rmtree(path_dest)

    return packet_id


def _packet_builder(
    root, id, name, path, path_src, entrypoint, parameters, search_options
) -> MetadataCore:
    root = root_open(root, locate=False)
    packet = Packet(
        root,
        path,
        name,
        id=id,
        locate=False,
        parameters=parameters,
    )

    packet.mark_file_immutable(entrypoint)

    try:
        orderly = _run_report_script(
            packet, path, path_src, entrypoint, search_options
        )
        _check_artefacts(orderly, path)
    except:
        packet.end(succesful=False)
        raise

    packet.add_custom_metadata("orderly", _custom_metadata(entrypoint, orderly))
    return packet.end()


def _run_report_script(
    packet, path, path_src, entrypoint, search_options
) -> OrderlyCustomMetadata:
    try:
        with ActiveOrderlyContext(packet, path_src, search_options) as orderly:
            # Calling runpy with the full path to the script gives us better
            # tracebacks
            runpy.run_path(
                str(path / entrypoint),
                run_name="__main__",
            )

    except Exception as error:
        # This is pretty barebones for now; we will need to do some
        # work to make sure that we retain enough contextual errors
        # for the user to see that the report failed, and that it
        # failed *because* something else failed.
        msg = "Running orderly report failed!"
        raise Exception(msg) from error

    return orderly


def _validate_src_directory(name, root) -> tuple[Path, str]:
    path = root.path / "src" / name
    entrypoint = f"{name}.py"

    if not path.joinpath(entrypoint).exists():
        msg = f"Did not find orderly report '{name}'"
        if path.is_dir():
            detail = f"The path 'src/{name}' exists but does not contain '{entrypoint}'"
        elif path.exists():
            detail = f"The path 'src/{name}' exists but is not a directory"
        else:
            detail = f"The path 'src/{name}' does not exist"
        msg = f"{msg}\n* {detail}"
        raise Exception(msg)
    return path, entrypoint


def _validate_parameters(given, defaults):
    if given is None:
        given = {}

    if given and not defaults:
        msg = "Parameters given, but none declared"
        raise Exception(msg)

    required = {k for k, v in defaults.items() if v is None}
    missing = required.difference(given.keys())
    if missing:
        msg = f"Missing parameters: {', '.join(missing)}"
        raise Exception(msg)

    extra = set(given.keys()).difference(defaults.keys())
    if extra:
        msg = f"Unknown parameters: {', '.join(extra)}"
        raise Exception(msg)

    ret = defaults.copy()

    for k, v in given.items():
        if not isinstance(v, (int, float, str)):
            msg = f"Expected parameter {k} to be a simple value"
            raise Exception(msg)
        ret[k] = v

    return ret


def _copy_resources_implicit(src, dest):
    for p in all_normal_files(src):
        p_dest = dest / p
        p_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src / p, p_dest)


def _custom_metadata(entrypoint, orderly):
    role = [{"path": entrypoint, "role": "orderly"}]
    for p in orderly.resources:
        role.append({"path": p, "role": "resource"})
    for p in orderly.shared_resources:
        role.append({"path": p, "role": "shared"})

    return {
        "role": role,
        "artefacts": orderly.artefacts,
        "description": orderly.description or Description.empty(),
        "shared": orderly.shared_resources,
    }


def _check_artefacts(metadata, path):
    missing = set()
    for artefact in metadata.artefacts:
        for file in artefact.files:
            if not path.joinpath(file).exists():
                missing.add(file)
    if missing:
        missing = ", ".join(f"'{x}'" for x in sorted(missing))
        msg = f"Script did not produce the expected artefacts: {missing}"
        raise Exception(msg)
