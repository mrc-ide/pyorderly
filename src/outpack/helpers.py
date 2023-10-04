from dataclasses import dataclass
from typing import Dict

from outpack.root import as_root


def copy_files(id, files, dest, *, root=None):
    root = as_root(root)
    plan = plan_copy_files(root, id, files)
    for here, there in plan.files.items():
        root.export_file(id, there, here, dest)
    return plan


def plan_copy_files(root, id, files):
    meta = root.index.metadata(id)
    if isinstance(files, str):
        files = {files: files}
    if isinstance(files, list):
        files = {x: x for x in files}
    known = [p.path for p in meta.files]
    for k, v in files.items():
        # TODO: check absolute paths
        if v.endswith("/") or k.endswith("/"):
            msg = "Directories not yet supported for export"
            raise Exception(msg)
        if v not in known:
            msg = f"Packet '{id}' does not contain the requested path '{v}'"
            raise Exception(msg)
    return Plan(id, meta.name, files)


@dataclass
class Plan:
    id: str
    name: str
    files: Dict[str, str]
