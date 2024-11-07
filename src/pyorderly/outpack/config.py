import os.path
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from pyorderly.outpack.schema import outpack_schema_version
from pyorderly.outpack.static import LOCATION_TYPES
from pyorderly.outpack.util import match_value


def read_config(root_path):
    with open(_config_path(root_path)) as f:
        s = f.read()
    return Config.from_json(s.strip())


def write_config(config, root_path):
    with open(_config_path(root_path), "w") as f:
        f.write(config.to_json())


def update_config(config, root_path):
    # TODO: Implement real update instead of overwriting see mrc-4600
    write_config(config, root_path)


def _encode_location_dict(d):
    return [x.to_dict() for x in d.values()]


def _decode_location_dict(d):
    return {x["name"]: Location.from_dict(x) for x in d}


@dataclass_json()
@dataclass
class ConfigCore:
    hash_algorithm: str
    path_archive: Optional[str]
    use_file_store: bool
    require_complete_tree: bool


# Note, using A002 (globally) and A003 noqa here to allow 'type' to be
# used as a field name and argument; this keeps the class close to the
# json names, and means that things read nicely (location.type rather
# than location.location_type). A similar issue occurs with 'hash'
@dataclass_json
@dataclass
class Location:
    name: str
    type: str
    args: Optional[dict] = None

    def __init__(self, name, type, args=None):
        self.name = name
        self.type = type
        self.args = args

        match_value(self.type, LOCATION_TYPES, "type")
        required = set()
        if type == "path":
            required = {"path"}
        elif type == "http":
            required = {"url"}
        elif type == "custom":
            required = {"driver"}

        present = set() if self.args is None else set(self.args.keys())
        missing = required - present
        if missing:
            missing_text = "', '".join(missing)
            msg = f"Fields missing from args: '{missing_text}'"
            raise Exception(msg)


@dataclass_json()
@dataclass
class Config:
    schema_version: str
    core: ConfigCore
    location: dict[str, Location] = field(
        metadata=config(
            encoder=_encode_location_dict, decoder=_decode_location_dict
        )
    )

    @staticmethod
    def new(
        *,
        path_archive="archive",
        use_file_store=False,
        require_complete_tree=False,
    ):
        if path_archive is None and not use_file_store:
            msg = "If 'path_archive' is None, 'use_file_store' must be True"
            raise Exception(msg)
        version = outpack_schema_version()
        core = ConfigCore(
            "sha256", path_archive, use_file_store, require_complete_tree
        )
        local = Location("local", "local")
        return Config(version, core, {"local": local})


def _config_path(root_path):
    return os.path.join(root_path, ".outpack", "config.json")
