import os.path
from typing import Any

from pydantic import (
    Field,
    field_serializer,
    field_validator,
    model_validator,
    validate_call,
)

from pyorderly.outpack.schema import outpack_schema_version, validate
from pyorderly.outpack.static import LOCATION_TYPES
from pyorderly.outpack.util import StrictModel, match_value


def read_config(root_path):
    with open(_config_path(root_path)) as f:
        s = f.read()
    return Config.model_validate_json(s)


def write_config(config, root_path):
    with open(_config_path(root_path), "w") as f:
        f.write(config.model_dump_json())


def update_config(config, root_path):
    # TODO: Implement real update instead of overwriting see mrc-4600
    write_config(config, root_path)


class ConfigCore(StrictModel):
    hash_algorithm: str
    path_archive: str | None
    use_file_store: bool
    require_complete_tree: bool


# Note, using A002 (globally) and A003 noqa here to allow 'type' to be
# used as a field name and argument; this keeps the class close to the
# json names, and means that things read nicely (location.type rather
# than location.location_type). A similar issue occurs with 'hash'
class Location(StrictModel):
    name: str
    type: str
    args: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_type(self):
        match_value(self.type, LOCATION_TYPES, "type")
        return self

    @model_validator(mode="after")
    def check_location_args(self):
        required = set()
        if self.type == "path":
            required = {"path"}
        elif self.type == "http":
            required = {"url"}
        elif self.type == "custom":
            required = {"driver"}

        present = self.args.keys()
        missing = required - present
        if missing:
            missing_text = "', '".join(missing)
            msg = f"Fields missing from args: '{missing_text}'"
            raise Exception(msg)
        return self


class Config(StrictModel):
    schema_version: str
    core: ConfigCore

    # This field is serialized as a list of locations rather than a
    # dictionary. See functions below.
    location: dict[str, Location]

    @field_validator("location", mode="before")
    @classmethod
    @validate_call
    def _validate_location(cls, d: list[Location]) -> dict[str, Location]:
        return {x.name: x for x in d}

    @field_serializer("location", mode="plain")
    @classmethod
    def _serialize_location(cls, d: dict[str, Location]) -> list[Location]:
        return list(d.values())

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
            hash_algorithm="sha256",
            path_archive=path_archive,
            use_file_store=use_file_store,
            require_complete_tree=require_complete_tree,
        )
        local = Location(name="local", type="local")
        return Config(
            schema_version=version,
            core=core,
            location=[local],
        )


def _config_path(root_path):
    return os.path.join(root_path, ".outpack", "config.json")
