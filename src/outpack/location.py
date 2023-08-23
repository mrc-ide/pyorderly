import os
from outpack.config import (
    Config,
    Location,
    read_config,
    update_config,
    write_config,
)
from outpack.root import root_open
from outpack.util import match_value

LOCATION_LOCAL = "local"
LOCATION_ORPHAN = "orphan"
LOCATION_RESERVED_NAME = [LOCATION_LOCAL, LOCATION_ORPHAN]
LOCATION_TYPES = [LOCATION_LOCAL, LOCATION_ORPHAN, "path"]


def orderly_location_list(root=None, locate=True):
    root = root_open(root, locate)
    return list(root.config.location.keys())


def orderly_location_add(name, type, args, root=None, locate=True):
    root = root_open(root, locate)
    # validate name is scalar character??
    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot add a location with reserved name '{name}'"
        raise Exception(msg)

    location_check_new_name(root, name)

    loc = new_location_entry(name, type, args)

    if type == "path":
        ## We won't be necessarily be able to do this _generally_ but
        ## here, let's confirm that we can read from the outpack archive
        ## at the requested path; this will just fail but without
        ## providing the user with anything actionable yet.
        root_open(loc.args["path"], locate=False)

    config = root.config
    config.location[name] = loc
    update_config(config, root.path)


def orderly_location_remove(name, root=None, locate=True):
    root = root_open(root, locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot remove default location '{name}'"
        raise Exception(msg)

    location_check_exists(root, name)
    config = root.config

    # TODO: mark packets as orphaned

    location_path = root.path / ".outpack" / "location" / name
    if location_path.exists():
        os.rmdir(location_path)

    # TODO: Rebuild the index after index has been added
    config.location.pop(name)
    update_config(config, root.path)


def location_check_new_name(root, name):
    if location_exists(root, name):
        msg = f"A location with name '{name}' already exists"
        raise Exception(msg)


def location_check_exists(root, name):
    if not location_exists(root, name):
        msg = f"No location with name '{name}' exists"
        raise Exception(msg)


def location_exists(root, name):
    return name in orderly_location_list(root)


def new_location_entry(name, type, args):
    match_value(type, LOCATION_TYPES, "type")
    if type == "path":
        required = ["path"]
    missing = set(required).difference(set(args.keys()))
    if len(missing) > 0:
        missing_text = "', '".join(missing)
        msg = f"Fields missing from args: '{missing_text}'"
        raise Exception(msg)
    return Location(name, type, args)
