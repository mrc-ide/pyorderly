import collections
import shutil

from outpack.config import Location, update_config
from outpack.root import root_open
from outpack.static import (LOCATION_RESERVED_NAME, LOCATION_LOCAL,
                            LOCATION_ORPHAN)


def outpack_location_list(root=None, *, locate=True):
    root = root_open(root, locate)
    return list(root.config.location.keys())


def outpack_location_add(name, type, args, root=None, *, locate=True):
    root = root_open(root, locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot add a location with reserved name '{name}'"
        raise Exception(msg)

    _location_check_new_name(root, name)

    loc = Location(name, type, args)

    if type == "path":
        root_open(loc.args["path"], locate=False)
    elif type in ("http", "custom"):
        msg = f"Cannot add a location with type '{type}' yet."
        raise Exception(msg)

    config = root.config
    config.location[name] = loc
    update_config(config, root.path)


def outpack_location_remove(name, root=None, *, locate=True):
    root = root_open(root, locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot remove default location '{name}'"
        raise Exception(msg)

    _location_check_exists(root, name)
    config = root.config

    # TODO: mark packets as orphaned mrc-4601

    location_path = root.path / ".outpack" / "location" / name
    if location_path.exists():
        ## Skipped on covr because this dir won't exist until packet pulling implemented
        shutil.rmtree(location_path)  # pragma: no cover

    root.index.rebuild()
    config.location.pop(name)
    update_config(config, root.path)


def outpack_location_rename(old, new, root=None, *, locate=True):
    root = root_open(root, locate)

    if old in LOCATION_RESERVED_NAME:
        msg = f"Cannot rename default location '{old}'"
        raise Exception(msg)

    _location_check_new_name(root, new)
    _location_check_exists(root, old)

    config = root.config
    new_loc = config.location.pop(old)
    new_loc.name = new
    config.location[new] = new_loc
    update_config(config, root.path)


def location_resolve_valid(location, root, include_local, include_orphan,
                            allow_no_locations):
    if location is None:
        location = outpack_location_list(root)
    elif (isinstance(location, str) and
          location not in outpack_location_list(root)):
        msg = f"Unknown location: '{location}'"
        raise Exception(msg)

    elif (isinstance(location, collections.abc.Iterable) and
           all(isinstance(item, str) for item in location)):
        unknown = set(location).difference(outpack_location_list(root))
        if len(unknown) > 0:
            unknown_text = "', '".join(unknown)
            msg = f"Unknown location: '{unknown_text}'"
            raise Exception(msg)
    else:
        msg = (f"Invalid input for 'location'; expected None or a list of "
               f"strings")
        raise Exception(msg)

    if not include_local and LOCATION_LOCAL in location:
        location.remove(LOCATION_LOCAL)
    if not include_orphan and LOCATION_ORPHAN in location:
        location.remove(LOCATION_ORPHAN)

    if len(location) == 0 and not allow_no_locations:
        raise Exception("No suitable location found")

    return location


def outpack_location_pull_metadata(location, root=None, *, locate=True):
    root = root_open(root, locate)
    location_name = location_resolve_valid(location, root)

    for name in location_name:
        location_pull_metadata(name, root)

    # TODO: deorphan recovered packets

def _location_check_new_name(root, name):
    if _location_exists(root, name):
        msg = f"A location with name '{name}' already exists"
        raise Exception(msg)


def _location_check_exists(root, name):
    if not _location_exists(root, name):
        msg = f"No location with name '{name}' exists"
        raise Exception(msg)


def _location_exists(root, name):
    return name in outpack_location_list(root)


def _location_pull_metadata(location_name, root):
    index = root.index.data()
    driver = _location_driver(location_name, root)

def _location_driver(location_name, root):
    i = root.config.location.name.index(location_name)