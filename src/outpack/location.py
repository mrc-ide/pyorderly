import collections
import os
import shutil

from outpack.config import Location, update_config
from outpack.hash import Hash, hash_validate_string
from outpack.location_path import OutpackLocationPath
from outpack.metadata import PacketLocation
from outpack.packet import mark_known
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
    elif isinstance(location, str):
        if location not in outpack_location_list(root):
            msg = f"Unknown location: '{location}'"
            raise Exception(msg)
        else:
            location = [location]
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


def outpack_location_pull_metadata(location=None, root=None, *, locate=True):
    root = root_open(root, locate)
    location_name = location_resolve_valid(location, root,
                                           include_local=False,
                                           include_orphan=False,
                                           allow_no_locations=True)

    for name in location_name:
        _location_pull_metadata(name, root)

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
    index = root.index
    index_data = index.data()
    driver = _location_driver(location_name, root)

    hint_remove = (f'Probably all you can do at this point is remove this '
                   f'location from your configuration by running '
                   f'orderly_location_remove("{location_name}")')

    known_there = driver.list()
    known_here = index_data.metadata.keys()
    new_packets = []
    for packet in known_there:
        if packet not in known_here:
            new_packets.append(packet)

    for packet_id in new_packets:
        metadata = driver.metadata(packet_id)[packet_id]
        expected_hash = known_there[packet_id].hash
        path_metadata = root.path / ".outpack" / "metadata"
        os.makedirs(path_metadata, exist_ok=True)
        filename = path_metadata / packet_id

        hash_validate_string(
            metadata, expected_hash,
            f"metadata for '{packet_id}' from '{location_name}'",
            [f"This is bad news, I'm afraid. Your location is sending data "
            f"that does not match the hash it says it does. Please let us "
            f"know how this might have happened.",
             hint_remove]
        )
        with open(filename, "w") as f:
            f.writelines(metadata)

    seen_packets = {}
    for location in index_data.location.values():
        for packet_id, packet in location.items():
            if seen_packets.get(packet_id) is None:
                seen_packets[packet_id] = [packet.hash]
            else:
                seen_packets[packet_id] = seen_packets[packet_id].append(packet.hash)
    seen_before = set(known_there.keys()).intersection(seen_packets.keys())

    mismatch_hashes = set()
    for packet_id in seen_before:
        hash_there = known_there[packet_id].hash
        hash_here = seen_packets[packet_id]
        for hash in hash_here:
            if hash != hash_there:
                mismatch_hashes.add(packet_id)

    if len(mismatch_hashes) > 0:
        id_text = "', '".join(mismatch_hashes)
        msg = (f"Location '{location_name}' has conflicting metadata\n"
               f"This is really bad news. We have been offered metadata "
               f"from '{location_name}' that has a different hash to "
               f"metadata that we have already imported from other "
               f"locations. I'm not going to import this new metadata, "
               f"but there's no guarantee that the older metadata is "
               f"actually what you want!\nConflicts for: '{id_text}'\n"
               f"We would be interested in this case, please let us know\n"
               f"{hint_remove}")
        raise Exception(msg)

    try:
        known_here = index.location(location_name)
    except KeyError:
        known_here = {}

    for packet_id in known_there:
        if packet_id not in known_here.keys():
            mark_known(root, packet_id, location_name,
                       known_there[packet_id].hash, known_there[packet_id].time)



def _location_driver(location_name, root):
    location = root.config.location[location_name]
    match location.type:
        case "path":
            return(OutpackLocationPath(location.args["path"]))
        case "http":
            raise Exception("Http remote not yet supported")
        case "custom":
            raise Exception("custom remote not yet supported")
