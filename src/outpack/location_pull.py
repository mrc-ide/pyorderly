import itertools
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple, Union

import humanize

from outpack.filestore import FileStore
from outpack.hash import hash_validate_string
from outpack.location import _location_driver, location_resolve_valid
from outpack.metadata import (
    MetadataCore,
    PacketFileWithLocation,
    PacketLocation,
)
from outpack.packet import mark_known
from outpack.root import OutpackRoot, find_file_by_hash, root_open
from outpack.search_options import SearchOptions
from outpack.static import LOCATION_LOCAL
from outpack.util import format_list, partition, pl


def outpack_location_pull_metadata(location=None, root=None, *, locate=True):
    root = root_open(root, locate=locate)
    location_name = location_resolve_valid(
        location,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )
    for name in location_name:
        driver = _location_driver(name, root)
        _pull_all_metadata(driver, root, name)
        known_packets = []
        for packet_location in root.index.all_locations().values():
            known_packets.extend(list(packet_location.values()))
        _validate_hashes(driver, name, known_packets)
        _mark_all_known(driver, root, name)

    # TODO: mrc-4601 deorphan recovered packets


def _pull_packet_metadata(driver, root, location_name, packet_id):
    metadata = driver.metadata(packet_id)[packet_id]
    expected_hash = driver.list()[packet_id].hash

    hash_validate_string(
        metadata,
        expected_hash,
        f"metadata for '{packet_id}' from '{location_name}'",
        [
            "This is bad news, I'm afraid. Your location is sending data "
            "that does not match the hash it says it does. Please let us "
            "know how this might have happened.",
            _get_remove_location_hint(location_name),
        ],
    )

    path_metadata = root.path / ".outpack" / "metadata"
    os.makedirs(path_metadata, exist_ok=True)
    filename = path_metadata / packet_id
    with open(filename, "w") as f:
        f.writelines(metadata)


def _get_remove_location_hint(location_name):
    return (
        f'Probably all you can do at this point is '
        f'remove this location from your configuration '
        f'by running '
        f'outpack_location_remove("{location_name}")'
    )


def _validate_hashes(driver, location_name, packets: List[PacketLocation]):
    mismatched_hashes = set()
    known_there = driver.list()
    for packet in packets:
        if known_there.get(packet.packet) is not None:
            hash_there = known_there[packet.packet].hash
            hash_here = packet.hash
            if hash_there != hash_here:
                mismatched_hashes.add(packet.packet)

    if mismatched_hashes:
        id_text = "', '".join(mismatched_hashes)
        msg = (
            f"Location '{location_name}' has conflicting metadata\n"
            f"This is really bad news. We have been offered metadata "
            f"from '{location_name}' that has a different hash to "
            f"metadata that we have already imported from other "
            f"locations. I'm not going to import this new metadata, "
            f"but there's no guarantee that the older metadata is "
            f"actually what you want!\nConflicts for: '{id_text}'\n"
            f"We would be interested in this case, please let us know\n"
            f"{_get_remove_location_hint(location_name)}"
        )
        raise Exception(msg)


def _mark_all_known(driver, root, location_name):
    try:
        known_here = root.index.location(location_name)
    except KeyError:
        known_here = {}

    known_there = driver.list()
    for packet_id in known_there:
        if packet_id not in known_here.keys():
            mark_known(
                root,
                packet_id,
                location_name,
                known_there[packet_id].hash,
                known_there[packet_id].time,
            )


def outpack_location_pull_packet(
    ids: Union[str, List[str]],
    *,
    options=None,
    recursive=None,
    root=None,
    locate=True,
):
    root = root_open(root, locate=locate)
    options = SearchOptions(options)
    if isinstance(ids, str):
        ids = [ids]
    plan = _location_build_pull_plan(ids, options.location, recursive, root)

    ## Warn people of extra pulls and skips
    if plan.info.n_extra > 0:
        print(
            f"Also pulling {plan.info.n_extra} "
            f"{pl(plan.info.n_extra, 'packet')}, which are "
            f"dependencies of those requested"
        )
    if plan.info.n_skip > 0:
        print(
            f"Skipping {plan.info.n_skip} of {plan.info.n_total} "
            f"{pl(plan.info.n_total, 'packet')} which are already "
            f"unpacked"
        )

    store, cleanup = _location_pull_files(plan.files, root)

    use_archive = root.config.core.path_archive is not None
    n_packets = len(plan.packets)
    time_start = time.time()
    for idx, packet in enumerate(plan.packets.values()):
        if use_archive:
            print(
                f"Writing files for '{packet.packet}' (packet {idx + 1}/"
                f"{n_packets})"
            )
            _location_pull_files_archive(packet.packet, store, root)

        mark_known(
            root, packet.packet, LOCATION_LOCAL, packet.hash, time.time()
        )

    print(
        f"Unpacked {n_packets} {pl(n_packets, 'packet')} in "
        f"{humanize.time.precisedelta(int(time.time() - time_start))}."
    )

    cleanup()
    return list(plan.packets.keys())


# This approach may be suboptimal in the case where the user does not
# already have a file store, as it means that files will be copied
# around and hashed more than ideal:
#
# * hash the candidate file
# * rehash on entry into the file store
# * copy into the file store
# * copy from the file store into the final location
#
# So in the case where a hash is only present once in a chain of
# packets being pulled this will be one too many hashes and one too
# many copies.
#
# However, this approach makes the logic fairly easy to deal with,
# and copes well with data races and corruption of data on disk
# (e.g., users having edited files that we rely on, or editing them
# after we hash them the first time).
def _location_pull_files(
    files: List[PacketFileWithLocation], root: OutpackRoot
) -> Tuple[FileStore, Callable[[], None]]:
    store = root.files
    if store is not None:

        def cleanup():
            return None

        exists, missing = partition(lambda file: store.exists(file.hash), files)

        if exists:
            print(
                f"Found {len(exists)} {pl(exists, 'file')} in the "
                f"file store"
            )
    else:
        print("Looking for suitable files already on disk")
        store = _temporary_filestore(root)

        def cleanup():
            store.destroy()

        missing = []
        no_found = 0
        for file in files:
            path = find_file_by_hash(root, file.hash)
            if path is not None:
                store.put(path, file.hash)
                no_found += 1
            else:
                missing.append(file)

        print(f"Found {no_found} {pl(no_found, 'file')} on disk ")

    if len(missing) == 0:
        print("All files available locally, no need to fetch any")
    else:
        locations = {file.location for file in missing}
        total_size = humanize.naturalsize(sum(file.size for file in missing))
        print(
            f"Need to fetch {len(missing)} {pl(missing, 'file')} "
            f"({total_size}) from {len(locations)} "
            f"{pl(locations, 'location')}"
        )
        for location in locations:
            from_this_location = [
                file for file in missing if file.location == location
            ]
            _location_pull_hash_store(
                from_this_location,
                location,
                _location_driver(location, root),
                store,
            )

    return store, cleanup


def _location_pull_hash_store(
    files: List[PacketFileWithLocation],
    location_name: str,
    driver,
    store: FileStore,
):
    no_of_files = len(files)
    # TODO: show a nice progress bar for users
    for idx, file in enumerate(files):
        print(
            f"Fetching file {idx + 1}/{no_of_files} "
            f"({humanize.naturalsize(file.size)}) from '{location_name}'"
        )
        tmp = driver.fetch_file(file.hash, store.tmp())
        store.put(tmp, file.hash)


def _location_pull_files_archive(packet_id: str, store, root: OutpackRoot):
    meta = root.index.metadata(packet_id)
    dest_root = (
        root.path / root.config.core.path_archive / meta.name / packet_id
    )
    for file in meta.files:
        store.get(file.hash, dest_root / file.path, overwrite=True)


def _pull_all_metadata(driver, root, location_name):
    known_there = driver.list()
    known_here = root.index.all_metadata().keys()
    for packet_id in known_there:
        if packet_id not in known_here:
            _pull_packet_metadata(driver, root, location_name, packet_id)


@dataclass
class PullPlanInfo:
    n_extra: int
    n_skip: int
    n_total: int


@dataclass
class LocationPullPlan:
    packets: Dict[str, PacketLocation]
    files: List[PacketFileWithLocation]
    info: PullPlanInfo


@dataclass
class PullPlanPackets:
    requested: List[str]
    full: List[str]
    skip: Set[str]
    fetch: Set[str]


def _location_build_pull_plan(
    packet_ids: List[str],
    locations: Optional[List[str]],
    recursive: Optional[bool],
    root: OutpackRoot,
) -> LocationPullPlan:
    packets = _location_build_pull_plan_packets(
        packet_ids, root, recursive=recursive
    )
    locations = _location_build_pull_plan_location(packets, locations, root)
    files = _location_build_pull_plan_files(packets.fetch, locations, root)
    fetch = _location_build_packet_locations(packets.fetch, locations, root)

    info = PullPlanInfo(
        n_extra=len(packets.full) - len(packets.requested),
        n_skip=len(packets.skip),
        n_total=len(packets.full),
    )

    return LocationPullPlan(packets=fetch, files=files, info=info)


def _location_build_pull_plan_packets(
    packet_ids: List[str], root: OutpackRoot, *, recursive: Optional[bool]
) -> PullPlanPackets:
    requested = packet_ids
    if recursive is None:
        recursive = root.config.core.require_complete_tree
    if root.config.core.require_complete_tree and not recursive:
        msg = """'recursive' must be True (or None) with your configuration
Because 'core.require_complete_tree' is true, we can't do a \
non-recursive pull, as this might leave an incomplete tree"""
        raise Exception(msg)

    index = root.index
    if recursive:
        full = _find_all_dependencies(packet_ids, index.all_metadata())
    else:
        full = packet_ids

    skip = set(full).intersection(index.unpacked())
    fetch = set(full).difference(skip)

    return PullPlanPackets(
        requested=requested, full=full, skip=skip, fetch=fetch
    )


def _find_all_dependencies(
    packet_ids: List[str], metadata: Dict[str, MetadataCore]
) -> List[str]:
    ret = set(packet_ids)
    packets = set(packet_ids)
    while packets:
        dependency_ids = {
            dependencies.packet
            for packet_id in packets
            if packet_id in metadata.keys()
            for dependencies in (
                []
                if metadata.get(packet_id) is None
                else metadata[packet_id].depends
            )
        }
        packets = dependency_ids.difference(ret)
        ret = packets.union(ret)

    return sorted(ret)


def _location_build_pull_plan_location(
    packets: PullPlanPackets, locations: Optional[List[str]], root: OutpackRoot
) -> List[str]:
    location_names = location_resolve_valid(
        locations,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=len(packets.fetch) == 0,
    )

    known_packets = [
        root.index.packets_in_location(location_name)
        for location_name in location_names
    ]
    missing = packets.fetch.difference(itertools.chain(*known_packets))
    if missing:
        extra = missing.difference(packets.requested)
        if extra:
            hint = (
                f"{len(extra)} missing "
                f"{pl(extra, 'packet was', 'packets were')} "
                f"requested as "
                f"{pl(extra, 'dependency', 'dependencies')} "
                f"of the {pl(extra, 'one')} you asked for: "
                f"{format_list(extra)}"
            )
        else:
            # In the case where the above is used, we probably have
            # up-to-date metadata, so we don't display this.
            hint = "Do you need to run 'outpack_location_pull_metadata()'?"

        msg = (
            f"Failed to find {pl(missing, 'packet')} "
            f"{format_list(missing)}\n"
            f"Looked in {pl(location_names, 'location')} "
            f"{format_list(location_names)}\n" + hint
        )
        raise Exception(msg)

    return location_names


def _location_build_pull_plan_files(
    packet_ids: Set[str], locations: List[str], root: OutpackRoot
) -> List[PacketFileWithLocation]:
    metadata = root.index.all_metadata()
    file_hashes = {
        file.hash
        for packet_id in packet_ids
        for file in metadata[packet_id].files
    }
    n_files = len(file_hashes)

    if n_files == 0:
        return []

    # Find first location within the set which contains each packet
    # We've already checked earlier that the file is in at least 1
    # location so we don't have to worry about that here
    all_files = []
    seen_hashes = set()
    for location_name in locations:
        location_packets = set(root.index.packets_in_location(location_name))
        packets_in_location = location_packets.intersection(packet_ids)
        for packet_id in packets_in_location:
            for file in metadata[packet_id].files:
                file_with_location = PacketFileWithLocation.from_packet_file(
                    file, location_name
                )
                if file.hash not in seen_hashes:
                    seen_hashes.add(file_with_location.hash)
                    all_files.append(file_with_location)

    return all_files


def _location_build_packet_locations(
    packets: Set[str], locations: List[str], root: OutpackRoot
) -> Dict[str, PacketLocation]:
    packets_fetch = {}
    for location in locations:
        packets_from_location = root.index.location(location)
        packets_in_this_location = packets & packets_from_location.keys()
        for packet_id in packets_in_this_location:
            packets_fetch[packet_id] = packets_from_location[packet_id]
    return packets_fetch


def _temporary_filestore(root: OutpackRoot) -> FileStore:
    return FileStore(root.path / "orderly" / "pull")
