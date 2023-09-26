import os

from outpack.metadata import PacketLocation, write_packet_location


def mark_packet_known(packet_id, location_name,
                      packet_location: PacketLocation, root):
    dest_dir = root.path / ".outpack" / "location" / location_name
    dest = dest_dir / packet_id
    os.makedirs(dest_dir, exist_ok=True)
    write_packet_location(dest, packet_location)