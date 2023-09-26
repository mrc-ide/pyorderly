import os


def mark_packet_known(packet_id, location_name, hash, time, root):
    dest_dir = root.path / ".outpack" / "location" / location_name
    dest = dest_dir / packet_id
    os.makedirs(dest_dir, exist_ok=True)
    with open(dest) as f:

        f.writelines(metadata)