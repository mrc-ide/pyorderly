import os
import shutil

import pytest

from pyorderly.outpack.index import Index
from pyorderly.outpack.metadata import read_metadata_core


def test_can_create_index():
    idx = Index("example")
    packet = "20230807-152344-ee606dce"
    expected = read_metadata_core("example/.outpack/metadata/" + packet)
    ids = sorted(os.listdir("example/.outpack/metadata"))
    # First read from disk:
    assert idx.metadata(packet) == expected
    # Second from cache:
    assert idx.metadata(packet) == expected
    metadata = idx.all_metadata()
    assert len(metadata) == 5
    locations = idx.all_locations()
    assert "local" in locations.keys()
    assert len(locations)
    assert metadata.keys() == idx.data.location["local"].keys()
    assert idx.data.unpacked == ids
    assert idx.location("local") == idx.data.location["local"]
    assert idx.unpacked() == ids


def test_rebuild_can_pick_up_deletions(tmp_path):
    shutil.copytree("example", tmp_path, dirs_exist_ok=True)
    idx1 = Index(tmp_path)
    idx2 = Index(tmp_path)
    packet = "20230807-152344-ee606dce"
    assert idx1.refresh()
    os.remove(tmp_path / ".outpack" / "metadata" / packet)
    os.remove(tmp_path / ".outpack" / "location" / "local" / packet)
    idx2.rebuild()
    assert idx1.metadata(packet).id == packet
    with pytest.raises(KeyError):
        idx2.metadata(packet)
