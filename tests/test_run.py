import os
import shutil
import pytest

from outpack.init import outpack_init
from outpack.run import orderly_run


## We're going to need a small test helper module here at some point,
## unfortunately pytest makes that totally unobvious how we do it, but
## we'll get there. For now inline the code as we use it.
def test_can_run_simple_example(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "data"
    path_src.mkdir(parents=True, exist_ok=True)
    shutil.copyfile("tests/examples/data/orderly.py",  path_src / "orderly.py")
    res = orderly_run("data", root=path)
    path_res = path / "archive" / "data" / res
    assert path_res.exists()
    assert not (path / "draft" / "data" / res).exists()
    # Then read the metadata in, why did we not make this easy?
    # meta = read_metadata(path, r.index.metadata(p.id)
