import shutil

from outpack.init import outpack_init
from outpack.root import root_open

from orderly.run import orderly_run


## We're going to need a small test helper module here at some point,
## unfortunately pytest makes that totally unobvious how we do it, but
## we'll get there. For now inline the code as we use it.
def test_can_run_simple_example(tmp_path):
    path = outpack_init(tmp_path)
    path_src = path / "src" / "data"
    path_src.mkdir(parents=True, exist_ok=True)
    shutil.copyfile("tests/examples/data/orderly.py", path_src / "orderly.py")
    res = orderly_run("data", root=path)
    path_res = path / "archive" / "data" / res
    assert path_res.exists()
    assert not (path / "draft" / "data" / res).exists()
    # TODO: need a nicer way of doing this, one that would be part of
    # the public API.
    meta = root_open(tmp_path, False).index.metadata(res)
    assert meta.id == res
    assert meta.name == "data"
    assert meta.parameters == {}
    assert list(meta.time.keys()) == ["start", "end"]
    assert len(meta.files) == 2
    assert {el.path for el in meta.files} == {"orderly.py", "result.txt"}
    assert meta.depends == []
    custom = {"orderly": {"role": [{"path": "orderly.py", "role": "orderly"}]}}
    assert meta.custom == custom
    assert meta.git is None
