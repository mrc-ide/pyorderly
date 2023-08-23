import runpy

from outpack.root import root_open
from outpack.ids import outpack_id


root = "tmp"
locate = False
name = "data"


def outpack_run(name, *, root=None, locate=True):
    root = root_open(root, locate)

    path_src = root.path / "src" / name
    if not path.is_dir() or path.joinpath("outpack.py").exists():
        raise Exception

    packet_id = outpack_id()
    path_dest = root.path / "draft" / name / packet_id
    path_dest.mkdir(parents=True)

    ## This plainly won't work generally; see how we deal with this
    ## issue in orderly2, but in general going the right thing is more
    ## subtle than you want.
    for p in path_src.iterdir():
        shutil.copy2(p, path_dest.joinpath(p.relative_to(path_src)))

    ## There are at least three things we might do here:
    ##
    ## runpy
    ## importlib
    ## call in separate process (e.g., with subprocess)
    runpy.run_path

    
