import datetime
import os
import runpy
import time
from contextlib import contextmanager
from pathlib import Path


def find_file_descend(filename, path):
    path = Path(path)
    root = Path(path.root)

    while path != root:
        attempt = path / filename
        if attempt.exists():
            return str(attempt.parent)
        path = path.parent

    return None


def iso_time_str(t):
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime(t))


def time_to_num(x):
    return x.timestamp()


def num_to_time(x):
    return datetime.datetime.fromtimestamp(x, datetime.timezone.utc)


# Recursively find all normal files below 'path', returning them
# relative to that path.
def all_normal_files(path):
    return [
        str(p.relative_to(path))
        for p in Path(path).rglob("*")
        if not p.is_dir()
    ]


def run_script(wd, path):
    with transient_working_directory(wd):
        # other ways to do this include importlib, subprocess and
        # multiprocess
        runpy.run_path(path)


@contextmanager
def transient_working_directory(path):
    origin = os.getcwd()
    try:
        if path is not None:
            os.chdir(path)
        yield
    finally:
        if path is not None:
            os.chdir(origin)


def assert_file_exists(path, *, workdir=None, name="File"):
    with transient_working_directory(workdir):
        if isinstance(path, list):
            missing = [str(p) for p in path if not os.path.exists(p)]
        else:
            missing = [] if os.path.exists(path) else [path]
    if len(missing):
        missing_str = ", ".join(missing)
        msg = f"{name} does not exist: {missing_str}"
        raise Exception(msg)


def expand_dirs(paths, *, workdir=None):
    if len(paths) == 0:
        return []
    ret = []
    with transient_working_directory(workdir):
        for p in paths:
            if os.path.isdir(p):
                ret += [os.path.join(p, f) for f in all_normal_files(p)]
            else:
                ret.append(str(p))
    return ret


def match_value(arg, choices, name):
    if arg not in choices:
        choices_str = "', '".join(choices)
        msg = f"{name} must be one of '{choices_str}'"
        raise Exception(msg)


def relative_path_array(files, name):
    if not isinstance(files, list):
        files = [files]
    for f in files:
        if os.path.isabs(f):
            msg = f"Expected {name} path '{f}' to be a relative path"
            raise Exception(msg)
    return files


def read_string(path):
    with open(path) as f:
        lines = f.readlines()
    contents = "\n".join(lines).strip()
    return contents
