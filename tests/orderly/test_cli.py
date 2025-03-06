import os
import re
from contextlib import contextmanager, nullcontext

from click.testing import CliRunner
from pytest_unordered import unordered

from pyorderly.cli import cli
from pyorderly.outpack.config import Location, read_config
from pyorderly.outpack.location import outpack_location_add_path
from pyorderly.outpack.root import OutpackRoot

from .. import helpers


# This exists as contextlib.chdir in Python 3.11+
@contextmanager
def chdir(path):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def invoke(*args, expected_exit_code=0, cwd=None):
    runner = CliRunner(mix_stderr=False)

    def cast(p):
        if isinstance(p, os.PathLike):
            return os.fspath(p)
        else:
            return p

    with chdir(cwd) if cwd else nullcontext():
        result = runner.invoke(cli, [cast(x) for x in args])
    assert result.exit_code == expected_exit_code

    return result


def test_can_init_repository(tmp_path):
    invoke("init", tmp_path)

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive == "archive"
    assert not cfg.core.use_file_store
    assert not cfg.core.require_complete_tree


def test_can_init_repository_with_file_store(tmp_path):
    invoke("init", tmp_path, "--use-file-store")

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive is None
    assert cfg.core.use_file_store


def test_can_init_repository_with_file_store_and_archive(tmp_path):
    invoke("init", tmp_path, "--use-file-store", "--archive", "storage")

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive == "storage"
    assert cfg.core.use_file_store


def test_run_prints_packet_id(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("data", root)

    result = invoke("run", "data", cwd=tmp_path)
    id = result.stdout.strip()

    assert (tmp_path / "archive" / "data" / id).exists()
    assert root.index.metadata(id).name == "data"


def test_can_pass_parameters_when_running(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-n", "a", "3.14"),
        *("-p", "b", "foo"),
        cwd=root,
    )
    id = result.stdout.strip()
    assert root.index.metadata(id).parameters == {"a": 3.14, "b": "foo"}

    result = invoke(
        "run",
        "parameters",
        *("-b", "a", "true"),
        *("-b", "b", "false"),
        cwd=root,
    )
    id = result.stdout.strip()
    assert root.index.metadata(id).parameters == {"a": True, "b": False}


def test_can_distinguish_int_or_float_parameters(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-n", "a", "3"),
        *("-n", "b", "3."),
        cwd=root,
    )
    id = result.stdout.strip()
    params = root.index.metadata(id).parameters
    assert isinstance(params["a"], int)
    assert isinstance(params["b"], float)


def test_parameters_are_not_converted_implicitly(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-p", "a", "3"),
        *("-p", "b", "true"),
        cwd=root,
    )
    id = result.stdout.strip()
    params = root.index.metadata(id).parameters
    assert isinstance(params["a"], str)
    assert isinstance(params["b"], str)
    assert params == {"a": "3", "b": "true"}


def test_cannot_pass_same_parameter_multiple_times(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-p", "a", "foo"),
        *("-p", "a", "bar"),
        *("-n", "b", "5"),
        *("-b", "b", "false"),
        cwd=root,
        expected_exit_code=1,
    )
    assert (
        result.stderr.strip()
        == "Parameters were specified multiple times: 'a', 'b'"
    )


def test_can_search_packets(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    ids = {helpers.create_random_packet(root, name="data") for _ in range(3)}

    result = invoke("search", "name == 'data'", cwd=root)
    assert set(result.stdout.splitlines()) == ids

    result = invoke("search", "name == 'other'", cwd=root, expected_exit_code=1)
    assert result.stderr.strip() == "No packets matching the query were found"


def test_search_options(tmp_path):
    root = helpers.create_temporary_roots(tmp_path, ["dst", "x", "y"])
    id_x = helpers.create_random_packet(name="data", root=root["x"])
    id_y = helpers.create_random_packet(name="data", root=root["y"])

    outpack_location_add_path("x", root["x"], root=root["dst"])
    outpack_location_add_path("y", root["y"], root=root["dst"])

    result = invoke(
        "search",
        "--allow-remote",
        "--pull-metadata",
        "name == 'data'",
        cwd=root["dst"],
    )
    assert result.stdout.splitlines() == unordered(id_x, id_y)

    result = invoke(
        "search",
        "--allow-remote",
        "--location=x",
        "name == 'data'",
        cwd=root["dst"],
    )
    assert result.stdout.splitlines() == [id_x]

    result = invoke(
        "search",
        "name == 'data'",
        cwd=root["dst"],
        expected_exit_code=1,
    )
    assert result.stderr.strip() == "No packets matching the query were found"


def test_can_add_locations(tmp_path):
    root = helpers.create_temporary_roots(tmp_path)

    invoke("location", "add", "foo", root["src"], cwd=root["dst"])
    invoke("location", "add", "bar", "ssh://127.0.0.1/foo", cwd=root["dst"])

    config = read_config(root["dst"])
    assert config.location == {
        "local": Location("local", "local", None),
        "foo": Location("foo", "path", {"path": os.fspath(root["src"])}),
        "bar": Location("bar", "ssh", {"url": "ssh://127.0.0.1/foo"}),
    }


def test_can_manage_locations(tmp_path):
    root = helpers.create_temporary_roots(tmp_path)

    result = invoke("location", "list", cwd=root["dst"])
    assert result.stdout.splitlines() == ["local"]

    invoke("location", "add", "foo", root["src"], cwd=root["dst"])

    result = invoke("location", "list", cwd=root["dst"])
    assert result.stdout.splitlines() == unordered("local", "foo")

    invoke(
        "location",
        "rename",
        "foo",
        "bar",
        cwd=root["dst"],
    )

    result = invoke("location", "list", cwd=root["dst"])
    assert result.stdout.splitlines() == unordered("local", "bar")

    invoke("location", "remove", "bar", cwd=root["dst"])

    result = invoke("location", "list", cwd=root["dst"])
    assert result.stdout.splitlines() == ["local"]


def test_cannot_add_location_with_unknown_protocol(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    result = invoke(
        "location",
        "add",
        "origin",
        "myproto://example.com",
        cwd=root,
        expected_exit_code=1,
    )
    assert re.search(
        "^Unsupported location protocol: 'myproto'$",
        result.stderr,
        re.MULTILINE,
    )
