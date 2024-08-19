import pytest

from pyorderly.outpack.config import Config, Location, read_config, write_config


def test_can_read_config():
    cfg = read_config("example")
    assert isinstance(cfg, Config)
    assert cfg.core.path_archive == "archive"
    assert cfg.core.use_file_store
    assert cfg.core.require_complete_tree is False
    assert len(cfg.location) == 1
    assert "local" in cfg.location
    local = cfg.location["local"]
    assert isinstance(local, Location)
    assert local.name == "local"
    assert local.type == "local"
    assert local.args == {}


def test_can_write_json(tmp_path):
    cfg = read_config("example")
    assert Config.from_json(cfg.to_json()) == cfg

    tmp_path.joinpath(".outpack").mkdir()
    write_config(cfg, tmp_path)
    assert read_config(tmp_path) == cfg


def test_can_create_new_config():
    cfg = Config.new()
    assert cfg.schema_version == "0.1.1"
    assert cfg.core.hash_algorithm == "sha256"
    assert cfg.core.path_archive == "archive"
    assert not cfg.core.use_file_store
    assert not cfg.core.require_complete_tree
    assert len(cfg.location) == 1
    assert list(cfg.location.keys()) == ["local"]
    assert cfg.location["local"].name == "local"
    assert cfg.location["local"].type == "local"


def test_can_validate_dependent_values_in_new_config():
    with pytest.raises(
        Exception,
        match="If 'path_archive' is None, 'use_file_store' must be True",
    ):
        Config.new(path_archive=None)
