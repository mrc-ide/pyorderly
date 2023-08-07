from outpack.config import Config, Location, read_config


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
