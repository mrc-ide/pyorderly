from pathlib import Path

from pyorderly.outpack.config import Config, read_config, write_config


def outpack_init(
    path,
    *,
    path_archive="archive",
    use_file_store=False,
    require_complete_tree=False,
):
    path = Path(path)
    if path.exists() and not path.is_dir():
        msg = f"Path '{path}' already exists but is not a directory"
        raise Exception(msg)

    # As in orderly2, there are unresolved questions about if we
    # allow initialising an outpack root into an existing
    # directory. Here, we'll allow it.

    config = Config.new(
        path_archive=path_archive,
        use_file_store=use_file_store,
        require_complete_tree=require_complete_tree,
    )

    path_outpack = path.joinpath(".outpack")

    if path_outpack.exists():
        _validate_same_core_configuration(config.core, read_config(path).core)
    else:
        path_outpack.mkdir(parents=True, exist_ok=True)
        path_outpack.joinpath("metadata").mkdir(parents=True, exist_ok=True)
        path_outpack.joinpath("location").mkdir(parents=True, exist_ok=True)
        path_outpack.joinpath("location/local").mkdir(
            parents=True, exist_ok=True
        )
        if path_archive is not None:
            path.joinpath(path_archive).mkdir(exist_ok=True)
        write_config(config, path)

    return path


def _validate_same_core_configuration(now, then):
    if now == then:
        return
    a = then.to_dict()
    b = now.to_dict()
    err = [
        f"* '{f}' was {a[f]} but {b[f]} requested"
        for f in a.keys()
        if a[f] != b[f]
    ]
    msg = "Trying to change configuration when re-initialising:\n" + "\n".join(
        err
    )
    raise Exception(msg)
