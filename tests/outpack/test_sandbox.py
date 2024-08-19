import os
import time
from pathlib import Path
from traceback import TracebackException

import pytest

from pyorderly.outpack.sandbox import run_in_sandbox


def sandbox_returns_value():
    return 42


def test_sandbox_returns_value():
    assert run_in_sandbox(sandbox_returns_value) == 42


def sandbox_accepts_arguments(x, y):
    return x * y


def test_sandbox_accepts_arguments():
    result = run_in_sandbox(sandbox_accepts_arguments, args=(42, 2))
    assert result == 84


def sandbox_propagates_exceptions():
    msg = "something bad"
    raise Exception(msg)


def test_sandbox_propagates_exceptions():
    with pytest.raises(Exception, match="something bad"):
        run_in_sandbox(sandbox_propagates_exceptions)


def test_sandbox_propagates_tracebacks():
    with pytest.raises(Exception, match="something bad") as e:
        run_in_sandbox(sandbox_propagates_exceptions)

    tb = TracebackException(e.type, e.value, e.tb)
    assert Path(tb.stack[-1].filename).name == "test_sandbox.py"
    assert tb.stack[-1].name == "sandbox_propagates_exceptions"


def sandbox_does_not_share_globals():
    global my_very_unique_name  # noqa: PLW0603
    my_very_unique_name = 42
    assert "my_very_unique_name" in globals()


def test_sandbox_does_not_share_globals():
    run_in_sandbox(sandbox_does_not_share_globals)
    assert "my_very_unique_name" not in globals()


def sandbox_can_run_in_different_directory():
    return (Path.cwd(), Path("hello.txt").read_text())


def test_sandbox_can_run_in_different_directory(tmp_path):
    tmp_path.joinpath("hello.txt").write_text("Hello")

    # The syspath is necessary for the sandbox to find the test file after we've
    # changed working directory. In real code we don't need it as outpack is in
    # the search path already.
    result = run_in_sandbox(
        sandbox_can_run_in_different_directory,
        cwd=tmp_path,
        syspath=[Path.cwd()],
    )

    assert result[0] == tmp_path
    assert result[1] == "Hello"


def sandbox_can_import_local_modules():
    import my_unique_module_name  # type: ignore

    return my_unique_module_name.hello()


def test_sandbox_can_import_local_modules(tmp_path):
    (tmp_path / "my_unique_module_name.py").write_text(
        "def hello(): return 'Hello'"
    )

    result = run_in_sandbox(
        sandbox_can_import_local_modules, cwd=tmp_path, syspath=[Path.cwd()]
    )
    assert result == "Hello"


def sandbox_does_not_share_modules_cache():
    import my_unique_module_name

    return my_unique_module_name.MESSAGE


def test_sandbox_does_not_share_modules_cache(tmp_path):
    module = tmp_path / "my_unique_module_name.py"
    module.write_text("MESSAGE = 'Hello'")

    # Python uses modification time to invalidate the __pycache__ directory it
    # creates. If we write to the module twice in short succession, it may have
    # the same timestamp and not get invalidated correctly. We set the first
    # timestamp 10 seconds in the past to avoid this.
    os.utime(module, (time.time() - 10, time.time() - 10))

    message = run_in_sandbox(
        sandbox_does_not_share_modules_cache, cwd=tmp_path, syspath=[Path.cwd()]
    )
    assert message == "Hello"

    module.write_text("MESSAGE = 'World'")
    message = run_in_sandbox(
        sandbox_does_not_share_modules_cache, cwd=tmp_path, syspath=[Path.cwd()]
    )
    assert message == "World"
