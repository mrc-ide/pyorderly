import os
import pickle
import subprocess
import sys

from tblib import pickling_support

from pyorderly.outpack.util import openable_temporary_file


def run_in_sandbox(target, args=(), cwd=None, syspath=None):
    """
    Run a function as a separate process.

    The function, its arguments and return value must be picklable.

    Parameters
    ----------
    target:
        The function to run in the subprocess. This function must be accessible
        by name from the top level of a module in order to be pickled.
    args:
        The arguments to be passed to the function
    cwd:
        The working directory in which the subprocess runs. If None, it inherits
        the current process' working directory.
    syspath:
        A list of paths to be added to the child process' Python search path.
        This is used when the target function's module is not globally
        available.
    """
    with openable_temporary_file() as input_file:
        with openable_temporary_file(mode="rb") as output_file:
            pickle.dump((target, args), input_file)
            input_file.flush()

            cmd = [
                sys.executable,
                "-m",
                "pyorderly.outpack.sandbox",
                input_file.name,
                output_file.name,
            ]

            if syspath is not None:
                env = os.environ.copy()
                pythonpath = ":".join(str(s) for s in syspath)

                if "PYTHONPATH" in env:
                    env["PYTHONPATH"] = f"{pythonpath}:{env['PYTHONPATH']}"
                else:
                    env["PYTHONPATH"] = pythonpath
            else:
                env = None

            p = subprocess.run(cmd, cwd=cwd, env=env, check=False)  # noqa: S603
            p.check_returncode()

            (ok, value) = pickle.load(output_file)  # noqa: S301
            if ok:
                return value
            else:
                raise value


if __name__ == "__main__":
    with open(sys.argv[1], "rb") as input_file:
        (target, args) = pickle.load(input_file)  # noqa: S301

    try:
        result = (True, target(*args))
    except BaseException as e:
        # This allows the traceback to be pickled and communicated out of the
        # the sandbox.
        pickling_support.install(e)
        result = (False, e)

    with open(sys.argv[2], "wb") as output_file:
        pickle.dump(result, output_file)
