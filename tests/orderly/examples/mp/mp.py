import multiprocessing
import random

import orderly

# TODO(mrc-5255)
# ruff: noqa: F821


def square(x):
    return x * x


if __name__ == "__main__":
    orderly.parameters(method=None)
    orderly.artefact("Squared numbers", "result.txt")

    mp = multiprocessing.get_context(method)  # type: ignore
    data = [random.random() for _ in range(10)]

    with mp.Pool(5) as p:
        result = p.map(square, data)

    with open("result.txt", "w") as f:
        f.writelines(f"{x}\n" for x in result)
