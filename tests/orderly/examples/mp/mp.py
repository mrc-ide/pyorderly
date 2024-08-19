import multiprocessing
import random

import pyorderly


def square(x):
    return x * x


if __name__ == "__main__":
    params = pyorderly.parameters(method=None)
    pyorderly.artefact("Squared numbers", "result.txt")

    mp = multiprocessing.get_context(params.method)
    data = [random.random() for _ in range(10)]

    with mp.Pool(5) as p:
        result = p.map(square, data)

    with open("result.txt", "w") as f:
        f.writelines(f"{x}\n" for x in result)
