import random

import pyorderly

d = [random.random() for _ in range(10)]
pyorderly.artefact("Random numbers", "result.txt")
with open("result.txt", "w") as f:
    f.writelines(f"{x}\n" for x in d)
