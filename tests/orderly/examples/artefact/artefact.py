import random

import orderly

d = [random.random() for _ in range(10)]
orderly.artefact("Random numbers", "result.txt")
with open("result.txt", "w") as f:
    f.writelines(f"{x}\n" for x in d)
