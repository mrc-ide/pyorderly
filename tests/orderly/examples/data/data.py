import random

d = [random.random() for _ in range(10)]  # noqa: S311
with open("result.txt", "w") as f:
    f.writelines(f"{x}\n" for x in d)
