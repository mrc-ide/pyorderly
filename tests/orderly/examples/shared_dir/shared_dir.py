from operator import mul

import pyorderly

pyorderly.shared_resource({"shared_data": "data"})

with open("shared_data/numbers.txt") as f:
    numbers = [float(x) for x in f.readlines()]
with open("shared_data/weights.txt") as f:
    weights = [float(x) for x in f.readlines()]

result = sum(map(mul, numbers, weights))
with open("result.txt", "w") as f:
    f.write(f"{result}\n")
