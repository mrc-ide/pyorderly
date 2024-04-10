from operator import mul

import orderly

orderly.shared_resource({"shared-data": "data"})

with open("shared-data/numbers.txt") as f:
    numbers = [float(x) for x in f.readlines()]
with open("shared-data/weights.txt") as f:
    weights = [float(x) for x in f.readlines()]

result = sum(map(mul, numbers, weights))
with open("result.txt", "w") as f:
    f.write(f"{result}\n")
