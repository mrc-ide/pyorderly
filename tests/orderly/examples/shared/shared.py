import pyorderly

pyorderly.shared_resource({"shared_data.txt": "numbers.txt"})

with open("shared_data.txt") as f:
    dat = [int(x) for x in f.readlines()]

with open("result.txt", "w") as f:
    f.write(f"{sum(dat)}\n")
