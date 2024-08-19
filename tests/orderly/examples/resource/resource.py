import pyorderly

pyorderly.resource("numbers.txt")
with open("numbers.txt") as f:
    dat = [int(x) for x in f.readlines()]

with open("result.txt", "w") as f:
    f.write(f"{sum(dat)}\n")
