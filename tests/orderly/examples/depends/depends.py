import pyorderly

pyorderly.dependency(None, "latest", {"input.txt": "result.txt"})
pyorderly.artefact("Summary", "summary.txt")

with open("input.txt") as f:
    d = [float(x.strip()) for x in f.readlines()]
total = sum(d)
with open("summary.txt", "w") as f:
    f.write(f"{total}\n")
