import pyorderly

parameters = pyorderly.parameters(a=1, b=None)
with open("result.txt", "w") as f:
    f.write(f"a: {parameters.a}\nb: {parameters.b}\n")
