import orderly

orderly.parameters(a=1, b=None)
with open("result.txt", "w") as f:
    f.write(f"a: {a}\nb: {b}\n")  # noqa: F821
