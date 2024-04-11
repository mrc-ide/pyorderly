import orderly

# We'll rethink this strategy soon
# ruff: noqa: F821

orderly.parameters(a=1, b=None)
with open("result.txt", "w") as f:
    f.write(f"a: {a}\nb: {b}\n")  # type: ignore
