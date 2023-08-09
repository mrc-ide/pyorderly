import hashlib
from dataclasses import dataclass


@dataclass
class Hash:
    algorithm: str
    digest: str

    def __str__(self):
        return f"{self.algorithm}:{self.digest}"


def hash_file(path, algorithm="sha256"):
    h = hashlib.new(algorithm)
    blocksize = 128 * h.block_size
    with open(path, "rb") as f:
        while chunk := f.read(blocksize):
            h.update(chunk)
    return Hash(algorithm, h.hexdigest())


def hash_string(data, algorithm):
    h = hashlib.new(algorithm)
    h.update(data.encode())
    h.hexdigest()
    return Hash(algorithm, h.hexdigest())


def hash_parse(string):
    if type(string) == Hash:
        return string
    return Hash(*string.split(":"))


def hash_validate(path, expected):
    h = hash_parse(expected)
    found = hash_file(path, h.algorithm)
    if found != h:
        msg = "\n".join(
            [
                f"Hash of '{path}' does not match:",
                f" - expected: {expected}",
                f" - found: {found}",
            ]
        )
        raise Exception(msg)
