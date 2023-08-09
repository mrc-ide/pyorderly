import hashlib
from dataclasses import dataclass


@dataclass
class Digest:
    algorithm: str
    value: str

    def __str__(self):
        return f"{self.algorithm}:{self.value}"


def digest_file(path, algorithm="sha256"):
    h = hashlib.new(algorithm)
    blocksize = 128 * h.block_size
    with open(path, "rb") as f:
        while chunk := f.read(blocksize):
            h.update(chunk)
    return Digest(algorithm, h.hexdigest())


def digest_string(data, algorithm):
    h = hashlib.new(algorithm)
    h.update(data.encode())
    h.hexdigest()
    return Digest(algorithm, h.hexdigest())


def digest_parse(string):
    if type(string) == Digest:
        return string
    return Digest(*string.split(":"))


def digest_validate(path, expected):
    h = digest_parse(expected)
    found = digest_file(path, h.algorithm)
    if found != h:
        msg = "\n".join(
            [
                f"Digest of '{path}' does not match:",
                f" - expected: {expected}",
                f" - found: {found}",
            ]
        )
        raise Exception(msg)
