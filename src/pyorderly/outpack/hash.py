import hashlib
from dataclasses import dataclass
from typing import TypeAlias


@dataclass
class Hash:
    algorithm: str
    value: str

    def __str__(self):
        return f"{self.algorithm}:{self.value}"


HashLike: TypeAlias = str | Hash


def hash_file(path: str, algorithm: str = "sha256") -> Hash:
    h = hashlib.new(algorithm)
    # In Python 3.11 this can be replaced with hashlib.file_digest
    blocksize = 128 * h.block_size
    with open(path, "rb") as f:
        while chunk := f.read(blocksize):
            h.update(chunk)
    return Hash(algorithm, h.hexdigest())


def hash_string(data: str, algorithm: str = "sha256") -> Hash:
    h = hashlib.new(algorithm)
    h.update(data.encode())
    return Hash(algorithm, h.hexdigest())


def hash_parse(string: str | Hash) -> Hash:
    if isinstance(string, Hash):
        return string
    return Hash(*string.split(":"))


def hash_validate(found: Hash, expected: Hash, name: str, body=None):
    if body is None:
        body = []
    if found != expected:
        msg = "\n".join(
            [
                f"Hash of {name} does not match:",
                f" - expected: {expected}",
                f" - found: {found}",
                *body,
            ]
        )
        raise Exception(msg)


def hash_validate_file(path, expected: HashLike, body=None):
    h = hash_parse(expected)
    found = hash_file(path, h.algorithm)
    hash_validate(found, h, f"'{path}'", body)


def hash_validate_string(data: str, expected: HashLike, name, body=None):
    h = hash_parse(expected)
    found = hash_string(data, h.algorithm)
    hash_validate(found, h, name, body)
