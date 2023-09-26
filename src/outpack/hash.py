import hashlib
from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Hash:
    algorithm: str
    value: str

    def __str__(self):
        return f"{self.algorithm}:{self.value}"


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


def hash_validate(found, expected, name):
    if found != expected:
        msg = "\n".join(
            [
                f"Hash of '{name}' does not match:",
                f" - expected: {expected}",
                f" - found: {found}",
            ]
        )
        raise Exception(msg)

def hash_validate_file(path, expected):
    h = hash_parse(expected)
    found = hash_file(path, h.algorithm)
    hash_validate(found, h, path)
def hash_validate_data(data, expected, name):
    h = hash_parse(expected)
    data =