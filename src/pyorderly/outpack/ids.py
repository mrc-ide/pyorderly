import math
import re
import secrets
import time

from pyorderly.outpack.util import iso_time_str

RE_ID = re.compile("^[0-9]{8}-[0-9]{6}-[0-9a-f]{8}$")


def fractional_to_bytes(x):
    return f"{math.floor((x % 1) * pow(256, 2)):04x}"


def outpack_id():
    t = time.time()
    rand = secrets.token_hex(2)
    return f"{iso_time_str(t)}-{fractional_to_bytes(t)}{rand}"


def is_outpack_id(x: str):
    return RE_ID.match(x)


def validate_outpack_id(x: str):
    if not RE_ID.match(x):
        msg = f"Malformed id '{x}'"
        raise Exception(msg)
