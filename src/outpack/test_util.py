from outpack.init import outpack_init
from outpack.root import root_open


def create_temporary_root(path):
    outpack_init(path)
    return root_open(path, False)
