from outpack.init import outpack_init
from outpack.root import root_open


def create_temporary_root(path, *, use_file_store=False):
    outpack_init(path, use_file_store=use_file_store)
    return root_open(path, False)
