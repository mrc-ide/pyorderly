import shutil

from outpack.root import root_open, find_file_by_hash
from outpack.static import LOCATION_LOCAL
from outpack.util import read_string


class OutpackLocationPath:

    def __init__(self, path):
        self.__root = root_open(path, locate=False)

    def list(self):
        return self.__root.index.location(LOCATION_LOCAL)

    def metadata(self, packet_ids):
        if isinstance(packet_ids, str):
            packet_ids = [packet_ids]

        all_ids = self.__root.index.location(LOCATION_LOCAL).keys()
        missing_ids = set(packet_ids).difference(all_ids)
        if len(missing_ids) > 0:
            missing_msg = "', '".join(missing_ids)
            msg = f"Some packet ids not found: '{missing_msg}'"
            raise Exception(msg)
        ret = {}
        for packet_id in packet_ids:
            path = self.__root.path / ".outpack" / "metadata" / packet_id
            ret[packet_id] = read_string(path)
        return ret

    def fetch_file(self, hash, dest):
        if self.__root.config.core.use_file_store:
            path = self.__root.files.filename(hash)
            if not path.exists():
                msg = f"Hash '{hash}' not found at location"
                raise Exception(msg)
        else:
            path = find_file_by_hash(self.__root, hash)
            if path is None:
                msg = f"Hash '{hash}' not found at location"
                raise Exception(msg)
        shutil.copyfile(path, dest)
        return dest

    def list_unknown_packets(self, ids):
        raise Exception("impl TODO")

    def list_unknown_files(self, hashes):
        raise Exception("impl TODO")

    def push_file(self, hash):
        raise Exception("impl TODO")

    def push_metadata(self, packet_id, hash, path):
        raise Exception("impl TODO")