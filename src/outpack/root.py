from pathlib import Path

from outpack.config import read_config
from outpack.filestore import FileStore


class Root:
    def __init__(self, path):
        self.path = Path(path)
        self.config = read_config(path)
        if self.config.core.use_file_store:
            self.files = FileStore(self.path / "files")
        # self.index = 
        
