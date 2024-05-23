import base64
import errno
from contextlib import ExitStack
from pathlib import PurePosixPath
from typing import Dict, List
from urllib.parse import urlparse

import paramiko

from outpack.config import Config
from outpack.hash import hash_parse
from outpack.location_driver import LocationDriver
from outpack.metadata import MetadataCore, PacketFile, PacketLocation
from outpack.static import LOCATION_LOCAL


def parse_ssh_url(url):
    """
    Parse the URL of an SSH location.

    URLs follow the form `ssh://username@hostname:port/path`, where path is
    interpreted as an absolute path (ie. the separating slash at the start of
    the path is included in it).

    However, if the path begins with `/~/` (as in `ssh://hostname/~/foo/bar`),
    then the rest of the path is assumed to be relative to the user's home
    directory.

    This generally follows Git's semantics. Git also supports more complicated
    expansions (eg. using another user's home directory, such as `/~alice/`),
    but it implements this by having a server-side binary, which we don't have.

    Git also supports an alternative SCP-like syntax, of the form
    `username@hostname:path`, where `path` is relative by default, and port
    numbers cannot be specified. The lack of scheme to identify the protocol
    makes these URLs inconvenient, so we don't support it.

    See https://git-scm.com/docs/git-pull#_git_urls.
    """
    parts = urlparse(url)
    if parts.scheme != "ssh":
        msg = "Protocol of SSH url must be 'ssh'"
        raise Exception(msg)

    if parts.path == "":
        msg = "No path specified for SSH location"
        raise Exception(msg)

    path = PurePosixPath(parts.path)

    # This condition is equivalent to path.is_relative_to("/~"), except that
    # function isn't available on Python 3.8.
    home = PurePosixPath("/~")
    if path == home or home in path.parents:
        path = path.relative_to(home)

    return parts.username, parts.hostname, parts.port, path


class OutpackLocationSSH(LocationDriver):
    _client: paramiko.SSHClient
    _sftp: paramiko.SFTPClient

    def __init__(self, url: str, known_hosts=None, password=None):
        (username, hostname, port, path) = parse_ssh_url(url)
        self._username = username
        self._hostname = hostname
        self._port = port or 22
        self._root = path
        self._known_hosts = known_hosts
        self._password = password
        self._stack = ExitStack()

    def __enter__(self):
        with ExitStack() as stack:
            client = stack.enter_context(paramiko.SSHClient())

            if self._known_hosts is not None:
                for hostname, keytype, data in self._known_hosts:
                    key = paramiko.RSAKey(data=base64.b64decode(data))
                    client.get_host_keys().add(hostname, keytype, key)
            else:
                client.load_system_host_keys()

            client.connect(
                self._hostname,
                username=self._username,
                port=self._port,
                password=self._password,
            )

            sftp = stack.enter_context(client.open_sftp())

            try:
                sftp.stat(str(self._root))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    msg = f"Path '{self._root}' on remote {self._hostname} does not exist"
                    raise Exception(msg) from None
                else:
                    raise

            path = self._root / ".outpack" / "config.json"
            try:
                with sftp.open(str(path)) as f:
                    self.config = Config.from_json(f.read().strip())  # type: ignore
            except OSError as e:
                if e.errno == errno.ENOENT:
                    msg = f"Path '{self._root}' on remote {self._hostname} is not a valid outpack repository"
                    raise Exception(msg) from None
                else:
                    raise

            self._client = client
            self._sftp = sftp
            self._stack = stack.pop_all()

            return self

    def __exit__(self, *args):
        return self._stack.__exit__(*args)

    def list(self) -> Dict[str, PacketLocation]:
        path = self._root / ".outpack" / "location" / LOCATION_LOCAL
        result = {}
        for packet in self._sftp.listdir(str(path)):
            with self._sftp.open(str(path / packet)) as f:
                result[packet] = PacketLocation.from_json(f.read().strip())  # type: ignore
        return result

    def metadata(self, ids: List[str]) -> Dict[str, str]:
        path = self._root / ".outpack" / "metadata"
        result = {}

        for packet in ids:
            with self._sftp.open(str(path / packet)) as f:
                result[packet] = f.read().decode("utf-8").strip()

        return result

    def fetch_file(self, packet: MetadataCore, file: PacketFile, dest: str):
        path = self._file_path(packet, file)
        if path is None:
            msg = f"Hash '{file.hash}' not found at location"
            raise Exception(msg)

        try:
            self._sftp.get(str(path), dest)
        except OSError as e:
            if e.errno == errno.ENOENT:
                msg = f"Hash '{file.hash}' not found at location"
                raise Exception(msg) from e
            else:
                raise

    def _file_path(self, packet: MetadataCore, file: PacketFile):
        if self.config.core.use_file_store:
            dat = hash_parse(file.hash)
            return (
                self._root
                / "files"
                / dat.algorithm
                / dat.value[:2]
                / dat.value[2:]
            )
        else:
            return (
                self._root
                / self.config.core.path_archive
                / packet.name
                / packet.id
                / file.path
            )
