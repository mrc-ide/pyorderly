import base64
import errno
import re
from contextlib import ExitStack
from pathlib import PurePosixPath
from typing import Dict, List
from urllib.parse import urlparse

import paramiko

from outpack.config import Config
from outpack.hash import hash_parse
from outpack.location import LocationDriver
from outpack.metadata import MetadataCore, PacketFile, PacketLocation
from outpack.static import LOCATION_LOCAL

SCP_URL = re.compile(r"((?P<username>[^@]+)@)?(?P<hostname>[^:]+):(?P<path>.*)")


def parse_ssh_url(url):
    """
    Parse the URL of an SSH location.

    We follow Git's definition of URLs, and support two syntaxes: a short
    `username@hostname:path` version, inspired by the scp command, and a longer
    more explicit `ssh://username@hostname:port/path`.

    The SCP syntax doesn't support configuring a port number (though an
    .ssh/config file can be used to work around that limitation).

    By default, the path in the SCP syntax is relative to the remote user's home
    directory, and `hostname:/foo/bar` can be used to specify an absolute path.
    The explicit URL syntax is always absolute.

    See https://git-scm.com/docs/git-pull#_git_urls.
    """
    if "://" in url:
        parts = urlparse(url)
        if parts.scheme != "ssh":
            msg = f"Protocol of SSH url must be 'ssh', not '{parts.scheme}'"
            raise Exception(msg)

        if parts.path == "":
            msg = "No path specified for SSH location"
            raise Exception(msg)

        return parts.username, parts.hostname, parts.port, parts.path

    elif m := SCP_URL.fullmatch(url):
        return (m.group("username"), m.group("hostname"), None, m.group("path"))
    else:
        msg = f"Invalid SSH url: '{url}'"
        raise Exception(msg)


class OutpackLocationSSH(LocationDriver):
    _client: paramiko.SSHClient
    _sftp: paramiko.SFTPClient

    def __init__(self, url: str, known_hosts=None, password=None):
        (username, hostname, port, path) = parse_ssh_url(url)
        self._username = username
        self._hostname = hostname
        self._port = port or 22
        self._root = PurePosixPath(path)
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
                result[packet] = f.read().decode("ascii").strip()

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
