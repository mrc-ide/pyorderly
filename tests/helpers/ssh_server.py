import base64
import os
import socket
import sys
import threading
from contextlib import AbstractContextManager, ExitStack
from pathlib import Path
from typing import Optional

import paramiko


class SSHServer(AbstractContextManager):
    """
    A test SSH server.

    This server should only be used for the purpose of integration testing: it
    allows connections with any username and password and exposes an SFTP
    interface with access to the entire host system.

    SFTP requests with relative paths begin at the given root. Absolute paths
    may also be specified by the client.
    """

    root: Path
    port: int
    host_key: paramiko.PKey

    def __init__(self, root, allowed_users: Optional[list[str]] = None):
        self.root = root
        self.allowed_users = allowed_users
        self.host_key = paramiko.RSAKey.generate(bits=1024)
        self.shutdown = threading.Event()

    def url(self, path: str, username=None):
        url = "ssh://"
        if username is not None:
            url += f"{username}@"
        url += f"127.0.0.1:{self.port}/{path}"
        return url

    @property
    def host_key_entry(self):
        return (
            f"[127.0.0.1]:{self.port}",
            "ssh-rsa",
            base64.b64encode(self.host_key.asbytes()),
        )

    def __enter__(self):
        with ExitStack() as stack:
            sock = stack.enter_context(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            )
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind to all interfaces on a random port. We read the port back
            # using getsockname to allow test code to connect.
            sock.bind(("", 0))
            sock.listen(5)

            (_, self.port) = sock.getsockname()

            t = threading.Thread(target=self._server_loop, args=(sock,))
            t.start()

            @stack.callback
            def cleanup():
                # Setting the shutdown event isn't enough, we also need to wake up
                # the thread from accept(). Faking a connection is the easiest way.
                self.shutdown.set()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("127.0.0.1", self.port))
                t.join()

            self._stack = stack.pop_all()
            return self

    def __exit__(self, *args):
        return self._stack.__exit__(*args)

    def _server_loop(self, sock: socket.socket):
        while True:
            client, addr = sock.accept()
            if self.shutdown.is_set():
                return

            t = paramiko.Transport(client)
            t.add_server_key(self.host_key)
            t.set_subsystem_handler(
                "sftp",
                paramiko.SFTPServer,
                SFTPServerInterface,
                self.root,
            )
            t.start_server(
                event=threading.Event(),
                server=ServerInterface(self.allowed_users),
            )


class ServerInterface(paramiko.ServerInterface):
    def __init__(self, allowed_users):
        self._allowed_users = allowed_users

    def get_allowed_auths(self, _username):
        return "password"

    def check_auth_password(self, username, _password):
        if self._allowed_users is None or username in self._allowed_users:
            return paramiko.common.AUTH_SUCCESSFUL
        else:
            return paramiko.common.AUTH_FAILED

    def check_channel_request(self, _kind, _chanid):
        return paramiko.common.OPEN_SUCCEEDED


class SFTPServerInterface(paramiko.SFTPServerInterface):
    def __init__(self, server, root):
        super().__init__(server)
        self.root = root

    def _resolve(self, path: str) -> Path:
        return self.root.joinpath(path)

    def open(self, path, flags, _attr):
        try:
            # We only ever do read-only operations on the remote, so at least for
            # the time being it is easiest to only support that. Otherwise we'd have
            # the translate flags to the mode string argument of `open`.
            if flags != os.O_RDONLY:
                return paramiko.sftp.SFTP_PERMISSION_DENIED

            handle = paramiko.SFTPHandle(flags)
            handle.readfile = self._resolve(path).open("rb")
            return handle
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        except Exception as e:
            print(e, file=sys.stderr)
            raise

    def stat(self, path):
        try:
            stat = self._resolve(path).stat()
            return paramiko.SFTPAttributes.from_stat(stat)
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        except Exception as e:
            print(e, file=sys.stderr)
            raise

    def list_folder(self, path):
        try:
            result = []
            path = self._resolve(path)
            for f in path.iterdir():
                attrs = paramiko.SFTPAttributes.from_stat(
                    f.stat(), str(f.relative_to(path))
                )
                result.append(attrs)
            return result
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        except Exception as e:
            print(e, file=sys.stderr)
            raise
