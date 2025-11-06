from keyring.backend import KeyringBackend


class MemoryKeyring(KeyringBackend):
    credentials: dict[tuple[str, str], str]

    def __init__(self):
        self.credentials = {}

    def set_password(self, service: str, username: str, password: str) -> None:
        self.credentials[service, username] = password

    def get_password(self, service: str, username: str) -> str | None:
        return self.credentials.get((service, username))
