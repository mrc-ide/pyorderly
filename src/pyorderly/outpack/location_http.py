import shutil
from typing import Dict, List, override
from urllib.parse import urljoin

import requests

from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation


def raise_http_error(response: requests.Response):
    if response.headers.get("Content-Type") == "application/json":
        result = response.json()
        # Unfortunately the schema is a bit inconsistent. Packit uses a
        # singular `error` whereas outpack_server uses a list of
        # `errors`.
        if "error" in result:
            detail = result["error"]["detail"]
        else:
            detail = result["errors"][0]["detail"]

        msg = f"{response.status_code} Error: {detail}"
        raise requests.HTTPError(msg)
    else:
        response.raise_for_status()


class OutpackHTTPClient(requests.Session):
    def __init__(self, url: str, authentication=None):
        super().__init__()
        self._base_url = url
        self._authentication = authentication

    @override
    def request(self, method, path, *args, **kwargs):
        if self._authentication is not None:
            headers = kwargs.setdefault("headers", {})
            headers.update(self._authentication())

        url = urljoin(self._base_url, path)
        response = super().request(method, url, *args, **kwargs)
        if not response.ok:
            raise_http_error(response)
        return response


class OutpackLocationHTTP(LocationDriver):
    def __init__(self, url: str, authentication=None):
        self._base_url = url
        self._client = OutpackHTTPClient(url, authentication)

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(self, *args):
        self._client.__exit__(*args)

    @override
    def list(self) -> Dict[str, PacketLocation]:
        response = self._client.get("metadata/list").json()
        data = response["data"]
        return {
            entry["packet"]: PacketLocation.from_dict(entry) for entry in data
        }

    @override
    def metadata(self, ids: List[str]) -> Dict[str, str]:
        result = {}
        for i in ids:
            result[i] = self._client.get(f"metadata/{i}/text").text

        return result

    @override
    def fetch_file(self, packet: MetadataCore, file: PacketFile, dest: str):
        response = self._client.get(f"file/{file.hash}", stream=True)
        with open(dest, "wb") as f:
            shutil.copyfileobj(response.raw, f)
