import functools
import re
import time
from dataclasses import dataclass
from typing import Optional, Union
from urllib.parse import urljoin

import requests
from dataclasses_json import DataClassJsonMixin, dataclass_json

from pyorderly.outpack.location_http import (
    OutpackHTTPClient,
    OutpackLocationHTTP,
)

# Surprisingly, we don't actually need the Client ID here to match the one
# used by Packit. It should be fine to hardcode a value regardless of which
# server we are talking to.
GITHUB_CLIENT_ID = "Ov23liUrbkR0qUtAO1zu"

GITHUB_DEVICE_CODE_URL = "https://github.com/login/device/code"
GITHUB_ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token"


@dataclass
class DeviceAuthorizationResponse(DataClassJsonMixin):
    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: Optional[int] = None


@dataclass
class AccessTokenResponse(DataClassJsonMixin):
    access_token: str
    token_type: str
    expires_in: Optional[int] = None


@dataclass_json
@dataclass
class ErrorResponse(DataClassJsonMixin):
    error: str
    error_description: Optional[str] = None


class OAuthDeviceClient:
    def __init__(
        self,
        client_id: str,
        device_code_url: str,
        access_token_url: str,
    ):
        self._client_id = client_id
        self._device_code_url = device_code_url
        self._access_token_url = access_token_url
        self._session = requests.Session()

    def __enter__(self):
        self._session.__enter__()
        return self

    def __exit__(self, *args):
        self._session.__exit__(*args)

    def authenticate(self, scope: str) -> AccessTokenResponse:
        parameters = self.start_device_authorization(scope)
        print(
            f"Visit {parameters.verification_uri} and enter the code <{parameters.user_code}>"
        )
        response = self.poll_access_token(parameters)
        return response

    def start_device_authorization(
        self, scope: str
    ) -> DeviceAuthorizationResponse:
        """
        Initiate the device authorization flow.

        This function returns a user code and verification URI which should be
        presented to the user. Additionally, it returns a device code which may
        be used to poll the access token endpoint until the authentication flow
        is complete.

        https://datatracker.ietf.org/doc/html/rfc8628#section-3.1
        https://datatracker.ietf.org/doc/html/rfc8628#section-3.2
        """
        r = self._session.post(
            self._device_code_url,
            data={"client_id": self._client_id, "scope": scope},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        return DeviceAuthorizationResponse.from_dict(r.json())

    def fetch_access_token(
        self, parameters: DeviceAuthorizationResponse
    ) -> Union[AccessTokenResponse, ErrorResponse]:
        """
        Fetch an access token from the authentication server.

        If authentication succeeds, an AccessTokenResponse is returned.
        Otherwise an ErrorResponse is returned. Depending on the error's code,
        the caller may call this function again (after a delay) to poll for new
        tokens.

        https://datatracker.ietf.org/doc/html/rfc8628#section-3.4
        https://datatracker.ietf.org/doc/html/rfc8628#section-3.5
        """
        r = self._session.post(
            self._access_token_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": parameters.device_code,
                "client_id": self._client_id,
            },
            headers={"Accept": "application/json"},
        )

        data = r.json()

        if "error" in data:
            return ErrorResponse.from_dict(r.json())
        else:
            r.raise_for_status()
            return AccessTokenResponse.from_dict(r.json())

    def poll_access_token(
        self,
        parameters: DeviceAuthorizationResponse,
    ) -> AccessTokenResponse:
        interval = parameters.interval
        if parameters.interval is None:  # pragma: no cover
            interval = 5
        else:
            interval = parameters.interval

        while True:
            response = self.fetch_access_token(parameters)
            if isinstance(response, AccessTokenResponse):
                return response
            elif response.error == "authorization_pending":
                pass
            elif response.error == "slow_down":  # pragma: no cover
                interval += 5
            else:
                if response.error_description is not None:
                    msg = f"Error while fetching access token: {response.error_description} ({response.error})"
                else:
                    msg = f"Error while fetching access token: {response.error}"
                raise Exception(msg)

            time.sleep(interval)


# The `functools.cache` decorator provides a very crude in-memory cache.
#
# If no token is provided we don't want to make the user have to authenticate
# interactively repeatedly; Even if a GitHub PAT is provided, trading it for a
# Packit token is slow and we wouldn't want to do it needlessly.
#
# This cache could be improved:
# - It should cache the tokens on disk (if the user requests it)
# - It should check for expiry of tokens and/or check for authentication errors
#   and purge offending tokens from it.
@functools.cache
def packit_authorisation(url: str, token: Optional[str]) -> dict[str, str]:
    # If a non-Github token is provided, we assume it is a native Packit token
    # and use that directly.
    if token is not None and not re.match("^gh._", token):
        return {"Authorization": f"Bearer {token}"}

    print(f"Logging in to {url}")
    if token is None:
        with OAuthDeviceClient(
            GITHUB_CLIENT_ID,
            GITHUB_DEVICE_CODE_URL,
            GITHUB_ACCESS_TOKEN_URL,
        ) as client:
            token = client.authenticate("read:org").access_token

    client = OutpackHTTPClient(url)
    response = client.post("packit/api/auth/login/api", json={"token": token})

    print("Logged in successfully")
    return {"Authorization": f"Bearer {response.json()['token']}"}


def outpack_location_packit(
    url: str, token: Optional[str] = None
) -> OutpackLocationHTTP:
    if not url.endswith("/"):
        url += "/"

    return OutpackLocationHTTP(
        urljoin(url, "packit/api/outpack/"),
        lambda: packit_authorisation(url, token),
    )
