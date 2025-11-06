import time
from datetime import UTC, datetime, timedelta

import keyring
import pydantic
import requests
from keyring.backend import KeyringBackend
from pydantic import BaseModel


class DeviceAuthorizationResponse(BaseModel):
    """https://datatracker.ietf.org/doc/html/rfc8628#section-3.2."""

    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int | None = None


class AccessTokenResponse(BaseModel):
    """https://datatracker.ietf.org/doc/html/rfc6749#section-5.1."""

    access_token: str
    refresh_token: str | None = None
    expires_in: int | None = None


class ErrorResponse(BaseModel):
    """https://datatracker.ietf.org/doc/html/rfc6749#section-5.2."""

    error: str
    error_description: str | None = None


class Credentials(BaseModel):
    """
    Credentials obtained from an OAuth provider.

    This is roughly the same as an AccessTokenResponse, except that the
    expiry time is absolute rather than relative.
    """

    access_token: str
    refresh_token: str | None = None
    expires_at: datetime | None = None

    @staticmethod
    def from_response(response: AccessTokenResponse) -> "Credentials":
        if response.expires_in is not None:
            expires_in = timedelta(seconds=response.expires_in)
            expires_at = datetime.now(UTC) + expires_in
        else:
            expires_at = None

        return Credentials(
            access_token=response.access_token,
            refresh_token=response.refresh_token,
            expires_at=expires_at,
        )

    def is_expired(self) -> bool:
        now = datetime.now(UTC)
        return self.expires_at is not None and self.expires_at < now


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

    def __enter__(self) -> "OAuthDeviceClient":
        self._session.__enter__()
        return self

    def __exit__(self, *args):
        self._session.__exit__(*args)

    def authenticate(self) -> AccessTokenResponse:
        parameters = self.start_device_authorization()
        print(
            f"Visit {parameters.verification_uri} and enter the code <{parameters.user_code}>"
        )
        response = self.poll_access_token(parameters)
        return response

    def start_device_authorization(self) -> DeviceAuthorizationResponse:
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
            data={"client_id": self._client_id},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        return DeviceAuthorizationResponse.model_validate(r.json(), strict=True)

    def fetch_access_token(
        self, parameters: DeviceAuthorizationResponse
    ) -> AccessTokenResponse | ErrorResponse:
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
            return ErrorResponse.model_validate(data, strict=True)
        else:
            r.raise_for_status()
            return AccessTokenResponse.model_validate(data, strict=True)

    def poll_access_token(
        self,
        parameters: DeviceAuthorizationResponse,
    ) -> AccessTokenResponse:
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


class TokenCache:
    name: str
    backend: KeyringBackend

    def __init__(self, name: str, backend: KeyringBackend | None = None):
        self.name = name
        if backend is None:
            self.backend = keyring.get_keyring()
        else:
            self.backend = backend

    def get(self, url: str) -> Credentials | None:
        data = self.backend.get_password(self.name, url)
        if data is None:
            return None

        try:
            credentials = Credentials.model_validate_json(data, strict=True)
        except pydantic.ValidationError:
            return None

        if credentials.is_expired():
            return None

        return credentials

    def save(self, url: str, credentials: Credentials) -> None:
        self.backend.set_password(self.name, url, credentials.model_dump_json())
