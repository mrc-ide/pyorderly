import time
from dataclasses import dataclass

import requests
from dataclasses_json import DataClassJsonMixin, dataclass_json


@dataclass
class DeviceAuthorizationResponse(DataClassJsonMixin):
    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int | None = None


@dataclass
class AccessTokenResponse(DataClassJsonMixin):
    access_token: str
    token_type: str
    expires_in: int | None = None


@dataclass_json
@dataclass
class ErrorResponse(DataClassJsonMixin):
    error: str
    error_description: str | None = None


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
        return DeviceAuthorizationResponse.from_dict(r.json())

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
            return ErrorResponse.from_dict(r.json())
        else:
            r.raise_for_status()
            return AccessTokenResponse.from_dict(r.json())

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
