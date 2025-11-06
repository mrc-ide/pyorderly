import functools
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import urljoin

from keyring.backends.null import Keyring as NullKeyring

from pyorderly.outpack.location_http import (
    OutpackLocationHTTP,
)
from pyorderly.outpack.oauth import Credentials, OAuthDeviceClient, TokenCache


def cached_authentication(url: str, cache: TokenCache) -> Credentials:
    credentials = cache.get(url)
    if credentials is not None:
        return credentials

    print(f"Logging in to {url}")
    with OAuthDeviceClient(
        client_id="pyorderly",
        device_code_url=urljoin(url, "packit/api/deviceAuth"),
        access_token_url=urljoin(url, "packit/api/deviceAuth/token"),
    ) as client:
        result = client.authenticate()

    if result.expires_in is not None:
        expires_at = datetime.now(UTC) + timedelta(seconds=result.expires_in)
    else:
        expires_at = None

    credentials = Credentials(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        expires_at=expires_at,
    )

    cache.save(url, credentials)

    print("Logged in successfully")
    return credentials


# The `functools.cache` decorator provides a very crude in-memory cache.
#
# We also cache credentials in the keyring, but we want to avoid hitting the
# keyring on every single request.
#
# This cache could be improved. It should check for expiry of tokens and/or
# check for authentication errors and purge offending tokens from it.
@functools.cache
def packit_authorisation(
    url: str, *, token: str | None, save_token: bool
) -> dict[str, str]:
    # If a non-Github token is provided, we assume it is a native Packit token
    # and use that directly.
    if token is not None:
        if re.match("^gh._", token):
            msg = (
                "Using a GitHub token to login to Packit isn't supported anymore. "
                "Either use a Packit token or omit the token to use interactive authentication."
            )
            raise Exception(msg)
        else:
            return {"Authorization": f"Bearer {token}"}
    else:
        backend = None if save_token else NullKeyring()
        cache = TokenCache(name="pyorderly", backend=backend)
        credentials = cached_authentication(url, cache)

        return {"Authorization": f"Bearer {credentials.access_token}"}


def outpack_location_packit(
    url: str,
    *,
    token: str | None = None,
    save_token: bool = True,
) -> OutpackLocationHTTP:
    if not url.endswith("/"):
        url += "/"

    return OutpackLocationHTTP(
        urljoin(url, "packit/api/outpack/"),
        lambda: packit_authorisation(url, token=token, save_token=save_token),
    )
