import functools
import re
from urllib.parse import urljoin

from pyorderly.outpack.location_http import (
    OutpackLocationHTTP,
)
from pyorderly.outpack.oauth import OAuthDeviceClient


# The `functools.cache` decorator provides a very crude in-memory cache.
#
# If no token is provided we don't want to make the user have to authenticate
# interactively repeatedly.
#
# This cache could be improved:
# - It should cache the tokens on disk (if the user requests it)
# - It should check for expiry of tokens and/or check for authentication errors
#   and purge offending tokens from it.
@functools.cache
def packit_authorisation(url: str, token: str | None) -> dict[str, str]:
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

    print(f"Logging in to {url}")
    with OAuthDeviceClient(
        client_id="pyorderly",
        device_code_url=urljoin(url, "api/deviceAuth"),
        access_token_url=urljoin(url, "api/deviceAuth/token"),
    ) as client:
        token = client.authenticate().access_token

    print("Logged in successfully")
    return {"Authorization": f"Bearer {token}"}


def outpack_location_packit(
    url: str, token: str | None = None
) -> OutpackLocationHTTP:
    if not url.endswith("/"):
        url += "/"

    return OutpackLocationHTTP(
        urljoin(url, "api/outpack/"),
        lambda: packit_authorisation(url, token),
    )
