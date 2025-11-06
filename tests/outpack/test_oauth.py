import json
import re
from datetime import UTC, datetime, timedelta

import pytest
import responses
from responses import matchers

from pyorderly.outpack.oauth import Credentials, OAuthDeviceClient, TokenCache

from ..helpers.keyring import MemoryKeyring

TEST_CLIENT_ID = "client-id"
TEST_DEVICE_CODE_URL = "https://example.com/device/code"
TEST_ACCESS_TOKEN_URL = "https://example.com/access_token"


def create_client():
    return OAuthDeviceClient(
        TEST_CLIENT_ID,
        TEST_DEVICE_CODE_URL,
        TEST_ACCESS_TOKEN_URL,
    )


@responses.activate(assert_all_requests_are_fired=True)
def test_successful_authentication(capsys):
    responses.post(
        TEST_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://example.com/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )

    m = matchers.urlencoded_params_matcher(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": "xxxxx",
            "client_id": TEST_CLIENT_ID,
        }
    )
    responses.post(
        TEST_ACCESS_TOKEN_URL,
        json={"error": "authorization_pending"},
        match=[m],
    )

    responses.post(
        TEST_ACCESS_TOKEN_URL,
        json={"access_token": "mytoken", "token_type": "bearer"},
        match=[m],
    )

    with create_client() as client:
        result = client.authenticate()
        assert result.access_token == "mytoken"

    captured = capsys.readouterr()
    assert "enter the code <1234-5678>" in captured.out


@responses.activate(assert_all_requests_are_fired=True)
def test_failed_authentication():
    responses.post(
        TEST_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://example.com/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )

    responses.post(TEST_ACCESS_TOKEN_URL, json={"error": "access_denied"})

    with create_client() as client:
        msg = "Error while fetching access token: access_denied"
        with pytest.raises(Exception, match=msg):
            client.authenticate()


@responses.activate(assert_all_requests_are_fired=True)
def test_failed_authentication_with_description():
    responses.post(
        TEST_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://example.com/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )

    responses.post(
        TEST_ACCESS_TOKEN_URL,
        json={
            "error": "access_denied",
            "error_description": "Access was denied",
        },
    )

    with create_client() as client:
        msg = "Error while fetching access token: Access was denied (access_denied)"
        with pytest.raises(Exception, match=re.escape(msg)):
            client.authenticate()


@pytest.mark.parametrize(
    "expires_in,expected",
    [
        (None, False),
        (timedelta(seconds=-60), True),
        (timedelta(seconds=60), False),
    ],
)
def test_credentials_is_expired(expires_in, expected):
    if expires_in is None:
        expires_at = None
    else:
        expires_at = datetime.now(UTC) + expires_in

    credentials = Credentials(access_token="", expires_at=expires_at)
    assert credentials.is_expired() == expected


def test_credentials_cache():
    cache = TokenCache(name="pyorderly", backend=MemoryKeyring())

    assert cache.get("https://example.com") is None

    credentials = Credentials(access_token="foobar")
    cache.save("https://example.com", credentials)

    assert cache.get("https://example.com") == credentials
    assert cache.get("https://other.com") is None


def test_expired_credentials_is_ignored(frozen_time):
    url = "https://example.com"
    cache = TokenCache(name="pyorderly", backend=MemoryKeyring())
    expires_at = datetime.now(UTC) + timedelta(seconds=30)

    credentials = Credentials(access_token="foobar", expires_at=expires_at)
    cache.save(url, credentials)

    assert cache.get(url) == credentials

    frozen_time.tick(timedelta(seconds=15))

    assert cache.get(url) == credentials

    frozen_time.tick(timedelta(seconds=60))

    assert cache.get(url) is None


def test_invalid_credentials_are_ignored():
    url = "https://example.com"
    backend = MemoryKeyring()
    cache = TokenCache(name="pyorderly", backend=backend)

    # Check that writing to the backend directly does what we expect
    credentials = {"access_token": "foo"}
    backend.set_password("pyorderly", url, json.dumps(credentials))
    assert cache.get("https://example.com").access_token == "foo"

    backend.set_password("pyorderly", url, "notvalidjson")
    assert cache.get("https://example.com") is None

    backend.set_password("pyorderly", url, json.dumps({}))
    assert cache.get("https://example.com") is None

    backend.set_password(
        "pyorderly",
        url,
        json.dumps({"access_token": "xx", "expires_at": "sss"}),
    )
    assert cache.get("https://example.com") is None
