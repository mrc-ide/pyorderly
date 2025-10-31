import re

import pytest
import responses
from responses import matchers

from pyorderly.outpack.oauth import OAuthDeviceClient

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
