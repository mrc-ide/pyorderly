from types import SimpleNamespace

import pytest
import responses
from responses import matchers

from pyorderly.outpack.location import outpack_location_add
from pyorderly.outpack.location_packit import (
    outpack_location_packit,
    packit_authorisation,
)
from pyorderly.outpack.location_pull import outpack_location_pull_metadata

from ..helpers import create_temporary_root


# This fixture automatically gets invoked by all tests in the file, and will
# clear the authorisation cache to avoid carrying tokens over.
@pytest.fixture(autouse=True)
def clear_authentication_cache():
    try:
        yield
    finally:
        packit_authorisation.cache_clear()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_pass_packit_token():
    responses.get(
        "https://example.com/api/outpack/metadata/list",
        json={"status": "success", "data": []},
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
    )

    location = outpack_location_packit("https://example.com", token="mytoken")
    location.list_packets()


def register_oauth_responses(token):
    device_code = responses.post(
        "https://example.com/api/deviceAuth",
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://example.com/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )
    access_token = responses.post(
        "https://example.com/api/deviceAuth/token",
        json={"access_token": token, "token_type": "bearer"},
    )
    return SimpleNamespace(device_code=device_code, access_token=access_token)


@responses.activate(assert_all_requests_are_fired=True)
def test_can_perform_interactive_authentication(capsys):
    register_oauth_responses(token="mytoken")

    responses.get(
        "https://example.com/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
        json={"status": "success", "data": []},
    )

    location = outpack_location_packit("https://example.com")
    location.list_packets()

    captured = capsys.readouterr()
    assert "enter the code <1234-5678>" in captured.out


@responses.activate(assert_all_requests_are_fired=True)
def test_authentication_is_cached():
    mocks = register_oauth_responses("mytoken")

    list_response = responses.get(
        "https://example.com/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
        json={"status": "success", "data": []},
    )

    location = outpack_location_packit("https://example.com")
    location.list_packets()

    assert mocks.device_code.call_count == 1
    assert mocks.access_token.call_count == 1
    assert list_response.call_count == 1

    location.list_packets()

    assert mocks.device_code.call_count == 1
    assert mocks.access_token.call_count == 1
    assert list_response.call_count == 2


def test_github_personal_token_is_rejected():
    location = outpack_location_packit("https://example.com", token="ghp_token")

    msg = "Using a GitHub token to login to Packit isn't supported anymore\\."
    with pytest.raises(Exception, match=msg):
        location.list_packets()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_add_packit_location(tmp_path):
    responses.get(
        "https://example.com/api/outpack/metadata/list",
        json={"status": "success", "data": []},
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
    )

    root = create_temporary_root(tmp_path)
    outpack_location_add(
        "upstream",
        "packit",
        {"url": "https://example.com", "token": "mytoken"},
        root,
    )
    outpack_location_pull_metadata("upstream", root=root)
