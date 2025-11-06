from types import SimpleNamespace

import keyring
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
from ..helpers.keyring import MemoryKeyring


# This fixture automatically gets invoked by all tests in the file.
#
# It configures an in-memory keyring just for the duration of the test and it
# clears the functools cache to avoid carrying tokens over.
@pytest.fixture(autouse=True)
def clear_authentication_cache():
    previous_keyring = keyring.get_keyring()
    try:
        keyring.set_keyring(MemoryKeyring())
        yield
    finally:
        keyring.set_keyring(previous_keyring)
        packit_authorisation.cache_clear()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_pass_packit_token():
    responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        json={"status": "success", "data": []},
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
    )

    location = outpack_location_packit("https://example.com", token="mytoken")
    location.list_packets()


def register_mock_responses(token):
    device_code = responses.post(
        "https://example.com/packit/api/deviceAuth",
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://example.com/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )
    access_token = responses.post(
        "https://example.com/packit/api/deviceAuth/token",
        json={"access_token": token, "token_type": "bearer"},
    )
    list_packets = responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": f"Bearer {token}"})],
        json={"status": "success", "data": []},
    )

    return SimpleNamespace(
        device_code=device_code,
        access_token=access_token,
        list_packets=list_packets,
    )


@responses.activate(assert_all_requests_are_fired=True)
def test_can_perform_interactive_authentication(capsys):
    register_mock_responses(token="mytoken")

    location = outpack_location_packit("https://example.com")
    location.list_packets()

    captured = capsys.readouterr()
    assert "enter the code <1234-5678>" in captured.out


@responses.activate(assert_all_requests_are_fired=True)
@pytest.mark.parametrize("save_token", [True, False])
@pytest.mark.parametrize("clear_memory", [True, False])
def test_authentication_is_cached(clear_memory, save_token):
    mocks = register_mock_responses("mytoken")

    location = outpack_location_packit(
        "https://example.com", save_token=save_token
    )
    location.list_packets()

    assert mocks.device_code.call_count == 1
    assert mocks.access_token.call_count == 1
    assert mocks.list_packets.call_count == 1

    # We have an in-memory cache that is active even when save_token is False.
    # Clearing it is roughly the equivalent of restarting the Python session.
    if clear_memory:
        packit_authorisation.cache_clear()

    location.list_packets()

    if clear_memory and not save_token:
        assert mocks.device_code.call_count == 2
        assert mocks.access_token.call_count == 2
    else:
        assert mocks.device_code.call_count == 1
        assert mocks.access_token.call_count == 1

    assert mocks.list_packets.call_count == 2


def test_github_personal_token_is_rejected():
    location = outpack_location_packit("https://example.com", token="ghp_token")

    msg = "Using a GitHub token to login to Packit isn't supported anymore\\."
    with pytest.raises(Exception, match=msg):
        location.list_packets()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_add_packit_location(tmp_path):
    register_mock_responses(token="mytoken")

    root = create_temporary_root(tmp_path)
    outpack_location_add(
        "upstream",
        "packit",
        {"url": "https://example.com"},
        root,
    )
    outpack_location_pull_metadata("upstream", root=root)
