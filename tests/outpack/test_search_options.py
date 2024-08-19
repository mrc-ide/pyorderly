import pytest

from pyorderly.outpack.search_options import SearchOptions


def test_defaults_are_reasonable():
    opts = SearchOptions()
    assert opts.location is None
    assert not opts.allow_remote
    assert not opts.pull_metadata


def test_can_set_options():
    opts = SearchOptions(
        location=["a", "b"], allow_remote=True, pull_metadata=True
    )
    assert opts.location == ["a", "b"]
    assert opts.allow_remote
    assert opts.pull_metadata


def test_can_convert_from_none():
    assert SearchOptions.create(None) == SearchOptions()


def test_can_convert_from_self():
    obj = SearchOptions(["a", "b"], True)
    assert SearchOptions.create(obj) == obj


def test_can_convert_from_dict():
    assert SearchOptions.create({"allow_remote": True}) == SearchOptions(
        allow_remote=True
    )
    assert SearchOptions.create(
        {"location": ["a"], "pull_metadata": True}
    ) == SearchOptions(["a"], False, True)


def test_can_reject_invalid_input():
    with pytest.raises(TypeError, match="Invalid object type"):
        SearchOptions.create([])
