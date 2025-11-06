import freezegun
import pytest


@pytest.fixture
def frozen_time():
    with freezegun.freeze_time() as ft:
        yield ft
