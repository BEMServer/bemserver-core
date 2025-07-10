"""Test process"""

from unittest import mock

import pytest

from bemserver_core.exceptions import BEMServerCoreProcessTimeoutError
from bemserver_core.process import process


class DummyException(Exception):
    """Dummy exception"""


@process
def power(a, b):
    return a**b


@process
def exception():
    raise DummyException("Dummy")


@mock.patch("bemserver_core.process.TIMEOUT", 0.1)
class TestProcess:
    def test_process_ok(self):
        assert power(42, 42) == 42**42

    def test_process_timeout(self):
        with pytest.raises(BEMServerCoreProcessTimeoutError):
            power(42, 42**42)

    def test_process_exception(self):
        with pytest.raises(DummyException, match="Dummy"):
            exception()
