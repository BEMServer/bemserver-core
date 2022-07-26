"""Base I/O tests"""
import pytest

from bemserver_core.input_output.base import BaseIO
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerCoreIOError


DUMMY_VALUE = "Dummy value"


class TestBaseIO:
    def test_base_io_gets_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            unit = BaseIO._get_unit_by_symbol("°C")
            assert unit.name == "degree Celsius"
            assert unit.symbol == "°C"

            with pytest.raises(
                BEMServerCoreIOError,
                match=f'Unknown unit: "{DUMMY_VALUE}"',
            ):
                BaseIO._get_unit_by_symbol(DUMMY_VALUE)
