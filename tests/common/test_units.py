"""Units tests"""
from unittest import mock

import pytest

from bemserver_core import BEMServerCore
from bemserver_core.common import ureg
from bemserver_core.exceptions import BEMServerCoreUndefinedUnitError


class TestUnits:
    def test_ureg_iter(self):
        assert "meter" in ureg

    def test_ureg_load_definitions(self):
        with mock.patch("pint.UnitRegistry.load_definitions") as load_mock:
            ureg.load_definitions("dummy_path")

        load_mock.assert_called_once()
        load_mock.assert_called_with("dummy_path")

    def test_ureg_get_name(self):
        assert ureg.get_name("meter") == "meter"
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.get_name("dummy")

    def test_reg_get_symbol(self):
        assert ureg.get_symbol("meter") == "m"
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.get_symbol("dummy")

    def test_ureg_loads_units_file_on_startup(self):
        assert "heating_celsius_degree_hour" in ureg


def test_bemserver_core_load_units_definitions_file():
    bsc = BEMServerCore()

    with mock.patch("bemserver_core.common.units.ureg.load_definitions") as load_mock:
        bsc.load_units_definitions_file("dummy_path")

    load_mock.assert_called_once()
    load_mock.assert_called_with("dummy_path")
