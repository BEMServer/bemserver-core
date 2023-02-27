"""Units tests"""
from unittest import mock

import pandas as pd

import pytest

from bemserver_core import BEMServerCore
from bemserver_core.common import ureg
from bemserver_core.exceptions import (
    BEMServerCoreUndefinedUnitError,
    BEMServerCoreDimensionalityError,
)


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

    def test_ureg_validate_unit(self):
        assert ureg.validate_unit("")
        assert ureg.validate_unit("m")
        assert ureg.validate_unit("meter")
        assert ureg.validate_unit("kWh")
        assert ureg.validate_unit("m/s")
        assert ureg.validate_unit("m3/m3")
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.validate_unit("wh")
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.validate_unit("dummy")

    def test_ureg_loads_units_file_on_startup(self):
        assert "heating_celsius_degree_hour" in ureg

    def test_ureg_convert(self):
        assert ureg.convert(1.0, "km", "m") == 1000.0
        assert ureg.convert(1.0, "m/s", "km/h") == pytest.approx(3.6)
        # convert returns a np.array, cast to list to compare
        assert list(ureg.convert([1.0, 2.0, 3.0], "km", "m")) == [
            1000.0,
            2000.0,
            3000.0,
        ]

    def test_ureg_convert_undefined_unit(self):
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.convert(1.0, "dummy", "m")
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.convert(1.0, "m", "dummy")

    def test_ureg_convert_dimensionality_error(self):
        with pytest.raises(BEMServerCoreDimensionalityError):
            ureg.convert(1.0, "m", "kW")

    def test_ureg_convert_df(self):
        data_df = pd.DataFrame({"id": [0, 1, 2]})
        ureg.convert_df(data_df, {"id": "km"}, {"id": "m"})
        assert data_df.equals(1000.0 * pd.DataFrame({"id": [0, 1, 2]}))

    def test_ureg_convert_df_undefined_unit(self):
        data_df = pd.DataFrame({"id": [0, 1, 2]})
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.convert_df(data_df, {"id": "dummy"}, {"id": "m"})
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            ureg.convert_df(data_df, {"id": "m"}, {"id": "dummy"})

    def test_ureg_convert_df_dimensionality_error(self):
        data_df = pd.DataFrame({"id": [0, 1, 2]})
        with pytest.raises(BEMServerCoreDimensionalityError):
            ureg.convert_df(data_df, {"id": "m"}, {"id": "kW"})


def test_bemserver_core_load_units_definitions_file():
    bsc = BEMServerCore()

    with mock.patch("bemserver_core.common.units.ureg.load_definitions") as load_mock:
        bsc.load_units_definitions_file("dummy_path")

    load_mock.assert_called_once()
    load_mock.assert_called_with("dummy_path")
