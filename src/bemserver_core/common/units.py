from pathlib import Path

import pint

from bemserver_core.exceptions import (
    BEMServerCoreDimensionalityError,
    BEMServerCoreUndefinedUnitError,
)

# This file adds units to Pint default units
BEMSERVER_UNITS_FILE = Path(__file__).parent / "units.txt"


class BEMServerUnitRegistry:
    def __init__(self):
        self._ureg = pint.UnitRegistry()

    def __iter__(self):
        return iter(self._ureg)

    def load_definitions(self, file_path):
        self._ureg.load_definitions(file_path)

    def validate_unit(self, unit_str):
        try:
            return self._ureg.Unit(unit_str)
        except (
            pint.errors.UndefinedUnitError,
            pint.errors.DefinitionSyntaxError,
        ) as exc:
            raise BEMServerCoreUndefinedUnitError(str(exc)) from exc

    def convert(self, data, src_unit, dest_unit):
        """Convert data from a unit to another

        :param Number|list data: Data to convert
        :param string src_unit: Source unit
        :param string dest_unit: Destination unit
        """
        try:
            return self._ureg.Quantity(data, src_unit).m_as(dest_unit)
        except pint.errors.UndefinedUnitError as exc:
            raise BEMServerCoreUndefinedUnitError(str(exc)) from exc
        except pint.errors.DimensionalityError as exc:
            raise BEMServerCoreDimensionalityError(str(exc)) from exc

    def convert_df(self, data_df, src_units, dest_units):
        """Convert data in a dataframe

        :param DataFrame data: DataFrame to convert
        :param dict src_units: Mapping of column name -> source unit
        :param dict dest_units: Mapping of column name -> destination unit

        Only columns in desc_units are converted. src_units is supposed to contain
        all keys in dest_units.
        """
        for col in data_df.columns:
            if col in dest_units:
                data_df[col] = self.convert(
                    data_df[col].values, src_units[col], dest_units[col]
                )


ureg = BEMServerUnitRegistry()
ureg.load_definitions(BEMSERVER_UNITS_FILE)
