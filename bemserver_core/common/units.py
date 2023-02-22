from pathlib import Path

import pint

from bemserver_core.exceptions import BEMServerCoreUndefinedUnitError


# This file adds units to Pint default units
BEMSERVER_UNITS_FILE = Path(__file__).parent / "units.txt"


class BEMServerUnitRegistry:
    def __init__(self):
        self._ureg = pint.UnitRegistry()

    def __iter__(self):
        return iter(self._ureg)

    def load_definitions(self, file_path):
        self._ureg.load_definitions(file_path)

    def get_name(self, unit_str):
        try:
            return self._ureg.get_name(unit_str)
        except pint.errors.UndefinedUnitError as exc:
            raise BEMServerCoreUndefinedUnitError(str(exc)) from exc

    def get_symbol(self, unit_str):
        try:
            return self._ureg.get_symbol(unit_str)
        except pint.errors.UndefinedUnitError as exc:
            raise BEMServerCoreUndefinedUnitError(str(exc)) from exc


ureg = BEMServerUnitRegistry()
ureg.load_definitions(BEMSERVER_UNITS_FILE)
