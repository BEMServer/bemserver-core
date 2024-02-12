"""Property types"""

import enum

from bemserver_core.exceptions import PropertyTypeInvalidError


class PropertyType(enum.Enum):
    integer = int
    float = float
    boolean = bool
    string = str

    def verify(self, val_in):
        """Check that the "val_in" type matches the expected property type.

        :param `str` val_in: The value to check, as a string.
        :raise `PropertyTypeInvalidError`:
            If `val_in` is not "true" or "false" for a boolean type.
            If `val_in` contains a float for an integer type.
        """
        val_in = str(val_in)  # ensure "val_in" is a string
        if self is self.boolean:
            if val_in not in ["true", "false"]:
                raise PropertyTypeInvalidError
        else:
            try:
                self.value(val_in)
            except ValueError as exc:
                raise PropertyTypeInvalidError from exc
