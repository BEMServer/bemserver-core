"""PropertyType tests"""

import pytest

from bemserver_core.common import PropertyType
from bemserver_core.exceptions import PropertyTypeInvalidError


class TestPropertyType:
    def test_property_type_verify(self):
        for val in [42, -42]:
            PropertyType.integer.verify(val)
        for val in [4.2, "4.2", "bad", True, None]:
            with pytest.raises(PropertyTypeInvalidError):
                PropertyType.integer.verify(val)

        for val in [42, -42, 4.2, -4.2, "4.2", "-4.2"]:
            PropertyType.float.verify(val)
        for val in ["bad", True, None]:
            with pytest.raises(PropertyTypeInvalidError):
                PropertyType.float.verify(val)

        for val in ["true", "false"]:
            PropertyType.boolean.verify(val)
        for val in [
            True,
            "True",
            "TRUE",
            "t",
            "T",
            1,
            "1",
            False,
            "False",
            "FALSE",
            "f",
            "F",
            0,
            "0",
            "bad",
            None,
        ]:
            with pytest.raises(PropertyTypeInvalidError):
                PropertyType.boolean.verify(val)

        for val in [
            42,
            -42,
            4.2,
            -4.2,
            "42",
            "-42",
            "4.2",
            "-4.2",
            "whatever string",
            True,
            False,
            None,
        ]:
            PropertyType.string.verify(val)
