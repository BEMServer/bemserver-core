"""Common"""

import enum

from .property_type import PropertyType  # noqa
from .units import ureg  # noqa


class AggregationFunctionsEnum(enum.StrEnum):
    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
