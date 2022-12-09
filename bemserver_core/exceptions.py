"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class BEMServerCorePeriodError(BEMServerCoreError):
    """Wrong time period unit or multiplier"""


class TimeseriesNotFoundError(BEMServerCoreError):
    """Timeseries not found"""


class BEMServerCoreCampaignError(BEMServerCoreError):
    """Campaign error"""


class BEMServerCoreCampaignScopeError(BEMServerCoreError):
    """Campaign scope error"""


class BEMServerCoreIOError(BEMServerCoreError):
    """Base IO error"""


class BEMServerCoreCSVIOError(BEMServerCoreIOError):
    """CSV IO error"""


class BEMServerCoreJSONIOError(BEMServerCoreIOError):
    """JSON IO error"""


class SitesCSVIOError(BEMServerCoreCSVIOError):
    """Sites CSV IO error"""


class TimeseriesCSVIOError(BEMServerCoreCSVIOError):
    """Timeseries CSV IO error"""


class TimeseriesDataIOError(BEMServerCoreIOError):
    """Timeseries data IO error"""


class TimeseriesDataIODatetimeError(BEMServerCoreIOError):
    """Timeseries data IO datetime error"""


class TimeseriesDataIOInvalidTimeseriesIDTypeError(TimeseriesDataIOError):
    """Timeseries data IO invalid timeseries ID type error"""


class TimeseriesDataIOInvalidBucketWidthError(TimeseriesDataIOError):
    """Timeseries data IO invalid bucket width error"""


class TimeseriesDataIOInvalidAggregationError(TimeseriesDataIOError):
    """Timeseries data IO invalid aggregation error"""


class TimeseriesDataCSVIOError(BEMServerCoreCSVIOError, TimeseriesDataIOError):
    """Timeseries data CSV IO error"""


class TimeseriesDataJSONIOError(BEMServerCoreJSONIOError, TimeseriesDataIOError):
    """Timeseries data JSON IO error"""


class BEMServerAuthorizationError(BEMServerCoreIOError):
    """Operation not autorized to current user"""


class PropertyTypeInvalidError(BEMServerCoreError):
    """Invalid property value type: cast error"""
