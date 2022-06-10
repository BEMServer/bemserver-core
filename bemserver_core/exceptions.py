"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class TimeseriesNotFoundError(BEMServerCoreError):
    """Timeseries not found"""


class BEMServerCoreIOError(BEMServerCoreError):
    """Base IO error"""


class BEMServerCoreCSVIOError(BEMServerCoreIOError):
    """CSV IO error"""


class SitesCSVIOError(BEMServerCoreCSVIOError):
    """Sites CSV IO error"""


class TimeseriesCSVIOError(BEMServerCoreCSVIOError):
    """Timeseries CSV IO error"""


class TimeseriesDataIOError(BEMServerCoreIOError):
    """Timeseries data IO error"""


class TimeseriesDataIOInvalidAggregationError(TimeseriesDataIOError):
    """Timeseries data IO invalid aggregation error"""


class TimeseriesDataCSVIOError(BEMServerCoreCSVIOError, TimeseriesDataIOError):
    """Timeseries data CSV IO error"""


class BEMServerAuthorizationError(BEMServerCoreIOError):
    """Operation not autorized to current user"""
