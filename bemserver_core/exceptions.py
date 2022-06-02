"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class SitesCSVIOError(BEMServerCoreError):
    """Sites CSV IO error"""


class BEMServerCoreIOError(BEMServerCoreError):
    """Base IO error"""


class TimeseriesCSVIOError(BEMServerCoreIOError):
    """Timeseries CSV IO error"""


class TimeseriesDataIOError(BEMServerCoreIOError):
    """Timeseries data IO error"""


class TimeseriesDataIOUnknownDataStateError(TimeseriesDataIOError):
    """Timeseries data IO unknown data state error"""


class TimeseriesDataIOUnknownTimeseriesError(TimeseriesDataIOError):
    """Timeseries data IO unknown timeseries error"""


class TimeseriesDataIOWriteError(TimeseriesDataIOError):
    """Timeseries data IO write error"""


class TimeseriesDataIOInvalidAggregationError(TimeseriesDataIOError):
    """Timeseries data IO invalid aggregation error"""


class TimeseriesDataCSVIOError(TimeseriesDataIOError):
    """Timeseries data CSV IO error"""


class BEMServerAuthorizationError(BEMServerCoreIOError):
    """Operation not autorized to current user"""
