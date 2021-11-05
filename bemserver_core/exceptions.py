"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class TimeseriesCSVIOError(BEMServerCoreError):
    """Timeseries CSV IO error"""


class BEMServerCoreUpdateError(BEMServerCoreError):
    """Update error"""


class BEMServerAuthorizationError(BEMServerCoreError):
    """Operation not autorized to current user"""
