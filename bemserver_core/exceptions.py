"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class SitesCSVIOError(BEMServerCoreError):
    """Sites CSV IO error"""


class TimeseriesDataCSVIOError(BEMServerCoreError):
    """Timeseries data CSV IO error"""


class BEMServerAuthorizationError(BEMServerCoreError):
    """Operation not autorized to current user"""
