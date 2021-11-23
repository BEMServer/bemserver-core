"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class TimeseriesCSVIOError(BEMServerCoreError):
    """Timeseries CSV IO error"""


class BEMServerAuthorizationError(BEMServerCoreError):
    """Operation not autorized to current user"""


class BEMServerUnknownCampaignError(BEMServerCoreError):
    """Campaign does not exist"""
