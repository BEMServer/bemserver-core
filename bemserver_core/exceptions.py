"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class TimeseriesCSVIOError(BEMServerCoreError):
    """Timeseries CSV IO error"""


class BEMServerAuthorizationError(BEMServerCoreError):
    """Operation not autorized to current user"""


class BEMServerCoreMissingCampaignError(BEMServerCoreError):
    """Operation requires a Campaign context"""
