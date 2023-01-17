"""Scheduled tasks"""
from pathlib import Path

from .cleanup import ST_CleanupByCampaign, ST_CleanupByTimeseries
from .check_missing import ST_CheckMissingByCampaign
from .check_outliers import ST_CheckOutliersByCampaign


__all__ = [
    "ST_CleanupByCampaign",
    "ST_CleanupByTimeseries",
    "ST_CheckMissingByCampaign",
    "ST_CheckOutliersByCampaign",
]


AUTH_MODEL_CLASSES = [
    ST_CleanupByCampaign,
    ST_CleanupByTimeseries,
    ST_CheckMissingByCampaign,
    ST_CheckOutliersByCampaign,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
