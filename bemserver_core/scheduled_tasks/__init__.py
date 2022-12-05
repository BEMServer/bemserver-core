"""Scheduled tasks"""
from pathlib import Path

from .cleanup import ST_CleanupByCampaign, ST_CleanupByTimeseries
from .check_missing import ST_CheckMissingByCampaign


__all__ = [
    "ST_CleanupByCampaign",
    "ST_CleanupByTimeseries",
    "ST_CheckMissingByCampaign",
]


AUTH_MODEL_CLASSES = [
    ST_CleanupByCampaign,
    ST_CleanupByTimeseries,
    ST_CheckMissingByCampaign,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
