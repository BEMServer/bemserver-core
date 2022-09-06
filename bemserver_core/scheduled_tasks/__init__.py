"""Scheduled tasks"""
from pathlib import Path

from .cleanup import ST_CleanupByCampaign, ST_CleanupByTimeseries


__all__ = [
    "ST_CleanupByCampaign",
    "ST_CleanupByTimeseries",
]


AUTH_MODEL_CLASSES = [
    ST_CleanupByCampaign,
    ST_CleanupByTimeseries,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
