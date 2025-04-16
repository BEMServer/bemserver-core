"""Scheduled tasks"""

from pathlib import Path

from . import (
    check_missing,  # noqa
    check_outliers,  # noqa
    cleanup,  # noqa
    download_weather_data,  # noqa
)
from .tasks import BEMServerCoreAsyncTask, BEMServerCoreScheduledTask, TaskByCampaign

__all__ = [
    "BEMServerCoreAsyncTask",
    "BEMServerCoreScheduledTask",
    "TaskByCampaign",
]


AUTH_MODEL_CLASSES = [
    TaskByCampaign,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
