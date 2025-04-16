"""Scheduled tasks"""

from pathlib import Path

from .tasks import TaskByCampaign

__all__ = [
    "TaskByCampaign",
]


AUTH_MODEL_CLASSES = [
    TaskByCampaign,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
