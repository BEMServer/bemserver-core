"""Scheduled tasks"""

from pathlib import Path

from .check_missing import ST_CheckMissingByCampaign
from .check_outliers import ST_CheckOutliersByCampaign
from .cleanup import ST_CleanupByCampaign, ST_CleanupByTimeseries
from .download_weather_data import (
    ST_DownloadWeatherDataBySite,
    ST_DownloadWeatherForecastDataBySite,
)

__all__ = [
    "ST_CleanupByCampaign",
    "ST_CleanupByTimeseries",
    "ST_CheckMissingByCampaign",
    "ST_CheckOutliersByCampaign",
    "ST_DownloadWeatherDataBySite",
    "ST_DownloadWeatherForecastDataBySite",
]


AUTH_MODEL_CLASSES = [
    ST_CleanupByCampaign,
    ST_CleanupByTimeseries,
    ST_CheckMissingByCampaign,
    ST_CheckOutliersByCampaign,
    ST_DownloadWeatherDataBySite,
    ST_DownloadWeatherForecastDataBySite,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
