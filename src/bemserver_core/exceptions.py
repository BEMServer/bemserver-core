"""Exceptions"""


class BEMServerCoreError(Exception):
    """Base BEMServer Core exception"""


class BEMServerCoreSettingsError(BEMServerCoreError):
    """Base BEMServer settings error"""


class BEMServerCorePeriodError(BEMServerCoreError):
    """Wrong time period unit or multiplier"""


class TimeseriesNotFoundError(BEMServerCoreError):
    """Timeseries not found"""


class BEMServerCoreCampaignError(BEMServerCoreError):
    """Campaign error"""


class BEMServerCoreCampaignScopeError(BEMServerCoreError):
    """Campaign scope error"""


class BEMServerCoreIOError(BEMServerCoreError):
    """Base IO error"""


class BEMServerCoreCSVIOError(BEMServerCoreIOError):
    """CSV IO error"""


class BEMServerCoreJSONIOError(BEMServerCoreIOError):
    """JSON IO error"""


class SitesCSVIOError(BEMServerCoreCSVIOError):
    """Sites CSV IO error"""


class TimeseriesCSVIOError(BEMServerCoreCSVIOError):
    """Timeseries CSV IO error"""


class TimeseriesDataIOError(BEMServerCoreIOError):
    """Timeseries data IO error"""


class TimeseriesDataIODatetimeError(TimeseriesDataIOError):
    """Timeseries data IO datetime error"""


class TimeseriesDataIOInvalidTimeseriesIDTypeError(TimeseriesDataIOError):
    """Timeseries data IO invalid timeseries ID type error"""


class TimeseriesDataIOInvalidBucketWidthError(TimeseriesDataIOError):
    """Timeseries data IO invalid bucket width error"""


class TimeseriesDataIOInvalidAggregationError(TimeseriesDataIOError):
    """Timeseries data IO invalid aggregation error"""


class TimeseriesDataCSVIOError(BEMServerCoreCSVIOError, TimeseriesDataIOError):
    """Timeseries data CSV IO error"""


class TimeseriesDataJSONIOError(BEMServerCoreJSONIOError, TimeseriesDataIOError):
    """Timeseries data JSON IO error"""


class BEMServerCoreUnitError(BEMServerCoreError):
    """Unit error"""


class BEMServerCoreUndefinedUnitError(BEMServerCoreUnitError):
    """Undefined unit error"""


class BEMServerCoreDimensionalityError(BEMServerCoreUnitError):
    """Dimensionality error"""


class BEMServerAuthorizationError(BEMServerCoreIOError):
    """Operation not autorized to current user"""


class PropertyTypeInvalidError(BEMServerCoreError):
    """Invalid property value type: cast error"""


class BEMServerCoreTaskError(BEMServerCoreError):
    """Error in task execution"""


class BEMServerCoreWeatherAPIError(BEMServerCoreError):
    """Error in weather API call"""


class BEMServerCoreWeatherAPIQueryError(BEMServerCoreWeatherAPIError):
    """Error in weather API query"""


class BEMServerCoreWeatherAPIResponseError(BEMServerCoreWeatherAPIError):
    """Error in weather API response"""


class BEMServerCoreWeatherAPIConnectionError(BEMServerCoreWeatherAPIError):
    """Error in weather API connection"""


class BEMServerCoreWeatherAPIAuthenticationError(BEMServerCoreWeatherAPIError):
    """Error in weather API authentication"""


class BEMServerCoreEnergyBreakdownProcessError(BEMServerCoreError):
    """Error in energy breakdown computation process"""


class BEMServerCoreEnergyBreakdownProcessZeroDivisionError(
    BEMServerCoreEnergyBreakdownProcessError
):
    """Division by zero in energy breakdown computation process"""


class BEMServerCoreWeatherProcessError(BEMServerCoreError):
    """Error in weather process"""


class BEMServerCoreWeatherProcessMissingCoordinatesError(
    BEMServerCoreWeatherProcessError
):
    """Missing coordinates in weather process"""


class BEMServerCoreDegreeDaysProcessError(BEMServerCoreError):
    """Error in degree days process"""


class BEMServerCoreDegreeDayProcessMissingTemperatureError(
    BEMServerCoreDegreeDaysProcessError
):
    """Missing air temperature in degree days process"""


class BEMServerCoreEnergyPowerProcessError(BEMServerCoreError):
    """Error in energy power conversion process"""


class BEMServerCoreEnergyPowerProcessMissingIntervalError(BEMServerCoreError):
    """Missing timeseries interval property"""


class BEMServerCoreScheduledTaskParametersError(BEMServerCoreError):
    """Error in scheduled task parameter"""
