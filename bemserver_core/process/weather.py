"""Weather forecast and historical data"""

import requests
from requests.exceptions import RequestException

from bemserver_core.exceptions import (
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
)

OIKOLAB_WEATHER_URL = "http://api.oikolab.com/weather"

OIKOLAB_WEATHER_PARAMETERS_MAPPING = {
    "AIR_TEMPERATURE": "temperature",
    "DEWPOINT_TEMPERATURE": "dewpoint_temperature",
    "WETBULB_TEMPERATURE": "wetbulb_temperature",
    "WIND_SPEED": "wind_speed",
    "WIND_DIRECTION": "wind_direction",
    "SURFACE_DIRECT_SOLAR_RADIATION": "surface_direct_solar_radiation",
    "SURFACE_DIFFUSE_SOLAR_RADIATION": "surface_diffuse_solar_radiation",
    "SURFACE_SOLAR_RADIATION": "surface_solar_radiation",
    "DIRECT_NORMAL_SOLAR_RADIATION": "direct_normal_solar_radiation",
    "RELATIVE_HUMIDITY": "relative_humidity",
    "SURFACE_PRESSURE": "surface_pressure",
    "TOTAL_PRECIPITATION": "total_precipitation",
}


class OikolabWeatherDataClient:
    def __init__(self, api_key):
        self._api_key = api_key

    def get_weather_data(self, params, latitude, longitude, dt_start, dt_end):
        try:
            resp = requests.get(
                url=OIKOLAB_WEATHER_URL,
                params={
                    "params": ", ".join(
                        [OIKOLAB_WEATHER_PARAMETERS_MAPPING[p] for p in params]
                    ),
                    "lat": latitude,
                    "lon": longitude,
                    "start_time": dt_start,
                    "end_time": dt_end,
                    "api_key": self._api_key,
                },
            )
        except RequestException as exc:
            raise BEMServerCoreWeatherAPIConnectionError(
                f"Error while connecting to the weather API: {exc}"
            ) from exc

        if resp.status_code != 200:
            raise BEMServerCoreWeatherAPIQueryError(
                f"Error while querying the weather API: {resp.text}"
            )

        return resp.json()
