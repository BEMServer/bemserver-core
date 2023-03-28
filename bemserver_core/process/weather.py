"""Weather forecast and historical data"""
import json

import pandas as pd
import requests
from requests.exceptions import RequestException

from bemserver_core.exceptions import (
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
    BEMServerCoreWeatherAPIResponseError,
)

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

OIKOLAB_WEATHER_PARAMETERS_REVERSE_MAPPING = {
    v: k for k, v in OIKOLAB_WEATHER_PARAMETERS_MAPPING.items()
}


class OikolabWeatherDataClient:
    def __init__(self, api_url, api_key):
        self._api_url = api_url
        self._api_key = api_key

    def get_weather_data(self, params, latitude, longitude, start_dt, end_dt):
        # Translate BSC names into Oikolab names
        oik_params = [OIKOLAB_WEATHER_PARAMETERS_MAPPING[p] for p in params]

        try:
            resp = requests.get(
                url=self._api_url,
                params={
                    "param": oik_params,
                    "lat": latitude,
                    "lon": longitude,
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "api-key": self._api_key,
                },
                # Set a long timeout as Oikolab responses may take a while
                timeout=60,
            )
        except RequestException as exc:
            raise BEMServerCoreWeatherAPIConnectionError(
                f"Error while connecting to weather API: {exc}"
            ) from exc

        if resp.status_code != 200:
            raise BEMServerCoreWeatherAPIQueryError(
                f"Error while querying weather API: {resp.text}"
            )

        try:
            ret_data = json.loads(resp.json()["data"])
        except (TypeError, json.decoder.JSONDecodeError) as exc:
            raise BEMServerCoreWeatherAPIResponseError(
                "Error in weather API response"
            ) from exc

        try:
            ret_df = pd.DataFrame(
                index=pd.DatetimeIndex(
                    pd.to_datetime(ret_data["index"], utc=True, unit="s"),
                    name="timestamp",
                ),
                data=ret_data["data"],
                # Strip (unit) from column names to facilitate selection
                columns=[str(col).split()[0] for col in ret_data["columns"]],
            )
            # Select data columns (drop metadata) and cast to float
            weather_df = ret_df[oik_params].astype(float)
        except (ValueError, KeyError) as exc:
            raise BEMServerCoreWeatherAPIResponseError(
                "Error in weather API response"
            ) from exc

        # Translate Oikolab names into BSC names
        weather_df.columns = [
            OIKOLAB_WEATHER_PARAMETERS_REVERSE_MAPPING[c] for c in weather_df.columns
        ]

        return weather_df
