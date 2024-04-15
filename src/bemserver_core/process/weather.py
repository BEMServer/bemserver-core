"""Weather forecast and historical data"""

import datetime as dt
import json

import pandas as pd

import requests
from requests.exceptions import RequestException

from bemserver_core.authorization import auth, get_current_user
from bemserver_core.exceptions import (
    BEMServerCoreSettingsError,
    BEMServerCoreWeatherAPIAuthenticationError,
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
    BEMServerCoreWeatherAPIResponseError,
    BEMServerCoreWeatherProcessMissingCoordinatesError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState, WeatherTimeseriesBySite
from bemserver_core.time_utils import floor

OIKOLAB_WEATHER_PARAMETERS = {
    "AIR_TEMPERATURE": ("temperature", "°C"),
    "DEWPOINT_TEMPERATURE": ("dewpoint_temperature", "°C"),
    "WETBULB_TEMPERATURE": ("wetbulb_temperature", "°C"),
    "WIND_SPEED": ("wind_speed", "m/s"),
    "WIND_DIRECTION": ("wind_direction", "degree"),
    "SURFACE_DIRECT_SOLAR_RADIATION": ("surface_direct_solar_radiation", "W/m^2"),
    "SURFACE_DIFFUSE_SOLAR_RADIATION": ("surface_diffuse_solar_radiation", "W/m^2"),
    "SURFACE_SOLAR_RADIATION": ("surface_solar_radiation", "W/m^2"),
    "DIRECT_NORMAL_SOLAR_RADIATION": ("direct_normal_solar_radiation", "W/m^2"),
    "RELATIVE_HUMIDITY": ("relative_humidity", "1"),
    "SURFACE_PRESSURE": ("surface_pressure", "Pa"),
    "TOTAL_PRECIPITATION": ("total_precipitation", "mm"),
}

OIKOLAB_WEATHER_PARAMETERS_NAMES_MAPPING = {
    k: v[0] for k, v in OIKOLAB_WEATHER_PARAMETERS.items()
}

OIKOLAB_WEATHER_PARAMETERS_NAMES_REVERSE_MAPPING = {
    v: k for k, v in OIKOLAB_WEATHER_PARAMETERS_NAMES_MAPPING.items()
}

OIKOLAB_WEATHER_PARAMETERS_UNITS_MAPPING = {
    k: v[1] for k, v in OIKOLAB_WEATHER_PARAMETERS.items()
}

OIKOLAB_REANALYSIS_SOURCE = "era5"
OIKOLAB_FORECAST_SOURCE = "gfs"


class OikolabWeatherDataClient:
    def __init__(self, api_url, api_key):
        self._api_url = api_url
        self._api_key = api_key

    def get_weather_data(
        self, params, latitude, longitude, start_dt, end_dt, forecast=False
    ):
        # Translate BSC names into Oikolab names
        oik_params = [OIKOLAB_WEATHER_PARAMETERS_NAMES_MAPPING[p] for p in params]

        # Exclude end_dt for consistency with TimeseriesDataIO default.
        # Oïkolab API is inclusive on start and end.
        oik_end_dt = floor(end_dt, "hour")
        if oik_end_dt == end_dt:
            oik_end_dt -= dt.timedelta(hours=1)

        model = OIKOLAB_FORECAST_SOURCE if forecast else OIKOLAB_REANALYSIS_SOURCE

        try:
            resp = requests.get(
                url=self._api_url,
                params={
                    "param": oik_params,
                    "lat": latitude,
                    "lon": longitude,
                    "start": start_dt.isoformat(),
                    "end": oik_end_dt.isoformat(),
                    "api-key": self._api_key,
                    "model": model,
                },
                # Set a long timeout as Oikolab responses may take a while
                timeout=60,
            )
        except RequestException as exc:
            raise BEMServerCoreWeatherAPIConnectionError(
                f"Error while connecting to weather API: {exc}"
            ) from exc

        if resp.status_code == 401:
            raise BEMServerCoreWeatherAPIAuthenticationError("Wrong API key.")
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
            timestamps = [int(d) for d in ret_data["index"]]
            ret_df = pd.DataFrame(
                index=pd.DatetimeIndex(
                    pd.to_datetime(timestamps, utc=True, unit="s"),
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
            OIKOLAB_WEATHER_PARAMETERS_NAMES_REVERSE_MAPPING[c]
            for c in weather_df.columns
        ]

        return weather_df


class WeatherDataProcessor:
    def __init__(self):
        self._api_url = None
        self._api_key = None

    def init_core(self, bsc):
        """Initialize with settings from BEMServerCore configuration"""
        self._api_url = bsc.config["WEATHER_DATA_CLIENT_API_URL"]
        self._api_key = bsc.config["WEATHER_DATA_CLIENT_API_KEY"]

    @property
    def client(self):
        """Make OikolabWeatherDataClient instance"""
        if not self._api_url or not self._api_key:
            raise BEMServerCoreSettingsError("Missing weather API settings.")
        return OikolabWeatherDataClient(self._api_url, self._api_key)

    def get_weather_data_for_site(self, site, start_dt, end_dt, forecast=False):
        """Get weather data for a site

        :param Site site: Site for which to get weather data
        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param bool forecast: Whether or not the data is past data or forecast
        """
        auth.authorize(get_current_user(), "get_weather_data", site)

        ds_clean = TimeseriesDataState.get(name="Clean").first()

        if wtsbs_l := list(
            WeatherTimeseriesBySite.get(site_id=site.id, forecast=forecast)
        ):
            params_l = [wtsbs.parameter.name for wtsbs in wtsbs_l]
            ts_l = [wtsbs.timeseries for wtsbs in wtsbs_l]

            latitude, longitude = site.latitude, site.longitude
            if latitude is None or longitude is None:
                raise BEMServerCoreWeatherProcessMissingCoordinatesError(
                    "Missing site coordinates."
                )

            weather_df = self.client.get_weather_data(
                params=params_l,
                latitude=site.latitude,
                longitude=site.longitude,
                start_dt=start_dt,
                end_dt=end_dt,
                forecast=forecast,
            )
            weather_df.columns = [ts.id for ts in ts_l]

            convert_from = {
                ts.id: OIKOLAB_WEATHER_PARAMETERS_UNITS_MAPPING[param]
                for ts, param in zip(ts_l, params_l)
            }

            tsdio.delete(start_dt, end_dt, ts_l, ds_clean)
            tsdio.set_timeseries_data(weather_df, ds_clean, convert_from=convert_from)


wdp = WeatherDataProcessor()
