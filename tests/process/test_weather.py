"""Weather tests"""

import datetime as dt
import json
from unittest.mock import patch

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from requests.exceptions import RequestException

from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreDimensionalityError,
    BEMServerCoreSettingsError,
    BEMServerCoreWeatherAPIAuthenticationError,
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
    BEMServerCoreWeatherAPIResponseError,
    BEMServerCoreWeatherProcessMissingCoordinatesError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState
from bemserver_core.process.weather import OikolabWeatherDataClient, wdp

OIKOLAB_RESPONSE_ATTRIBUTES = {
    "processing_time": 1.89,
    "n_parameter_months": 1,
    "gfs_reference_time": "2023-03-27 00 UTC",
    "next_gfs_update": "in 2.7 hours (approx)",
    "source": "ERA5 (2018) [...]",
    "notes": "GFS forecast data is updated every 6 hours [...]",
}


class TestWeatherClient:
    @patch("requests.get")
    @pytest.mark.parametrize("forecast", (False, True))
    def test_get_weather_data(self, mock_get, forecast):
        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)
        oik_end_dt = dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc)

        resp_data = {
            "columns": [
                "coordinates (lat,lon)",
                "model (name)",
                "model elevation (surface)",
                "utc_offset (hrs)",
                "temperature (degC)",
                "relative_humidity (0-1)",
            ],
            "index": [f"{start_dt.timestamp():0.0f}", f"{oik_end_dt.timestamp():0.0f}"],
            "data": [
                ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, 0.78],
                ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.78],
            ],
        }
        resp_json = {
            "attributes": OIKOLAB_RESPONSE_ATTRIBUTES,
            "data": json.dumps(resp_data),
        }
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp_json

        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        resp_df = client.get_weather_data(
            params=["AIR_TEMPERATURE", "RELATIVE_HUMIDITY"],
            latitude=46.0,
            longitude=6.0,
            start_dt=start_dt,
            end_dt=end_dt,
            forecast=forecast,
        )

        mock_get.assert_called_with(
            url="dummy-url",
            params={
                "param": ["temperature", "relative_humidity"],
                "lat": 46.0,
                "lon": 6.0,
                "start": start_dt.isoformat(),
                "end": oik_end_dt.isoformat(),
                "api-key": "dummy-key",
                "model": "gfs" if forecast else "era5",
            },
            timeout=60,
        )

        index = pd.DatetimeIndex(
            ["2020-01-01T00:00:00+00:00", "2020-01-01T01:00:00+00:00"],
            name="timestamp",
            tz="UTC",
        )
        expected_data_df = pd.DataFrame(
            {"AIR_TEMPERATURE": [2.45, 2.59], "RELATIVE_HUMIDITY": [0.78, 0.78]},
            index=index,
        )
        assert_frame_equal(resp_df, expected_data_df)

    @patch("requests.get")
    def test_get_weather_data_auth_error(self, mock_get):
        mock_get.return_value.status_code = 401
        mock_get.return_value.text = (
            '{ "statusCode": 401, '
            '"message": "Access denied due to invalid subscription key. '
            'Make sure to provide a valid key for an active subscription." }'
        )
        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        with pytest.raises(
            BEMServerCoreWeatherAPIAuthenticationError,
            match="Wrong API key.",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE"],
                latitude="dummy",
                longitude=6.0,
                start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
                end_dt=dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc),
            )

    @patch("requests.get")
    def test_get_weather_data_query_error(self, mock_get):
        mock_get.return_value.status_code = 400
        mock_get.return_value.text = "Query error"
        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        with pytest.raises(
            BEMServerCoreWeatherAPIQueryError,
            match="Error while querying weather API: Query error",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE"],
                latitude="dummy",
                longitude=6.0,
                start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
                end_dt=dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc),
            )

    @patch("requests.get")
    def test_get_weather_data_connexion_error(self, mock_get):
        mock_get.side_effect = RequestException("Connection error")
        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        with pytest.raises(
            BEMServerCoreWeatherAPIConnectionError,
            match="Error while connecting to weather API: Connection error",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE"],
                latitude=46.0,
                longitude=6.0,
                start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
                end_dt=dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc),
            )

    @pytest.mark.parametrize(
        "resp_json",
        (
            "dummy",
            {"attributes": OIKOLAB_RESPONSE_ATTRIBUTES, "data": "dummy"},
        ),
    )
    @patch("requests.get")
    def test_get_weather_data_response_invalid_response_json_error(
        self, mock_get, resp_json
    ):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp_json

        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        with pytest.raises(
            BEMServerCoreWeatherAPIResponseError,
            match="Error in weather API response",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE", "RELATIVE_HUMIDITY"],
                latitude=46.0,
                longitude=6.0,
                start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
                end_dt=dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc),
            )

    @pytest.mark.parametrize(
        "resp_data_index, resp_data_data",
        (
            # Missing index
            (
                None,
                [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, 0.78],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.78],
                ],
            ),
            # Missing data
            (
                [1577836800, 1577840400],
                None,
            ),
            # Length of values does not match length of index
            (
                [1577836800],
                [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, 0.78],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.78],
                ],
            ),
            # 6 columns passed, passed data had 5 columns
            (
                [1577836800, 1577840400],
                [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59],
                ],
            ),
            # Invalid timestamp
            (
                [1577836800, "dummy"],
                [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, 0.78],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.78],
                ],
            ),
            # Invalid value
            (
                [1577836800, 1577840400],
                [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, "dummy"],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.78],
                ],
            ),
        ),
    )
    @patch("requests.get")
    def test_get_weather_data_response_invalid_response_data_error(
        self, mock_get, resp_data_index, resp_data_data
    ):
        resp_data = {
            "columns": [
                "coordinates (lat,lon)",
                "model (name)",
                "model elevation (surface)",
                "utc_offset (hrs)",
                "temperature (degC)",
                "relative_humidity (0-1)",
            ],
        }
        if resp_data_index is not None:
            resp_data["index"] = resp_data_index
        if resp_data_data is not None:
            resp_data["data"] = resp_data_data

        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "attributes": OIKOLAB_RESPONSE_ATTRIBUTES,
            "data": json.dumps(resp_data),
        }

        client = OikolabWeatherDataClient("dummy-url", "dummy-key")
        with pytest.raises(
            BEMServerCoreWeatherAPIResponseError,
            match="Error in weather API response",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE", "RELATIVE_HUMIDITY"],
                latitude=46.0,
                longitude=6.0,
                start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
                end_dt=dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc),
            )


class TestWeatherDataProcessor:
    @pytest.mark.usefixtures("as_admin")
    @pytest.mark.usefixtures("bemservercore")
    @pytest.mark.parametrize(
        "config",
        ({"WEATHER_DATA_CLIENT_API_KEY": "dummy-key"},),
        indirect=True,
    )
    @patch("requests.get")
    def test_get_weather_data_for_site(
        self, mock_get, sites, weather_timeseries_by_sites
    ):
        site_1 = sites[0]
        site_2 = sites[1]
        wtbs_1 = weather_timeseries_by_sites[0]
        wtbs_2 = weather_timeseries_by_sites[1]

        air_temp_ts = wtbs_1.timeseries
        rh_ts = wtbs_2.timeseries
        ds_clean = TimeseriesDataState.get(name="Clean").first()

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)
        oik_end_dt = dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc)

        # The payload should contain only air temp or RH depending on the call
        # but for the purpose of the test, it is easier here to write a single payload.
        # Likewise, the model would be either era5 or gfs.
        resp_data = {
            "columns": [
                "coordinates (lat,lon)",
                "model (name)",
                "model elevation (surface)",
                "utc_offset (hrs)",
                "temperature (degC)",
                "relative_humidity (0-1)",
            ],
            "index": [f"{start_dt.timestamp():0.0f}", f"{oik_end_dt.timestamp():0.0f}"],
            "data": [
                [
                    f"({site_1.latitude}, {site_1.longitude})",
                    "dummy",
                    694.09,
                    1.0,
                    2.45,
                    0.78,
                ],
                [
                    f"({site_1.latitude}, {site_1.longitude})",
                    "dummy",
                    694.09,
                    1.0,
                    2.59,
                    0.79,
                ],
            ],
        }
        resp_json = {
            "attributes": OIKOLAB_RESPONSE_ATTRIBUTES,
            "data": json.dumps(resp_data),
        }
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = resp_json

        # Test site 1, air temp
        wdp.get_weather_data_for_site(site_1, start_dt, end_dt)

        data_df_1 = tsdio.get_timeseries_data(
            start_dt,
            end_dt,
            (air_temp_ts, rh_ts),
            ds_clean,
            col_label="name",
        )
        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
            ],
            name="timestamp",
            tz="UTC",
        )
        expected_data_df = pd.DataFrame(
            {"Timeseries 1": [2.45, 2.59], "Timeseries 2": [np.nan, np.nan]},
            index=index,
        )
        assert_frame_equal(data_df_1, expected_data_df, check_names=False)

        # Test site 2, RH forecast
        # Also check units conversion from 1 to %
        wdp.get_weather_data_for_site(site_2, start_dt, end_dt, forecast=True)

        data_df_2 = tsdio.get_timeseries_data(
            start_dt,
            end_dt,
            (air_temp_ts, rh_ts),
            ds_clean,
            col_label="name",
        )
        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
            ],
            name="timestamp",
            tz="UTC",
        )
        expected_data_df = pd.DataFrame(
            {"Timeseries 1": [2.45, 2.59], "Timeseries 2": [78.0, 79.0]}, index=index
        )
        assert_frame_equal(data_df_2, expected_data_df, check_names=False)

        # Test again to check data is deleted/overwritten (with a unit conversion)
        air_temp_ts.unit_symbol = "°F"

        wdp.get_weather_data_for_site(site_1, start_dt, end_dt)

        data_df_3 = tsdio.get_timeseries_data(
            start_dt,
            end_dt,
            (air_temp_ts, rh_ts),
            ds_clean,
            col_label="name",
        )
        expected_data_df = pd.DataFrame(
            {"Timeseries 1": [36.410, 36.662], "Timeseries 2": [78.0, 79.0]},
            index=index,
        )
        assert_frame_equal(data_df_3, expected_data_df, check_names=False)

        # Test failure + rollback: data not deleted
        air_temp_ts.unit_symbol = "m/s"

        db.session.commit()
        try:
            wdp.get_weather_data_for_site(site_1, start_dt, end_dt)
        except BEMServerCoreDimensionalityError:
            pass
        db.session.rollback()

        data_df_3 = tsdio.get_timeseries_data(
            start_dt,
            end_dt,
            (air_temp_ts, rh_ts),
            ds_clean,
            col_label="name",
        )
        assert_frame_equal(data_df_3, expected_data_df, check_names=False)

    @pytest.mark.usefixtures("as_admin")
    @pytest.mark.usefixtures("bemservercore")
    @pytest.mark.parametrize(
        "config",
        ({"WEATHER_DATA_CLIENT_API_KEY": "dummy-key"},),
        indirect=True,
    )
    @patch("requests.get")
    def test_get_weather_data_for_site_no_call_if_no_ts(self, mock_get, sites):
        site_1 = sites[0]

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)

        wdp.get_weather_data_for_site(site_1, start_dt, end_dt)
        mock_get.assert_not_called()

    @pytest.mark.usefixtures("as_admin")
    @pytest.mark.usefixtures("bemservercore")
    @pytest.mark.usefixtures("weather_timeseries_by_sites")
    @pytest.mark.parametrize(
        "config",
        (
            {
                "WEATHER_DATA_CLIENT_API_URL": "dummy-url",
                "WEATHER_DATA_CLIENT_API_KEY": "",
            },
            {
                "WEATHER_DATA_CLIENT_API_URL": "",
                "WEATHER_DATA_CLIENT_API_KEY": "dummy-key",
            },
            {
                "WEATHER_DATA_CLIENT_API_URL": "",
                "WEATHER_DATA_CLIENT_API_KEY": "",
            },
        ),
        indirect=True,
    )
    def test_get_weather_data_for_site_api_settings_error(self, sites):
        site_1 = sites[0]

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)

        with pytest.raises(
            BEMServerCoreSettingsError, match="Missing weather API settings."
        ):
            wdp.get_weather_data_for_site(site_1, start_dt, end_dt)

    @pytest.mark.usefixtures("as_admin")
    @pytest.mark.usefixtures("bemservercore")
    @pytest.mark.parametrize(
        "config",
        ({"WEATHER_DATA_CLIENT_API_KEY": "dummy-key"},),
        indirect=True,
    )
    @pytest.mark.parametrize(
        "missing_coords",
        (("longitude", "latitude"), ("longitude",), ("latitude",)),
    )
    @pytest.mark.usefixtures("weather_timeseries_by_sites")
    def test_get_weather_data_for_site_missing_coordinates(self, sites, missing_coords):
        site_1 = sites[0]

        for coord in missing_coords:
            setattr(site_1, coord, None)

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)

        with pytest.raises(
            BEMServerCoreWeatherProcessMissingCoordinatesError,
            match="Missing site coordinates.",
        ):
            wdp.get_weather_data_for_site(site_1, start_dt, end_dt)

    @pytest.mark.usefixtures("bemservercore")
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_get_weather_data_for_site_as_user(self, users, sites):
        user_2 = users[1]
        site_1 = sites[0]

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)

        with CurrentUser(user_2):
            with pytest.raises(BEMServerAuthorizationError):
                wdp.get_weather_data_for_site(site_1, start_dt, end_dt)
