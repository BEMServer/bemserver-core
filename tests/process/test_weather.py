"""Weather tests"""
import datetime as dt
import json
from unittest.mock import patch

import pytest

import pandas as pd
from requests.exceptions import RequestException

from bemserver_core.process.weather import OikolabWeatherDataClient

from bemserver_core.exceptions import (
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
    BEMServerCoreWeatherAPIResponseError,
)


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
    def test_get_weather_data(self, mock_get):
        resp_data = {
            "columns": [
                "coordinates (lat,lon)",
                "model (name)",
                "model elevation (surface)",
                "utc_offset (hrs)",
                "temperature (degC)",
                "relative_humidity (0-1)",
            ],
            "index": [1577836800, 1577840400],
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
            start_dt=dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
            end_dt=dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc),
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
        assert resp_df.equals(expected_data_df)

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
                end_dt=dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc),
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
                end_dt=dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc),
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
                end_dt=dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc),
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
                end_dt=dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc),
            )
