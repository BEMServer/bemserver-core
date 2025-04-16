"""Download weather data task tests"""

import datetime as dt
import json
from unittest.mock import patch

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import OpenBar
from bemserver_core.exceptions import BEMServerCoreScheduledTaskParametersError
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState
from bemserver_core.tasks.download_weather_data import download_weather_data

OIKOLAB_RESPONSE_ATTRIBUTES = {
    "processing_time": 1.89,
    "n_parameter_months": 1,
    "gfs_reference_time": "2023-03-27 00 UTC",
    "next_gfs_update": "in 2.7 hours (approx)",
    "source": "ERA5 (2018) [...]",
    "notes": "GFS forecast data is updated every 6 hours [...]",
}


class TestDownloadWeatherDataScheduledTask:
    @pytest.mark.usefixtures("weather_timeseries_by_sites")
    @pytest.mark.parametrize("sites", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    @pytest.mark.parametrize(
        "config",
        ({"WEATHER_DATA_CLIENT_API_KEY": "dummy-key"},),
        indirect=True,
    )
    @patch("requests.get")
    @pytest.mark.parametrize("forecast", (False, True))
    def test_download_weather_data(
        self, mock_get, users, campaigns, timeseries, forecast
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        temp_site_1_ts = timeseries[0]
        rh_site_2_ts = timeseries[1]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        start_dt = dt.datetime(2020, 1, 1, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, tzinfo=dt.timezone.utc)
        oik_end_dt = dt.datetime(2020, 1, 1, 1, 0, tzinfo=dt.timezone.utc)

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()

            # Check no TS data
            data_df = tsdio.get_timeseries_data(
                start_dt,
                end_dt,
                (temp_site_1_ts, rh_site_2_ts),
                ds_clean,
                col_label="name",
            )
            assert data_df.empty

            # Mock call
            resp_data = {
                "columns": [
                    "coordinates (lat,lon)",
                    "model (name)",
                    "model elevation (surface)",
                    "utc_offset (hrs)",
                    "temperature (degC)",
                    "relative_humidity (0-1)",
                ],
                "index": [
                    f"{start_dt.timestamp():0.0f}",
                    f"{oik_end_dt.timestamp():0.0f}",
                ],
                "data": [
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.45, 0.78],
                    ["(46.0, 6.0)", "era5", 694.09, 1.0, 2.59, 0.79],
                ],
            }
            resp_json = {
                "attributes": OIKOLAB_RESPONSE_ATTRIBUTES,
                "data": json.dumps(resp_data),
            }
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = resp_json

            # Call service at end_dt for last 2 hours, get 1 2-hour period before
            if forecast is False:
                download_weather_data(
                    campaign_1, start_dt, end_dt, ["Site 1"], forecast=forecast
                )
            else:
                download_weather_data(
                    campaign_2, start_dt, end_dt, ["Site 2"], forecast=forecast
                )

            # Check mock call
            if forecast is False:
                call_params = {
                    "param": ["temperature"],
                    "lat": 43.47394,
                    "lon": -1.50940,
                }
            else:
                call_params = {
                    "param": ["relative_humidity"],
                    "lat": 44.84325,
                    "lon": -0.56262,
                }

            mock_get.assert_called_with(
                url="https://api.oikolab.com/weather",
                params={
                    **call_params,
                    "start": start_dt.isoformat(),
                    "end": oik_end_dt.isoformat(),
                    "api-key": "dummy-key",
                    "model": "gfs" if forecast else "era5",
                },
                timeout=60,
            )

            # Check TS data
            data_df = tsdio.get_timeseries_data(
                start_dt,
                end_dt,
                (temp_site_1_ts, rh_site_2_ts),
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
            if forecast is False:
                expected_data_df = pd.DataFrame(
                    {"Timeseries 1": [2.45, 2.59], "Timeseries 2": [np.nan, np.nan]},
                    index=index,
                )
            else:
                expected_data_df = pd.DataFrame(
                    {"Timeseries 1": [np.nan, np.nan], "Timeseries 2": [78.0, 79.0]},
                    index=index,
                )
            assert_frame_equal(data_df, expected_data_df, check_names=False)

            # Test "site does not exist" exception
            with pytest.raises(BEMServerCoreScheduledTaskParametersError):
                download_weather_data(
                    campaign_1, start_dt, end_dt, ["Site 42"], forecast=forecast
                )
