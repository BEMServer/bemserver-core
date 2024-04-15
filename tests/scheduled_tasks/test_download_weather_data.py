"""Download weather data task tests"""

import datetime as dt
import json
from unittest.mock import patch

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.input_output import tsdio
from bemserver_core.model import Site, TimeseriesDataState
from bemserver_core.scheduled_tasks.download_weather_data import (
    ST_DownloadWeatherDataBySite,
    ST_DownloadWeatherForecastDataBySite,
    download_weather_data,
)


class TestST_DownloadWeatherDataBySiteModel:
    @pytest.mark.parametrize(
        "st_dwdbs_cls",
        (ST_DownloadWeatherDataBySite, ST_DownloadWeatherForecastDataBySite),
    )
    @pytest.mark.usefixtures("st_download_weather_data_by_sites")
    def test_st_download_weather_data_by_site_get_all_as_admin(
        self, users, campaigns, sites, st_dwdbs_cls
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        site_1 = sites[0]
        site_2 = sites[1]

        with OpenBar():
            site_3 = Site.new(name="Site 3", campaign_id=campaign_2.id)
            db.session.flush()

        sites += (site_3,)

        with CurrentUser(admin_user):
            assert len(list(st_dwdbs_cls.get())) < len(sites)
            assert len(list(st_dwdbs_cls.get_all())) == len(sites)

            assert len(list(st_dwdbs_cls.get_all(is_enabled=True))) == 1
            assert len(list(st_dwdbs_cls.get_all(is_enabled=False))) == 2

            ret = list(st_dwdbs_cls.get_all(site_id=site_1.id))
            assert len(ret) == 1
            assert ret[0][2] == site_1.name
            ret = list(st_dwdbs_cls.get_all(site_id=site_3.id))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name

            ret = st_dwdbs_cls.get_all(site_id=site_3.id, is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(st_dwdbs_cls.get_all(site_id=site_3.id, is_enabled=False))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name

            ret = list(st_dwdbs_cls.get_all(campaign_id=campaign_1.id))
            assert len(ret) == 1
            assert ret[0][2] == site_1.name
            ret = list(st_dwdbs_cls.get_all(campaign_id=campaign_2.id))
            assert len(ret) == 2
            # assert ret[0][2] == site_3.name

            ret = list(st_dwdbs_cls.get_all(in_site_name="1"))
            assert len(ret) == 1
            assert ret[0][2] == site_1.name
            ret = list(st_dwdbs_cls.get_all(in_site_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name
            ret = st_dwdbs_cls.get_all(in_site_name="3", is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(st_dwdbs_cls.get_all(in_site_name="3", is_enabled=False))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name
            ret = list(st_dwdbs_cls.get_all(in_site_name="non-existent"))
            assert len(ret) == 0

            ret = list(st_dwdbs_cls.get_all(sort=["+site_name"]))
            assert len(ret) == 3
            assert ret[0][2] == site_1.name
            assert ret[1][2] == site_2.name
            assert ret[2][2] == site_3.name
            ret = list(st_dwdbs_cls.get_all(sort=["-site_name"]))
            assert len(ret) == 3
            assert ret[0][2] == site_3.name
            assert ret[1][2] == site_2.name
            assert ret[2][2] == site_1.name
            ret = st_dwdbs_cls.get_all(sort=["-site_name"], is_enabled=True)
            assert len(list(ret)) == 1
            if st_dwdbs_cls is ST_DownloadWeatherDataBySite:
                assert ret[0][2] == site_1.name
            else:
                assert ret[0][2] == site_2.name
            ret = list(st_dwdbs_cls.get_all(sort=["-site_name"], is_enabled=False))
            assert len(ret) == 2
            assert ret[0][2] == site_3.name
            if st_dwdbs_cls is ST_DownloadWeatherDataBySite:
                assert ret[1][2] == site_2.name
            else:
                assert ret[1][2] == site_1.name

    @pytest.mark.parametrize(
        "st_dwdbs_cls",
        (ST_DownloadWeatherDataBySite, ST_DownloadWeatherForecastDataBySite),
    )
    @pytest.mark.usefixtures("st_download_weather_data_by_sites")
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("st_download_weather_data_by_sites")
    def test_st_download_weather_data_by_site_get_all_as_user(
        self, users, sites, st_dwdbs_cls
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        site_1 = sites[0]
        site_2 = sites[1]

        with OpenBar():
            site_3 = Site.new(name="Site 3", campaign=site_2.campaign)
            db.session.flush()

        sites += (site_3,)

        with CurrentUser(user_1):
            assert len(list(st_dwdbs_cls.get())) == 1
            assert len(list(st_dwdbs_cls.get_all())) == 2

            if st_dwdbs_cls is ST_DownloadWeatherDataBySite:
                assert len(list(st_dwdbs_cls.get_all(is_enabled=True))) == 0
                assert len(list(st_dwdbs_cls.get_all(is_enabled=False))) == 2
            else:
                assert len(list(st_dwdbs_cls.get_all(is_enabled=True))) == 1
                assert len(list(st_dwdbs_cls.get_all(is_enabled=False))) == 1

            assert len(list(st_dwdbs_cls.get_all(site_id=site_1.id))) == 0
            ret = list(st_dwdbs_cls.get_all(site_id=site_3.id))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name

            ret = st_dwdbs_cls.get_all(site_id=site_3.id, is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(st_dwdbs_cls.get_all(site_id=site_3.id, is_enabled=False))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name

            assert len(list(st_dwdbs_cls.get_all(in_site_name="1"))) == 0
            ret = list(st_dwdbs_cls.get_all(in_site_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name
            ret = st_dwdbs_cls.get_all(in_site_name="3", is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(st_dwdbs_cls.get_all(in_site_name="3", is_enabled=False))
            assert len(ret) == 1
            assert ret[0][2] == site_3.name
            ret = st_dwdbs_cls.get_all(in_site_name="non-existent")
            assert len(list(ret)) == 0

            ret = list(st_dwdbs_cls.get_all(sort=["+site_name"]))
            assert len(ret) == 2
            assert ret[0][2] == site_2.name
            assert ret[1][2] == site_3.name
            ret = list(st_dwdbs_cls.get_all(sort=["-site_name"]))
            assert len(ret) == 2
            assert ret[0][2] == site_3.name
            assert ret[1][2] == site_2.name
            ret = st_dwdbs_cls.get_all(sort=["-site_name"], is_enabled=True)
            if st_dwdbs_cls is ST_DownloadWeatherDataBySite:
                assert len(list(ret)) == 0
            else:
                assert len(list(ret)) == 1
                assert ret[0][2] == site_2.name

    @pytest.mark.parametrize(
        "st_dwdbs_cls",
        (ST_DownloadWeatherDataBySite, ST_DownloadWeatherForecastDataBySite),
    )
    @pytest.mark.usefixtures("st_download_weather_data_by_sites")
    def test_st_download_weather_data_by_site_delete_cascade(
        self, users, sites, st_dwdbs_cls
    ):
        admin_user = users[0]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            assert len(list(st_dwdbs_cls.get())) == 2
            site_1.delete()
            db.session.commit()
            assert len(list(st_dwdbs_cls.get())) == 1

    @pytest.mark.parametrize(
        "st_dwdbs_cls",
        (ST_DownloadWeatherDataBySite, ST_DownloadWeatherForecastDataBySite),
    )
    def test_st_download_weather_data_by_site_authorizations_as_admin(
        self, users, sites, st_dwdbs_cls
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        site_1 = sites[0]
        site_2 = sites[1]

        with CurrentUser(admin_user):
            st_cbc_1 = st_dwdbs_cls.new(site_id=site_1.id)
            db.session.commit()
            st_cbc = st_dwdbs_cls.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(st_dwdbs_cls.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(site_id=site_2.id)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.parametrize(
        "st_dwdbs_cls",
        (ST_DownloadWeatherDataBySite, ST_DownloadWeatherForecastDataBySite),
    )
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_download_weather_data_by_site_authorizations_as_user(
        self, users, sites, st_download_weather_data_by_sites, st_dwdbs_cls
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        site_1 = sites[0]
        site_2 = sites[1]
        if st_dwdbs_cls == ST_DownloadWeatherDataBySite:
            st_cbc_1 = st_download_weather_data_by_sites[0]
            st_cbc_2 = st_download_weather_data_by_sites[1]
        else:
            st_cbc_1 = st_download_weather_data_by_sites[2]
            st_cbc_2 = st_download_weather_data_by_sites[3]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                st_dwdbs_cls.new(site_id=site_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_dwdbs_cls.get_by_id(st_cbc_1.id)
            st_dwdbs_cls.get_by_id(st_cbc_2.id)
            stcs = list(st_dwdbs_cls.get())
            assert stcs == [st_cbc_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.update(site_id=site_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.delete()


OIKOLAB_RESPONSE_ATTRIBUTES = {
    "processing_time": 1.89,
    "n_parameter_months": 1,
    "gfs_reference_time": "2023-03-27 00 UTC",
    "next_gfs_update": "in 2.7 hours (approx)",
    "source": "ERA5 (2018) [...]",
    "notes": "GFS forecast data is updated every 6 hours [...]",
}


class TestDownloadWeatherDataScheduledTask:
    @pytest.mark.usefixtures("st_download_weather_data_by_sites")
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
    def test_download_weather_data(self, mock_get, users, timeseries, forecast):
        admin_user = users[0]
        assert admin_user.is_admin
        temp_site_1_ts = timeseries[0]
        rh_site_2_ts = timeseries[1]

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
            download_weather_data(end_dt, "hour", 2, 1, 0, forecast=forecast)

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
