"""Cleanup task tests"""
import datetime as dt

import pandas as pd

import pytest
from tests.utils import create_timeseries_data

from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.scheduled_tasks.cleanup import (
    ST_CleanupByCampaign,
    ST_CleanupByTimeseries,
    cleanup_scheduled_task,
)
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestST_CleanupByCampaignModel:
    @pytest.mark.usefixtures("st_cleanups_by_timeseries")
    def test_st_cleanup_by_campaign_delete_cascade(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        st_cbc_2 = st_cleanups_by_campaigns[1]

        with CurrentUser(admin_user):
            assert len(list(ST_CleanupByCampaign.get())) == 2
            assert len(list(ST_CleanupByTimeseries.get())) == 2
            campaign_1.delete()
            db.session.commit()
            assert len(list(ST_CleanupByCampaign.get())) == 1
            assert len(list(ST_CleanupByTimeseries.get())) == 1
            st_cbc_2.delete()
            db.session.commit()
            assert not list(ST_CleanupByTimeseries.get())

    def test_st_cleanup_by_campaign_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            st_cbc_1 = ST_CleanupByCampaign.new(campaign_id=campaign_1.id, enabled=True)
            db.session.add(st_cbc_1)
            db.session.commit()
            st_cbc = ST_CleanupByCampaign.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(ST_CleanupByCampaign.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(enabled=False)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_cleanup_by_campaign_authorizations_as_user(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_2 = campaigns[1]
        st_cbc_2 = st_cleanups_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByCampaign.new(campaign_id=campaign_2.id, enabled=True)
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByCampaign.get_by_id(st_cbc_2.id)
            stcs = list(ST_CleanupByCampaign.get())
            assert stcs == []
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_2.update(enabled=False)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_2.delete()


class TestST_CleanupByTimeseriesModel:
    @pytest.mark.usefixtures("st_cleanups_by_timeseries")
    def test_st_cleanup_by_timeseries_delete_cascade(self, users, timeseries):
        admin_user = users[0]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            assert len(list(ST_CleanupByTimeseries.get())) == 2
            ts_1.delete()
            db.session.commit()
            assert len(list(ST_CleanupByTimeseries.get())) == 1

    def test_st_cleanup_by_timeseries_authorizations_as_admin(
        self, users, timeseries, st_cleanups_by_campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        st_cbc_1 = st_cleanups_by_campaigns[0]

        with CurrentUser(admin_user):
            st_cbt_1 = ST_CleanupByTimeseries.new(
                st_cleanup_by_campaign_id=st_cbc_1.id,
                timeseries_id=ts_1.id,
            )
            db.session.add(st_cbt_1)
            db.session.commit()
            st_cbt = ST_CleanupByTimeseries.get_by_id(st_cbt_1.id)
            assert st_cbt.id == st_cbt_1.id
            st_cbcs_ = list(ST_CleanupByTimeseries.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbt_1.id
            st_cbt.update(enabled=False)
            st_cbt.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_cleanup_by_timeseries_authorizations_as_user(
        self, users, timeseries, st_cleanups_by_campaigns, st_cleanups_by_timeseries
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_2 = timeseries[1]
        st_cbc_2 = st_cleanups_by_campaigns[1]
        st_cbt_2 = st_cleanups_by_timeseries[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByTimeseries.new(
                    st_cleanup_by_campaign_id=st_cbc_2.id,
                    timeseries_id=ts_2.id,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByTimeseries.get_by_id(st_cbt_2.id)
            stcs = list(ST_CleanupByTimeseries.get())
            assert stcs == []
            with pytest.raises(BEMServerAuthorizationError):
                st_cbt_2.update(last_timestamp=dt.datetime.now(tz=dt.timezone.utc))
            with pytest.raises(BEMServerAuthorizationError):
                st_cbt_2.delete()


class TestCleanupScheduledTask:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_cleanup_scheduled_task(self, users, timeseries, st_cleanups_by_campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        st_cbc_2 = st_cleanups_by_campaigns[1]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ds_2 = TimeseriesDataState.get(name="Clean").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            tsp_0_min = TimeseriesPropertyData(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            st_cbc_2.enabled = False
            db.session.add(tsp_0_min)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="12H")
        values = [0, 13]
        create_timeseries_data(ts_0, ds_1, timestamps, values)
        create_timeseries_data(ts_1, ds_1, timestamps, values)

        with OpenBar():

            cleanup_scheduled_task.apply()

            # Campaign 1, TS 0, min 12, max None, [0, 13] -> [-, 13]
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_0,), ds_2)
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T12:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [13.0]
            expected_data_df = pd.DataFrame({ts_0.id: val_0}, index=index)
            assert data_df.equals(expected_data_df)

            # Campaign 2 (disabled), TS 1 -> no clean data
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_1,), ds_2)
            index = pd.DatetimeIndex([], name="timestamp", tz="UTC")
            no_data_df = pd.DataFrame({ts_1.id: []}, index=index)
            assert data_df.equals(no_data_df)
