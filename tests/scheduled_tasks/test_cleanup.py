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
    def test_st_cleanup_by_campaign_get_all_as_admin(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(admin_user):
            assert len(list(ST_CleanupByCampaign.get())) < len(campaigns)
            assert len(list(ST_CleanupByCampaign.get_all())) == len(campaigns)

            assert len(list(ST_CleanupByCampaign.get_all(is_enabled=True))) == 1
            assert len(list(ST_CleanupByCampaign.get_all(is_enabled=False))) == 2

            ret = list(ST_CleanupByCampaign.get_all(campaign_id=campaign_1.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CleanupByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CleanupByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CleanupByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = list(ST_CleanupByCampaign.get_all(in_campaign_name="1"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CleanupByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CleanupByCampaign.get_all(in_campaign_name="3", is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(
                ST_CleanupByCampaign.get_all(in_campaign_name="3", is_enabled=False)
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = list(ST_CleanupByCampaign.get_all(in_campaign_name="non-existent"))
            assert len(ret) == 0

            ret = list(ST_CleanupByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_1.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_3.name
            ret = list(ST_CleanupByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_1.name
            ret = ST_CleanupByCampaign.get_all(sort=["-campaign_name"], is_enabled=True)
            assert len(list(ret)) == 1
            ret = list(
                ST_CleanupByCampaign.get_all(sort=["-campaign_name"], is_enabled=False)
            )
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_cleanup_by_campaign_get_all_as_user(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(user_1):
            assert len(list(ST_CleanupByCampaign.get())) == 1
            assert len(list(ST_CleanupByCampaign.get_all())) == 2

            assert len(list(ST_CleanupByCampaign.get_all(is_enabled=True))) == 0
            assert len(list(ST_CleanupByCampaign.get_all(is_enabled=False))) == 2

            assert (
                len(list(ST_CleanupByCampaign.get_all(campaign_id=campaign_1.id))) == 0
            )
            ret = list(ST_CleanupByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CleanupByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CleanupByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            assert len(list(ST_CleanupByCampaign.get_all(in_campaign_name="1"))) == 0
            ret = list(ST_CleanupByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CleanupByCampaign.get_all(in_campaign_name="3", is_enabled=True)
            assert len(list(ret)) == 0
            ret = list(
                ST_CleanupByCampaign.get_all(in_campaign_name="3", is_enabled=False)
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CleanupByCampaign.get_all(in_campaign_name="non-existent")
            assert len(list(ret)) == 0

            ret = list(ST_CleanupByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_2.name
            assert ret[1][2] == campaign_3.name
            ret = list(ST_CleanupByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            ret = ST_CleanupByCampaign.get_all(sort=["-campaign_name"], is_enabled=True)
            assert len(list(ret)) == 0

    def test_st_cleanup_by_campaign_delete_cascade(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            assert len(list(ST_CleanupByCampaign.get())) == 2
            campaign_1.delete()
            db.session.commit()
            assert len(list(ST_CleanupByCampaign.get())) == 1

    def test_st_cleanup_by_campaign_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            st_cbc_1 = ST_CleanupByCampaign.new(campaign_id=campaign_1.id)
            db.session.commit()
            st_cbc = ST_CleanupByCampaign.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(ST_CleanupByCampaign.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(campaign_id=campaign_2.id)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_cleanup_by_campaign_authorizations_as_user(
        self, users, campaigns, st_cleanups_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        st_cbc_1 = st_cleanups_by_campaigns[0]
        st_cbc_2 = st_cleanups_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByCampaign.new(campaign_id=campaign_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByCampaign.get_by_id(st_cbc_1.id)
            ST_CleanupByCampaign.get_by_id(st_cbc_2.id)
            stcs = list(ST_CleanupByCampaign.get())
            assert stcs == [st_cbc_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.delete()


class TestST_CleanupByTimeseriesModel:
    @pytest.mark.usefixtures("st_cleanups_by_timeseries")
    @pytest.mark.parametrize("timeseries", (10,), indirect=True)
    def test_st_cleanup_by_timeseries_get_all_as_admin(
        self, users, campaigns, timeseries
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        ts_1 = timeseries[0]
        ts_4 = timeseries[3]
        ts_7 = timeseries[6]

        with CurrentUser(admin_user):
            st_cbt_1 = ST_CleanupByTimeseries.get(timeseries_id=ts_1.id).first()
            assert st_cbt_1 is not None
            assert st_cbt_1.last_timestamp is None
            st_cbt_1.last_timestamp = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
            st_cbt_4 = ST_CleanupByTimeseries.new(
                timeseries_id=ts_4.id,
                last_timestamp=dt.datetime(2022, 9, 22, tzinfo=dt.timezone.utc),
            )
            st_cbt_7 = ST_CleanupByTimeseries.new(
                timeseries_id=ts_7.id,
                last_timestamp=dt.datetime(2022, 9, 22, tzinfo=dt.timezone.utc),
            )
            db.session.commit()

            assert len(list(ST_CleanupByTimeseries.get())) < len(timeseries)
            assert len(list(ST_CleanupByTimeseries.get_all())) == len(timeseries)

            camp_1_ts = [x for x in timeseries if x.campaign_id == campaign_1.id]
            assert len(camp_1_ts) == 4
            ret = list(ST_CleanupByTimeseries.get_all(campaign_id=campaign_1.id))
            assert len(ret) == len(camp_1_ts)

            ret = ST_CleanupByTimeseries.get_all(
                campaign_id=campaign_1.id, in_timeseries_name="Timeseries 1"
            )
            assert len(list(ret)) == 2
            ret = ST_CleanupByTimeseries.get_all(
                campaign_id=campaign_1.id, in_timeseries_name="Timeseries 7"
            )
            assert len(list(ret)) == 1
            ret = ST_CleanupByTimeseries.get_all(
                campaign_id=campaign_1.id, in_timeseries_name="Timeseries 2"
            )
            assert len(list(ret)) == 0
            ret = ST_CleanupByTimeseries.get_all(in_timeseries_name="Timeseries 2")
            assert len(list(ret)) == 1

            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_1.id,
                    sort=["+last_timestamp", "+timeseries_name"],
                )
            )
            assert len(ret) == len(camp_1_ts)
            assert ret[0][1] == ts_1.id
            assert ret[0][4] == st_cbt_1.last_timestamp
            assert ret[1][1] == ts_4.id
            assert ret[1][4] == st_cbt_4.last_timestamp
            assert ret[2][1] == ts_7.id
            assert ret[2][4] == st_cbt_7.last_timestamp
            assert ret[3][4] is None
            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_1.id,
                    sort=["-last_timestamp", "-timeseries_name"],
                )
            )
            assert len(ret) == len(camp_1_ts)
            assert ret[0][1] == ts_7.id
            assert ret[0][4] == st_cbt_7.last_timestamp
            assert ret[0][2] == "Timeseries 7"
            assert ret[1][1] == ts_4.id
            assert ret[1][4] == st_cbt_4.last_timestamp
            assert ret[1][2] == "Timeseries 4"
            assert ret[2][1] == ts_1.id
            assert ret[2][4] == st_cbt_1.last_timestamp
            assert ret[2][2] == "Timeseries 1"
            assert ret[3][4] is None
            assert ret[3][2] == "Timeseries 10"
            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_1.id, sort=["+timeseries_name"]
                )
            )
            assert len(ret) == len(camp_1_ts)
            assert ret[0][2] == "Timeseries 1"
            assert ret[1][2] == "Timeseries 10"
            assert ret[2][2] == "Timeseries 4"
            assert ret[3][2] == "Timeseries 7"
            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_1.id, sort=["-timeseries_name"]
                )
            )
            assert len(ret) == len(camp_1_ts)
            assert ret[0][2] == "Timeseries 7"
            assert ret[1][2] == "Timeseries 4"
            assert ret[2][2] == "Timeseries 10"
            assert ret[3][2] == "Timeseries 1"

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("st_cleanups_by_timeseries")
    @pytest.mark.parametrize("timeseries", (10,), indirect=True)
    def test_st_cleanup_by_timeseries_get_all_as_user(
        self, users, campaigns, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]
        ts_1 = timeseries[0]
        ts_5 = timeseries[4]
        ts_8 = timeseries[7]
        ts_9 = timeseries[8]

        st_cbt_1_last_timestamp = dt.datetime(2022, 9, 22, tzinfo=dt.timezone.utc)
        st_cbt_5_last_timestamp = dt.datetime(2022, 7, 1, 12, tzinfo=dt.timezone.utc)
        st_cbt_8_last_timestamp = dt.datetime(2022, 7, 1, 11, tzinfo=dt.timezone.utc)
        st_cbt_9_last_timestamp = dt.datetime(2022, 7, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(admin_user):
            st_cbt_1 = ST_CleanupByTimeseries.get(timeseries_id=ts_1.id).first()
            assert st_cbt_1 is not None
            assert st_cbt_1.last_timestamp is None
            st_cbt_1.last_timestamp = st_cbt_1_last_timestamp
            ST_CleanupByTimeseries.new(
                timeseries_id=ts_5.id,
                last_timestamp=st_cbt_5_last_timestamp,
            )
            ST_CleanupByTimeseries.new(
                timeseries_id=ts_8.id,
                last_timestamp=st_cbt_8_last_timestamp,
            )
            ST_CleanupByTimeseries.new(
                timeseries_id=ts_9.id,
                last_timestamp=st_cbt_9_last_timestamp,
            )
            db.session.commit()

        with CurrentUser(user_1):
            assert len(list(ST_CleanupByTimeseries.get())) == 4
            assert len(list(ST_CleanupByTimeseries.get_all())) == 6

            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByTimeseries.get_all(campaign_id=campaign_1.id)

            ret = list(ST_CleanupByTimeseries.get_all(campaign_id=campaign_2.id))
            assert len(ret) == 3
            ts_ids = [x[1] for x in ret]
            assert ts_1.id not in ts_ids
            assert ts_5.id in ts_ids
            assert ts_8.id in ts_ids
            assert ts_9.id not in ts_ids

            ret = list(ST_CleanupByTimeseries.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 3
            ts_ids = [x[1] for x in ret]
            assert ts_1.id not in ts_ids
            assert ts_5.id not in ts_ids
            assert ts_8.id not in ts_ids
            assert ts_9.id in ts_ids

            ret = ST_CleanupByTimeseries.get_all(
                campaign_id=campaign_2.id, in_timeseries_name="Timeseries 1"
            )
            assert len(list(ret)) == 0
            ret = ST_CleanupByTimeseries.get_all(
                campaign_id=campaign_2.id, in_timeseries_name="Timeseries 2"
            )
            assert len(list(ret)) == 1
            ret = ST_CleanupByTimeseries.get_all(in_timeseries_name="Timeseries")
            assert len(list(ret)) == 6

            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_2.id, sort=["+last_timestamp"]
                )
            )
            assert len(ret) == 3
            assert ret[0][1] == ts_8.id
            assert ret[0][4] == st_cbt_8_last_timestamp
            assert ret[1][1] == ts_5.id
            assert ret[1][4] == st_cbt_5_last_timestamp
            assert ret[2][4] is None
            ret = list(
                ST_CleanupByTimeseries.get_all(
                    campaign_id=campaign_2.id, sort=["-last_timestamp"]
                )
            )
            assert len(ret) == 3
            assert ret[0][1] == ts_5.id
            assert ret[0][4] == st_cbt_5_last_timestamp
            assert ret[1][1] == ts_8.id
            assert ret[1][4] == st_cbt_8_last_timestamp
            assert ret[2][4] is None

    @pytest.mark.usefixtures("st_cleanups_by_timeseries")
    def test_st_cleanup_by_timeseries_delete_cascade(self, users, timeseries):
        admin_user = users[0]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            assert len(list(ST_CleanupByTimeseries.get())) == 2
            ts_1.delete()
            db.session.commit()
            assert len(list(ST_CleanupByTimeseries.get())) == 1

    def test_st_cleanup_by_timeseries_filters_as_admin(
        self, users, campaigns, st_cleanups_by_timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        st_cbt_1 = st_cleanups_by_timeseries[0]

        with CurrentUser(admin_user):
            st_cbt_l = list(ST_CleanupByTimeseries.get(campaign_id=campaign_1.id))
            assert len(st_cbt_l) == 1
            assert st_cbt_l[0] == st_cbt_1

    def test_st_cleanup_by_timeseries_authorizations_as_admin(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with CurrentUser(admin_user):
            st_cbt_1 = ST_CleanupByTimeseries.new(timeseries_id=ts_1.id)
            db.session.commit()
            st_cbt = ST_CleanupByTimeseries.get_by_id(st_cbt_1.id)
            assert st_cbt.id == st_cbt_1.id
            st_cbts_ = list(ST_CleanupByTimeseries.get())
            assert len(st_cbts_) == 1
            assert st_cbts_[0].id == st_cbt_1.id
            st_cbt.update(timeseries_id=ts_2.id)
            st_cbt.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_st_cleanup_by_timeseries_authorizations_as_user(
        self, users, timeseries, st_cleanups_by_timeseries
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_2 = timeseries[1]
        st_cbt_1 = st_cleanups_by_timeseries[0]
        st_cbt_2 = st_cleanups_by_timeseries[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByTimeseries.new(timeseries_id=ts_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ST_CleanupByTimeseries.get_by_id(st_cbt_1.id)
            assert st_cbt_2 == ST_CleanupByTimeseries.get_by_id(st_cbt_2.id)
            stcs = list(ST_CleanupByTimeseries.get())
            assert stcs == [st_cbt_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbt_2.update(last_timestamp=dt.datetime.now(tz=dt.timezone.utc))
            with pytest.raises(BEMServerAuthorizationError):
                st_cbt_2.delete()


class TestCleanupScheduledTask:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_cleanup_scheduled_task(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        campaign_1 = campaigns[0]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ds_2 = TimeseriesDataState.get(name="Clean").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            ST_CleanupByCampaign.new(campaign_id=campaign_1.id)
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="12H")
        values = [0, 13]
        create_timeseries_data(ts_0, ds_1, timestamps, values)
        create_timeseries_data(ts_1, ds_1, timestamps, values)

        with OpenBar():

            assert ST_CleanupByTimeseries.get(timeseries_id=ts_0.id).first() is None
            assert ST_CleanupByTimeseries.get(timeseries_id=ts_1.id).first() is None

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
            st_cbt_1 = ST_CleanupByTimeseries.get(timeseries_id=ts_0.id).first()
            assert st_cbt_1.last_timestamp == dt.datetime(
                2020, 1, 1, 12, tzinfo=dt.timezone.utc
            )

            # Campaign 2 (disabled), TS 1 -> no clean data
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_1,), ds_2)
            index = pd.DatetimeIndex([], name="timestamp", tz="UTC")
            no_data_df = pd.DataFrame({ts_1.id: []}, index=index)
            assert data_df.equals(no_data_df)
            assert ST_CleanupByTimeseries.get(timeseries_id=ts_1.id).first() is None

            # Run a second time to test the case where CBT already exists
            cleanup_scheduled_task.apply()

            st_cbt_1 = ST_CleanupByTimeseries.get(timeseries_id=ts_0.id).first()
            assert st_cbt_1.last_timestamp == dt.datetime(
                2020, 1, 1, 12, tzinfo=dt.timezone.utc
            )
