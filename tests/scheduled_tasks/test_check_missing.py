"""Check missing data task tests"""
import datetime as dt

import pandas as pd

import pytest
from tests.utils import create_timeseries_data

from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
    Event,
    TimeseriesByEvent,
)
from bemserver_core.scheduled_tasks.check_missing import (
    ST_CheckMissingByCampaign,
    check_missing_ts_data,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestST_CheckMissingByCampaignModel:
    def test_st_check_missing_by_campaign_get_all_as_admin(
        self, users, campaigns, st_check_missings_by_campaigns
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(admin_user):
            assert len(list(ST_CheckMissingByCampaign.get())) < len(campaigns)
            assert len(list(ST_CheckMissingByCampaign.get_all())) == len(campaigns)

            assert len(list(ST_CheckMissingByCampaign.get_all(is_enabled=True))) == 1
            assert len(list(ST_CheckMissingByCampaign.get_all(is_enabled=False))) == 2

            ret = list(ST_CheckMissingByCampaign.get_all(campaign_id=campaign_1.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CheckMissingByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CheckMissingByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckMissingByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = list(ST_CheckMissingByCampaign.get_all(in_campaign_name="1"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CheckMissingByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckMissingByCampaign.get_all(
                in_campaign_name="3", is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckMissingByCampaign.get_all(
                    in_campaign_name="3", is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = list(
                ST_CheckMissingByCampaign.get_all(in_campaign_name="non-existent")
            )
            assert len(ret) == 0

            ret = list(ST_CheckMissingByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_1.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_3.name
            ret = list(ST_CheckMissingByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_1.name
            ret = ST_CheckMissingByCampaign.get_all(
                sort=["-campaign_name"], is_enabled=True
            )
            assert len(list(ret)) == 1
            ret = list(
                ST_CheckMissingByCampaign.get_all(
                    sort=["-campaign_name"], is_enabled=False
                )
            )
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_check_missing_by_campaign_get_all_as_user(
        self, users, campaigns, st_check_missings_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(user_1):
            assert len(list(ST_CheckMissingByCampaign.get())) == 1
            assert len(list(ST_CheckMissingByCampaign.get_all())) == 2

            assert len(list(ST_CheckMissingByCampaign.get_all(is_enabled=True))) == 0
            assert len(list(ST_CheckMissingByCampaign.get_all(is_enabled=False))) == 2

            assert (
                len(list(ST_CheckMissingByCampaign.get_all(campaign_id=campaign_1.id)))
                == 0
            )
            ret = list(ST_CheckMissingByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CheckMissingByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckMissingByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            assert (
                len(list(ST_CheckMissingByCampaign.get_all(in_campaign_name="1"))) == 0
            )
            ret = list(ST_CheckMissingByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckMissingByCampaign.get_all(
                in_campaign_name="3", is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckMissingByCampaign.get_all(
                    in_campaign_name="3", is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckMissingByCampaign.get_all(in_campaign_name="non-existent")
            assert len(list(ret)) == 0

            ret = list(ST_CheckMissingByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_2.name
            assert ret[1][2] == campaign_3.name
            ret = list(ST_CheckMissingByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            ret = ST_CheckMissingByCampaign.get_all(
                sort=["-campaign_name"], is_enabled=True
            )
            assert len(list(ret)) == 0

    def test_st_check_missing_by_campaign_delete_cascade(
        self, users, campaigns, st_check_missings_by_campaigns
    ):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            assert len(list(ST_CheckMissingByCampaign.get())) == 2
            campaign_1.delete()
            db.session.commit()
            assert len(list(ST_CheckMissingByCampaign.get())) == 1

    def test_st_check_missing_by_campaign_authorizations_as_admin(
        self, users, campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            st_cbc_1 = ST_CheckMissingByCampaign.new(campaign_id=campaign_1.id)
            db.session.add(st_cbc_1)
            db.session.commit()
            st_cbc = ST_CheckMissingByCampaign.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(ST_CheckMissingByCampaign.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(campaign_id=campaign_2.id)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_check_missing_by_campaign_authorizations_as_user(
        self, users, campaigns, st_check_missings_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        st_cbc_1 = st_check_missings_by_campaigns[0]
        st_cbc_2 = st_check_missings_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CheckMissingByCampaign.new(campaign_id=campaign_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ST_CheckMissingByCampaign.get_by_id(st_cbc_1.id)
            ST_CheckMissingByCampaign.get_by_id(st_cbc_2.id)
            stcs = list(ST_CheckMissingByCampaign.get())
            assert stcs == [st_cbc_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.delete()


class TestCheckMissingScheduledTask:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_check_missing_ts_data(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        # 10 min, full
        ts_0 = timeseries[0]
        # None, 50% missing
        ts_1 = timeseries[1]
        # 10 min, 50% missing
        ts_2 = timeseries[2]
        # None, no data
        ts_3 = timeseries[3]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            interval_prop = TimeseriesProperty.get(name="Interval").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=interval_prop.id,
                value="600",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=interval_prop.id,
                value="600",
            )
            ST_CheckMissingByCampaign.new(campaign_id=campaign_1.id)
            ST_CheckMissingByCampaign.new(campaign_id=campaign_2.id)
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        intermediate_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, end_dt, inclusive="left", freq="600S")
        values_1 = range(len(timestamps_1))
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = pd.date_range(
            start_dt, intermediate_dt, inclusive="left", freq="600S"
        )
        values_2 = range(len(timestamps_2))
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = pd.date_range(
            start_dt, intermediate_dt, inclusive="left", freq="600S"
        )
        values_3 = range(len(timestamps_3))
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with OpenBar():

            # Min ratio = 40 % -> 1 TS with missing data

            assert not list(Event.get())

            check_missing_ts_data(end_dt, "day", 1, min_completeness_ratio=0.4)

            events = list(Event.get())
            assert len(events) == 1
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_3.campaign_scope.id
            assert event_1.category == "Data missing"
            assert event_1.level == "WARNING"
            assert event_1.timestamp == end_dt
            assert event_1.source == "BEMServer - Check missing data"
            assert event_1.description == "Missing timeseries: ['Timeseries 4']"

            tbes = list(TimeseriesByEvent.get())
            assert len(tbes) == 1
            tbe_1 = tbes[0]
            assert tbe_1.event_id == event_1.id
            assert tbe_1.timeseries_id == ts_3.id

            event_1.delete()
            db.session.flush()

            # Min ratio = 90 % -> 2 TS with missing data (different campaign scope)

            assert not list(Event.get())
            assert not list(TimeseriesByEvent.get())

            check_missing_ts_data(end_dt, "day", 1, min_completeness_ratio=0.9)

            events = list(Event.get())
            assert len(events) == 2
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_2.campaign_scope.id
            assert event_1.category == "Data missing"
            assert event_1.level == "WARNING"
            assert event_1.timestamp == end_dt
            assert event_1.source == "BEMServer - Check missing data"
            assert event_1.description == "Missing timeseries: ['Timeseries 3']"
            event_2 = events[1]
            assert event_2.campaign_scope_id == ts_3.campaign_scope.id
            assert event_2.category == "Data missing"
            assert event_2.level == "WARNING"
            assert event_2.timestamp == end_dt
            assert event_2.source == "BEMServer - Check missing data"
            assert event_2.description == "Missing timeseries: ['Timeseries 4']"

            tbes = list(TimeseriesByEvent.get())
            assert len(tbes) == 2
            assert {tbe.timeseries_id for tbe in tbes} == {ts_2.id, ts_3.id}

            event_1.delete()
            event_2.delete()
            db.session.flush()

            # Min ratio = 90 % -> 3 TS with missing data (2 in same campaign scope)

            # Set interval for ts_1 to detect (50 %) missing data
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=interval_prop.id,
                value="600",
            )

            assert not list(Event.get())
            assert not list(TimeseriesByEvent.get())

            check_missing_ts_data(end_dt, "day", 1, min_completeness_ratio=0.9)

            events = list(Event.get())
            assert len(events) == 2
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_2.campaign_scope.id
            assert event_1.category == "Data missing"
            assert event_1.level == "WARNING"
            assert event_1.timestamp == end_dt
            assert event_1.source == "BEMServer - Check missing data"
            assert event_1.description == "Missing timeseries: ['Timeseries 3']"
            event_2 = events[1]
            assert event_2.campaign_scope_id == ts_3.campaign_scope.id
            assert event_2.category == "Data missing"
            assert event_2.level == "WARNING"
            assert event_2.timestamp == end_dt
            assert event_2.source == "BEMServer - Check missing data"
            assert (
                event_2.description
                == "Missing timeseries: ['Timeseries 2', 'Timeseries 4']"
            )
            tbes = list(TimeseriesByEvent.get())
            assert len(tbes) == 3
            tbe = list(TimeseriesByEvent.get(timeseries_id=ts_1.id))[0]
            assert tbe.event_id == event_2.id
            tbe = list(TimeseriesByEvent.get(timeseries_id=ts_2.id))[0]
            assert tbe.event_id == event_1.id
            tbe = list(TimeseriesByEvent.get(timeseries_id=ts_3.id))[0]
            assert tbe.event_id == event_2.id
