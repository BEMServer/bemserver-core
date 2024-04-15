"""Check outliers task tests"""

import datetime as dt

import pytest

import sqlalchemy as sqla

import pandas as pd

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.model import (
    Event,
    EventCategory,
    EventLevelEnum,
    TimeseriesByEvent,
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.scheduled_tasks.check_outliers import (
    ST_CheckOutliersByCampaign,
    check_outliers_ts_data,
)
from tests.utils import create_timeseries_data


class TestST_CheckOutliersByCampaignModel:
    @pytest.mark.usefixtures("st_check_outliers_by_campaigns")
    def test_st_check_outliers_by_campaign_get_all_as_admin(self, users, campaigns):
        admin_user = users[0]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(admin_user):
            assert len(list(ST_CheckOutliersByCampaign.get())) < len(campaigns)
            assert len(list(ST_CheckOutliersByCampaign.get_all())) == len(campaigns)

            assert len(list(ST_CheckOutliersByCampaign.get_all(is_enabled=True))) == 1
            assert len(list(ST_CheckOutliersByCampaign.get_all(is_enabled=False))) == 2

            ret = list(ST_CheckOutliersByCampaign.get_all(campaign_id=campaign_1.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CheckOutliersByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CheckOutliersByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckOutliersByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = list(ST_CheckOutliersByCampaign.get_all(in_campaign_name="1"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_1.name
            ret = list(ST_CheckOutliersByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckOutliersByCampaign.get_all(
                in_campaign_name="3", is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckOutliersByCampaign.get_all(
                    in_campaign_name="3", is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = list(
                ST_CheckOutliersByCampaign.get_all(in_campaign_name="non-existent")
            )
            assert len(ret) == 0

            ret = list(ST_CheckOutliersByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_1.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_3.name
            ret = list(ST_CheckOutliersByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 3
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            assert ret[2][2] == campaign_1.name
            ret = ST_CheckOutliersByCampaign.get_all(
                sort=["-campaign_name"], is_enabled=True
            )
            assert len(list(ret)) == 1
            ret = list(
                ST_CheckOutliersByCampaign.get_all(
                    sort=["-campaign_name"], is_enabled=False
                )
            )
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("st_check_outliers_by_campaigns")
    def test_st_check_outliers_by_campaign_get_all_as_user(self, users, campaigns):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_3 = campaigns[2]

        with CurrentUser(user_1):
            assert len(list(ST_CheckOutliersByCampaign.get())) == 1
            assert len(list(ST_CheckOutliersByCampaign.get_all())) == 2

            assert len(list(ST_CheckOutliersByCampaign.get_all(is_enabled=True))) == 0
            assert len(list(ST_CheckOutliersByCampaign.get_all(is_enabled=False))) == 2

            assert (
                len(list(ST_CheckOutliersByCampaign.get_all(campaign_id=campaign_1.id)))
                == 0
            )
            ret = list(ST_CheckOutliersByCampaign.get_all(campaign_id=campaign_3.id))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            ret = ST_CheckOutliersByCampaign.get_all(
                campaign_id=campaign_3.id, is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckOutliersByCampaign.get_all(
                    campaign_id=campaign_3.id, is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name

            assert (
                len(list(ST_CheckOutliersByCampaign.get_all(in_campaign_name="1"))) == 0
            )
            ret = list(ST_CheckOutliersByCampaign.get_all(in_campaign_name="3"))
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckOutliersByCampaign.get_all(
                in_campaign_name="3", is_enabled=True
            )
            assert len(list(ret)) == 0
            ret = list(
                ST_CheckOutliersByCampaign.get_all(
                    in_campaign_name="3", is_enabled=False
                )
            )
            assert len(ret) == 1
            assert ret[0][2] == campaign_3.name
            ret = ST_CheckOutliersByCampaign.get_all(in_campaign_name="non-existent")
            assert len(list(ret)) == 0

            ret = list(ST_CheckOutliersByCampaign.get_all(sort=["+campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_2.name
            assert ret[1][2] == campaign_3.name
            ret = list(ST_CheckOutliersByCampaign.get_all(sort=["-campaign_name"]))
            assert len(ret) == 2
            assert ret[0][2] == campaign_3.name
            assert ret[1][2] == campaign_2.name
            ret = ST_CheckOutliersByCampaign.get_all(
                sort=["-campaign_name"], is_enabled=True
            )
            assert len(list(ret)) == 0

    @pytest.mark.usefixtures("st_check_outliers_by_campaigns")
    def test_st_check_outliers_by_campaign_delete_cascade(self, users, campaigns):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            assert len(list(ST_CheckOutliersByCampaign.get())) == 2
            campaign_1.delete()
            db.session.commit()
            assert len(list(ST_CheckOutliersByCampaign.get())) == 1

    def test_st_check_outliers_by_campaign_authorizations_as_admin(
        self, users, campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            st_cbc_1 = ST_CheckOutliersByCampaign.new(campaign_id=campaign_1.id)
            db.session.commit()
            st_cbc = ST_CheckOutliersByCampaign.get_by_id(st_cbc_1.id)
            assert st_cbc.id == st_cbc_1.id
            st_cbcs_ = list(ST_CheckOutliersByCampaign.get())
            assert len(st_cbcs_) == 1
            assert st_cbcs_[0].id == st_cbc_1.id
            st_cbc.update(campaign_id=campaign_2.id)
            st_cbc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_st_check_outliers_by_campaign_authorizations_as_user(
        self, users, campaigns, st_check_outliers_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        st_cbc_1 = st_check_outliers_by_campaigns[0]
        st_cbc_2 = st_check_outliers_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ST_CheckOutliersByCampaign.new(campaign_id=campaign_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ST_CheckOutliersByCampaign.get_by_id(st_cbc_1.id)
            ST_CheckOutliersByCampaign.get_by_id(st_cbc_2.id)
            stcs = list(ST_CheckOutliersByCampaign.get())
            assert stcs == [st_cbc_2]
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                st_cbc_1.delete()


class TestCheckOutliersScheduledTask:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_check_outliers_ts_data(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        # Min/Max, no data
        ts_0 = timeseries[0]
        # None
        ts_1 = timeseries[1]
        # Max only
        ts_2 = timeseries[2]
        # Min/Max
        ts_3 = timeseries[3]

        assert ts_0.campaign_scope_id == ts_2.campaign_scope_id
        assert ts_1.campaign_scope_id == ts_3.campaign_scope_id

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            ts_p_max = TimeseriesProperty.get(name="Max").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_max.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=ts_p_max.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_max.id,
                value="42",
            )
            ec_data_outliers = EventCategory.get(name="Data outliers").first()
            ec_data_no_outliers = EventCategory.get(name="No data outliers").first()
            ST_CheckOutliersByCampaign.new(campaign_id=campaign_1.id)
            ST_CheckOutliersByCampaign.new(campaign_id=campaign_2.id)
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="5h")
        values = [0, 13, 33, 42, 69]
        create_timeseries_data(ts_1, ds_1, timestamps, values)
        create_timeseries_data(ts_2, ds_1, timestamps, values)
        create_timeseries_data(ts_3, ds_1, timestamps, values)

        with OpenBar():
            # Min ratio = 90 % -> 2 TS with outliers data (different campaign scopes)

            assert not list(Event.get())
            assert not list(TimeseriesByEvent.get())

            check_dt_1 = end_dt
            check_outliers_ts_data(check_dt_1, "day", 1, min_correctness_ratio=0.9)

            events = list(Event.get(category=ec_data_outliers))
            assert len(events) == 2
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_0.campaign_scope.id
            assert event_1.category == ec_data_outliers
            assert event_1.level == EventLevelEnum.WARNING
            assert event_1.timestamp == check_dt_1
            assert event_1.source == "BEMServer - Check outliers"
            assert (
                event_1.description
                == "The following timeseries have outliers: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_1))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id
            event_2 = events[1]
            assert event_2.campaign_scope_id == ts_1.campaign_scope.id
            assert event_2.category == ec_data_outliers
            assert event_2.level == EventLevelEnum.WARNING
            assert event_2.timestamp == check_dt_1
            assert event_2.source == "BEMServer - Check outliers"
            assert (
                event_2.description
                == "The following timeseries have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_2))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id

            # Min ratio = 90 % -> 3 TS with outliers data (2 in same campaign scope)

            # Set interval for ts_1 to detect (50 %) outliers data
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_min.id,
                value="12",
            )

            check_dt_2 = end_dt + dt.timedelta(seconds=1)
            check_outliers_ts_data(check_dt_2, "day", 1, min_correctness_ratio=0.9)

            # TS 0 Campaign scope
            # 1 new outliers event from last iteration (TS 2)
            # 1 already outliers event (TS 2)
            # 0 no outlier event (TS 0 was never outliers)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 2 outliers)
            assert events[0] == event_1
            # New event (TS 2 still outliers)
            event_3 = events[1]
            assert event_3.timestamp == check_dt_2
            assert event_3.level == EventLevelEnum.INFO
            assert event_3.source == "BEMServer - Check outliers"
            assert (
                event_3.description
                == "The following timeseries still have outliers: Timeseries 3"
            )
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            # No event because TS 0 was never outliers
            assert not events

            # TS 1 Campaign scope
            # 1 newly outliers event from last iteration (TS 3)
            # 1 newly outliers event (TS 1)
            # 1 already outliers event (TS 3)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.WARNING,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 3 outliers)
            assert events[0] == event_2
            # New event (TS 1 outliers)
            event_4 = events[1]
            assert event_4.timestamp == check_dt_2
            assert event_4.source == "BEMServer - Check outliers"
            assert (
                event_4.description
                == "The following timeseries have outliers: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_4))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.INFO,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 1
            # New event (TS 3 still having outliers)
            event_5 = events[0]
            assert event_5.timestamp == check_dt_2
            assert event_5.level == EventLevelEnum.INFO
            assert event_5.source == "BEMServer - Check outliers"
            assert (
                event_5.description
                == "The following timeseries still have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_5))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert not events

            # Min ratio = 70 % -> 1 TS with outliers data (TS 3)

            check_dt_3 = end_dt + dt.timedelta(seconds=2)
            check_outliers_ts_data(check_dt_3, "day", 1, min_correctness_ratio=0.7)

            # TS 0 Campaign scope
            # 1 no outlier event (TS 2)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                    timestamp=check_dt_3,
                )
            )
            assert not events
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                    timestamp=check_dt_3,
                )
            )
            assert len(events) == 1
            # TS 0 never gets a no outlier event because it was never outliers
            event_6 = events[0]
            assert event_6.level == EventLevelEnum.INFO
            assert event_6.source == "BEMServer - Check outliers"
            assert (
                event_6.description
                == "The following timeseries don't have outliers anymore: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_6))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id

            # TS 1 Campaign scope
            # 1 already outliers event (TS 3)
            # 1 no outliers event (TS 1)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    timestamp=check_dt_3,
                )
            )
            assert len(events) == 1
            event_7 = events[0]
            assert event_7.level == EventLevelEnum.INFO
            assert event_7.source == "BEMServer - Check outliers"
            assert (
                event_7.description
                == "The following timeseries still have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_7))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    timestamp=check_dt_3,
                )
            )
            assert len(events) == 1
            event_8 = events[0]
            assert event_8.level == EventLevelEnum.INFO
            assert event_8.source == "BEMServer - Check outliers"
            assert (
                event_8.description
                == "The following timeseries don't have outliers anymore: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_8))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
