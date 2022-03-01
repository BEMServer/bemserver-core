"""Timeseries tests"""
import pytest

from bemserver_core.model import (
    TimeseriesProperty,
    TimeseriesDataState,
    TimeseriesGroup,
    TimeseriesGroupByUser,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestTimeseriesPropertyModel:
    def test_timeseries_property_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            assert not list(TimeseriesProperty.get())
            ts_property_1 = TimeseriesProperty.new(name="Min")
            db.session.add(ts_property_1)
            db.session.commit()
            assert TimeseriesProperty.get_by_id(ts_property_1.id) == ts_property_1
            assert len(list(TimeseriesProperty.get())) == 1
            ts_property_1.update(name="Max")
            ts_property_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_properties")
    def test_timeseries_property_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(TimeseriesProperty.get())
            ts_property_1 = TimeseriesProperty.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesProperty.new(
                    name="Frequency",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.update(name="Mean")
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.delete()


class TestTimeseriesDataStateModel:
    def test_timeseries_data_state_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_data_states = len(list(TimeseriesDataState.get()))
            ts_data_state_1 = TimeseriesDataState.new(
                name="Quality",
            )
            db.session.add(ts_data_state_1)
            db.session.commit()
            TimeseriesDataState.get_by_id(ts_data_state_1.id)
            ts_data_states = list(TimeseriesDataState.get())
            assert len(ts_data_states) == nb_ts_data_states + 1
            ts_data_state_1.update(name="Qualität")
            ts_data_state_1.delete()
            db.session.commit()

    def test_timeseries_data_state_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_data_states = list(TimeseriesDataState.get())
            ts_data_state_1 = TimeseriesDataState.get_by_id(ts_data_states[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesDataState.new(
                    name="Quality",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ts_data_state_1.update(name="Qualität")
            with pytest.raises(BEMServerAuthorizationError):
                ts_data_state_1.delete()


class TestTimeseriesGroupModel:
    def test_timeseries_group_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            tsg_1 = TimeseriesGroup.new(
                name="TS Group 1",
            )
            db.session.add(tsg_1)
            db.session.commit()

            timeseries_group = TimeseriesGroup.get_by_id(tsg_1.id)
            assert timeseries_group.id == tsg_1.id
            assert timeseries_group.name == tsg_1.name
            timeseries_groups = list(TimeseriesGroup.get())
            assert len(timeseries_groups) == 1
            assert timeseries_groups[0].id == tsg_1.id
            timeseries_group.update(name="Super timeseries 1")
            timeseries_group.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_groups_by_users")
    def test_timeseries_group_authorizations_as_user(self, users, timeseries_groups):
        user_1 = users[1]
        assert not user_1.is_admin
        tsg_1 = timeseries_groups[0]
        tsg_2 = timeseries_groups[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroup.new(
                    name="TS Group 1",
                )

            timeseries_group = TimeseriesGroup.get_by_id(tsg_2.id)
            timeseries_group_list = list(TimeseriesGroup.get())
            assert len(timeseries_group_list) == 1
            assert timeseries_group_list[0].id == tsg_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroup.get_by_id(tsg_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries_group.update(name="Super timeseries 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries_group.delete()


class TestTimeseriesGroupByUserModel:
    def test_timeseries_group_by_user_authorizations_as_admin(
        self, users, timeseries_groups
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        timeseries_group_1 = timeseries_groups[0]
        timeseries_group_2 = timeseries_groups[1]

        with CurrentUser(admin_user):
            tgbu_1 = TimeseriesGroupByUser.new(
                user_id=user_1.id,
                timeseries_group_id=timeseries_group_1.id,
            )
            db.session.add(tgbu_1)
            db.session.commit()

            tgbu = TimeseriesGroupByUser.get_by_id(tgbu_1.id)
            assert tgbu.id == tgbu_1.id
            tgbus = list(TimeseriesGroupByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_1.id
            tgbu.update(timeseries_group_id=timeseries_group_2.id)
            tgbu.delete()

    def test_timeseries_group_by_user_authorizations_as_user(
        self, users, timeseries_groups, timeseries_groups_by_users
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        timeseries_group_1 = timeseries_groups[0]
        timeseries_group_2 = timeseries_groups[1]
        tgbu_1 = timeseries_groups_by_users[0]
        tgbu_2 = timeseries_groups_by_users[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroupByUser.new(
                    user_id=user_1.id,
                    timeseries_group_id=timeseries_group_2.id,
                )

            tgbu = TimeseriesGroupByUser.get_by_id(tgbu_2.id)
            tgbus = list(TimeseriesGroupByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroupByUser.get_by_id(tgbu_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.update(timeseries_group_id=timeseries_group_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.delete()


class TestTimeseriesModel:
    @pytest.mark.usefixtures("timeseries_groups_by_users")
    @pytest.mark.usefixtures("timeseries_groups_by_campaigns")
    def test_timeseries_filter_by_campaign_or_user(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        user_1 = users[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with CurrentUser(admin_user):
            ts_l = list(Timeseries.get(campaign_id=campaign_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

        with CurrentUser(admin_user):
            ts_l = list(Timeseries.get(user_id=user_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

        with CurrentUser(admin_user):
            ts_l = list(Timeseries.get(user_id=user_1.id, campaign_id=campaign_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

    def test_timeseries_authorizations_as_admin(self, users, timeseries_groups):
        admin_user = users[0]
        assert admin_user.is_admin
        tsg_1 = timeseries_groups[0]

        with CurrentUser(admin_user):
            ts_1 = Timeseries.new(
                name="Timeseries 1",
                group_id=tsg_1.id,
            )
            db.session.add(ts_1)
            db.session.commit()

            ts = Timeseries.get_by_id(ts_1.id)
            assert ts.id == ts_1.id
            assert ts.name == ts_1.name
            ts_l = list(Timeseries.get())
            assert len(ts_l) == 1
            assert ts_l[0].id == ts_1.id
            ts.update(name="Super timeseries 1")
            ts.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_groups_by_users")
    def test_timeseries_authorizations_as_user(
        self, users, timeseries, timeseries_groups
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        tsg_1 = timeseries_groups[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.new(
                    name="Timeseries 1",
                    group_id=tsg_1.id,
                )

            timeseries = Timeseries.get_by_id(ts_2.id)
            timeseries_list = list(Timeseries.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.get_by_id(ts_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.update(name="Super timeseries 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.delete()


class TestTimeseriesPropertyDataModel:
    def test_timeseries_property_data_authorizations_as_admin(
        self, users, timeseries, timeseries_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        tsp_1 = timeseries_properties[0]

        with CurrentUser(admin_user):
            assert not list(TimeseriesPropertyData.get())
            tspd_1 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_1.id,
                value=12,
            )
            db.session.add(tspd_1)
            db.session.commit()

            tspd = TimeseriesPropertyData.get_by_id(tspd_1.id)
            assert tspd.id == tspd_1.id
            tspd_l = list(TimeseriesPropertyData.get())
            assert len(tspd_l) == 1
            assert tspd_l[0].id == tspd.id
            tspd.update(value=42)
            tspd.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_groups_by_users")
    def test_timeseries_property_data_authorizations_as_user(
        self,
        users,
        timeseries_properties,
        timeseries,
        timeseries_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tsp_1 = timeseries_properties[0]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        tspd_1 = timeseries_property_data[0]

        with CurrentUser(user_1):
            assert not list(TimeseriesPropertyData.get(timeseries_id=ts_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesPropertyData.new(
                    timeseries_id=ts_2.id,
                    property_id=tsp_1.id,
                    value=12,
                )

            tspd_l = list(TimeseriesPropertyData.get(timeseries_id=ts_2.id))
            assert len(tspd_l) == 2
            tspd_2 = tspd_l[0]
            tspd = TimeseriesPropertyData.get_by_id(tspd_2.id)
            assert tspd.id == tspd_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesPropertyData.get_by_id(tspd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.update(data_state_id=2)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.delete()


class TestTimeseriesByDataStateModel:
    def test_timeseries_by_data_state_authorizations_as_admin(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            tsbds_1 = TimeseriesByDataState.new(
                timeseries_id=ts_1.id,
                data_state_id=1,
            )
            db.session.commit()

            tsbds = TimeseriesByDataState.get_by_id(tsbds_1.id)
            assert tsbds.id == tsbds_1.id
            assert tsbds.data_state_id == 1
            tsbds_l = list(TimeseriesByDataState.get())
            assert len(tsbds_l) == 1
            assert tsbds_l[0].id == tsbds.id
            tsbds.update(data_state_id=2)
            db.session.commit()
            tsbds.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_groups_by_users")
    def test_timeseries_by_data_state_authorizations_as_user(
        self, users, timeseries, timeseries_by_data_states
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with CurrentUser(user_1):
            timeseries_list = list(TimeseriesByDataState.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            tsbds = TimeseriesByDataState.get_by_id(ts_2.id)
            tsbds.update(data_state_id=2)
            db.session.commit()
            tsbds.delete()
            db.session.commit()
            TimeseriesByDataState.new(
                timeseries_id=ts_2.id,
                data_state_id=2,
            )
            db.session.commit()

            # Not member of group
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByDataState.new(
                    timeseries_id=ts_1.id,
                    data_state_id=1,
                )
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByDataState.get_by_id(ts_1.id)
