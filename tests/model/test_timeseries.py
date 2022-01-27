"""Timeseries tests"""
import pytest

from bemserver_core.model import TimeseriesGroup, Timeseries, TimeseriesGroupByUser
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestTimeseriesGroupModel:
    def test_timeseries_group_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            tg_1 = TimeseriesGroup.new(
                name="TS Group 1",
            )
            db.session.add(tg_1)
            db.session.commit()

            timeseries_group = TimeseriesGroup.get_by_id(tg_1.id)
            assert timeseries_group.id == tg_1.id
            assert timeseries_group.name == tg_1.name
            timeseries_groups = list(TimeseriesGroup.get())
            assert len(timeseries_groups) == 1
            assert timeseries_groups[0].id == tg_1.id
            timeseries_group.update(name="Super timeseries 1")
            timeseries_group.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("timeseries_groups_by_campaigns")
    def test_timeseries_group_authorizations_as_user(self, users, timeseries_groups):
        user_1 = users[1]
        assert not user_1.is_admin
        tg_1 = timeseries_groups[0]
        tg_2 = timeseries_groups[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroup.new(
                    name="TS Group 1",
                )

            timeseries_group = TimeseriesGroup.get_by_id(tg_2.id)
            timeseries_group_list = list(TimeseriesGroup.get())
            assert len(timeseries_group_list) == 1
            assert timeseries_group_list[0].id == tg_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesGroup.get_by_id(tg_1.id)
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
    @pytest.mark.usefixtures("timeseries_groups_by_campaigns")
    def test_timeseries_filter_by_campaign(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            timeseries = list(Timeseries.get(campaign_id=campaign_1.id))
            print(timeseries)

    def test_timeseries_authorizations_as_admin(self, users, timeseries_groups):
        admin_user = users[0]
        assert admin_user.is_admin
        tg_1 = timeseries_groups[0]

        with CurrentUser(admin_user):
            ts_1 = Timeseries.new(
                name="Timeseries 1",
                group_id=tg_1.id,
            )
            db.session.add(ts_1)
            db.session.commit()

            timeseries = Timeseries.get_by_id(ts_1.id)
            assert timeseries.id == ts_1.id
            assert timeseries.name == ts_1.name
            ts_l = list(Timeseries.get())
            assert len(ts_l) == 1
            assert ts_l[0].id == ts_1.id
            timeseries.update(name="Super timeseries 1")
            timeseries.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("timeseries_groups_by_campaigns")
    def test_timeseries_authorizations_as_user(
        self, users, timeseries, timeseries_groups
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        tg_1 = timeseries_groups[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.new(
                    name="Timeseries 1",
                    group_id=tg_1.id,
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
