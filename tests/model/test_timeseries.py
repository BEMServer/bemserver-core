"""Timeseries tests"""
import pytest

from bemserver_core.model import (
    TimeseriesProperty,
    TimeseriesDataState,
    TimeseriesClusterGroup,
    TimeseriesClusterGroupByUser,
    TimeseriesCluster,
    TimeseriesClusterPropertyData,
    Timeseries,
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


class TestTimeseriesClusterGroupModel:
    def test_timeseries_cluster_group_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            tscg_1 = TimeseriesClusterGroup.new(
                name="TS Group 1",
            )
            db.session.add(tscg_1)
            db.session.commit()

            timeseries_cluster_group = TimeseriesClusterGroup.get_by_id(tscg_1.id)
            assert timeseries_cluster_group.id == tscg_1.id
            assert timeseries_cluster_group.name == tscg_1.name
            timeseries_cluster_groups = list(TimeseriesClusterGroup.get())
            assert len(timeseries_cluster_groups) == 1
            assert timeseries_cluster_groups[0].id == tscg_1.id
            timeseries_cluster_group.update(name="Super timeseries cluster 1")
            timeseries_cluster_group.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    def test_timeseries_cluster_group_authorizations_as_user(
        self, users, timeseries_cluster_groups
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tscg_1 = timeseries_cluster_groups[0]
        tscg_2 = timeseries_cluster_groups[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroup.new(
                    name="TS Group 1",
                )

            timeseries_cluster_group = TimeseriesClusterGroup.get_by_id(tscg_2.id)
            timeseries_cluster_group_list = list(TimeseriesClusterGroup.get())
            assert len(timeseries_cluster_group_list) == 1
            assert timeseries_cluster_group_list[0].id == tscg_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroup.get_by_id(tscg_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries_cluster_group.update(name="Super timeseries cluster 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries_cluster_group.delete()


class TestTimeseriesClusterGroupByUserModel:
    def test_timeseries_cluster_group_by_user_authorizations_as_admin(
        self, users, timeseries_cluster_groups
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        timeseries_cluster_group_1 = timeseries_cluster_groups[0]
        timeseries_cluster_group_2 = timeseries_cluster_groups[1]

        with CurrentUser(admin_user):
            tgbu_1 = TimeseriesClusterGroupByUser.new(
                user_id=user_1.id,
                timeseries_cluster_group_id=timeseries_cluster_group_1.id,
            )
            db.session.add(tgbu_1)
            db.session.commit()

            tgbu = TimeseriesClusterGroupByUser.get_by_id(tgbu_1.id)
            assert tgbu.id == tgbu_1.id
            tgbus = list(TimeseriesClusterGroupByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_1.id
            tgbu.update(timeseries_cluster_group_id=timeseries_cluster_group_2.id)
            tgbu.delete()

    def test_timeseries_cluster_group_by_user_authorizations_as_user(
        self, users, timeseries_cluster_groups, timeseries_cluster_groups_by_users
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        timeseries_cluster_group_1 = timeseries_cluster_groups[0]
        timeseries_cluster_group_2 = timeseries_cluster_groups[1]
        tgbu_1 = timeseries_cluster_groups_by_users[0]
        tgbu_2 = timeseries_cluster_groups_by_users[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroupByUser.new(
                    user_id=user_1.id,
                    timeseries_cluster_group_id=timeseries_cluster_group_2.id,
                )

            tgbu = TimeseriesClusterGroupByUser.get_by_id(tgbu_2.id)
            tgbus = list(TimeseriesClusterGroupByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroupByUser.get_by_id(tgbu_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.update(timeseries_cluster_group_id=timeseries_cluster_group_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.delete()


class TestTimeseriesClusterModel:
    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    @pytest.mark.usefixtures("timeseries_cluster_groups_by_campaigns")
    def test_timeseries_cluster_filter_by_campaign_or_user(
        self, users, timeseries_clusters, campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        user_1 = users[1]
        ts_1 = timeseries_clusters[0]
        ts_2 = timeseries_clusters[1]

        with CurrentUser(admin_user):
            ts_l = list(TimeseriesCluster.get(campaign_id=campaign_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

        with CurrentUser(admin_user):
            ts_l = list(TimeseriesCluster.get(user_id=user_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

        with CurrentUser(admin_user):
            ts_l = list(
                TimeseriesCluster.get(user_id=user_1.id, campaign_id=campaign_2.id)
            )
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

    def test_timeseries_cluster_authorizations_as_admin(
        self, users, timeseries_cluster_groups
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        tscg_1 = timeseries_cluster_groups[0]

        with CurrentUser(admin_user):
            ts_1 = TimeseriesCluster.new(
                name="Timeseries 1",
                group_id=tscg_1.id,
            )
            db.session.add(ts_1)
            db.session.commit()

            tsc = TimeseriesCluster.get_by_id(ts_1.id)
            assert tsc.id == ts_1.id
            assert tsc.name == ts_1.name
            ts_l = list(TimeseriesCluster.get())
            assert len(ts_l) == 1
            assert ts_l[0].id == ts_1.id
            tsc.update(name="Super timeseries cluster 1")
            tsc.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    def test_timeseries_cluster_authorizations_as_user(
        self, users, timeseries_clusters, timeseries_cluster_groups
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tsc_1 = timeseries_clusters[0]
        tsc_2 = timeseries_clusters[1]
        tscg_1 = timeseries_cluster_groups[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesCluster.new(
                    name="Timeseries 1",
                    group_id=tscg_1.id,
                )

            timeseries = TimeseriesCluster.get_by_id(tsc_2.id)
            timeseries_list = list(TimeseriesCluster.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == tsc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesCluster.get_by_id(tsc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.update(name="Super timeseries 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.delete()


class TestTimeseriesClusterPropertyDataModel:
    def test_timeseries_cluster_property_data_authorizations_as_admin(
        self, users, timeseries_clusters, timeseries_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        tsc_1 = timeseries_clusters[0]
        tsp_1 = timeseries_properties[0]

        with CurrentUser(admin_user):
            assert not list(TimeseriesClusterPropertyData.get())
            tscpd_1 = TimeseriesClusterPropertyData.new(
                cluster_id=tsc_1.id,
                property_id=tsp_1.id,
                value=12,
            )
            db.session.add(tscpd_1)
            db.session.commit()

            tscpd = TimeseriesClusterPropertyData.get_by_id(tscpd_1.id)
            assert tscpd.id == tscpd_1.id
            tscpd_l = list(TimeseriesClusterPropertyData.get())
            assert len(tscpd_l) == 1
            assert tscpd_l[0].id == tscpd.id
            tscpd.update(value=42)
            tscpd.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    def test_timeseries_cluster_property_data_authorizations_as_user(
        self,
        users,
        timeseries_properties,
        timeseries_clusters,
        timeseries_cluster_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tsp_1 = timeseries_properties[0]
        tsc_1 = timeseries_clusters[0]
        tsc_2 = timeseries_clusters[1]
        tscpd_1 = timeseries_cluster_property_data[0]

        with CurrentUser(user_1):
            assert not list(TimeseriesClusterPropertyData.get(cluster_id=tsc_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterPropertyData.new(
                    cluster_id=tsc_2.id,
                    property_id=tsp_1.id,
                    value=12,
                )

            tscpd_l = list(TimeseriesClusterPropertyData.get(cluster_id=tsc_2.id))
            assert len(tscpd_l) == 2
            tscpd_2 = tscpd_l[0]
            tscpd = TimeseriesClusterPropertyData.get_by_id(tscpd_2.id)
            assert tscpd.id == tscpd_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterPropertyData.get_by_id(tscpd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tscpd_2.update(data_state_id=2)
            with pytest.raises(BEMServerAuthorizationError):
                tscpd_2.delete()


class TestTimeseriesModel:
    def test_timeseries_authorizations_as_admin(self, users, timeseries_clusters):
        admin_user = users[0]
        assert admin_user.is_admin
        tsc_1 = timeseries_clusters[0]

        with CurrentUser(admin_user):
            ts_1 = Timeseries.new(
                cluster_id=tsc_1.id,
                data_state_id=1,
            )
            db.session.add(ts_1)
            db.session.commit()

            ts_ = Timeseries.get_by_id(ts_1.id)
            assert ts_.id == ts_1.id
            assert ts_.data_state_id == 1
            ts_l = list(Timeseries.get())
            assert len(ts_l) == 1
            assert ts_l[0].id == ts_.id
            ts_.update(data_state_id=2)
            ts_.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    def test_timeseries_authorizations_as_user(
        self, users, timeseries_clusters, timeseries
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        tsc_1 = timeseries_clusters[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.new(
                    cluster_id=tsc_1.id,
                    data_state_id=1,
                )

            timeseries = Timeseries.get_by_id(ts_2.id)
            timeseries_list = list(Timeseries.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.get_by_id(ts_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.update(data_state_id=2)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.delete()
