"""Energy tests"""

import pytest

from bemserver_core.model import (
    EnergySource,
    EnergyEndUse,
    EnergyConsumptionTimeseriesBySite,
    EnergyConsumptionTimeseriesByBuilding,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestEnergySourceModel:
    def test_energy_source_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_properties = len(list(EnergySource.get()))
            energy_source_1 = EnergySource.new(name="Custom")
            db.session.commit()
            assert EnergySource.get_by_id(energy_source_1.id) == energy_source_1
            assert len(list(EnergySource.get())) == nb_ts_properties + 1
            energy_source_1.update(name="Super custom")
            energy_source_1.delete()
            db.session.commit()

    def test_energy_source_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(EnergySource.get())
            energy_source_1 = EnergySource.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergySource.new(
                    name="Custom",
                )
            with pytest.raises(BEMServerAuthorizationError):
                energy_source_1.update(name="Super custom")
            with pytest.raises(BEMServerAuthorizationError):
                energy_source_1.delete()


class TestEnergyEndUseModel:
    def test_energy_end_use_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_properties = len(list(EnergyEndUse.get()))
            energy_end_use_1 = EnergyEndUse.new(name="Custom")
            db.session.commit()
            assert EnergyEndUse.get_by_id(energy_end_use_1.id) == energy_end_use_1
            assert len(list(EnergyEndUse.get())) == nb_ts_properties + 1
            energy_end_use_1.update(name="Super custom")
            energy_end_use_1.delete()
            db.session.commit()

    def test_energy_end_use_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(EnergyEndUse.get())
            energy_end_use_1 = EnergyEndUse.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyEndUse.new(
                    name="Custom",
                )
            with pytest.raises(BEMServerAuthorizationError):
                energy_end_use_1.update(name="Super custom")
            with pytest.raises(BEMServerAuthorizationError):
                energy_end_use_1.delete()


class TestEnergyConsumptionTimeseriesBySiteModel:
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_sites")
    def test_energy_consumption_timeseries_by_site_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 2
            energy_source_1.delete()
            db.session.commit()
            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 1
            energy_end_use_2.delete()
            db.session.commit()
            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 0

    def test_energy_consumption_timeseries_by_site_authorizations_as_admin(
        self, users, timeseries, sites
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]

            assert not list(EnergyConsumptionTimeseriesBySite.get())
            ectbs_1 = EnergyConsumptionTimeseriesBySite.new(
                site_id=site_1.id,
                source_id=energy_source_1.id,
                end_use_id=energy_end_use_1.id,
                timeseries_id=ts_1.id,
                wh_conversion_factor=1000,
            )
            db.session.commit()
            EnergyConsumptionTimeseriesBySite.get_by_id(ectbs_1.id)
            ectbs_l = list(EnergyConsumptionTimeseriesBySite.get())
            assert len(ectbs_l) == 1
            ectbs_1.update(wh_conversion_factor=100)
            ectbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_sites")
    def test_energy_consumption_timeseries_by_site_authorizations_as_user(
        self, users, timeseries, sites
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        site_2 = sites[1]

        with CurrentUser(user_1):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]

            ectbs_l = list(EnergyConsumptionTimeseriesBySite.get())
            ectbs_2 = EnergyConsumptionTimeseriesBySite.get_by_id(ectbs_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyConsumptionTimeseriesBySite.new(
                    site_id=site_2.id,
                    source_id=energy_source_1.id,
                    end_use_id=energy_end_use_1.id,
                    timeseries_id=ts_2.id,
                    wh_conversion_factor=1000,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.update(wh_conversion_factor=100)
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.delete()


class TestEnergyConsumptionTimeseriesByBuildingModel:
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_buildings")
    def test_energy_consumption_timeseries_by_building_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 2
            energy_source_1.delete()
            db.session.commit()
            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 1
            energy_end_use_2.delete()
            db.session.commit()
            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 0

    def test_energy_consumption_timeseries_by_building_authorizations_as_admin(
        self, users, timeseries, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        building_1 = buildings[0]

        with CurrentUser(admin_user):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]

            assert not list(EnergyConsumptionTimeseriesByBuilding.get())
            ectbb_1 = EnergyConsumptionTimeseriesByBuilding.new(
                building_id=building_1.id,
                source_id=energy_source_1.id,
                end_use_id=energy_end_use_1.id,
                timeseries_id=ts_1.id,
                wh_conversion_factor=1000,
            )
            db.session.commit()
            EnergyConsumptionTimeseriesByBuilding.get_by_id(ectbb_1.id)
            ectbb_l = list(EnergyConsumptionTimeseriesByBuilding.get())
            assert len(ectbb_l) == 1
            ectbb_1.update(wh_conversion_factor=100)
            ectbb_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_buildings")
    def test_energy_consumption_timeseries_by_building_authorizations_as_user(
        self, users, timeseries, buildings
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        building_2 = buildings[1]

        with CurrentUser(user_1):
            energy_source_1 = EnergySource.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]

            ectbb_l = list(EnergyConsumptionTimeseriesByBuilding.get())
            ectbb_2 = EnergyConsumptionTimeseriesByBuilding.get_by_id(ectbb_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyConsumptionTimeseriesByBuilding.new(
                    building_id=building_2.id,
                    source_id=energy_source_1.id,
                    end_use_id=energy_end_use_1.id,
                    timeseries_id=ts_2.id,
                    wh_conversion_factor=1000,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbb_2.update(wh_conversion_factor=100)
            with pytest.raises(BEMServerAuthorizationError):
                ectbb_2.delete()
