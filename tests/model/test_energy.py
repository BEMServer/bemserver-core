"""Energy tests"""

import pytest

from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.model import (
    Energy,
    EnergyConsumptionTimeseriesByBuilding,
    EnergyConsumptionTimeseriesBySite,
    EnergyEndUse,
    EnergyProductionTechnology,
    EnergyProductionTimeseriesByBuilding,
    EnergyProductionTimeseriesBySite,
)


class TestEnergyModel:
    def test_energy_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_properties = len(list(Energy.get()))
            energy_1 = Energy.new(name="Custom")
            db.session.commit()
            assert Energy.get_by_id(energy_1.id) == energy_1
            assert len(list(Energy.get())) == nb_ts_properties + 1
            energy_1.update(name="Super custom")
            energy_1.delete()
            db.session.commit()

    def test_energy_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(Energy.get())
            energy_1 = Energy.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                Energy.new(
                    name="Custom",
                )
            with pytest.raises(BEMServerAuthorizationError):
                energy_1.update(name="Super custom")
            with pytest.raises(BEMServerAuthorizationError):
                energy_1.delete()


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


class TestEnergyProductionTechnologyModel:
    def test_energy_production_technology_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_properties = len(list(EnergyProductionTechnology.get()))
            ept_1 = EnergyProductionTechnology.new(name="Custom")
            db.session.commit()
            assert EnergyProductionTechnology.get_by_id(ept_1.id) == ept_1
            assert len(list(EnergyProductionTechnology.get())) == nb_ts_properties + 1
            ept_1.update(name="Super custom")
            ept_1.delete()
            db.session.commit()

    def test_energy_production_technology_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(EnergyProductionTechnology.get())
            ept_1 = EnergyProductionTechnology.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyProductionTechnology.new(
                    name="Custom",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ept_1.update(name="Super custom")
            with pytest.raises(BEMServerAuthorizationError):
                ept_1.delete()


class TestEnergyConsumptionTimeseriesBySiteModel:
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_sites")
    def test_energy_consumption_timeseries_by_site_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 2
            energy_1.delete()
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
            energy_1 = Energy.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert not list(EnergyConsumptionTimeseriesBySite.get())
            ectbs_1 = EnergyConsumptionTimeseriesBySite.new(
                site_id=site_1.id,
                energy_id=energy_1.id,
                end_use_id=energy_end_use_1.id,
                timeseries_id=ts_1.id,
            )
            db.session.commit()
            EnergyConsumptionTimeseriesBySite.get_by_id(ectbs_1.id)
            ectbs_l = list(EnergyConsumptionTimeseriesBySite.get())
            assert len(ectbs_l) == 1
            ectbs_1.update(end_use_id=energy_end_use_2.id)
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
            energy_1 = Energy.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            ectbs_l = list(EnergyConsumptionTimeseriesBySite.get())
            ectbs_2 = EnergyConsumptionTimeseriesBySite.get_by_id(ectbs_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyConsumptionTimeseriesBySite.new(
                    site_id=site_2.id,
                    energy_id=energy_1.id,
                    end_use_id=energy_end_use_1.id,
                    timeseries_id=ts_2.id,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.update(end_use_id=energy_end_use_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.delete()


class TestEnergyConsumptionTimeseriesByBuildingModel:
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_buildings")
    def test_energy_consumption_timeseries_by_building_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 2
            energy_1.delete()
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
            energy_1 = Energy.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            assert not list(EnergyConsumptionTimeseriesByBuilding.get())
            ectbb_1 = EnergyConsumptionTimeseriesByBuilding.new(
                building_id=building_1.id,
                energy_id=energy_1.id,
                end_use_id=energy_end_use_1.id,
                timeseries_id=ts_1.id,
            )
            db.session.commit()
            EnergyConsumptionTimeseriesByBuilding.get_by_id(ectbb_1.id)
            ectbb_l = list(EnergyConsumptionTimeseriesByBuilding.get())
            assert len(ectbb_l) == 1
            ectbb_1.update(end_use_id=energy_end_use_2.id)
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
            energy_1 = Energy.get()[0]
            energy_end_use_1 = EnergyEndUse.get()[0]
            energy_end_use_2 = EnergyEndUse.get()[1]

            ectbb_l = list(EnergyConsumptionTimeseriesByBuilding.get())
            ectbb_2 = EnergyConsumptionTimeseriesByBuilding.get_by_id(ectbb_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyConsumptionTimeseriesByBuilding.new(
                    building_id=building_2.id,
                    energy_id=energy_1.id,
                    end_use_id=energy_end_use_1.id,
                    timeseries_id=ts_2.id,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbb_2.update(end_use_id=energy_end_use_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ectbb_2.delete()


class TestEnergyProductionTimeseriesBySiteModel:
    @pytest.mark.usefixtures("energy_production_timeseries_by_sites")
    def test_energy_production_timeseries_by_site_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            assert len(list(EnergyProductionTimeseriesBySite.get())) == 2
            energy_1.delete()
            db.session.commit()
            assert len(list(EnergyProductionTimeseriesBySite.get())) == 1
            energy_prod_tech_2.delete()
            db.session.commit()
            assert len(list(EnergyProductionTimeseriesBySite.get())) == 0

    def test_energy_production_timeseries_by_site_authorizations_as_admin(
        self, users, timeseries, sites
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_prod_tech_1 = EnergyProductionTechnology.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            assert not list(EnergyProductionTimeseriesBySite.get())
            ectbs_1 = EnergyProductionTimeseriesBySite.new(
                site_id=site_1.id,
                energy_id=energy_1.id,
                prod_tech_id=energy_prod_tech_1.id,
                timeseries_id=ts_1.id,
            )
            db.session.commit()
            EnergyProductionTimeseriesBySite.get_by_id(ectbs_1.id)
            ectbs_l = list(EnergyProductionTimeseriesBySite.get())
            assert len(ectbs_l) == 1
            ectbs_1.update(prod_tech_id=energy_prod_tech_2.id)
            ectbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("energy_production_timeseries_by_sites")
    def test_energy_production_timeseries_by_site_authorizations_as_user(
        self, users, timeseries, sites
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        site_2 = sites[1]

        with CurrentUser(user_1):
            energy_1 = Energy.get()[0]
            energy_prod_tech_1 = EnergyProductionTechnology.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            ectbs_l = list(EnergyProductionTimeseriesBySite.get())
            ectbs_2 = EnergyProductionTimeseriesBySite.get_by_id(ectbs_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyProductionTimeseriesBySite.new(
                    site_id=site_2.id,
                    energy_id=energy_1.id,
                    prod_tech_id=energy_prod_tech_1.id,
                    timeseries_id=ts_2.id,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.update(prod_tech_id=energy_prod_tech_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.delete()


class TestEnergyProductionTimeseriesByBuildingModel:
    @pytest.mark.usefixtures("energy_production_timeseries_by_buildings")
    def test_energy_production_timeseries_by_building_delete_cascade(self, users):
        admin_user = users[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            assert len(list(EnergyProductionTimeseriesByBuilding.get())) == 2
            energy_1.delete()
            db.session.commit()
            assert len(list(EnergyProductionTimeseriesByBuilding.get())) == 1
            energy_prod_tech_2.delete()
            db.session.commit()
            assert len(list(EnergyProductionTimeseriesByBuilding.get())) == 0

    def test_energy_production_timeseries_by_building_authorizations_as_admin(
        self, users, timeseries, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        building_1 = buildings[0]

        with CurrentUser(admin_user):
            energy_1 = Energy.get()[0]
            energy_prod_tech_1 = EnergyProductionTechnology.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            assert not list(EnergyProductionTimeseriesByBuilding.get())
            eptbb_1 = EnergyProductionTimeseriesByBuilding.new(
                building_id=building_1.id,
                energy_id=energy_1.id,
                prod_tech_id=energy_prod_tech_1.id,
                timeseries_id=ts_1.id,
            )
            db.session.commit()
            EnergyProductionTimeseriesByBuilding.get_by_id(eptbb_1.id)
            eptbb_l = list(EnergyProductionTimeseriesByBuilding.get())
            assert len(eptbb_l) == 1
            eptbb_1.update(prod_tech_id=energy_prod_tech_2.id)
            eptbb_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("energy_production_timeseries_by_buildings")
    def test_energy_production_timeseries_by_building_authorizations_as_user(
        self, users, timeseries, buildings
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        building_2 = buildings[1]

        with CurrentUser(user_1):
            energy_1 = Energy.get()[0]
            energy_prod_tech_1 = EnergyProductionTechnology.get()[0]
            energy_prod_tech_2 = EnergyProductionTechnology.get()[1]

            eptbb_l = list(EnergyProductionTimeseriesByBuilding.get())
            eptbb_2 = EnergyProductionTimeseriesByBuilding.get_by_id(eptbb_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EnergyProductionTimeseriesByBuilding.new(
                    building_id=building_2.id,
                    energy_id=energy_1.id,
                    prod_tech_id=energy_prod_tech_1.id,
                    timeseries_id=ts_2.id,
                )
            with pytest.raises(BEMServerAuthorizationError):
                eptbb_2.update(prod_tech_id=energy_prod_tech_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                eptbb_2.delete()
