"""Site tests"""
import pytest

from bemserver_core.model import (
    StructuralElementProperty,
    SiteProperty,
    BuildingProperty,
    StoreyProperty,
    SpaceProperty,
    ZoneProperty,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    SitePropertyData,
    BuildingPropertyData,
    StoreyPropertyData,
    SpacePropertyData,
    ZonePropertyData,
    TimeseriesBySite,
    TimeseriesByBuilding,
    TimeseriesByStorey,
    TimeseriesBySpace,
    TimeseriesByZone,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestStructuralElementPropertyModel:
    @pytest.mark.usefixtures("site_property_data")
    @pytest.mark.usefixtures("building_property_data")
    @pytest.mark.usefixtures("storey_property_data")
    @pytest.mark.usefixtures("space_property_data")
    @pytest.mark.usefixtures("zone_property_data")
    def test_structural_element_property_delete_cascade(
        self, users, structural_element_properties
    ):
        """Test property delete cascades to property data

        StructuralElementProperty -> SiteProperty -> SitePropertyData
        and likewise for BuildingProperty,...
        """
        admin_user = users[0]
        sep_1 = structural_element_properties[0]

        with CurrentUser(admin_user):
            assert len(list(SiteProperty.get())) == 2
            assert len(list(SitePropertyData.get())) == 2
            assert len(list(BuildingProperty.get())) == 2
            assert len(list(BuildingPropertyData.get())) == 2
            assert len(list(StoreyProperty.get())) == 2
            assert len(list(StoreyPropertyData.get())) == 2
            assert len(list(SpaceProperty.get())) == 2
            assert len(list(SpacePropertyData.get())) == 2
            assert len(list(ZoneProperty.get())) == 2
            assert len(list(ZonePropertyData.get())) == 2

            sep_1.delete()
            db.session.commit()
            assert len(list(SiteProperty.get())) == 1
            assert len(list(SitePropertyData.get())) == 1
            assert len(list(BuildingProperty.get())) == 1
            assert len(list(BuildingPropertyData.get())) == 1
            assert len(list(StoreyProperty.get())) == 1
            assert len(list(StoreyPropertyData.get())) == 1
            assert len(list(SpaceProperty.get())) == 1
            assert len(list(SpacePropertyData.get())) == 1
            assert len(list(ZoneProperty.get())) == 1
            assert len(list(ZonePropertyData.get())) == 1

    def test_structural_element_property_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            sep_1 = StructuralElementProperty.new(name="Area")
            db.session.add(sep_1)
            db.session.commit()
            StructuralElementProperty.get_by_id(sep_1.id)
            seps = list(StructuralElementProperty.get())
            assert len(seps) == 1
            sep_1.update(name="Area")
            sep_1.delete()
            db.session.commit()

    def test_structural_element_property_authorizations_as_user(
        self, users, structural_element_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]

        with CurrentUser(user_1):
            sep = StructuralElementProperty.get_by_id(sep_1.id)
            assert sep.name == sep_1.name
            with pytest.raises(BEMServerAuthorizationError):
                StructuralElementProperty.new(
                    name="Whatever",
                )
            with pytest.raises(BEMServerAuthorizationError):
                sep_1.update(name="Whatever")
            with pytest.raises(BEMServerAuthorizationError):
                sep_1.delete()


class TestSitePropertyModel:
    def test_site_property_authorizations_as_admin(
        self, users, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            site_p_1 = SiteProperty.new(structural_element_property_id=sep_1.id)
            db.session.add(site_p_1)
            db.session.commit()
            SiteProperty.get_by_id(site_p_1.id)
            site_ps = list(SiteProperty.get())
            assert len(site_ps) == 1
            site_p_1.update(structural_element_property_id=sep_2.id)
            site_p_1.delete()
            db.session.commit()

    def test_site_property_authorizations_as_user(
        self, users, structural_element_properties, site_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]
        site_p_1 = site_properties[0]

        with CurrentUser(user_1):
            SiteProperty.get_by_id(site_p_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                SiteProperty.new(structural_element_property_id=sep_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                site_p_1.update(structural_element_property_id=sep_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                site_p_1.delete()


class TestBuildingPropertyModel:
    def test_building_property_authorizations_as_admin(
        self, users, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            building_p_1 = BuildingProperty.new(structural_element_property_id=sep_1.id)
            db.session.add(building_p_1)
            db.session.commit()
            BuildingProperty.get_by_id(building_p_1.id)
            building_ps = list(BuildingProperty.get())
            assert len(building_ps) == 1
            building_p_1.update(structural_element_property_id=sep_2.id)
            building_p_1.delete()
            db.session.commit()

    def test_building_property_authorizations_as_user(
        self, users, structural_element_properties, building_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]
        building_p_1 = building_properties[0]

        with CurrentUser(user_1):
            BuildingProperty.get_by_id(building_p_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                BuildingProperty.new(structural_element_property_id=sep_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                building_p_1.update(structural_element_property_id=sep_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                building_p_1.delete()


class TestStoreyPropertyModel:
    def test_storey_property_authorizations_as_admin(
        self, users, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            storey_p_1 = StoreyProperty.new(structural_element_property_id=sep_1.id)
            db.session.add(storey_p_1)
            db.session.commit()
            StoreyProperty.get_by_id(storey_p_1.id)
            storey_ps = list(StoreyProperty.get())
            assert len(storey_ps) == 1
            storey_p_1.update(structural_element_property_id=sep_2.id)
            storey_p_1.delete()
            db.session.commit()

    def test_storey_property_authorizations_as_user(
        self, users, structural_element_properties, storey_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]
        storey_p_1 = storey_properties[0]

        with CurrentUser(user_1):
            StoreyProperty.get_by_id(storey_p_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                StoreyProperty.new(structural_element_property_id=sep_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                storey_p_1.update(structural_element_property_id=sep_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                storey_p_1.delete()


class TestSpacePropertyModel:
    def test_space_property_authorizations_as_admin(
        self, users, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            space_p_1 = SpaceProperty.new(structural_element_property_id=sep_1.id)
            db.session.add(space_p_1)
            db.session.commit()
            SpaceProperty.get_by_id(space_p_1.id)
            space_ps = list(SpaceProperty.get())
            assert len(space_ps) == 1
            space_p_1.update(structural_element_property_id=sep_2.id)
            space_p_1.delete()
            db.session.commit()

    def test_space_property_authorizations_as_user(
        self, users, structural_element_properties, space_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]
        space_p_1 = space_properties[0]

        with CurrentUser(user_1):
            SpaceProperty.get_by_id(space_p_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                SpaceProperty.new(structural_element_property_id=sep_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                space_p_1.update(structural_element_property_id=sep_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                space_p_1.delete()


class TestZonePropertyModel:
    def test_zone_property_authorizations_as_admin(
        self, users, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            zone_p_1 = ZoneProperty.new(structural_element_property_id=sep_1.id)
            db.session.add(zone_p_1)
            db.session.commit()
            ZoneProperty.get_by_id(zone_p_1.id)
            zone_ps = list(ZoneProperty.get())
            assert len(zone_ps) == 1
            zone_p_1.update(structural_element_property_id=sep_2.id)
            zone_p_1.delete()
            db.session.commit()

    def test_zone_property_authorizations_as_user(
        self, users, structural_element_properties, zone_properties
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]
        zone_p_1 = zone_properties[0]

        with CurrentUser(user_1):
            ZoneProperty.get_by_id(zone_p_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ZoneProperty.new(structural_element_property_id=sep_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                zone_p_1.update(structural_element_property_id=sep_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                zone_p_1.delete()


class TestSiteModel:
    @pytest.mark.usefixtures("spaces")
    @pytest.mark.usefixtures("site_property_data")
    @pytest.mark.usefixtures("timeseries_by_sites")
    def test_site_delete_cascade(self, users, sites):
        admin_user = users[0]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            assert len(list(Building.get())) == 2
            assert len(list(Storey.get())) == 2
            assert len(list(Space.get())) == 2
            assert len(list(SitePropertyData.get())) == 2
            assert len(list(TimeseriesBySite.get())) == 2

            site_1.delete()
            db.session.commit()
            assert len(list(Building.get())) == 1
            assert len(list(Storey.get())) == 1
            assert len(list(Space.get())) == 1
            assert len(list(SitePropertyData.get())) == 1
            assert len(list(TimeseriesBySite.get())) == 1

    def test_site_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin

        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            site_1 = Site.new(
                name="Site 1",
                campaign_id=campaign_1.id,
            )
            db.session.add(site_1)
            db.session.commit()

            site = Site.get_by_id(site_1.id)
            assert site.id == site_1.id
            assert site.name == site_1.name
            sites = list(Site.get())
            assert len(sites) == 1
            assert sites[0].id == site_1.id
            site.update(name="Super site 1")
            site.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_site_authorizations_as_user(self, users, sites, campaigns):
        user_1 = users[1]
        assert not user_1.is_admin
        site_1 = sites[0]
        site_2 = sites[1]

        campaign_1 = campaigns[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Site.new(
                    name="Site 1",
                    campaign_id=campaign_1.id,
                )

            site = Site.get_by_id(site_2.id)
            sites = list(Site.get())
            assert len(sites) == 1
            assert sites[0].id == site_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Site.get_by_id(site_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                site.update(name="Super site 1")
            with pytest.raises(BEMServerAuthorizationError):
                site.delete()


class TestBuildingModel:
    @pytest.mark.usefixtures("spaces")
    @pytest.mark.usefixtures("building_property_data")
    @pytest.mark.usefixtures("timeseries_by_buildings")
    def test_building_delete_cascade(self, users, buildings):
        admin_user = users[0]
        building_1 = buildings[0]

        with CurrentUser(admin_user):
            assert len(list(Storey.get())) == 2
            assert len(list(Space.get())) == 2
            assert len(list(BuildingPropertyData.get())) == 2
            assert len(list(TimeseriesByBuilding.get())) == 2

            building_1.delete()
            db.session.commit()
            assert len(list(Storey.get())) == 1
            assert len(list(Space.get())) == 1
            assert len(list(BuildingPropertyData.get())) == 1
            assert len(list(TimeseriesByBuilding.get())) == 1

    def test_building_filters_as_admin(self, users, campaigns, buildings):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        building_1 = buildings[0]

        with CurrentUser(admin_user):
            buildings_l = list(Building.get(campaign_id=campaign_1.id))
            assert len(buildings_l) == 1
            assert buildings_l[0] == building_1

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_building_filters_as_user(self, users, campaigns, buildings):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        building_2 = buildings[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                list(Building.get(campaign_id=campaign_1.id))
            buildings_l = list(Building.get(campaign_id=campaign_2.id))
            assert len(buildings_l) == 1
            assert buildings_l[0] == building_2

    def test_building_authorizations_as_admin(self, users, sites):
        admin_user = users[0]
        assert admin_user.is_admin

        site_1 = sites[0]

        with CurrentUser(admin_user):
            building_1 = Building.new(
                name="Building 1",
                site_id=site_1.id,
            )
            db.session.add(building_1)
            db.session.commit()

            building = Building.get_by_id(building_1.id)
            assert building.id == building_1.id
            assert building.name == building_1.name
            buildings = list(Building.get())
            assert len(buildings) == 1
            assert buildings[0].id == building_1.id
            building.update(name="Super building 1")
            building.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_building_authorizations_as_user(self, users, buildings, sites):
        user_1 = users[1]
        assert not user_1.is_admin
        building_1 = buildings[0]
        building_2 = buildings[1]

        site_1 = sites[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Building.new(
                    name="Building 1",
                    site_id=site_1.id,
                )

            building = Building.get_by_id(building_2.id)
            buildings = list(Building.get())
            assert len(buildings) == 1
            assert buildings[0].id == building_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Building.get_by_id(building_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                building.update(name="Super building 1")
            with pytest.raises(BEMServerAuthorizationError):
                building.delete()


class TestStoreyModel:
    @pytest.mark.usefixtures("spaces")
    @pytest.mark.usefixtures("storey_property_data")
    @pytest.mark.usefixtures("timeseries_by_storeys")
    def test_storey_delete_cascade(self, users, storeys):
        admin_user = users[0]
        storey_1 = storeys[0]

        with CurrentUser(admin_user):
            assert len(list(Space.get())) == 2
            assert len(list(StoreyPropertyData.get())) == 2
            assert len(list(TimeseriesByStorey.get())) == 2

            storey_1.delete()
            db.session.commit()
            assert len(list(Space.get())) == 1
            assert len(list(StoreyPropertyData.get())) == 1
            assert len(list(TimeseriesByStorey.get())) == 1

    def test_storey_filters_as_admin(self, users, campaigns, sites, storeys):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        storey_1 = storeys[0]

        with CurrentUser(admin_user):
            storeys_l = list(Storey.get(campaign_id=campaign_1.id))
            assert len(storeys_l) == 1
            assert storeys_l[0] == storey_1
            storeys_l = list(Storey.get(site_id=site_1.id))
            assert len(storeys_l) == 1
            assert storeys_l[0] == storey_1

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_storey_filters_as_user(self, users, campaigns, sites, storeys):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        site_1 = sites[0]
        site_2 = sites[1]
        storey_2 = storeys[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                list(Storey.get(campaign_id=campaign_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                list(Storey.get(site_id=site_1.id))
            storeys_l = list(Storey.get(campaign_id=campaign_2.id))
            assert len(storeys_l) == 1
            assert storeys_l[0] == storey_2
            storeys_l = list(Storey.get(site_id=site_2.id))
            assert len(storeys_l) == 1
            assert storeys_l[0] == storey_2

    def test_storey_authorizations_as_admin(self, users, buildings):
        admin_user = users[0]
        assert admin_user.is_admin

        building_1 = buildings[0]

        with CurrentUser(admin_user):
            storey_1 = Storey.new(
                name="Storey 1",
                building_id=building_1.id,
            )
            db.session.add(storey_1)
            db.session.commit()

            storey = Storey.get_by_id(storey_1.id)
            assert storey.id == storey_1.id
            assert storey.name == storey_1.name
            storeys = list(Storey.get())
            assert len(storeys) == 1
            assert storeys[0].id == storey_1.id
            storey.update(name="Super storey 1")
            storey.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_storey_authorizations_as_user(self, users, storeys, buildings):
        user_1 = users[1]
        assert not user_1.is_admin
        storey_1 = storeys[0]
        storey_2 = storeys[1]

        building_1 = buildings[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Storey.new(
                    name="Storey 1",
                    building_id=building_1.id,
                )

            storey = Storey.get_by_id(storey_2.id)
            storeys = list(Storey.get())
            assert len(storeys) == 1
            assert storeys[0].id == storey_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Storey.get_by_id(storey_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                storey.update(name="Super storey 1")
            with pytest.raises(BEMServerAuthorizationError):
                storey.delete()


class TestSpaceModel:
    @pytest.mark.usefixtures("space_property_data")
    @pytest.mark.usefixtures("timeseries_by_spaces")
    def test_space_delete_cascade(self, users, spaces):
        admin_user = users[0]
        space_1 = spaces[0]

        with CurrentUser(admin_user):
            assert len(list(SpacePropertyData.get())) == 2
            assert len(list(TimeseriesBySpace.get())) == 2

            space_1.delete()
            db.session.commit()
            assert len(list(SpacePropertyData.get())) == 1
            assert len(list(TimeseriesBySpace.get())) == 1

    def test_space_filters_as_admin(self, users, campaigns, sites, buildings, spaces):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        space_1 = spaces[0]

        with CurrentUser(admin_user):
            spaces_l = list(Space.get(campaign_id=campaign_1.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_1
            spaces_l = list(Space.get(site_id=site_1.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_1
            spaces_l = list(Space.get(building_id=building_1.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_1

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_space_filters_as_user(self, users, campaigns, sites, buildings, spaces):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        site_1 = sites[0]
        site_2 = sites[1]
        building_1 = buildings[0]
        building_2 = buildings[1]
        space_2 = spaces[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                list(Space.get(campaign_id=campaign_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                list(Space.get(site_id=site_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                list(Space.get(building_id=building_1.id))
            spaces_l = list(Space.get(campaign_id=campaign_2.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_2
            spaces_l = list(Space.get(site_id=site_2.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_2
            spaces_l = list(Space.get(building_id=building_2.id))
            assert len(spaces_l) == 1
            assert spaces_l[0] == space_2

    def test_space_authorizations_as_admin(self, users, storeys):
        admin_user = users[0]
        assert admin_user.is_admin

        storey_1 = storeys[0]

        with CurrentUser(admin_user):
            space_1 = Space.new(
                name="Space 1",
                storey_id=storey_1.id,
            )
            db.session.add(space_1)
            db.session.commit()

            space = Space.get_by_id(space_1.id)
            assert space.id == space_1.id
            assert space.name == space_1.name
            spaces = list(Space.get())
            assert len(spaces) == 1
            assert spaces[0].id == space_1.id
            space.update(name="Super space 1")
            space.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_space_authorizations_as_user(self, users, spaces, storeys):
        user_1 = users[1]
        assert not user_1.is_admin
        space_1 = spaces[0]
        space_2 = spaces[1]

        storey_1 = storeys[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Space.new(
                    name="Space 1",
                    storey_id=storey_1.id,
                )

            space = Space.get_by_id(space_2.id)
            spaces = list(Space.get())
            assert len(spaces) == 1
            assert spaces[0].id == space_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Space.get_by_id(space_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                space.update(name="Super space 1")
            with pytest.raises(BEMServerAuthorizationError):
                space.delete()


class TestZoneModel:
    @pytest.mark.usefixtures("zone_property_data")
    @pytest.mark.usefixtures("timeseries_by_zones")
    def test_zone_delete_cascade(self, users, zones):
        admin_user = users[0]
        zone_1 = zones[0]

        with CurrentUser(admin_user):
            assert len(list(ZonePropertyData.get())) == 2
            assert len(list(TimeseriesByZone.get())) == 2

            zone_1.delete()
            db.session.commit()
            assert len(list(ZonePropertyData.get())) == 1
            assert len(list(TimeseriesByZone.get())) == 1

    def test_zone_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin

        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            zone_1 = Zone.new(
                name="Zone 1",
                campaign_id=campaign_1.id,
            )
            db.session.add(zone_1)
            db.session.commit()

            zone = Zone.get_by_id(zone_1.id)
            assert zone.id == zone_1.id
            assert zone.name == zone_1.name
            zones = list(Zone.get())
            assert len(zones) == 1
            assert zones[0].id == zone_1.id
            zone.update(name="Super zone 1")
            zone.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_zone_authorizations_as_user(self, users, zones, campaigns):
        user_1 = users[1]
        assert not user_1.is_admin
        zone_1 = zones[0]
        zone_2 = zones[1]

        campaign_1 = campaigns[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Zone.new(
                    name="Zone 1",
                    campaign_id=campaign_1.id,
                )

            zone = Zone.get_by_id(zone_2.id)
            zones = list(Zone.get())
            assert len(zones) == 1
            assert zones[0].id == zone_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Zone.get_by_id(zone_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                zone.update(name="Super zone 1")
            with pytest.raises(BEMServerAuthorizationError):
                zone.delete()


class TestSitePropertyDataModel:
    def test_site_property_data_authorizations_as_admin(
        self, users, sites, site_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        site_1 = sites[0]
        site_p_1 = site_properties[0]
        site_p_2 = site_properties[1]

        with CurrentUser(admin_user):
            site_p_1 = SitePropertyData.new(
                site_id=site_1.id,
                site_property_id=site_p_1.id,
                value=12,
            )
            db.session.add(site_p_1)
            db.session.commit()
            SitePropertyData.get_by_id(site_p_1.id)
            site_ps = list(SitePropertyData.get())
            assert len(site_ps) == 1
            site_p_1.update(structural_element_property_data_id=site_p_2.id)
            site_p_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_site_property_data_authorizations_as_user(
        self,
        users,
        sites,
        site_properties,
        site_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        site_2 = sites[1]
        site_p_1 = site_properties[0]
        spd_1 = site_property_data[0]
        spd_2 = site_property_data[1]

        with CurrentUser(user_1):
            SitePropertyData.get_by_id(spd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                SitePropertyData.get_by_id(spd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                SitePropertyData.new(
                    site_id=site_2.id,
                    site_property_id=site_p_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.delete()


class TestBuildingPropertyDataModel:
    def test_building_property_data_authorizations_as_admin(
        self, users, buildings, building_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        building_1 = buildings[0]
        building_p_1 = building_properties[0]
        building_p_2 = building_properties[1]

        with CurrentUser(admin_user):
            building_p_1 = BuildingPropertyData.new(
                building_id=building_1.id,
                building_property_id=building_p_1.id,
                value=12,
            )
            db.session.add(building_p_1)
            db.session.commit()
            BuildingPropertyData.get_by_id(building_p_1.id)
            building_ps = list(BuildingPropertyData.get())
            assert len(building_ps) == 1
            building_p_1.update(structural_element_property_data_id=building_p_2.id)
            building_p_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_building_property_data_authorizations_as_user(
        self,
        users,
        buildings,
        building_properties,
        building_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        building_2 = buildings[1]
        building_p_1 = building_properties[0]
        bpd_1 = building_property_data[0]
        bpd_2 = building_property_data[1]

        with CurrentUser(user_1):
            BuildingPropertyData.get_by_id(bpd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                BuildingPropertyData.get_by_id(bpd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                BuildingPropertyData.new(
                    building_id=building_2.id,
                    building_property_id=building_p_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                bpd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                bpd_2.delete()


class TestStoreyPropertyDataModel:
    def test_storey_property_data_authorizations_as_admin(
        self, users, storeys, storey_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        storey_1 = storeys[0]
        storey_p_1 = storey_properties[0]
        storey_p_2 = storey_properties[1]

        with CurrentUser(admin_user):
            storey_p_1 = StoreyPropertyData.new(
                storey_id=storey_1.id,
                storey_property_id=storey_p_1.id,
                value=12,
            )
            db.session.add(storey_p_1)
            db.session.commit()
            StoreyPropertyData.get_by_id(storey_p_1.id)
            storey_ps = list(StoreyPropertyData.get())
            assert len(storey_ps) == 1
            storey_p_1.update(structural_element_property_data_id=storey_p_2.id)
            storey_p_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_storey_property_data_authorizations_as_user(
        self,
        users,
        storeys,
        storey_properties,
        storey_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        storey_2 = storeys[1]
        storey_p_1 = storey_properties[0]
        spd_1 = storey_property_data[0]
        spd_2 = storey_property_data[1]

        with CurrentUser(user_1):
            StoreyPropertyData.get_by_id(spd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                StoreyPropertyData.get_by_id(spd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                StoreyPropertyData.new(
                    storey_id=storey_2.id,
                    storey_property_id=storey_p_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.delete()


class TestSpacePropertyDataModel:
    def test_space_property_data_authorizations_as_admin(
        self, users, spaces, space_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        space_1 = spaces[0]
        space_p_1 = space_properties[0]
        space_p_2 = space_properties[1]

        with CurrentUser(admin_user):
            space_p_1 = SpacePropertyData.new(
                space_id=space_1.id,
                space_property_id=space_p_1.id,
                value=12,
            )
            db.session.add(space_p_1)
            db.session.commit()
            SpacePropertyData.get_by_id(space_p_1.id)
            space_ps = list(SpacePropertyData.get())
            assert len(space_ps) == 1
            space_p_1.update(structural_element_property_data_id=space_p_2.id)
            space_p_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_space_property_data_authorizations_as_user(
        self,
        users,
        spaces,
        space_properties,
        space_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        space_2 = spaces[1]
        space_p_1 = space_properties[0]
        spd_1 = space_property_data[0]
        spd_2 = space_property_data[1]

        with CurrentUser(user_1):
            SpacePropertyData.get_by_id(spd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                SpacePropertyData.get_by_id(spd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                SpacePropertyData.new(
                    space_id=space_2.id,
                    space_property_id=space_p_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                spd_2.delete()


class TestZonePropertyDataModel:
    def test_zone_property_data_authorizations_as_admin(
        self, users, zones, zone_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        zone_1 = zones[0]
        zone_p_1 = zone_properties[0]
        zone_p_2 = zone_properties[1]

        with CurrentUser(admin_user):
            zone_p_1 = ZonePropertyData.new(
                zone_id=zone_1.id,
                zone_property_id=zone_p_1.id,
                value=12,
            )
            db.session.add(zone_p_1)
            db.session.commit()
            ZonePropertyData.get_by_id(zone_p_1.id)
            zone_ps = list(ZonePropertyData.get())
            assert len(zone_ps) == 1
            zone_p_1.update(structural_element_property_data_id=zone_p_2.id)
            zone_p_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_zone_property_data_authorizations_as_user(
        self,
        users,
        zones,
        zone_properties,
        zone_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        zone_2 = zones[1]
        zone_p_1 = zone_properties[0]
        zpd_1 = zone_property_data[0]
        zpd_2 = zone_property_data[1]

        with CurrentUser(user_1):
            ZonePropertyData.get_by_id(zpd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                ZonePropertyData.get_by_id(zpd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ZonePropertyData.new(
                    zone_id=zone_2.id,
                    zone_property_id=zone_p_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                zpd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                zpd_2.delete()
