"""Site tests"""
import sqlalchemy as sqla

import pytest

from bemserver_core.model import (
    StructuralElementProperty,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    StructuralElementPropertyData,
    TimeseriesBySite,
    TimeseriesByBuilding,
    TimeseriesByStorey,
    TimeseriesBySpace,
    TimeseriesByZone,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.common import PropertyType
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    PropertyTypeInvalidError,
)


class TestStructuralElementPropertyModel:
    @pytest.mark.usefixtures("site_property_data")
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
            assert len(list(StructuralElementPropertyData.get())) == 4

            sep_1.delete()
            db.session.commit()
            assert len(list(StructuralElementPropertyData.get())) == 3

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

    def test_structural_element_property_cannot_change_type(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            sep = StructuralElementProperty(
                name="New property",
                value_type=PropertyType.integer,
            )
            assert sep.id is None
            sep.value_type = PropertyType.float
            db.session.add(sep)
            db.session.commit()
            assert sep.id is not None
            sep.value_type = PropertyType.boolean
            db.session.add(sep)
            with pytest.raises(
                sqla.exc.IntegrityError,
                match="value_type cannot be modified",
            ):
                db.session.commit()


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
            assert len(list(StructuralElementPropertyData.get())) == 4
            assert len(list(TimeseriesBySite.get())) == 2

            site_1.delete()
            db.session.commit()
            assert len(list(Building.get())) == 1
            assert len(list(Storey.get())) == 1
            assert len(list(Space.get())) == 1
            assert len(list(StructuralElementPropertyData.get())) == 3
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
            assert len(list(StructuralElementPropertyData.get())) == 4
            assert len(list(TimeseriesByBuilding.get())) == 2

            building_1.delete()
            db.session.commit()
            assert len(list(Storey.get())) == 1
            assert len(list(Space.get())) == 1
            assert len(list(StructuralElementPropertyData.get())) == 3
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
            assert len(list(StructuralElementPropertyData.get())) == 4
            assert len(list(TimeseriesByStorey.get())) == 2

            storey_1.delete()
            db.session.commit()
            assert len(list(Space.get())) == 1
            assert len(list(StructuralElementPropertyData.get())) == 3
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
            assert len(list(StructuralElementPropertyData.get())) == 4
            assert len(list(TimeseriesBySpace.get())) == 2

            space_1.delete()
            db.session.commit()
            assert len(list(StructuralElementPropertyData.get())) == 3
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
            assert len(list(StructuralElementPropertyData.get())) == 4
            assert len(list(TimeseriesByZone.get())) == 2

            zone_1.delete()
            db.session.commit()
            assert len(list(StructuralElementPropertyData.get())) == 3
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


class TestStructuralElementPropertyDataModel:
    def test_structural_element_property_data_authorizations_as_admin(
        self, users, sites, structural_element_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        site_1 = sites[0]
        sep_1 = structural_element_properties[0]
        sep_2 = structural_element_properties[1]

        with CurrentUser(admin_user):
            sepd_1 = StructuralElementPropertyData.new(
                structural_element_id=site_1.id,
                structural_element_property_id=sep_1.id,
                value=12,
            )
            db.session.add(sepd_1)
            db.session.commit()
            StructuralElementPropertyData.get_by_id(sepd_1.id)
            sepds = list(StructuralElementPropertyData.get())
            assert len(sepds) == 1
            sepd_1.update(structural_element_property_data_id=sep_2.id)
            sepd_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_structural_element_property_data_authorizations_as_user(
        self,
        users,
        sites,
        structural_element_properties,
        site_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        site_2 = sites[1]
        sep_1 = structural_element_properties[0]
        sepd_1 = site_property_data[0]
        sepd_2 = site_property_data[1]

        with CurrentUser(user_1):
            print("======================================")
            seps = StructuralElementPropertyData.get()
            print(seps)
            seps = list(seps)
            print(seps)
            return

            StructuralElementPropertyData.get_by_id(sepd_2.id)

            with pytest.raises(BEMServerAuthorizationError):
                StructuralElementPropertyData.get_by_id(sepd_1.id)

            with pytest.raises(BEMServerAuthorizationError):
                StructuralElementPropertyData.new(
                    structural_element_id=site_2.id,
                    structural_element_property_id=sep_1.id,
                    value=12,
                )
            with pytest.raises(BEMServerAuthorizationError):
                sepd_2.update(value="69")
            with pytest.raises(BEMServerAuthorizationError):
                sepd_2.delete()


#     def test_site_property_data_type_validation_as_admin(
#         self, users, sites, site_properties
#     ):
#         admin_user = users[0]
#         assert admin_user.is_admin
#
#         site_1 = sites[0]
#         site_p_1 = site_properties[0]
#         site_p_2 = site_properties[1]
#         site_p_3 = site_properties[2]
#         site_p_4 = site_properties[3]
#
#         with CurrentUser(admin_user):
#             # Property value is expected to be an integer.
#             assert (
#                 site_p_1.structural_element_property.value_type is PropertyType.integer
#             )
#             site_pd_1 = SitePropertyData.new(
#                 site_id=site_1.id,
#                 site_property_id=site_p_1.id,
#                 value=42,
#             )
#             db.session.commit()
#             assert site_pd_1.value == "42"
#             site_pd_1.value = "666"
#             db.session.add(site_pd_1)
#             db.session.commit()
#             assert site_pd_1.value == "666"
#             # Invalid property value types.
#             for val in ["bad", "4.2", 4.2, False, None]:
#                 site_pd_1.value = val
#                 db.session.add(site_pd_1)
#                 with pytest.raises(PropertyTypeInvalidError):
#                     db.session.commit()
#                 assert site_pd_1.value == val
#                 db.session.rollback()
#
#             # Property value is expected to be a float.
#             assert site_p_2.structural_element_property.value_type is PropertyType.float
#             site_pd_2 = SitePropertyData.new(
#                 site_id=site_1.id,
#                 site_property_id=site_p_2.id,
#                 value=4.2,
#             )
#             db.session.commit()
#             assert site_pd_2.value == "4.2"
#             for val, exp_res in [("66.6", "66.6"), (42, "42")]:
#                 site_pd_2.value = val
#                 db.session.add(site_pd_2)
#                 db.session.commit()
#                 assert site_pd_2.value == exp_res
#             # Invalid property value types.
#             for val in ["bad", False, None]:
#                 site_pd_2.value = val
#                 db.session.add(site_pd_2)
#                 with pytest.raises(PropertyTypeInvalidError):
#                     db.session.commit()
#                 assert site_pd_2.value == val
#                 db.session.rollback()
#
#             # Property value is expected to be a boolean.
#             assert (
#                 site_p_3.structural_element_property.value_type is PropertyType.boolean
#             )
#             site_pd_3 = SitePropertyData.new(
#                 site_id=site_1.id,
#                 site_property_id=site_p_3.id,
#                 value="true",
#             )
#             db.session.commit()
#             assert site_pd_3.value == "true"
#             site_pd_3.value = "false"
#             db.session.add(site_pd_3)
#             db.session.commit()
#             assert site_pd_3.value == "false"
#             # Invalid property value types.
#             for val in [True, False, 1, 0, "1", "0", "bad", 42, None]:
#                 site_pd_3.value = val
#                 db.session.add(site_pd_3)
#                 with pytest.raises(PropertyTypeInvalidError):
#                     db.session.commit()
#                 assert site_pd_3.value == val
#                 db.session.rollback()
#
#             # Property value is expected to be a string.
#             assert (
#                 site_p_4.structural_element_property.value_type is PropertyType.string
#             )
#             site_pd_4 = SitePropertyData.new(
#                 site_id=site_1.id,
#                 site_property_id=site_p_4.id,
#                 value=12,
#             )
#             db.session.commit()
#             assert site_pd_4.value == "12"
#             for val, exp_res in [
#                 ("everything works", "everything works"),
#                 (True, "true"),
#             ]:
#                 site_pd_4.value = val
#                 db.session.add(site_pd_4)
#                 db.session.commit()
#                 assert site_pd_4.value == exp_res
#
#     def test_site_property_data_cannot_change_site_or_property(
#         self, users, sites, site_properties
#     ):
#         admin_user = users[0]
#         assert admin_user.is_admin
#
#         site_1 = sites[0]
#         site_2 = sites[1]
#         site_p_1 = site_properties[0]
#         site_p_2 = site_properties[1]
#
#         with CurrentUser(admin_user):
#             spd = SitePropertyData(
#                 site_id=site_1.id,
#                 site_property_id=site_p_1.id,
#                 value=12,
#             )
#             assert spd.id is None
#             spd.site_property_id = site_p_2.id
#             db.session.add(spd)
#             db.session.commit()
#             assert spd.id is not None
#             spd.site_id = site_2.id
#             db.session.add(spd)
#             with pytest.raises(
#                 sqla.exc.IntegrityError,
#                 match="site_id cannot be modified",
#             ):
#                 db.session.commit()
#             db.session.rollback()
#             spd.site_property_id = site_p_1.id
#             db.session.add(spd)
#             with pytest.raises(
#                 sqla.exc.IntegrityError,
#                 match="site_property_id cannot be modified",
#             ):
#                 db.session.commit()
#             db.session.rollback()
