"""Sites I/O tests"""

import io

import pytest

from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerCoreCSVIOError,
    BEMServerCoreIOError,
    SitesCSVIOError,
)
from bemserver_core.input_output import sites_csv_io
from bemserver_core.model import (
    Building,
    BuildingPropertyData,
    Site,
    SitePropertyData,
    Space,
    SpacePropertyData,
    Storey,
    StoreyPropertyData,
    Zone,
    ZonePropertyData,
)

DUMMY_ID = 69
DUMMY_NAME = "Dummy name"


class TestSitesCSVIO:
    @pytest.mark.usefixtures("site_properties")
    def test_site_data_io_import_csv(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        assert not db.session.query(Site).all()

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Site 1,Great site 1,abcdefghijklmnopqrtsuv,1000\n"
            "Site 2,Great site 2,,\n"
        )
        csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

        sites = db.session.query(Site).all()
        assert len(sites) == 2
        site_1 = sites[0]

        site_property_data = db.session.query(SitePropertyData).all()
        assert len(site_property_data) == 1
        assert site_property_data[0].value == "1000"
        assert site_property_data[0].site_id == site_1.id

    @pytest.mark.usefixtures("site_properties")
    def test_site_data_io_import_csv_update(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Site 1,Great site 1,,12\n"
            "Site 1,Great site 2,,42\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

        sites = db.session.query(Site).all()
        assert len(sites) == 1
        site_1 = sites[0]
        assert site_1.description == "Great site 2"

        site_property_data = db.session.query(SitePropertyData).all()
        assert len(site_property_data) == 1
        assert site_property_data[0].value == "42"
        assert site_property_data[0].site_id == site_1.id

    def test_site_data_io_import_csv_missing_column(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name\nTest"

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    def test_site_data_io_import_csv_empty_file(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = ""

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Empty CSV file"):
                sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    def test_site_data_io_import_csv_unknown_property(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Site 1,Great site 1,,1000\n"
            "Site 2,Great site 2,,2000\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(SitesCSVIOError, match='Unknown property: "Area"'):
                sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    def test_site_data_io_import_csv_too_many_cols(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name,Description,IFC_ID\nSite 1,Great site 1,,"

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    def test_site_data_io_import_csv_data_error(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name,Description,IFC_ID\n" + 100 * "A" + ",Great site 1,\n"
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError, match=f'Site "{100 * "A"}" can\'t be created.'
            ):
                sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    @pytest.mark.usefixtures("site_properties")
    def test_site_data_io_import_csv_property_data_error(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\nSite 1,Great site 1,," + 200 * "A" + "\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError,
                match='Site "Site 1" property "Area" can\'t be created.',
            ):
                sites_csv_io.import_csv(campaign_1, sites_csv=csv_file)

    @pytest.mark.usefixtures("building_properties")
    def test_building_data_io_import_csv(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        assert not db.session.query(Building).all()

        csv_file = (
            "Name,Description,Site,IFC_ID,Area\n"
            f"Building 1,Great building 1,{site_1.name},abcdefghijklmnopqrtsuv,1000\n"
            f"Building 2,Great building 2,{site_1.name},,\n"
        )
        csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

        buildings = db.session.query(Building).all()
        assert len(buildings) == 2
        building_1 = buildings[0]

        building_property_data = db.session.query(BuildingPropertyData).all()
        assert len(building_property_data) == 1
        assert building_property_data[0].value == "1000"
        assert building_property_data[0].building_id == building_1.id

    @pytest.mark.usefixtures("building_properties")
    def test_building_data_io_import_csv_update(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,IFC_ID,Area\n"
            f"Building 1,Great building 1,{site_1.name},,12\n"
            f"Building 1,Great building 2,{site_1.name},,42\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

        buildings = db.session.query(Building).all()
        assert len(buildings) == 1
        building_1 = buildings[0]
        assert building_1.description == "Great building 2"

        building_property_data = db.session.query(BuildingPropertyData).all()
        assert len(building_property_data) == 1
        assert building_property_data[0].value == "42"
        assert building_property_data[0].building_id == building_1.id

    def test_building_data_io_import_csv_missing_column(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = f"Name,Site,IFC_ID\nBuilding 1,{site_1.name}\n"

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    def test_building_data_io_import_csv_unknown_property(
        self, users, campaigns, sites
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,IFC_ID,Area\n"
            f"Building 1,Great building 1,{site_1.name},,1000\n"
            f"Building 2,Great building 2,{site_1.name},,2000\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(SitesCSVIOError, match='Unknown property: "Area"'):
                sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    def test_building_data_io_import_csv_unknown_site(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,Site,IFC_ID\n"
            "Building 1,Great building 1,Dummy,\n"
            "Building 2,Great building 2,Dummy,\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreIOError, match='Unknown site: "Dummy"'):
                sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    def test_building_data_io_import_csv_too_many_cols(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,IFC_ID\n"
            f"Building 1,Great building 1,{site_1.name},,\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    def test_building_data_io_import_csv_data_error(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,IFC_ID\n"
            + 100 * "A"
            + f",Great building 1,{site_1.name},\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError, match=f'Building "{100 * "A"}" can\'t be created.'
            ):
                sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    @pytest.mark.usefixtures("building_properties")
    def test_building_data_io_import_csv_property_data_error(
        self, users, campaigns, sites
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,IFC_ID,Area\n"
            f"Building 1,Great building 1,{site_1.name},," + 200 * "A" + "\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError,
                match='Building "Building 1" property "Area" can\'t be created.',
            ):
                sites_csv_io.import_csv(campaign_1, buildings_csv=csv_file)

    @pytest.mark.usefixtures("storey_properties")
    def test_storey_data_io_import_csv(self, users, campaigns, sites, buildings):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        assert not db.session.query(Storey).all()

        csv_file = (
            "Name,Description,Site,Building,IFC_ID,Area\n"
            f"Storey 1,Great storey 1,{site_1.name},{building_1.name},"
            "abcdefghijklmnopqrtsuv,1000\n"
            f"Storey 2,Great storey 2,{site_1.name},{building_1.name},"
            ",\n"
        )
        csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

        storeys = db.session.query(Storey).all()
        assert len(storeys) == 2
        storey_1 = storeys[0]

        storey_property_data = db.session.query(StoreyPropertyData).all()
        assert len(storey_property_data) == 1
        assert storey_property_data[0].value == "1000"
        assert storey_property_data[0].storey_id == storey_1.id

    @pytest.mark.usefixtures("storey_properties")
    def test_storey_data_io_import_csv_update(self, users, campaigns, sites, buildings):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID,Area\n"
            f"Storey 1,Great storey 1,{site_1.name},{building_1.name},,12\n"
            f"Storey 1,Great storey 2,{site_1.name},{building_1.name},,42\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

        storeys = db.session.query(Storey).all()
        assert len(storeys) == 1
        storey_1 = storeys[0]
        assert storey_1.description == "Great storey 2"

        storey_property_data = db.session.query(StoreyPropertyData).all()
        assert len(storey_property_data) == 1
        assert storey_property_data[0].value == "42"
        assert storey_property_data[0].storey_id == storey_1.id

    def test_storey_data_io_import_csv_missing_column(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = f"Name,Site,Building\nStorey 1,{site_1.name},{building_1.name}\n"

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    def test_storey_data_io_import_csv_unknown_property(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID,Area\n"
            f"Storey 1,Great storey 1,{site_1.name},{building_1.name},,1000\n"
            f"Storey 2,Great storey 2,{site_1.name},{building_1.name},,2000\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(SitesCSVIOError, match='Unknown property: "Area"'):
                sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    def test_storey_data_io_import_csv_unknown_building(self, users, campaigns, sites):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID\n"
            f"Storey 1,Great storey 1,{site_1.name},Dummy,\n"
            f"Storey 2,Great storey 2,{site_1.name},Dummy,\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(
                BEMServerCoreIOError, match='Unknown building: "Site 1/Dummy"'
            ):
                sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    def test_storey_data_io_import_csv_too_many_cols(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID\n"
            f"Storey 1,Great storey 1,{site_1.name},{building_1.name},,\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    def test_storey_data_io_import_csv_data_error(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID\n"
            + 100 * "A"
            + f",Great storey 1,{site_1.name},{building_1.name},\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError, match=f'Storey "{100 * "A"}" can\'t be created.'
            ):
                sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    @pytest.mark.usefixtures("storey_properties")
    def test_storey_data_io_import_csv_property_data_error(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,IFC_ID,Area\n"
            f"Storey 1,Great storey 1,{site_1.name},{building_1.name},,"
            + 200 * "A"
            + "\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError,
                match='Storey "Storey 1" property "Area" can\'t be created.',
            ):
                sites_csv_io.import_csv(campaign_1, storeys_csv=csv_file)

    @pytest.mark.usefixtures("space_properties")
    def test_space_data_io_import_csv(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        assert not db.session.query(Space).all()

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID,Area\n"
            "Space 1,Great space 1,"
            f"{site_1.name},{building_1.name},{storey_1.name},"
            "abcdefghijklmnopqrtsuv,1000\n"
            "Space 2,Great space 2,"
            f"{site_1.name},{building_1.name},{storey_1.name},"
            ",\n"
        )
        csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

        spaces = db.session.query(Space).all()
        assert len(spaces) == 2
        space_1 = spaces[0]

        space_property_data = db.session.query(SpacePropertyData).all()
        assert len(space_property_data) == 1
        assert space_property_data[0].value == "1000"
        assert space_property_data[0].space_id == space_1.id

    @pytest.mark.usefixtures("space_properties")
    def test_space_data_io_import_csv_update(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID,Area\n"
            "Space 1,Great space 1,"
            f"{site_1.name},{building_1.name},{storey_1.name},,12\n"
            "Space 1,Great space 2,"
            f"{site_1.name},{building_1.name},{storey_1.name},,42\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

        spaces = db.session.query(Space).all()
        assert len(spaces) == 1
        space_1 = spaces[0]
        assert space_1.description == "Great space 2"

        space_property_data = db.session.query(SpacePropertyData).all()
        assert len(space_property_data) == 1
        assert space_property_data[0].value == "42"
        assert space_property_data[0].space_id == space_1.id

    def test_space_data_io_import_csv_missing_column(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Site,Building,Storey\n"
            f"Space 1,{site_1.name},{building_1.name},{storey_1.name}\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    def test_space_data_io_import_csv_unknown_property(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID,Area\n"
            "Space 1,Great space 1,"
            f"{site_1.name},{building_1.name},{storey_1.name},,1000\n"
            "Space 2,Great space 2,"
            f"{site_1.name},{building_1.name},{storey_1.name},,2000\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(SitesCSVIOError, match='Unknown property: "Area"'):
                sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    def test_space_data_io_import_csv_unknown_storey(
        self, users, campaigns, sites, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID\n"
            f"Storey 1,Great space 1,{site_1.name},{building_1.name},Dummy,\n"
            f"Storey 2,Great space 2,{site_1.name},{building_1.name},Dummy,\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(
                BEMServerCoreIOError, match='Unknown storey: "Site 1/Building 1/Dummy"'
            ):
                sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    def test_space_data_io_import_csv_too_many_cols(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID\n"
            "Space 1,Great storey 1,"
            f"{site_1.name},{building_1.name},{storey_1.name},,\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    def test_space_data_io_import_csv_data_error(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID\n"
            + 100 * "A"
            + f",Great space 1,{site_1.name},{building_1.name},{storey_1.name},\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError, match=f'Space "{100 * "A"}" can\'t be created.'
            ):
                sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    @pytest.mark.usefixtures("space_properties")
    def test_space_data_io_import_csv_property_data_error(
        self, users, campaigns, sites, buildings, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]

        csv_file = (
            "Name,Description,Site,Building,Storey,IFC_ID,Area\n"
            f"Space 1,Great space 1,{site_1.name},{building_1.name},{storey_1.name},,"
            + 200 * "A"
            + "\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError,
                match='Space "Space 1" property "Area" can\'t be created.',
            ):
                sites_csv_io.import_csv(campaign_1, spaces_csv=csv_file)

    @pytest.mark.usefixtures("zone_properties")
    def test_zone_data_io_import_csv(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        assert not db.session.query(Zone).all()

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Zone 1,Great zone 1,abcdefghijklmnopqrtsuv,1000\n"
            "Zone 2,Great zone 2,,\n"
        )
        csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

        zones = db.session.query(Zone).all()
        assert len(zones) == 2
        zone_1 = zones[0]

        zone_property_data = db.session.query(ZonePropertyData).all()
        assert len(zone_property_data) == 1
        assert zone_property_data[0].zone_id == zone_1.id

    @pytest.mark.usefixtures("zone_properties")
    def test_zone_data_io_import_csv_update(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Zone 1,Great zone 1,,12\n"
            "Zone 1,Great zone 2,,42\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

        zones = db.session.query(Zone).all()
        assert len(zones) == 1
        zone_1 = zones[0]
        assert zone_1.description == "Great zone 2"

        zone_property_data = db.session.query(ZonePropertyData).all()
        assert len(zone_property_data) == 1
        assert zone_property_data[0].value == "42"
        assert zone_property_data[0].zone_id == zone_1.id

    def test_zone_data_io_import_csv_missing_column(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name\nTest"

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

    def test_zone_data_io_import_csv_unknown_property(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\n"
            "Zone 1,Great zone 1,,1000\n"
            "Zone 2,Great zone 2,,2000\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(SitesCSVIOError, match='Unknown property: "Area"'):
                sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

    def test_zone_data_io_import_csv_too_many_cols(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name,Description,IFC_ID\nZone 1,Great zone 1,,\n"

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

    def test_zone_data_io_import_csv_data_error(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = "Name,Description,IFC_ID\n" + 100 * "A" + ",Great zone 1,\n"
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError, match=f'Zone "{100 * "A"}" can\'t be created.'
            ):
                sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

    @pytest.mark.usefixtures("zone_properties")
    def test_zone_data_io_import_csv_property_data_error(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        csv_file = (
            "Name,Description,IFC_ID,Area\nZone 1,Great zone 1,," + 200 * "A" + "\n"
        )
        with CurrentUser(admin_user):
            with pytest.raises(
                SitesCSVIOError,
                match='Zone "Zone 1" property "Area" can\'t be created.',
            ):
                sites_csv_io.import_csv(campaign_1, zones_csv=csv_file)

    @pytest.mark.usefixtures("site_properties")
    @pytest.mark.usefixtures("building_properties")
    @pytest.mark.usefixtures("storey_properties")
    @pytest.mark.usefixtures("space_properties")
    @pytest.mark.usefixtures("zone_properties")
    def test_site_data_csv_io_import_csv(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        sites_csv = (
            "Name,Description,IFC_ID,Area\n"
            "Site 1,Great site 1,abcdefghijklmnopqrtsuv,1000\n"
            "Site 2,Great site 2,,2000\n"
        )
        buildings_csv = (
            "Name,Description,Site,IFC_ID,Area\n"
            "Building 1,Great building 1,Site 1,bcdefghijklmnopqrtsuvw,,1000\n"
            "Building 2,Great building 2,Site 2,,2000\n"
        )
        storeys_csv = (
            "Name,Description,Site,Building,IFC_ID,Area\n"
            "Storey 1,Great storey 1,Site 1,Building 1,cdefghijklmnopqrtsuvwx,1000\n"
            "Storey 2,Great storey 2,Site 2,Building 2,,2000\n"
        )
        spaces_csv = (
            "Name,Description,Site,Building,Storey,IFC_ID,Area\n"
            "Storey 1,Great storey 1,Site 1,Building 1,Storey 1,"
            "defghijklmnopqrtsuvwxy,1000\n"
            "Storey 2,Great storey 2,Site 2,Building 2,Storey 2,"
            ",2000\n"
        )
        zones_csv = (
            "Name,Description,IFC_ID,Area\n"
            "Zone 1,Great zone 1,efghijklmnopqrtsuvwxyz,1000\n"
            "Zone 2,Great zone 2,,2000\n"
        )

        with CurrentUser(admin_user):
            sites_csv_io.import_csv(
                campaign_1,
                sites_csv=sites_csv,
                buildings_csv=buildings_csv,
                storeys_csv=storeys_csv,
                spaces_csv=spaces_csv,
                zones_csv=zones_csv,
            )

        sites = db.session.query(Site).all()
        assert len(sites) == 2
        buildings = db.session.query(Building).all()
        assert len(buildings) == 2
        storeys = db.session.query(Storey).all()
        assert len(storeys) == 2
        spaces = db.session.query(Space).all()
        assert len(spaces) == 2
        zones = db.session.query(Zone).all()
        assert len(zones) == 2
