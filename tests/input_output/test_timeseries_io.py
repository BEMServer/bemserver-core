"""Timeseries I/O tests"""

import io

import pytest

from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerCoreCSVIOError,
    BEMServerCoreIOError,
    TimeseriesCSVIOError,
)
from bemserver_core.input_output import timeseries_csv_io
from bemserver_core.model import (
    Timeseries,
    TimeseriesByBuilding,
    TimeseriesBySite,
    TimeseriesBySpace,
    TimeseriesByStorey,
    TimeseriesByZone,
    TimeseriesPropertyData,
)

DUMMY_ID = 69
DUMMY_NAME = "Dummy name"


class TestTimeseriesCSVIO:
    def test_timeseries_csv_io_import_csv(
        self,
        users,
        campaigns,
        sites,
        buildings,
        storeys,
        spaces,
        zones,
        campaign_scopes,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]
        space_1 = spaces[0]
        zone_1 = zones[0]

        timeseries = db.session.query(Timeseries).all()
        assert not timeseries

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,"
            "Storey,Space,Zone,Min,Max\n"
            f"Temp 1,Temperature,°C,{cs_1.name},{site_1.name},{building_1.name},"
            f"{storey_1.name},{space_1.name},,-10,60\n"
            f"Temp 2,Temperature,°C,{cs_1.name},{site_1.name},{building_1.name},"
            f"{storey_1.name},,{zone_1.name},,60\n"
            f"Temp 3,Temperature,°C,{cs_1.name},{site_1.name},{building_1.name},"
            f",,{zone_1.name},,\n"
            f"Temp 4,Temperature,°C,{cs_1.name},{site_1.name},,"
            f",,{zone_1.name},,\n"
        )
        timeseries_csv = io.StringIO(timeseries_csv)

        with CurrentUser(admin_user):
            timeseries_csv_io.import_csv(
                campaign_1,
                timeseries_csv=timeseries_csv,
            )

        timeseries = db.session.query(Timeseries).all()
        assert len(timeseries) == 4

        timeseries = db.session.query(TimeseriesBySite).all()
        assert len(timeseries) == 1

        timeseries = db.session.query(TimeseriesByBuilding).all()
        assert len(timeseries) == 1

        timeseries = db.session.query(TimeseriesByStorey).all()
        assert len(timeseries) == 1

        timeseries = db.session.query(TimeseriesBySpace).all()
        assert len(timeseries) == 1

        timeseries = db.session.query(TimeseriesByZone).all()
        assert len(timeseries) == 3

        timeseries_property_data = db.session.query(TimeseriesPropertyData).all()
        assert len(timeseries_property_data) == 3

        timeseries_2 = db.session.query(Timeseries).filter_by(name="Temp 2").first()
        timeseries_2_property_data = (
            db.session.query(TimeseriesPropertyData)
            .filter_by(timeseries_id=timeseries_2.id)
            .all()
        )
        assert len(timeseries_2_property_data) == 1
        assert timeseries_2_property_data[0].value == "60"

    def test_timeseries_csv_io_import_csv_update(
        self,
        users,
        campaigns,
        campaign_scopes,
        sites,
        buildings,
        storeys,
        spaces,
        zones,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]
        space_1 = spaces[0]
        zone_1 = zones[0]

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone,Min\n"
            f"Temp 1,Temperature 1,,{cs_1.name},"
            f"{site_1.name},{building_1.name},{storey_1.name},{space_1.name},,12\n"
            f"Temp 1,Temperature 2,,{cs_1.name},"
            f"{site_1.name},{building_1.name},{storey_1.name},,{zone_1.name},42\n"
        )

        with CurrentUser(admin_user):
            timeseries_csv_io.import_csv(
                campaign_1,
                timeseries_csv=timeseries_csv,
            )

        timeseries = db.session.query(Timeseries).all()
        assert len(timeseries) == 1
        timeseries_1 = timeseries[0]
        assert timeseries_1.description == "Temperature 2"

        timeseries_property_data = db.session.query(TimeseriesPropertyData).all()
        assert len(timeseries_property_data) == 1
        assert timeseries_property_data[0].value == "42"
        assert timeseries_property_data[0].timeseries_id == timeseries_1.id

        for ts_relation_table in (
            TimeseriesBySite,
            TimeseriesByBuilding,
            TimeseriesBySpace,
        ):
            relations = db.session.query(ts_relation_table).all()
            assert not relations
        for ts_relation_table in (
            TimeseriesByStorey,
            TimeseriesByZone,
        ):
            relations = db.session.query(ts_relation_table).all()
            assert len(relations) == 1

    def test_timeseries_csv_io_import_csv_missing_column(
        self,
        users,
        campaigns,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        timeseries_csv = (
            "Name,Unit,Campaign scope,Site,Building,Storey,Space,Zone\n"
            "Space_1_Temp,°C,Campaign scope 1,Site 1,Building 1,"
            "Storey 1,Space 1,Zone 1\n"
        )
        timeseries_csv = io.StringIO(timeseries_csv)

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Missing columns"):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )

    def test_timeseries_csv_io_import_csv_empty_file(
        self,
        users,
        campaigns,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]

        timeseries_csv = ""
        timeseries_csv = io.StringIO(timeseries_csv)

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCSVIOError, match="Empty CSV file"):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )

    @pytest.mark.parametrize("missing", ("site", "building", "storey"))
    def test_timeseries_csv_io_import_csv_missing_parent(
        self,
        users,
        campaigns,
        sites,
        buildings,
        storeys,
        spaces,
        campaign_scopes,
        missing,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]
        space_1 = spaces[0]

        row = f"Space_1_Temp,Temperature,°C,{cs_1.name},"
        if missing == "site":
            row += f",{building_1.name},{storey_1.name},{space_1.name},,\n"
        elif missing == "building":
            row += f"{site_1.name},,{storey_1.name},{space_1.name},,\n"
        else:
            row += f"{site_1.name},{building_1.name},,{space_1.name},,\n"

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone\n"
            + row
        )

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesCSVIOError, match=f"Missing {missing}"):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )

    def test_timeseries_csv_io_import_csv_too_many_cols(
        self, users, campaigns, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone\n"
            f"Temp 1,,,{cs_1.name},,,,,,\n"
        )

        with CurrentUser(admin_user):
            timeseries_csv_io.import_csv(
                campaign_1,
                timeseries_csv=timeseries_csv,
            )

    @pytest.mark.parametrize(
        "unknown", ("campaign scope", "site", "building", "storey", "space")
    )
    def test_timeseries_csv_io_import_csv_unknown_fk(
        self,
        users,
        campaigns,
        sites,
        buildings,
        storeys,
        spaces,
        campaign_scopes,
        unknown,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]
        site_1 = sites[0]
        building_1 = buildings[0]
        storey_1 = storeys[0]
        space_1 = spaces[0]

        row = "Space_1_Temp,Temperature,°C,"
        if unknown == "campaign scope":
            row += (
                f"DUMMY_NAME,{site_1.name},{building_1.name},"
                f"{storey_1.name},{space_1.name},,\n"
            )
        elif unknown == "site":
            row += (
                f"{cs_1.name},DUMMY_NAME,{building_1.name},"
                f"{storey_1.name},{space_1.name},,\n"
            )
        elif unknown == "building":
            row += (
                f"{cs_1.name},{site_1.name},DUMMY_NAME,"
                "{storey_1.name},{space_1.name},,\n"
            )
        elif unknown == "storey":
            row += (
                f"{cs_1.name},{site_1.name},{building_1.name},"
                f"DUMMY_NAME,{space_1.name},,\n"
            )
        else:
            row += (
                f"{cs_1.name},{site_1.name},{building_1.name},"
                f"{storey_1.name},DUMMY_NAME,,\n"
            )

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone\n"
            + row
        )

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreIOError, match=f"Unknown {unknown}"):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )

    @pytest.mark.parametrize(
        ("ts_name", "row"),
        (
            # Name too long
            (100 * "A", 100 * "A" + ",,,{cs_name},,,,,\n"),
            # Invalid unit
            ("Test", "Test,,Dummy,{cs_name},,,,,\n"),
        ),
    )
    def test_timeseries_csv_io_import_csv_data_error(
        self, users, campaigns, campaign_scopes, ts_name, row
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone\n"
            + row.format(cs_name=cs_1.name)
        )

        with CurrentUser(admin_user):
            with pytest.raises(
                TimeseriesCSVIOError,
                match=f'Timeseries "{ts_name}" can\'t be created.',
            ):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )

    def test_timeseries_csv_io_import_csv_property_data_error(
        self, users, campaigns, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        timeseries_csv = (
            "Name,Description,Unit,Campaign scope,Site,Building,Storey,Space,Zone,"
            "Min,Max\n"
            f"Temp 1,,,{cs_1.name},,,,,,wrong,\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(
                TimeseriesCSVIOError,
                match='Timeseries "Temp 1" property "Min" can\'t be created.',
            ):
                timeseries_csv_io.import_csv(
                    campaign_1,
                    timeseries_csv=timeseries_csv,
                )
