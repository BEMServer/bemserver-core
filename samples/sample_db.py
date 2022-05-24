"""This script generates data about Nobatek buildings

Most of the data is made up. The point is to fill a demo instance with realistic data.
"""
import os
import sys
from pathlib import Path
import datetime as dt
import logging

from bemserver_core import BEMServerCore
from bemserver_core.database import db
from bemserver_core.authorization import OpenBar
from bemserver_core import model
from bemserver_core.input_output import sites_csv_io, timeseries_csv_io


SAMPLE_FILES = Path(__file__).parent / "csv_files"


# Allow the use of a .env file to store SQLALCHEMY_DATABASE_URI environment variable
DOTENV_FILE = Path(__file__).parent.parent / ".env"
try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv(DOTENV_FILE)

logger = logging.getLogger("bemserver-create-sample-db")

DB_URL = os.getenv("SQLALCHEMY_DATABASE_URI")
if DB_URL is None:
    logger.error("SQLALCHEMY_DATABASE_URI environment variable not set.")
    sys.exit()


bsc = BEMServerCore()
bsc.init_auth()
db.set_db_url(DB_URL)

with OpenBar():

    # Create user groups / users

    ug_admins = model.UserGroup.new(name="BEMServer admins")
    ug_owners = model.UserGroup.new(name="Building owners")
    ug_occupants = model.UserGroup.new(name="Occupants")
    ug_bipv = model.UserGroup.new(name="BIPV maintainers")
    ug_bet = model.UserGroup.new(name="BET maintainers")
    ug_partners = model.UserGroup.new(name="Partners")
    db.session.flush()

    bet_1 = model.User.new(
        name="Jane",
        email="jane@doe.com",
        is_admin=False,
        is_active=False,
    )
    bet_1.set_password("D0e")
    occupant_1 = model.User.new(
        name="John",
        email="john@doe.com",
        is_admin=False,
        is_active=False,
    )
    occupant_1.set_password("D0e")
    db.session.flush()

    model.UserByUserGroup.new(
        user_id=bet_1.id,
        user_group_id=ug_bet.id,
    )
    model.UserByUserGroup.new(
        user_id=occupant_1.id,
        user_group_id=ug_occupants.id,
    )
    db.session.flush()

    # Create campaigns / campaign scopes

    campaign_epc = model.Campaign.new(
        name="Nobatek offices EPC",
        description="Nobatek buildings energy performance contracting",
        start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
    )
    campaign_bet = model.Campaign.new(
        name="BET windows tests",
        description="Innovative windows assessment",
        start_time=dt.datetime(2021, 6, 1, tzinfo=dt.timezone.utc),
        end_time=dt.datetime(2022, 7, 1, tzinfo=dt.timezone.utc),
    )
    db.session.flush()

    cs_1_weather = model.CampaignScope.new(
        name="Weather",
        campaign_id=campaign_epc.id,
    )
    cs_1_comfort = model.CampaignScope.new(
        name="Building comfort conditions",
        campaign_id=campaign_epc.id,
    )
    cs_1_energy = model.CampaignScope.new(
        name="Building energy consumptions",
        campaign_id=campaign_epc.id,
    )
    cs_1_bipv = model.CampaignScope.new(
        name="BIPV",
        campaign_id=campaign_epc.id,
    )

    cs_2_weather = model.CampaignScope.new(
        name="Weather",
        campaign_id=campaign_bet.id,
    )
    cs_2_bet_hall = model.CampaignScope.new(
        name="BET Hallway",
        campaign_id=campaign_bet.id,
    )
    cs_2_bet_cell_1 = model.CampaignScope.new(
        name="BET Cell 1",
        campaign_id=campaign_bet.id,
    )
    cs_2_bet_cell_2 = model.CampaignScope.new(
        name="BET Cell 2",
        campaign_id=campaign_bet.id,
    )
    db.session.flush()

    model.UserGroupByCampaign.new(
        user_group_id=ug_admins.id,
        campaign_id=campaign_epc.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_owners.id,
        campaign_id=campaign_epc.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_occupants.id,
        campaign_id=campaign_epc.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_bipv.id,
        campaign_id=campaign_epc.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_admins.id,
        campaign_id=campaign_bet.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_bet.id,
        campaign_id=campaign_bet.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_partners.id,
        campaign_id=campaign_bet.id,
    )
    db.session.flush()

    for cs in [cs_1_weather, cs_1_comfort, cs_1_energy, cs_1_bipv]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_owners.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_1_weather, cs_1_comfort]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_occupants.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_1_weather, cs_1_bipv]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_bipv.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_2_weather, cs_2_bet_hall, cs_2_bet_cell_1, cs_2_bet_cell_2]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_bet.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_2_weather, cs_2_bet_hall, cs_2_bet_cell_1]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_partners.id,
            campaign_scope_id=cs.id,
        )
    db.session.flush()

    # Create site properties

    sep_1 = model.StructuralElementProperty.new(name="Area")
    db.session.flush()

    site_p_1 = model.SiteProperty.new(structural_element_property_id=sep_1.id)
    building_p_1 = model.BuildingProperty.new(structural_element_property_id=sep_1.id)
    storey_p_1 = model.StoreyProperty.new(structural_element_property_id=sep_1.id)
    space_p_1 = model.SpaceProperty.new(structural_element_property_id=sep_1.id)
    zone_p_1 = model.ZoneProperty.new(structural_element_property_id=sep_1.id)
    db.session.flush()

    # Create timeseries properties

    model.TimeseriesProperty.new(name="Min")
    model.TimeseriesProperty.new(name="Max")
    db.session.flush()

    db.session.commit()

    for campaign, subdir in {campaign_epc: "epc", campaign_bet: "bet"}.items():
        with (
            open(SAMPLE_FILES / subdir / "sites.csv") as sites_csv,
            open(SAMPLE_FILES / subdir / "buildings.csv") as buildings_csv,
            open(SAMPLE_FILES / subdir / "storeys.csv") as storeys_csv,
            open(SAMPLE_FILES / subdir / "spaces.csv") as spaces_csv,
            open(SAMPLE_FILES / subdir / "zones.csv") as zones_csv,
        ):
            sites_csv_io.import_csv(
                campaign,
                sites_csv=sites_csv,
                buildings_csv=buildings_csv,
                storeys_csv=storeys_csv,
                spaces_csv=spaces_csv,
            )

        with open(SAMPLE_FILES / subdir / "timeseries.csv") as timeseries_csv:
            timeseries_csv_io.import_csv(campaign, timeseries_csv)
