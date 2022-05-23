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

    ug_admins = model.UserGroup.new(name="Admins")
    ug_owners = model.UserGroup.new(name="Owners")
    ug_occupants = model.UserGroup.new(name="Occupants")
    ug_bipv = model.UserGroup.new(name="BIPV maintainers")
    ug_partners = model.UserGroup.new(name="Partners")
    db.session.flush()

    admin_1 = model.User.new(
        name="Chuck",
        email="chuck@norris.com",
        is_admin=True,
        is_active=True,
    )
    admin_1.set_password("N0rr1s")
    occupant_1 = model.User.new(
        name="John",
        email="john@test.com",
        is_admin=False,
        is_active=True,
    )
    occupant_1.set_password("D0e")
    db.session.flush()

    model.UserByUserGroup.new(
        user_id=admin_1.id,
        user_group_id=ug_admins.id,
    )
    model.UserByUserGroup.new(
        user_id=occupant_1.id,
        user_group_id=ug_occupants.id,
    )
    db.session.flush()

    # Create campaigns / campaign scopes

    campaign_1 = model.Campaign.new(
        name="2020 campaign",
        start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
        end_time=dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
    )
    campaign_2 = model.Campaign.new(
        name="2021 - 2025 campaign",
        start_time=dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
        end_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
    )
    db.session.flush()

    cs_1_1 = model.CampaignScope.new(
        name="Weather",
        campaign_id=campaign_1.id,
    )
    cs_1_2 = model.CampaignScope.new(
        name="Building comfort conditions",
        campaign_id=campaign_1.id,
    )
    cs_1_3 = model.CampaignScope.new(
        name="Building energy consumptions",
        campaign_id=campaign_1.id,
    )
    cs_1_4 = model.CampaignScope.new(
        name="BIPV",
        campaign_id=campaign_1.id,
    )
    cs_2_1 = model.CampaignScope.new(
        name="Weather",
        campaign_id=campaign_2.id,
    )
    cs_2_2 = model.CampaignScope.new(
        name="Building comfort conditions",
        campaign_id=campaign_2.id,
    )
    cs_2_3 = model.CampaignScope.new(
        name="Building energy consumptions",
        campaign_id=campaign_2.id,
    )
    cs_2_4 = model.CampaignScope.new(
        name="BIPV",
        campaign_id=campaign_2.id,
    )
    db.session.flush()

    model.UserGroupByCampaign.new(
        user_group_id=ug_admins.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_owners.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_occupants.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_bipv.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_partners.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_admins.id,
        campaign_id=campaign_2.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_owners.id,
        campaign_id=campaign_2.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_occupants.id,
        campaign_id=campaign_2.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_bipv.id,
        campaign_id=campaign_2.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=ug_partners.id,
        campaign_id=campaign_2.id,
    )
    db.session.flush()

    for cs in [cs_1_1, cs_1_2, cs_1_3, cs_1_4, cs_2_1, cs_2_2, cs_2_3, cs_2_4]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_owners.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_1_1, cs_1_2, cs_2_1, cs_2_2]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_occupants.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_1_1, cs_1_4, cs_2_1, cs_2_4]:
        model.UserGroupByCampaignScope.new(
            user_group_id=ug_bipv.id,
            campaign_scope_id=cs.id,
        )
    for cs in [cs_1_1, cs_2_1]:
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

    for campaign in (campaign_1, campaign_2):
        with (
            open(SAMPLE_FILES / "sites.csv") as sites_csv,
            open(SAMPLE_FILES / "buildings.csv") as buildings_csv,
            open(SAMPLE_FILES / "storeys.csv") as storeys_csv,
            open(SAMPLE_FILES / "spaces.csv") as spaces_csv,
            open(SAMPLE_FILES / "zones.csv") as zones_csv,
        ):
            sites_csv_io.import_csv(
                campaign,
                sites_csv=sites_csv,
                buildings_csv=buildings_csv,
                storeys_csv=storeys_csv,
                spaces_csv=spaces_csv,
            )

        with open(SAMPLE_FILES / "timeseries.csv") as timeseries_csv:
            timeseries_csv_io.import_csv(campaign, timeseries_csv)
