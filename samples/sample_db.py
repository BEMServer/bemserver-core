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
try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv("bemserver-core/.env")

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

    user_group_1 = model.UserGroup.new(
        name="User group 1",
    )
    user_group_2 = model.UserGroup.new(
        name="User group 2",
    )
    db.session.flush()

    user_1 = model.User.new(
        name="Chuck",
        email="chuck@test.com",
        is_admin=True,
        is_active=True,
    )
    user_1.set_password("N0rr1s")
    user_2 = model.User.new(
        name="John",
        email="john@test.com",
        is_admin=False,
        is_active=True,
    )
    user_2.set_password("D0e")
    db.session.flush()

    ubug_1 = model.UserByUserGroup.new(
        user_id=user_1.id,
        user_group_id=user_group_1.id,
    )
    ubug_2 = model.UserByUserGroup.new(
        user_id=user_2.id,
        user_group_id=user_group_2.id,
    )
    db.session.flush()

    # Create campaigns / campaign scopes

    campaign_1 = model.Campaign.new(
        name="Campaign 1",
        start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
    )
    campaign_2 = model.Campaign.new(
        name="Campaign 2",
        start_time=dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
    )
    db.session.flush()

    cs_1_1 = model.CampaignScope.new(
        name="Scope 1",
        campaign_id=campaign_1.id,
    )
    cs_1_2 = model.CampaignScope.new(
        name="Scope 2",
        campaign_id=campaign_1.id,
    )
    cs_2_1 = model.CampaignScope.new(
        name="Scope 1",
        campaign_id=campaign_2.id,
    )
    cs_2_2 = model.CampaignScope.new(
        name="Scope 2",
        campaign_id=campaign_2.id,
    )
    db.session.flush()

    model.UserGroupByCampaign.new(
        user_group_id=user_group_1.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=user_group_2.id,
        campaign_id=campaign_1.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=user_group_1.id,
        campaign_id=campaign_2.id,
    )
    model.UserGroupByCampaign.new(
        user_group_id=user_group_2.id,
        campaign_id=campaign_2.id,
    )
    db.session.flush()

    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_1.id,
        campaign_scope_id=cs_1_1.id,
    )
    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_1.id,
        campaign_scope_id=cs_1_2.id,
    )
    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_1.id,
        campaign_scope_id=cs_2_1.id,
    )
    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_1.id,
        campaign_scope_id=cs_2_2.id,
    )
    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_2.id,
        campaign_scope_id=cs_1_1.id,
    )
    model.UserGroupByCampaignScope.new(
        user_group_id=user_group_2.id,
        campaign_scope_id=cs_1_2.id,
    )
    db.session.flush()

    # Create site properties

    sep_1 = model.StructuralElementProperty.new(
        name="Area",
    )
    db.session.flush()

    site_p_1 = model.SiteProperty.new(
        structural_element_property_id=sep_1.id,
    )
    db.session.flush()

    building_p_1 = model.BuildingProperty.new(
        structural_element_property_id=sep_1.id,
    )
    db.session.flush()

    storey_p_1 = model.StoreyProperty.new(
        structural_element_property_id=sep_1.id,
    )
    db.session.flush()

    space_p_1 = model.SpaceProperty.new(
        structural_element_property_id=sep_1.id,
    )
    db.session.flush()

    zone_p_1 = model.ZoneProperty.new(
        structural_element_property_id=sep_1.id,
    )
    db.session.flush()

    # Create timeseries properties

    ts_p_1 = model.TimeseriesProperty.new(
        name="Min",
    )
    ts_p_2 = model.TimeseriesProperty.new(
        name="Max",
    )
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
