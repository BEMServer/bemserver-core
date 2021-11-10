"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db
from bemserver_core.model.campaigns import (
    Campaign, TimeseriesByCampaign, TimeseriesByCampaignByUser
)
from bemserver_core.auth import AuthMixin, BEMServerAuthorizationError


class TimeseriesData(AuthMixin, Base):
    __tablename__ = "timeseries_data"
    __table_args__ = (
        sqla.PrimaryKeyConstraint("timeseries_id", "timestamp"),
    )

    timestamp = sqla.Column(
        sqla.DateTime(timezone=True)
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey('timeseries.id'),
        nullable=False,
    )
    timeseries = sqla.orm.relationship('Timeseries')
    value = sqla.Column(sqla.Float)

    @staticmethod
    def check_user_can_read_timeseries_data_for_campaign(
        user, campaign_id, timeseries_id
    ):
        """Check user can read timeseries data for a given campaign

        User can read timeseries data if
        - user can read campaign
        - timeseries in campaign
        """
        Campaign.check_user_can_read_campaign(user, campaign_id)
        stmt = sqla.select(TimeseriesByCampaign).where(
            TimeseriesByCampaign.timeseries_id == timeseries_id,
            TimeseriesByCampaign.campaign_id == campaign_id,
        )
        if not db.session.execute(stmt).all():
            raise BEMServerAuthorizationError("Timeseries not in Campaign")

    @staticmethod
    def check_user_can_write_timeseries_data_for_campaign(
        user, campaign_id, timeseries_id
    ):
        """Check user can write timeseries data for a given campaign

        User can write timeseries data if
        - user can read campaign
        - timeseries in campaign
        - user associated with timeseries x campaign association
        """
        Campaign.check_user_can_read_campaign(user, campaign_id)
        stmt = sqla.select(TimeseriesByCampaign).where(
            TimeseriesByCampaign.timeseries_id == timeseries_id,
            TimeseriesByCampaign.campaign_id == campaign_id,
        )
        tbc = db.session.execute(stmt).scalar()
        if tbc is None:
            raise BEMServerAuthorizationError("Timeseries not in Campaign")
        if not user.is_admin:
            stmt = sqla.select(TimeseriesByCampaignByUser).where(
                TimeseriesByCampaignByUser.timeseries_by_campaign_id == tbc.id,
                TimeseriesByCampaignByUser.user_id == user.id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError(
                    "User can't write TimeseriesData for Campaign")

    @staticmethod
    def _check_campaign(user, start_dt, end_dt, campaign_id=None):
        if not user.is_admin:
            # Check user specified campaign_id
            if not campaign_id:
                raise BEMServerAuthorizationError(
                    "User must specify Campaign ID")
        if campaign_id is not None:
            # Check campaign exists
            campaign = Campaign.get_by_id(campaign_id)
            if campaign is None:
                raise BEMServerAuthorizationError("Invalid Campaign ID")
            # Check date range is in campaign
            if (
                (campaign.start_time and start_dt < campaign.start_time) or
                (campaign.end_time and end_dt > campaign.end_time)
            ):
                raise BEMServerAuthorizationError("Time range out of Campaign")

    @classmethod
    def check_can_export(cls, start_dt, end_dt, timeseries, campaign_id=None):
        current_user = cls.current_user()
        TimeseriesData._check_campaign(
            current_user,
            start_dt,
            end_dt,
            campaign_id=campaign_id,
        )
        if campaign_id is not None:
            for ts_id in timeseries:
                (
                    TimeseriesData
                    .check_user_can_read_timeseries_data_for_campaign(
                        current_user, campaign_id, ts_id)
                )

    @classmethod
    def check_can_import(cls, start_dt, end_dt, timeseries, campaign_id=None):
        current_user = cls.current_user()
        TimeseriesData._check_campaign(
            current_user,
            start_dt,
            end_dt,
            campaign_id=campaign_id,
        )
        if campaign_id is not None:
            for ts_id in timeseries:
                (
                    TimeseriesData
                    .check_user_can_write_timeseries_data_for_campaign(
                        current_user, campaign_id, ts_id)
                )


sqla.event.listen(
    TimeseriesData.__table__,
    "after_create",
    sqla.DDL(
        "SELECT create_hypertable("
        "  '%(table)s',"
        "  'timestamp',"
        "  create_default_indexes => False"
        ");"
    )
)
