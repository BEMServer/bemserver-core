"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db
from bemserver_core.model.campaigns import TimeseriesByCampaign
from bemserver_core.authorization import (
    auth, AuthMixin, BEMServerAuthorizationError,
    get_current_user, get_current_campaign)
from bemserver_core.exceptions import BEMServerCoreMissingCampaignError


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
    def _authorize(user, action, campaign, timeseries_id):
        stmt = sqla.select(TimeseriesByCampaign).where(
            TimeseriesByCampaign.timeseries_id == timeseries_id,
            TimeseriesByCampaign.campaign_id == campaign.id,
        )
        tbc = db.session.execute(stmt).scalar()
        if tbc is None:
            raise BEMServerAuthorizationError("Timeseries not in Campaign")
        auth.authorize(user, action, tbc)

    @classmethod
    def check_can_export(cls, start_dt, end_dt, timeseries):
        current_user = get_current_user()
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        else:
            current_campaign.auth_dates((start_dt, end_dt, ))
            for ts_id in timeseries:
                cls._authorize(
                    current_user, "read_data", current_campaign, ts_id)

    @classmethod
    def check_can_import(cls, start_dt, end_dt, timeseries):
        current_user = get_current_user()
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        else:
            current_campaign.auth_dates((start_dt, end_dt, ))
            for ts_id in timeseries:
                cls._authorize(
                    current_user, "write_data", current_campaign, ts_id)


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
