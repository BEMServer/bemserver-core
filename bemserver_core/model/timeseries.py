"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.model.campaigns import TimeseriesByCampaign, UserByCampaign
from bemserver_core.database import Base, db
from bemserver_core.auth import AuthMixin, BEMServerAuthorizationError


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit = sqla.Column(sqla.String(20))
    min_value = sqla.Column(sqla.Float)
    max_value = sqla.Column(sqla.Float)

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        current_user = cls.current_user()
        if not current_user.is_admin:
            if not campaign_id:
                raise BEMServerAuthorizationError(
                    "User must specify Campaign ID")
            # Check User can read campaign
            stmt = sqla.select(UserByCampaign).where(
                UserByCampaign.user_id == current_user.id,
                UserByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read Campaign")

        query = super().get(**kwargs)

        if campaign_id:
            query = query.join(TimeseriesByCampaign).filter_by(
                campaign_id=campaign_id)

        return query

    def check_read_permissions(self, current_user, campaign_id=None):
        """Check user can read timeseries"""
        if not current_user.is_admin:
            if campaign_id is None:
                raise BEMServerAuthorizationError("Campaign ID not provided")
            # Check User can read campaign
            stmt = sqla.select(UserByCampaign).where(
                UserByCampaign.user_id == current_user.id,
                UserByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read Campaign")
            # Check Timeseries is in Campaign
            stmt = sqla.select(TimeseriesByCampaign).where(
                TimeseriesByCampaign.timeseries_id == self.id,
                TimeseriesByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("Timeseries not in campaign")

    @classmethod
    def get_by_id(cls, item_id, campaign_id=None):
        if campaign_id is not None:
            # Check Timeseries is in Campaign
            stmt = sqla.select(TimeseriesByCampaign).where(
                TimeseriesByCampaign.timeseries_id == item_id,
                TimeseriesByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                return None
        return super().get_by_id(item_id, campaign_id=campaign_id)
