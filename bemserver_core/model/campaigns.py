"""Campaings"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db
from bemserver_core.auth import (
    AuthMixin, CURRENT_USER, BEMServerAuthorizationError
)


class Campaign(AuthMixin, Base):
    __tablename__ = "campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    start_time = sqla.Column(sqla.DateTime(timezone=True))
    end_time = sqla.Column(sqla.DateTime(timezone=True))

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        query = super().get(**kwargs)
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            query = query.join(UserByCampaign).filter(
                UserByCampaign.user_id == current_user.id
            )
        return query

    def check_read_permissions(self, current_user, **kwargs):
        """Check user can read campaign"""
        if not current_user.is_admin:
            stmt = sqla.select(UserByCampaign).where(
                UserByCampaign.user_id == current_user.id,
                UserByCampaign.campaign_id == self.id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read campaign")


class UserByCampaign(AuthMixin, Base):
    """User x Campaign associations

    Users associated with a campaign have read permissions campaign-wise
    """
    __tablename__ = "users_by_campaigns"
    __table_args__ = (
        sqla.UniqueConstraint("campaign_id", "user_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"))
    user_id = sqla.Column(sqla.ForeignKey("users.id"))

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        query = super().get(**kwargs)
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            query = query.filter(UserByCampaign.user_id == current_user.id)
        return query

    def check_read_permissions(self, current_user, **kwargs):
        """Check user can read user by campaign"""
        if not current_user.is_admin and current_user.id != self.user_id:
            raise BEMServerAuthorizationError("User can't read UserByCampaign")


class TimeseriesByCampaign(AuthMixin, Base):
    """Timeseries x Campaign associations

    Timeseries associated with a campaign can be read by all campaign users
    for the campaign time range.
    """
    __tablename__ = "timeseries_by_campaigns"
    __table_args__ = (
        sqla.UniqueConstraint("campaign_id", "timeseries_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"))
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"))

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
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
            query = query.filter_by(campaign_id=campaign_id)

        return query

    def check_read_permissions(self, current_user, campaign_id=None):
        """Check user can read timeseries by campaign"""
        if current_user and not current_user.is_admin:
            if campaign_id is None:
                raise BEMServerAuthorizationError("Campaign ID not provided")
            # Check User can read campaign
            stmt = sqla.select(UserByCampaign).where(
                UserByCampaign.user_id == current_user.id,
                UserByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read Campaign")
            # Check TimeseriesByCampaign is in Campaign
            if not self.campaign_id == campaign_id:
                raise BEMServerAuthorizationError("Timeseries not in campaign")


class TimeseriesByCampaignByUser(AuthMixin, Base):
    """Timeseries x Campaign x User associations

    Users associated with a Timeseries x Campaign association get write access
    to the timeseries for the campaign time range.
    """
    __tablename__ = "timeseries_by_campaigns_by_users"
    __table_args__ = (
        sqla.UniqueConstraint("user_id", "timeseries_by_campaign_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"))
    timeseries_by_campaign_id = sqla.Column(
        sqla.ForeignKey("timeseries_by_campaigns.id"),
    )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
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

        if current_user and not current_user.is_admin:
            query = query.filter(
                TimeseriesByCampaignByUser.user_id == current_user.id
            )

        if campaign_id:
            query = query.join(TimeseriesByCampaign).filter(
                TimeseriesByCampaign.campaign_id == campaign_id
            )
        return query

    def check_read_permissions(self, current_user, campaign_id=None):
        """Check user can read timeseries by campaign by user"""
        if current_user and not current_user.is_admin:
            if campaign_id is None:
                raise BEMServerAuthorizationError("Campaign ID not provided")
            # Check User can read campaign
            stmt = sqla.select(UserByCampaign).where(
                UserByCampaign.user_id == current_user.id,
                UserByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read Campaign")
            # Check TimeseriesByCampaignByUser is in Campaign
            stmt = sqla.select(TimeseriesByCampaign).where(
                TimeseriesByCampaign.id == self.timeseries_by_campaign_id,
                TimeseriesByCampaign.campaign_id == campaign_id,
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError(
                    "Timeseries by capaign not in campaign")
            # Check TimeseriesByCampaignByUser belongs to User
            if current_user.id != self.user_id:
                raise BEMServerAuthorizationError(
                    "User can't read TimeseriesByCampaignByUser")
