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
        """Check read persmissions"""
        if not current_user.is_admin:
            stmt = sqla.select(UserByCampaign).where(
                sqla.and_(
                    UserByCampaign.user_id == current_user.id,
                    UserByCampaign.campaign_id == self.id
                )
            )
            if not db.session.execute(stmt).all():
                raise BEMServerAuthorizationError("User can't read campaign")


class UserByCampaign(Base):
    """User x Campaign associations

    Users associated with a campaign have read permissions campaign-wise
    """
    __tablename__ = "users_by_campaigns"
    __table_args__ = (
        sqla.UniqueConstraint("campaign_id", "user_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("campaigns.id"),
    )
    user_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("users.id"),
    )

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        query = super().get(**kwargs)
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            query = query.filter(UserByCampaign.user_id == current_user.id)
        return query

    def check_read_permissions(self, current_user, **kwargs):
        """Check read persmissions"""
        if not current_user.is_admin and current_user.id != self.id:
            raise BEMServerAuthorizationError("User can't read UserByCampaign")


class TimeseriesByCampaign(Base):
    """Timeseries x Campaign associations

    Timeseries associated with a campaign can be read by all campaign users
    for the campaign time range.
    """
    __tablename__ = "timeseries_by_campaigns"
    __table_args__ = (
        sqla.UniqueConstraint("campaign_id", "timeseries_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("campaigns.id"),
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id"),
    )


class TimeseriesByCampaignByUser(Base):
    """Timeseries x Campaign x User associations

    Users associated with a Timeseries x Campaign association get write access
    to the timeseries for the campaign time range.
    """
    __tablename__ = "timeseries_by_campaigns_by_users"
    __table_args__ = (
        sqla.UniqueConstraint("user_id", "timeseries_by_campaign_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("users.id"),
    )
    timeseries_by_campaign_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries_by_campaigns.id"),
    )
