"""Campaings"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import (
    AuthMixin, auth, query_builder, Relation)


class Campaign(AuthMixin, Base):
    __tablename__ = "campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    start_time = sqla.Column(sqla.DateTime(timezone=True))
    end_time = sqla.Column(sqla.DateTime(timezone=True))

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "users_by_campaigns": Relation(
                    kind="many",
                    other_type="UserByCampaign",
                    my_field="id",
                    other_field="campaign_id",
                ),
            },
            build_query=query_builder(cls),
        )


class UserByCampaign(AuthMixin, Base):
    """User x Campaign associations

    Users associated with a campaign have read permissions campaign-wise
    """
    __tablename__ = "users_by_campaigns"
    __table_args__ = (
        sqla.UniqueConstraint("campaign_id", "user_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user": Relation(
                    kind="one",
                    other_type="User",
                    my_field="user_id",
                    other_field="id",
                ),
            },
            build_query=query_builder(cls),
        )


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
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "campaign": Relation(
                    kind="one",
                    other_type="Campaign",
                    my_field="campaign_id",
                    other_field="id",
                ),
                "timeseries_by_campaigns_by_users": Relation(
                    kind="many",
                    other_type="TimeseriesByCampaignByUser",
                    my_field="id",
                    other_field="timeseries_by_campaign_id",
                ),
            },
            build_query=query_builder(cls),
        )


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
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user": Relation(
                    kind="one",
                    other_type="User",
                    my_field="user_id",
                    other_field="id",
                ),
            },
            build_query=query_builder(cls),
        )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id:
            query = query.join(TimeseriesByCampaign).filter(
                TimeseriesByCampaign.campaign_id == campaign_id
            )
        return query
