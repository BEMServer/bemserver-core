"""Campaings"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation


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
        )


class UserByCampaign(AuthMixin, Base):
    """User x Campaign associations"""

    __tablename__ = "users_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "user_id"),)

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
        )


class TimeseriesGroupByCampaign(AuthMixin, Base):
    """Timeseries x Campaign associations

    Timeseries associated with a campaign can be read/written by all campaign
    users for the campaign time range.
    """

    __tablename__ = "timeseries_groups_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "timeseries_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"))
    timeseries_group_id = sqla.Column(sqla.ForeignKey("timeseries_groups.id"))

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
                "timeseries_group": Relation(
                    kind="one",
                    other_type="TimeseriesGroup",
                    my_field="timeseries_group_id",
                    other_field="id",
                ),
            },
        )
