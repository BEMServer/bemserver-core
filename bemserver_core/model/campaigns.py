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
                "user_groups_by_campaigns": Relation(
                    kind="many",
                    other_type="UserGroupByCampaign",
                    my_field="id",
                    other_field="campaign_id",
                ),
            },
        )


class UserGroupByCampaign(AuthMixin, Base):
    """UserGroup x Campaign associations"""

    __tablename__ = "user_groups_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("user_groups.id"), nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user_group": Relation(
                    kind="one",
                    other_type="UserGroup",
                    my_field="user_group_id",
                    other_field="id",
                ),
            },
        )
