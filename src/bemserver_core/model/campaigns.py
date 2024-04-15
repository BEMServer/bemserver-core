"""Campaings"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.database import Base, make_columns_read_only


class Campaign(AuthMixin, Base):
    __tablename__ = "campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    start_time = sqla.Column(sqla.DateTime(timezone=True))
    end_time = sqla.Column(sqla.DateTime(timezone=True))
    timezone = sqla.Column(sqla.String(40), nullable=False, default="UTC")

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


class CampaignScope(AuthMixin, Base):
    __tablename__ = "c_scopes"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)

    campaign = sqla.orm.relationship(
        Campaign,
        backref=sqla.orm.backref("campaign_scopes", cascade="all, delete-orphan"),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user_groups_by_campaign_scopes": Relation(
                    kind="many",
                    other_type="UserGroupByCampaignScope",
                    my_field="id",
                    other_field="campaign_scope_id",
                ),
            },
        )


class UserGroupByCampaign(AuthMixin, Base):
    """UserGroup x Campaign associations"""

    __tablename__ = "u_groups_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("u_groups.id"), nullable=False)

    campaign = sqla.orm.relationship(
        Campaign,
        backref=sqla.orm.backref(
            "user_groups_by_campaigns", cascade="all, delete-orphan"
        ),
    )
    user_group = sqla.orm.relationship(
        "UserGroup",
        backref=sqla.orm.backref(
            "user_groups_by_campaigns", cascade="all, delete-orphan"
        ),
    )

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


class UserGroupByCampaignScope(AuthMixin, Base):
    """UserGroup x CampaignScope associations"""

    __tablename__ = "u_groups_by_c_scopes"
    __table_args__ = (sqla.UniqueConstraint("campaign_scope_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("u_groups.id"), nullable=False)

    campaign_scope = sqla.orm.relationship(
        CampaignScope,
        backref=sqla.orm.backref(
            "user_groups_by_campaign_scopes", cascade="all, delete-orphan"
        ),
    )
    user_group = sqla.orm.relationship(
        "UserGroup",
        backref=sqla.orm.backref(
            "user_groups_by_campaign_scopes", cascade="all, delete-orphan"
        ),
    )

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


def init_db_campaigns_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        CampaignScope.campaign_id,
    )
