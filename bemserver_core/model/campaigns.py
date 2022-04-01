"""Campaings"""
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

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


class CampaignScope(AuthMixin, Base):
    __tablename__ = "campaign_scopes"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))

    # Use getter/setter to prevent modifying campaign after commit
    @sqlaorm.declared_attr
    def _campaign_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("campaigns.id"), nullable=False
        )

    @hybrid_property
    def campaign_id(self):
        return self._campaign_id

    @campaign_id.setter
    def campaign_id(self, campaign_id):
        if self.id is not None:
            raise AttributeError("campaign_id cannot be modified")
        self._campaign_id = campaign_id

    campaigns = sqla.orm.relationship(
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

    __tablename__ = "user_groups_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("user_groups.id"), nullable=False)

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

    __tablename__ = "user_groups_by_campaign_scopes"
    __table_args__ = (sqla.UniqueConstraint("campaign_scope_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_scope_id = sqla.Column(
        sqla.ForeignKey("campaign_scopes.id"), nullable=False
    )
    user_group_id = sqla.Column(sqla.ForeignKey("user_groups.id"), nullable=False)

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
