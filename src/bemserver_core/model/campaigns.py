"""Campaings"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMgrMixin
from bemserver_core.database import Base, db, make_columns_read_only

from .users import UserByUserGroup, UserGroup


class Campaign(AuthMgrMixin, Base):
    __tablename__ = "campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    start_time = sqla.Column(sqla.DateTime(timezone=True))
    end_time = sqla.Column(sqla.DateTime(timezone=True))
    timezone = sqla.Column(sqla.String(40), nullable=False, default="UTC")

    @classmethod
    def authorize_query(cls, actor, query):
        return UserGroupByCampaign.authorize_query(
            actor,
            query.join(UserGroupByCampaign),
        )

    def authorize_read(self, actor):
        return self.is_member(actor)

    def is_member(self, user):
        return bool(
            db.session.query(
                db.session.query(UserGroupByCampaign)
                .join(UserGroup)
                .join(UserByUserGroup)
                .filter(UserByUserGroup.user_id == user.id)
                .filter(UserGroupByCampaign.campaign_id == self.id)
                .exists()
            ).scalar()
        )


class CampaignScope(AuthMgrMixin, Base):
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
    def authorize_query(cls, actor, query):
        return UserGroupByCampaignScope.authorize_query(
            actor,
            query.join(UserGroupByCampaignScope),
        )

    def authorize_read(self, actor):
        return self.is_member(actor)

    def is_member(self, user):
        return bool(
            db.session.query(
                db.session.query(UserGroupByCampaignScope)
                .join(UserGroup)
                .join(UserByUserGroup)
                .filter(UserByUserGroup.user_id == user.id)
                .filter(UserGroupByCampaignScope.campaign_scope_id == self.id)
                .exists()
            ).scalar()
        )


class UserGroupByCampaign(AuthMgrMixin, Base):
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
    def authorize_query(cls, actor, query):
        return UserGroup.authorize_query(actor, query.join(UserGroup))

    def authorize_read(self, actor):
        return db.session.query(
            db.session.query(UserByUserGroup)
            .filter(UserByUserGroup.user_id == actor.id)
            .filter(UserByUserGroup.user_group_id == self.user_group_id)
            .exists()
        ).scalar()


class UserGroupByCampaignScope(AuthMgrMixin, Base):
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
    def authorize_query(cls, actor, query):
        return UserGroup.authorize_query(actor, query.join(UserGroup))

    def authorize_read(self, actor):
        return db.session.query(
            db.session.query(UserByUserGroup)
            .filter(UserByUserGroup.user_id == actor.id)
            .filter(UserByUserGroup.user_group_id == self.user_group_id)
            .exists()
        ).scalar()


def init_db_campaigns_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        CampaignScope.campaign_id,
    )
