"""Notification"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.authorization import auth, AuthMixin, Relation, get_current_user
from bemserver_core.model.users import User
from bemserver_core.model.campaigns import Campaign, CampaignScope
from bemserver_core.model.events import Event


class Notification(AuthMixin, Base):
    __tablename__ = "notifs"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    timestamp = sqla.Column(sqla.DateTime(timezone=True), nullable=False)
    read = sqla.Column(sqla.Boolean, nullable=False, default=False)

    user = sqla.orm.relationship(
        "User",
        backref=sqla.orm.backref("notifs", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event", backref=sqla.orm.backref("notifs", cascade="all, delete-orphan")
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
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id is not None:
            Campaign.get_by_id(campaign_id)
            cs_alias = sqla.orm.aliased(CampaignScope)
            event_alias = sqla.orm.aliased(Event)
            query = (
                query.join(event_alias, cls.event_id == event_alias.id)
                .join(cs_alias, event_alias.campaign_scope_id == cs_alias.id)
                .filter(cs_alias.campaign_id == campaign_id)
            )
        return query

    @classmethod
    def get_count_by_campaign(cls, user_id, read=None):
        """Get notification count by campaign

        :param int user_id: User about which to query.
        :param bool read: Count only read/unread notifications.
            Defaults to None, which means count all notifications.
        """
        user = User.get_by_id(user_id)
        auth.authorize(get_current_user(), "count_notifications", user)

        stmt = (
            sqla.select(
                Campaign.id,
                Campaign.name,
                sqla.func.count(cls.id),
            )
            .join(Event)
            .join(CampaignScope)
            .join(Campaign)
            .filter(cls.user_id == user_id)
            .group_by(Campaign.id)
            .order_by(Campaign.id)
        )
        if read is not None:
            stmt = stmt.filter(cls.read == read)

        ret = {
            "campaigns": [
                {
                    "campaign_id": c_id,
                    "campaign_name": c_name,
                    "count": count,
                }
                for c_id, c_name, count in db.session.execute(stmt).all()
            ]
        }
        ret["total"] = sum(c["count"] for c in ret["campaigns"])
        return ret

    @classmethod
    def mark_all_as_read(cls, user_id, campaign_id=None):
        """Mark notifications as read

        :param int user_id: User for which to mark notifications as read.
        :param int campaign_id: Only mark notification as read for this campaign.
            Defaults to None, which means all campaigns.
        """
        user = User.get_by_id(user_id)
        auth.authorize(get_current_user(), "mark_notifications", user)

        stmt = sqla.update(cls).values(read=True).where(cls.user_id == user_id)

        if campaign_id is not None:
            subq = (
                sqla.select(cls.id)
                .join(Event)
                .join(CampaignScope)
                .join(Campaign)
                .filter(Campaign.id == campaign_id)
            )
            stmt = stmt.where(cls.id.in_(subq))

        db.session.execute(stmt)


def init_db_events_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        Notification.user_id,
        Notification.event_id,
        Notification.timestamp,
    )
