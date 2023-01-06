"""Notification"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.authorization import auth, AuthMixin, Relation


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
    db.session.commit()
