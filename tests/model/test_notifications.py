"""Notification tests"""
import datetime as dt
import sqlalchemy as sqla

import pytest

from bemserver_core.model import Notification
from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestNotificationModel:
    @pytest.mark.usefixtures("as_admin")
    def test_notification_read_only_fields(self, users, events):
        """Check read-only fields can't be modified

        This is kind of a "framework test".
        """
        user_1 = users[0]
        user_2 = users[1]
        event_1 = events[0]
        event_2 = events[1]

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)
        timestamp_2 = dt.datetime(2020, 6, 1, tzinfo=dt.timezone.utc)

        notif_1 = Notification.new(
            user_id=user_1.id,
            event_id=event_1.id,
            timestamp=timestamp_1,
        )
        db.session.commit()

        notif_1.update(user_id=user_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="user_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()
        notif_1.update(event_id=event_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="event_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()
        notif_1.update(timestamp=timestamp_2)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="timestamp cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

    def test_notification_filters_as_admin(self, users, campaigns, notifications):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        notif_1 = notifications[0]

        with CurrentUser(admin_user):
            notifs_l = list(Notification.get())
            assert len(notifs_l) == 2
            notifs_l = list(Notification.get(campaign_id=campaign_1.id))
            assert len(notifs_l) == 1
            assert notifs_l[0] == notif_1

    def test_notification_authorizations_as_admin(self, users, events):
        admin_user = users[0]
        assert admin_user.is_admin
        event_1 = events[0]

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(admin_user):
            notifs = list(Notification.get())
            assert not notifs
            notif_1 = Notification.new(
                user_id=admin_user.id,
                event_id=event_1.id,
                timestamp=timestamp_1,
            )
            db.session.flush()
            assert Notification.get_by_id(notif_1.id) == notif_1
            db.session.commit()
            notif_1.update(read=True)
            db.session.commit()
            notif_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_notification_authorizations_as_user(self, users, events, notifications):
        user_1 = users[1]
        assert not user_1.is_admin
        event_1 = events[0]
        notif_1 = notifications[0]
        notif_2 = notifications[1]
        assert notif_2.user == user_1

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(user_1):

            notifs = list(Notification.get())
            assert notifs == [notif_2]
            with pytest.raises(BEMServerAuthorizationError):
                Notification.new(
                    user_id=user_1.id,
                    event_id=event_1.id,
                    timestamp=timestamp_1,
                )
                db.session.rollback()
            assert Notification.get_by_id(notif_2.id) == notif_2
            notif_2.update(read=False)
            db.session.commit()
            with pytest.raises(BEMServerAuthorizationError):
                notif_2.delete()
            with pytest.raises(BEMServerAuthorizationError):
                Notification.get_by_id(notif_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                notif_1.update(read=True)
