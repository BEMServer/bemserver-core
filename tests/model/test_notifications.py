"""Notification tests"""

import datetime as dt
from email.message import EmailMessage
from unittest import mock

import pytest

import sqlalchemy as sqla

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreTaskError,
)
from bemserver_core.model import Event, EventLevelEnum, Notification
from bemserver_core.model.notifications import send_notification_email

DUMMY_ID = 69


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

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_notification_filters_as_user(self, users, campaigns, notifications):
        user_2 = users[1]
        assert not user_2.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        notif_2 = notifications[1]

        with CurrentUser(user_2):
            notifs_l = list(Notification.get())
            assert len(notifs_l) == 1
            with pytest.raises(BEMServerAuthorizationError):
                notifs_l = list(Notification.get(campaign_id=campaign_1.id))
            notifs_l = list(Notification.get(campaign_id=campaign_2.id))
            assert len(notifs_l) == 1
            assert notifs_l[0] == notif_2

    @pytest.mark.usefixtures("notifications")
    def test_notification_get_count_by_campaign_as_admin(
        self, users, campaign_scopes, event_categories
    ):
        admin_user = users[0]
        user_2 = users[1]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ec_1 = event_categories[0]

        timestamp_1 = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)

        with OpenBar():
            event_1 = Event.new(
                campaign_scope_id=cs_1.id,
                timestamp=timestamp_1,
                category_id=ec_1.id,
                level=EventLevelEnum.WARNING,
                source="src",
            )
            event_2 = Event.new(
                campaign_scope_id=cs_2.id,
                timestamp=timestamp_1,
                category_id=ec_1.id,
                level=EventLevelEnum.WARNING,
                source="src",
            )
            db.session.flush()
            Notification.new(
                user_id=admin_user.id,
                event_id=event_1.id,
                timestamp=timestamp_1,
                read=False,
            )
            Notification.new(
                user_id=admin_user.id,
                event_id=event_2.id,
                timestamp=timestamp_1,
                read=True,
            )
            db.session.flush()

        with CurrentUser(admin_user):
            notif_count = Notification.get_count_by_campaign(user_2.id)
            assert notif_count == {
                "total": 1,
                "campaigns": [
                    {
                        "campaign_id": 2,
                        "campaign_name": "Campaign 2",
                        "count": 1,
                    }
                ],
            }
            notif_count = Notification.get_count_by_campaign(admin_user.id)
            assert notif_count == {
                "total": 3,
                "campaigns": [
                    {
                        "campaign_id": 1,
                        "campaign_name": "Campaign 1",
                        "count": 2,
                    },
                    {
                        "campaign_id": 2,
                        "campaign_name": "Campaign 2",
                        "count": 1,
                    },
                ],
            }
            notif_count = Notification.get_count_by_campaign(admin_user.id, read=False)
            assert notif_count == {
                "total": 2,
                "campaigns": [
                    {
                        "campaign_id": 1,
                        "campaign_name": "Campaign 1",
                        "count": 2,
                    },
                ],
            }

    @pytest.mark.usefixtures("notifications")
    def test_notification_get_count_by_campaign_as_user(self, users):
        admin_user = users[0]
        user_2 = users[1]
        assert not user_2.is_admin

        with CurrentUser(user_2):
            with pytest.raises(BEMServerAuthorizationError):
                notif_count = Notification.get_count_by_campaign(admin_user.id)
            notif_count = Notification.get_count_by_campaign(user_2.id)
            assert notif_count == {
                "total": 1,
                "campaigns": [
                    {
                        "campaign_id": 2,
                        "campaign_name": "Campaign 2",
                        "count": 1,
                    }
                ],
            }
            notif_count = Notification.get_count_by_campaign(user_2.id, read=False)
            assert notif_count == {
                "total": 0,
                "campaigns": [],
            }

    @pytest.mark.usefixtures("notifications")
    def test_notification_mark_all_as_read_as_admin(
        self, users, campaigns, campaign_scopes, event_categories
    ):
        admin_user = users[0]
        user_2 = users[1]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ec_1 = event_categories[0]

        timestamp_1 = dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc)

        with OpenBar():
            event_1 = Event.new(
                campaign_scope_id=cs_1.id,
                timestamp=timestamp_1,
                category_id=ec_1.id,
                level=EventLevelEnum.WARNING,
                source="src",
            )
            event_2 = Event.new(
                campaign_scope_id=cs_2.id,
                timestamp=timestamp_1,
                category_id=ec_1.id,
                level=EventLevelEnum.WARNING,
                source="src",
            )
            db.session.flush()
            Notification.new(
                user_id=user_2.id,
                event_id=event_1.id,
                timestamp=timestamp_1,
                read=False,
            )
            Notification.new(
                user_id=user_2.id,
                event_id=event_2.id,
                timestamp=timestamp_1,
                read=False,
            )
            db.session.flush()

        with CurrentUser(admin_user):
            notifs_l = list(Notification.get(user_id=admin_user.id, read=False))
            assert len(notifs_l) == 1
            Notification.mark_all_as_read(admin_user.id)
            notifs_l = list(Notification.get(user_id=admin_user.id, read=False))
            assert len(notifs_l) == 0

            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert len(notifs_l) == 2
            Notification.mark_all_as_read(user_2.id, campaign_id=campaign_1.id)
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert len(notifs_l) == 1
            Notification.mark_all_as_read(user_2.id, campaign_id=campaign_2.id)
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert not notifs_l

    def test_notification_mark_all_as_read_as_user(
        self, users, campaigns, notifications
    ):
        admin_user = users[0]
        user_2 = users[1]
        assert not user_2.is_admin
        campaign_2 = campaigns[1]
        notif_2 = notifications[1]

        with CurrentUser(user_2):
            with pytest.raises(BEMServerAuthorizationError):
                Notification.mark_all_as_read(admin_user.id)
            notif_2.read = False
            db.session.flush()
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert len(notifs_l) == 1
            Notification.mark_all_as_read(user_2.id, campaign_id=campaign_2.id)
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert not notifs_l
            notif_2.read = False
            db.session.flush()
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert len(notifs_l) == 1
            Notification.mark_all_as_read(user_2.id)
            notifs_l = list(Notification.get(user_id=user_2.id, read=False))
            assert len(notifs_l) == 0

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


@pytest.mark.parametrize(
    "config",
    (
        {
            "SMTP_ENABLED": True,
            "SMTP_FROM_ADDR": "test@bemserver.org",
            "SMTP_HOST": "bemserver.org",
        },
    ),
    indirect=True,
)
@mock.patch("smtplib.SMTP")
@pytest.mark.usefixtures("as_admin")
def test_send_notification_email_task(smtp_mock, users, events):
    """Test send_email called on insert"""

    user_1 = users[0]
    event_1 = events[0]

    timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

    with pytest.raises(BEMServerCoreTaskError):
        send_notification_email(DUMMY_ID)

    notif_1 = Notification.new(
        user_id=user_1.id,
        event_id=event_1.id,
        timestamp=timestamp_1,
    )
    db.session.flush()

    with smtp_mock() as smtp:
        smtp.send_message.assert_not_called()

        send_notification_email(notif_1.id)

        smtp.send_message.assert_called_once()
        assert not smtp.send_message.call_args.kwargs
        call_args = smtp.send_message.call_args.args
        assert len(call_args) == 1
        msg = call_args[0]
        assert isinstance(msg, EmailMessage)
        assert msg["From"] == "test@bemserver.org"
        assert msg["To"] == user_1.email
        assert msg["Subject"] == "[Campaign 1] WARNING: Custom event category 1"
        assert msg.get_content() == "\n"


@pytest.mark.parametrize(
    "config",
    (
        {
            "SMTP_ENABLED": True,
            "SMTP_FROM_ADDR": "test@bemserver.org",
            "SMTP_HOST": "bemserver.org",
        },
    ),
    indirect=True,
)
@pytest.mark.usefixtures("as_admin")
@mock.patch("bemserver_core.model.notifications.send_notification_email.delay")
def test_notification_after_insert(send_notification_email_delay_mock, users, events):
    """Test notification mail task called on insert"""

    user_1 = users[0]
    event_1 = events[0]

    timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

    notif_1 = Notification.new(
        user_id=user_1.id,
        event_id=event_1.id,
        timestamp=timestamp_1,
    )

    # Flush. send_email not called.
    db.session.flush()
    send_notification_email_delay_mock.assert_not_called()

    # Commit. send_email called.
    db.session.commit()
    send_notification_email_delay_mock.assert_called_once()
    send_notification_email_delay_mock.assert_called_with(notif_1.id)

    # Commit after flush + delete. Notify not called.
    send_notification_email_delay_mock.reset_mock()
    notif = Notification.new(
        user_id=user_1.id,
        event_id=event_1.id,
        timestamp=timestamp_1,
    )
    db.session.flush()
    notif.delete()
    db.session.commit()
    send_notification_email_delay_mock.assert_not_called()

    # Commit after flush + rollback. Notify not called.
    send_notification_email_delay_mock.reset_mock()
    notif = Notification.new(
        user_id=user_1.id,
        event_id=event_1.id,
        timestamp=timestamp_1,
    )
    db.session.flush()
    db.session.rollback()
    db.session.commit()
    send_notification_email_delay_mock.assert_not_called()
