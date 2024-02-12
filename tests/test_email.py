"""Email tests"""

from email.message import EmailMessage
from unittest import mock

import pytest

from bemserver_core.email import ems, send_email


class TestEmail:
    @mock.patch("bemserver_core.email.send_email.delay")
    def test_email_send_smtp_disabled(self, send_email_delay_mock, bemservercore):
        assert bemservercore.config["SMTP_ENABLED"] is False
        ems.send(["test@test.com"], "Test", "Test")
        send_email_delay_mock.assert_not_called()

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
    def test_email_send_smtp_enabled(self, smtp_mock, bemservercore):
        assert bemservercore.config["SMTP_ENABLED"] is True
        ems.send(["test1@test.com", "test2@test.com"], "Test subject", "Test content")
        with smtp_mock() as smtp:
            smtp.send_message.assert_called_once()
            assert not smtp.send_message.call_args.kwargs
            call_args = smtp.send_message.call_args.args
            assert len(call_args) == 1
            msg = call_args[0]
            assert isinstance(msg, EmailMessage)
            assert msg["From"] == "test@bemserver.org"
            assert msg["To"] == "test1@test.com, test2@test.com"
            assert msg["Subject"] == "Test subject"
            assert msg.get_content() == "Test content\n"

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
    @pytest.mark.usefixtures("bemservercore")
    @mock.patch("smtplib.SMTP")
    def test_email_send_email_task(self, smtp_mock):
        send_email(["test1@test.com", "test2@test.com"], "Test subject", "Test content")
        with smtp_mock() as smtp:
            smtp.send_message.assert_called_once()
            assert not smtp.send_message.call_args.kwargs
            call_args = smtp.send_message.call_args.args
            assert len(call_args) == 1
            msg = call_args[0]
            assert msg["From"] == "test@bemserver.org"
            assert msg["To"] == "test1@test.com, test2@test.com"
            assert msg["Subject"] == "Test subject"
            assert msg.get_content() == "Test content\n"
