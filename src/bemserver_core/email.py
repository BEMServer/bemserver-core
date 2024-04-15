"""Email"""

import smtplib
from email.message import EmailMessage

from bemserver_core.celery import celery, logger


class EmailSender:
    """Basic mail sender class

    This implementation does not provide SMTP authentication. It assumes an open
    relay is available, typically localhost.
    """

    def __init__(self):
        self._enabled = False
        self._sender_addr = None
        self._host = None

    def init_core(self, bsc):
        """Initialize with settings from BEMServerCore configuration"""
        self._enabled = bsc.config["SMTP_ENABLED"]
        self._sender_addr = bsc.config["SMTP_FROM_ADDR"]
        self._host = bsc.config["SMTP_HOST"]

    def send(self, dest_addrs, subject, content):
        """Create and send message"""
        if self._enabled:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self._sender_addr
            msg["To"] = ", ".join(dest_addrs)
            msg.set_content(content)
            with smtplib.SMTP(self._host, timeout=3) as smtp:
                smtp.send_message(msg)


ems = EmailSender()


@celery.task(name="Email")
def send_email(dest_addrs, subject, content):
    """Send message in a task"""
    logger.info("Send email to %", dest_addrs)
    ems.send(dest_addrs, subject, content)
