"""SMTP email helpers for account verification."""

from __future__ import annotations

import logging
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmailSender:
    host: str = ""
    port: int = 587
    username: str = ""
    password: str = ""
    sender: str = ""
    tls: bool = True

    @property
    def configured(self) -> bool:
        return bool(self.host and self.sender)

    def send_verification_otp(self, recipient: str, otp: str) -> dict[str, object]:
        if not self.configured:
            raise RuntimeError("SMTP is not configured")

        message = EmailMessage()
        message["Subject"] = "Your GivEVC Portal verification code"
        message["From"] = self.sender
        message["To"] = recipient
        message.set_content(
            "\n".join(
                (
                    "Hello,",
                    "",
                    f"Your GivEVC Portal verification code is: {otp}",
                    "",
                    "This code expires in 10 minutes.",
                    "If you did not create this account, you can ignore this email.",
                )
            )
        )

        if self.tls and self.port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.host, self.port, context=context, timeout=15) as smtp:
                self._login_if_needed(smtp)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(self.host, self.port, timeout=15) as smtp:
                if self.tls:
                    smtp.starttls(context=ssl.create_default_context())
                self._login_if_needed(smtp)
                smtp.send_message(message)
        return {"sent": True, "mode": "smtp"}

    def _login_if_needed(self, smtp: smtplib.SMTP) -> None:
        if self.username:
            smtp.login(self.username, self.password)

    def send_password_reset(self, recipient: str, reset_url: str) -> dict[str, object]:
        if not self.configured:
            raise RuntimeError("SMTP is not configured")

        message = EmailMessage()
        message["Subject"] = "Reset your GivEVC Portal password"
        message["From"] = self.sender
        message["To"] = recipient
        message.set_content(
            "\n".join(
                (
                    "Hello,",
                    "",
                    "A password reset was requested for your GivEVC Portal account.",
                    "",
                    f"Reset your password here: {reset_url}",
                    "",
                    "This link expires in 15 minutes.",
                    "If you did not request a password reset, you can ignore this email.",
                )
            )
        )

        if self.tls and self.port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.host, self.port, context=context, timeout=15) as smtp:
                self._login_if_needed(smtp)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(self.host, self.port, timeout=15) as smtp:
                if self.tls:
                    smtp.starttls(context=ssl.create_default_context())
                self._login_if_needed(smtp)
                smtp.send_message(message)
        return {"sent": True, "mode": "smtp"}

    def send_test_email(self, recipient: str) -> dict[str, object]:
        if not self.configured:
            raise RuntimeError("SMTP is not configured")

        message = EmailMessage()
        message["Subject"] = "GivEVC Portal SMTP test"
        message["From"] = self.sender
        message["To"] = recipient
        message.set_content(
            "\n".join(
                (
                    "Hello,",
                    "",
                    "This is a test email from GivEVC Portal.",
                    "If you received this message, the SMTP settings are working.",
                )
            )
        )

        if self.tls and self.port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.host, self.port, context=context, timeout=15) as smtp:
                self._login_if_needed(smtp)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(self.host, self.port, timeout=15) as smtp:
                if self.tls:
                    smtp.starttls(context=ssl.create_default_context())
                self._login_if_needed(smtp)
                smtp.send_message(message)
        return {"sent": True, "mode": "smtp"}
