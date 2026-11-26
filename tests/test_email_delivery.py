"""
Unit tests for sending emails to several mail servers (Mailpit + a real account).
A fake SMTP class records what would be sent, so no mail server is needed.
"""

from email.message import EmailMessage

import pytest

from src import alerting, config


class FakeSMTP:
    """Pretends to be smtplib.SMTP and records every email 'sent'."""
    sent = []          # (host, from, to, used_tls, logged_in_as)
    fail_hosts = set()

    def __init__(self, host, port, timeout=None):
        if host in FakeSMTP.fail_hosts:
            raise OSError("connection refused")
        self.host, self.tls, self.user = host, False, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        self.tls = True

    def login(self, user, password):
        self.user = user

    def send_message(self, msg):
        FakeSMTP.sent.append((self.host, msg["From"], msg["To"], self.tls, self.user))


@pytest.fixture
def fake_smtp(monkeypatch):
    FakeSMTP.sent, FakeSMTP.fail_hosts = [], set()
    monkeypatch.setattr(alerting.smtplib, "SMTP", FakeSMTP)
    # Mailpit settings
    monkeypatch.setattr(config, "SMTP_HOST", "localhost")
    monkeypatch.setattr(config, "SMTP_USE_TLS", False)
    monkeypatch.setattr(config, "SMTP_USER", "")
    monkeypatch.setattr(config, "ALERT_EMAIL_FROM", "alerts@heartbeat.local")
    monkeypatch.setattr(config, "ALERT_EMAIL_TO", "doctor@heartbeat.local")
    # Real email: off by default in each test
    monkeypatch.setattr(config, "REAL_SMTP_HOST", "")
    return monkeypatch


def enable_real_email(monkeypatch, to="me@example.com, nurse@example.com"):
    monkeypatch.setattr(config, "REAL_SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setattr(config, "REAL_SMTP_USE_TLS", True)
    monkeypatch.setattr(config, "REAL_SMTP_USER", "me@gmail.com")
    monkeypatch.setattr(config, "REAL_SMTP_PASSWORD", "app-password")
    monkeypatch.setattr(config, "REAL_ALERT_EMAIL_FROM", "")
    monkeypatch.setattr(config, "REAL_ALERT_EMAIL_TO", to)


def message():
    msg = EmailMessage()
    msg["Subject"] = "test"
    msg.set_content("hello")
    return msg


def test_only_mailpit_when_real_email_not_configured(fake_smtp):
    assert alerting.send_message(message()) is True
    assert FakeSMTP.sent == [("localhost", "alerts@heartbeat.local", "doctor@heartbeat.local", False, None)]


def test_sends_to_both_when_real_email_configured(fake_smtp):
    enable_real_email(fake_smtp)
    assert alerting.send_message(message()) is True
    assert FakeSMTP.sent == [
        ("localhost", "alerts@heartbeat.local", "doctor@heartbeat.local", False, None),
        # real email: sender defaults to the login, TLS on, logged in, several recipients
        ("smtp.gmail.com", "me@gmail.com", "me@example.com, nurse@example.com", True, "me@gmail.com"),
    ]


def test_one_server_failing_does_not_stop_the_other(fake_smtp):
    enable_real_email(fake_smtp)
    FakeSMTP.fail_hosts = {"localhost"}  # Mailpit is down
    assert alerting.send_message(message()) is True
    assert [host for host, *_ in FakeSMTP.sent] == ["smtp.gmail.com"]


def test_returns_false_when_every_server_fails(fake_smtp):
    enable_real_email(fake_smtp)
    FakeSMTP.fail_hosts = {"localhost", "smtp.gmail.com"}
    assert alerting.send_message(message()) is False


def test_real_email_without_recipients_is_skipped(fake_smtp):
    enable_real_email(fake_smtp, to="")
    assert alerting.send_message(message()) is True  # Mailpit still worked
    assert [host for host, *_ in FakeSMTP.sent] == ["localhost"]
