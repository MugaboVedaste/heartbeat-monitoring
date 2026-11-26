"""
Send one test email to every configured mail server, to check your email settings.

    python -m src.send_test_email

It prints which servers it will use and whether each one worked.
"""

import logging
from datetime import datetime, timezone
from email.message import EmailMessage

from src.alerting import _send_via, configured_mail_servers


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    servers = configured_mail_servers()
    if not servers:
        print("No mail servers configured. Check SMTP_HOST / REAL_SMTP_HOST in .env")
        return

    for server in servers:
        msg = EmailMessage()
        msg["Subject"] = "Heartbeat Monitoring: test email"
        msg.set_content(
            f"This is a test email from the Heartbeat Monitoring System.\n\n"
            f"Mail server: {server.name} ({server.host}:{server.port})\n"
            f"Sent at: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}\n\n"
            f"If you can read this, alert emails will reach you.\n"
        )
        print(f"[{server.name}] {server.host}:{server.port}  from {server.sender}  to {', '.join(server.recipients)} ...")
        ok = _send_via(server, msg)
        print(f"[{server.name}] {'SENT' if ok else 'FAILED (see error above)'}")


if __name__ == "__main__":
    main()
