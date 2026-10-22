"""Алерты на почту по SMTP с SSL. Настройки - переменные окружения SMTP_* и ALERT_EMAIL_TO."""

import logging
import os
import smtplib
from email.message import EmailMessage

log = logging.getLogger(__name__)


def send_alert(subject: str, body: str, env: dict[str, str] | None = None) -> bool:
    """Отправляет письмо; без настроек SMTP только пишет предупреждение в лог."""
    env = os.environ if env is None else env
    user, recipient = env.get("SMTP_USER"), env.get("ALERT_EMAIL_TO")
    if not (user and recipient and env.get("SMTP_HOST")):
        log.warning("SMTP не настроен, алерт не отправлен: %s", subject)
        return False

    message = EmailMessage()
    message["From"] = user
    message["To"] = recipient
    message["Subject"] = f"[citybikes] {subject}"
    message.set_content(body)
    with smtplib.SMTP_SSL(env["SMTP_HOST"], int(env.get("SMTP_PORT", 465)), timeout=30) as smtp:
        smtp.login(user, env["SMTP_PASSWORD"])
        smtp.send_message(message)
    log.info("Алерт отправлен на %s: %s", recipient, subject)
    return True
