"""Alerting shared by the Grontia DAGs (no DAG is defined here).

Alerts always go to the task log, and additionally to:
- a webhook, when GRONTIA_ALERT_WEBHOOK_URL is set: POSTed as {"text": ...},
  the payload Slack, Mattermost, Google Chat and Discord's /slack endpoint accept;
- an e-mail, when GRONTIA_ALERT_EMAIL_TO and GRONTIA_SMTP_PASSWORD are set
  (GRONTIA_SMTP_HOST/PORT/USER default to Gmail on 587 with STARTTLS; Gmail
  needs an app password, not the account password).
"""
from __future__ import annotations

import json
import logging
import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DBT_RUN_RESULTS = Path("/tmp/dbt/target/run_results.json")


def send_alert(title: str, lines: list[str]) -> None:
    text = "\n".join([f"[grontia] {title}", *lines])
    logger.warning(text)
    _send_webhook(text)
    _send_email(f"[grontia] {title}", "\n".join(lines))


def _send_webhook(text: str) -> None:
    url = os.getenv("GRONTIA_ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"text": text}, timeout=10).raise_for_status()
    except requests.RequestException:
        # A broken webhook must never mask the original failure
        logger.exception("Could not deliver alert to GRONTIA_ALERT_WEBHOOK_URL")


def _send_email(subject: str, body: str) -> None:
    to, password = os.getenv("GRONTIA_ALERT_EMAIL_TO"), os.getenv("GRONTIA_SMTP_PASSWORD")
    if not to or not password:
        return
    user = os.getenv("GRONTIA_SMTP_USER") or to
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, user, to
    msg.set_content(body + "\n\nAirflow: http://localhost:8080\n")
    try:
        with smtplib.SMTP(os.getenv("GRONTIA_SMTP_HOST", "smtp.gmail.com"),
                          int(os.getenv("GRONTIA_SMTP_PORT", "587")), timeout=20) as smtp:
            smtp.starttls()
            smtp.login(user, password)
            smtp.send_message(msg)
    except (OSError, smtplib.SMTPException):
        # A broken mailbox must never mask the original failure
        logger.exception("Could not e-mail the alert to GRONTIA_ALERT_EMAIL_TO")


def dbt_problems(not_before: datetime | None = None) -> list[dict] | None:
    """Non-passing nodes of the last dbt invocation, or None when there is no
    run_results.json written after not_before (dbt crashed before finishing)."""
    if not DBT_RUN_RESULTS.exists():
        return None
    run = json.loads(DBT_RUN_RESULTS.read_text())
    generated_at = datetime.fromisoformat(run["metadata"]["generated_at"].replace("Z", "+00:00"))
    if not_before and generated_at < not_before:
        return None
    return [
        {"node": r["unique_id"], "status": r["status"], "failures": r.get("failures"),
         "message": (r.get("message") or "")[:200]}
        for r in run["results"]
        if r["status"] not in ("success", "pass")
    ]


def _format(problems: list[dict]) -> list[str]:
    return [f"- {p['status'].upper()} {p['node']} ({p['failures']} rows) {p['message']}".rstrip()
            for p in problems]


def task_failure_alert(context) -> None:
    """on_failure_callback: fires once a task has exhausted its retries."""
    ti = context["ti"]
    send_alert(
        f"{ti.dag_id}.{ti.task_id} failed",
        [f"run: {ti.run_id}", f"error: {context.get('exception')}"],
    )


def dbt_failure_alert(context) -> None:
    """on_failure_callback for the dbt task: lists the models/tests that broke."""
    ti = context["ti"]
    problems = dbt_problems(not_before=ti.start_date)
    if problems is None:
        lines = ["dbt stopped before writing run_results.json (compile or connection error)",
                 f"error: {context.get('exception')}"]
    else:
        lines = _format([p for p in problems if p["status"] in ("error", "fail")])
    send_alert(f"{ti.dag_id}.{ti.task_id} failed (run {ti.run_id})", lines)
