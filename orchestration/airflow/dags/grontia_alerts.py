"""Alerting shared by the Grontia DAGs (no DAG is defined here).

Alerts always go to the task log. When GRONTIA_ALERT_WEBHOOK_URL is set they are
also POSTed as {"text": ...}, the payload Slack, Mattermost, Google Chat and
Discord's /slack-compatible endpoint all accept.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DBT_RUN_RESULTS = Path("/tmp/dbt/target/run_results.json")


def send_alert(title: str, lines: list[str]) -> None:
    text = "\n".join([f"[grontia] {title}", *lines])
    logger.warning(text)
    url = os.getenv("GRONTIA_ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"text": text}, timeout=10).raise_for_status()
    except requests.RequestException:
        # A broken webhook must never mask the original failure
        logger.exception("Could not deliver alert to GRONTIA_ALERT_WEBHOOK_URL")


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
