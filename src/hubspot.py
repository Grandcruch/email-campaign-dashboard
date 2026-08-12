"""
hubspot.py — Fetch HubSpot email campaigns (v3) and delivery stats (v1).
"""

import time
import threading

import requests
from datetime import date, timedelta
from dataclasses import dataclass

from .auth import hubspot_headers
from .parser import parse_campaign_name, ParsedCampaign
from .config import DATA_START_DATE
from ._http_retry import get_with_retry

V3_EMAILS_URL = "https://api.hubapi.com/marketing/v3/emails"
V1_CAMPAIGN_URL = "https://api.hubapi.com/email/public/v1/campaigns"

# Must match dashboard.PIPELINE_VERSION — the dashboard compares them at
# startup to detect Streamlit Cloud soft-reloads that keep stale src modules.
MODULE_VERSION = "2026-08-06.1"


# HubSpot private apps allow 110 requests / 10s. Resolving v1 stats runs on a
# thread pool, so pace the combined request rate well under that ceiling —
# from a datacenter host (Streamlit Cloud) unpaced parallel calls hit 429s,
# which silently turned real campaigns into STATS_UNAVAILABLE.
_MIN_V1_INTERVAL = 0.14  # seconds between v1 requests (~7/s)
_v1_lock = threading.Lock()
_v1_last_ts = 0.0


def _throttled_v1_get(url: str, **kwargs) -> requests.Response:
    """Rate-limited + retrying GET for the v1 campaign endpoint."""
    global _v1_last_ts
    with _v1_lock:
        wait = _MIN_V1_INTERVAL - (time.monotonic() - _v1_last_ts)
        if wait > 0:
            time.sleep(wait)
        _v1_last_ts = time.monotonic()
    return get_with_retry(url, **kwargs)


@dataclass
class CampaignRecord:
    """A fully-resolved campaign with parsed fields + delivery stats."""
    parsed: ParsedCampaign
    # HubSpot IDs
    hubspot_v3_email_id: str = ""
    hubspot_v1_campaign_id: str = ""
    # Delivery stats
    delivered: int = 0
    opened: int = 0
    clicked: int = 0
    sent: int = 0
    bounced: int = 0
    unsubscribed: int = 0
    # Email content metadata
    subject: str = ""


def _fetch_v3_emails(token: str) -> list[dict]:
    """
    Paginate through v3 emails ordered by publishDate descending.
    Stop once we pass DATA_START_DATE (with some buffer for date format mismatches).
    """
    headers = hubspot_headers(token)
    all_emails: list[dict] = []
    url = V3_EMAILS_URL
    params: dict = {"limit": 100, "orderBy": "-publishDate"}

    while True:
        resp = get_with_retry(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        all_emails.extend(results)

        # Check if we've gone far enough back — look at last email's publishDate
        last_pub = results[-1].get("publishDate", "")
        stop_date = str(DATA_START_DATE - timedelta(days=30))
        if last_pub and last_pub[:10] < stop_date:
            # Well past our start date; stop paginating
            break

        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        params["after"] = after

    print(f"  [hubspot] Fetched {len(all_emails)} v3 emails")
    return all_emails


def _resolve_v1_stats(token: str, campaign_ids: list[str]) -> tuple[dict | None, bool]:
    """
    Try each ID from allEmailCampaignIds in the v1 endpoint.

    Returns (stats_or_None, request_failed). `request_failed` is True when a
    lookup errored out (network/429/5xx after retries) rather than legitimately
    returning no delivery data — callers must NOT mark those campaigns
    STATS_UNAVAILABLE, since that silently drops real campaigns from the
    dashboard and from the QA known-code set.
    """
    headers = hubspot_headers(token)
    failed = False
    for cid in campaign_ids:
        try:
            resp = _throttled_v1_get(
                f"{V1_CAMPAIGN_URL}/{cid}",
                headers=headers,
            )
            if resp.status_code == 404:
                continue  # genuinely no such campaign — not a failure
            if resp.status_code != 200:
                failed = True
                continue
            data = resp.json()
            counters = data.get("counters", {})
            if counters.get("delivered", 0) > 0:
                return {"id": str(cid), "counters": counters, "name": data.get("name", "")}, False
        except requests.RequestException:
            failed = True
            continue
    return None, failed


def fetch_campaigns(token: str) -> list[CampaignRecord]:
    """
    Main entry point: fetch v3 emails, parse names, resolve v1 stats.
    Returns list of CampaignRecord for all campaigns >= DATA_START_DATE.
    """
    raw_emails = _fetch_v3_emails(token)
    records: list[CampaignRecord] = []
    # Records whose v1 stats still need resolving: (record, allEmailCampaignIds)
    pending: list[tuple[CampaignRecord, list]] = []

    for email in raw_emails:
        name = email.get("name", "")
        if not name:
            continue

        # Parse campaign name
        parsed = parse_campaign_name(name)

        # Skip if date out of range or unparseable date
        if parsed.qa_bucket == "DATE_OUT_OF_RANGE":
            continue
        if parsed.parsed_send_date is None:
            continue
        if parsed.parsed_send_date < DATA_START_DATE:
            continue

        record = CampaignRecord(
            parsed=parsed,
            hubspot_v3_email_id=str(email.get("id", "")),
            subject=email.get("subject", ""),
        )
        records.append(record)

        # If parse failed (LEGACY_FORMAT, PARSE_ERROR), no stats needed (QA only)
        if parsed.qa_bucket in ("PARSE_ERROR", "LEGACY_FORMAT"):
            continue

        all_ids = email.get("allEmailCampaignIds", [])
        if not all_ids:
            parsed.qa_bucket = "STATS_UNAVAILABLE"
            continue

        pending.append((record, all_ids))

    # Resolve v1 stats in parallel. Requests are globally throttled (see
    # _throttled_v1_get) so the pool cannot exceed HubSpot's rate limit.
    from concurrent.futures import ThreadPoolExecutor

    def _resolve_one(item):
        record, all_ids = item
        stats, failed = _resolve_v1_stats(token, all_ids)
        return record, stats, failed

    stats_failures: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for record, v1, failed in pool.map(_resolve_one, pending):
            if v1 is None:
                if failed:
                    # HubSpot was unreachable/rate-limited — do NOT silently
                    # drop the campaign. Keep its bucket so it stays in the
                    # main table and in the QA known-code set.
                    stats_failures.append(record.parsed.raw_name)
                else:
                    record.parsed.qa_bucket = "STATS_UNAVAILABLE"
                continue
            counters = v1["counters"]
            record.hubspot_v1_campaign_id = v1["id"]
            record.delivered = counters.get("delivered", 0)
            record.opened = counters.get("open", 0)
            record.clicked = counters.get("click", 0)
            record.sent = counters.get("sent", 0)
            record.bounced = counters.get("bounce", 0)
            record.unsubscribed = counters.get("unsubscribed", 0)
            # Final check: delivered > 0 is the primary sent check
            if record.delivered == 0:
                record.parsed.qa_bucket = "STATS_UNAVAILABLE"

    print(f"  [hubspot] {len(records)} campaigns in scope (>= {DATA_START_DATE})")
    if stats_failures:
        print(f"  [hubspot] WARNING: delivery stats unreachable for "
              f"{len(stats_failures)} campaign(s) — retained with 0 delivered:")
        for name in stats_failures[:10]:
            print(f"    - {name}")
    fetch_campaigns.last_stats_failures = stats_failures
    return records


fetch_campaigns.last_stats_failures = []
