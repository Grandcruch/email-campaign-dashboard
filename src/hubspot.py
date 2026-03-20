"""
hubspot.py — Fetch HubSpot email campaigns (v3) and delivery stats (v1).
"""

import requests
from datetime import date
from dataclasses import dataclass

from .auth import hubspot_headers
from .parser import parse_campaign_name, ParsedCampaign
from .config import DATA_START_DATE

V3_EMAILS_URL = "https://api.hubapi.com/marketing/v3/emails"
V1_CAMPAIGN_URL = "https://api.hubapi.com/email/public/v1/campaigns"


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
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        all_emails.extend(results)

        # Check if we've gone far enough back — look at last email's publishDate
        last_pub = results[-1].get("publishDate", "")
        if last_pub and last_pub[:10] < "2026-02-20":
            # Well past our start date; stop paginating
            break

        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        params["after"] = after

    print(f"  [hubspot] Fetched {len(all_emails)} v3 emails")
    return all_emails


def _resolve_v1_stats(token: str, campaign_ids: list[str]) -> dict | None:
    """
    Try each ID from allEmailCampaignIds in the v1 endpoint.
    Return the first one that resolves with delivered > 0, or None.
    """
    headers = hubspot_headers(token)
    for cid in campaign_ids:
        try:
            resp = requests.get(
                f"{V1_CAMPAIGN_URL}/{cid}",
                headers=headers,
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            counters = data.get("counters", {})
            if counters.get("delivered", 0) > 0:
                return {"id": str(cid), "counters": counters, "name": data.get("name", "")}
        except requests.RequestException:
            continue
    return None


def fetch_campaigns(token: str) -> list[CampaignRecord]:
    """
    Main entry point: fetch v3 emails, parse names, resolve v1 stats.
    Returns list of CampaignRecord for all campaigns >= DATA_START_DATE.
    """
    raw_emails = _fetch_v3_emails(token)
    records: list[CampaignRecord] = []

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
        )

        # If parse failed (LEGACY_FORMAT, PARSE_ERROR), still add to records for QA
        if parsed.qa_bucket in ("PARSE_ERROR", "LEGACY_FORMAT"):
            records.append(record)
            continue

        # Resolve v1 stats
        all_ids = email.get("allEmailCampaignIds", [])
        if not all_ids:
            parsed.qa_bucket = "STATS_UNAVAILABLE"
            records.append(record)
            continue

        v1 = _resolve_v1_stats(token, all_ids)
        if v1 is None:
            parsed.qa_bucket = "STATS_UNAVAILABLE"
            records.append(record)
            continue

        # Populate stats
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
            parsed.qa_bucket = "STATS_UNAVAILABLE"

        records.append(record)

    print(f"  [hubspot] {len(records)} campaigns in scope (>= {DATA_START_DATE})")
    return records
