"""
hubspot.py — Fetch HubSpot email campaigns and their delivery stats (v3).
"""

from datetime import date, timedelta
from dataclasses import dataclass, field

from .auth import hubspot_headers
from .parser import parse_campaign_name, ParsedCampaign
from .config import DATA_START_DATE
from ._http_retry import get_with_retry

V3_EMAILS_URL = "https://api.hubapi.com/marketing/v3/emails"
V1_CAMPAIGN_IDS_URL = "https://api.hubapi.com/email/public/v1/campaigns/by-id"

# Must match dashboard.PIPELINE_VERSION — the dashboard compares them at
# startup to detect Streamlit Cloud soft-reloads that keep stale src modules.
MODULE_VERSION = "2026-11-03.4"


@dataclass
class CampaignRecord:
    """A fully-resolved campaign with parsed fields + delivery stats."""
    parsed: ParsedCampaign
    # HubSpot IDs
    hubspot_v3_email_id: str = ""
    hubspot_v1_campaign_id: str = ""            # ids below, comma-joined for reports
    # allEmailCampaignIds that really exist. One email can span several ids and
    # most listed ids do not exist; the v1 events API rejects those with a 400.
    hubspot_v1_campaign_ids: list[str] = field(default_factory=list)
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
    params: dict = {"limit": 100, "orderBy": "-publishDate", "includeStats": "true"}

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


def _fetch_existing_campaign_ids(token: str) -> set[str] | None:
    """All v1 campaign ids that exist in the account. None if the fetch failed."""
    headers = hubspot_headers(token)
    existing: set[str] = set()
    params: dict = {"limit": 1000}
    try:
        while True:
            resp = get_with_retry(V1_CAMPAIGN_IDS_URL, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            existing.update(str(c.get("id")) for c in data.get("campaigns", []))
            if not data.get("hasMore"):
                return existing
            params["offset"] = data.get("offset")
    except Exception as exc:
        print(f"  [hubspot] WARNING: campaign id list unavailable: {exc}")
        return None


def fetch_campaigns(token: str) -> list[CampaignRecord]:
    """
    Main entry point: fetch v3 emails with their stats and parse names.
    Returns list of CampaignRecord for all campaigns >= DATA_START_DATE.

    Delivery stats are HubSpot's email-level totals, which cover every send of
    the email. Reading a single v1 campaign id instead undercounts emails that
    HubSpot split across several ids.
    """
    raw_emails = _fetch_v3_emails(token)
    existing_ids = _fetch_existing_campaign_ids(token)
    records: list[CampaignRecord] = []
    stats_failures: list[str] = []

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

        all_ids = [str(i) for i in email.get("allEmailCampaignIds", [])]
        record.hubspot_v1_campaign_ids = [i for i in all_ids if i in (existing_ids or ())]
        record.hubspot_v1_campaign_id = ",".join(record.hubspot_v1_campaign_ids)

        counters = (email.get("stats") or {}).get("counters") or {}
        record.delivered = counters.get("delivered", 0)
        record.opened = counters.get("open", 0)
        record.clicked = counters.get("click", 0)
        record.sent = counters.get("sent", 0)
        record.bounced = counters.get("bounce", 0)
        record.unsubscribed = counters.get("unsubscribed", 0)

        # If parse failed (LEGACY_FORMAT, PARSE_ERROR), keep it for QA as-is
        if parsed.qa_bucket in ("PARSE_ERROR", "LEGACY_FORMAT"):
            continue

        if not counters and record.hubspot_v1_campaign_ids:
            # Real campaign but HubSpot returned no stats — do NOT silently
            # drop it as unsent. It stays in the main table and QA code set.
            stats_failures.append(parsed.raw_name)
        elif record.delivered == 0:
            parsed.qa_bucket = "STATS_UNAVAILABLE"

    print(f"  [hubspot] {len(records)} campaigns in scope (>= {DATA_START_DATE})")
    if stats_failures:
        print(f"  [hubspot] WARNING: delivery stats missing for "
              f"{len(stats_failures)} campaign(s) — retained with 0 delivered:")
        for name in stats_failures[:10]:
            print(f"    - {name}")
    fetch_campaigns.last_stats_failures = stats_failures
    return records


fetch_campaigns.last_stats_failures = []
