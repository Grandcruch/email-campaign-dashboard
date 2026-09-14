"""
journey_attribution.py — Evidence-based attribution for the "Current Offers"
email section (live since 2026-09-11).

For every order that redeemed a campaign discount code after the section
launch, reconstruct the buyer's HubSpot journey (clicks + deliveries, 30-day
lookback) and classify the redemption into one of four EVIDENCE BUCKETS:

    A  section_only   — clicked the section's link for that code and NEVER
                        received the code's original offer email.
                        Strongest section attribution.
    B  section_and_original — clicked the section's link AND received (or
                        clicked) the original email. Directional evidence.
    C  original_only  — clicked/received the original email; no section click.
    D  no_evidence    — no click trail at all (open-only, typed code,
                        different device/email at checkout). Honestly
                        unattributable; never assigned to either side.

Section links are identified by `utm_term=offer_recs` — the only UTM our
section adds (HubSpot appends its own utm_source/medium/campaign to all
links, so those are useless for separating the two paths).

DESIGN NOTES
- Never blended into the locked primary metrics (Attributed Revenue etc.).
- Order revenue is counted ONCE per order even when multiple codes are
  redeemed; the order's bucket is the strongest evidence across its codes
  (A > B > C > D). Per-code evidence rows are kept for the drill-down.
- Classification for an order is FINAL once computed (clicks cannot appear
  retroactively before an already-placed order), so results are cached by
  order id in output/journey_cache.csv and never re-fetched.
- All HubSpot event calls are throttled to stay under the rate limit.
- No raw customer emails are persisted — the cache and UI show a masked
  form (first 3 chars + ***).
"""

import os
import re
import json
import time
import threading
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from .auth import hubspot_headers
from ._http_retry import get_with_retry
from .config import OUTPUT_DIR
from .parser import parse_campaign_name
from .shopify_orders import _fetch_orders_in_window, _normalize_code

EVENTS_URL = "https://api.hubapi.com/email/public/v1/events"

# First send carrying the Current Offers section (2026-09-11 Lafite email —
# verified against stored email content + updatedAt timestamps).
SECTION_LAUNCH_DATE = date(2026, 9, 11)

# How far back to scan a buyer's HubSpot activity before their order.
JOURNEY_LOOKBACK_DAYS = 30

CACHE_FILE = os.path.join(OUTPUT_DIR, "journey_cache.csv")
CACHE_VERSION = 1

_NON_CAMPAIGN_CODE_RE = re.compile(r"^(grandcru|gcla-|thankyou|lastchance)", re.I)

BUCKET_LABELS = {
    "A": "A — Section only (never received original email)",
    "B": "B — Section + original email (directional)",
    "C": "C — Original email only",
    "D": "D — No click evidence (unattributable)",
}
_BUCKET_RANK = {"A": 0, "B": 1, "C": 2, "D": 3}

# ── Throttle (shared across threads, mirrors hubspot.py) ─────────────────────
_MIN_INTERVAL = 0.14
_lock = threading.Lock()
_last_ts = 0.0


def _throttled_get(url: str, **kwargs):
    global _last_ts
    with _lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_ts)
        if wait > 0:
            time.sleep(wait)
        _last_ts = time.monotonic()
    return get_with_retry(url, **kwargs)


def _mask(email: str) -> str:
    return (email or "")[:3] + "***"


# ── HubSpot event fetchers ───────────────────────────────────────────────────

def _paged_events(token: str, params: dict) -> list[dict]:
    headers = hubspot_headers(token)
    events, offset = [], None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        resp = _throttled_get(EVENTS_URL, headers=headers, params=p)
        resp.raise_for_status()
        data = resp.json()
        events.extend(data.get("events", []))
        if not data.get("hasMore"):
            break
        offset = data.get("offset")
    return events


def fetch_section_clicks(token: str, since: date) -> pd.DataFrame:
    """All clicks on section URLs (utm_term=offer_recs) since `since`."""
    start_ms = int(datetime(since.year, since.month, since.day,
                            tzinfo=timezone.utc).timestamp() * 1000)
    events = _paged_events(token, {"eventType": "CLICK",
                                   "startTimestamp": start_ms, "limit": 1000})
    rows = []
    for e in events:
        u = e.get("url") or ""
        if "offer_recs" not in u:
            continue
        code = u.split("/discount/")[1].split("?")[0] if "/discount/" in u else ""
        rows.append({
            "clicked_at": datetime.fromtimestamp(e.get("created", 0) / 1000,
                                                 tz=timezone.utc),
            "recipient_masked": _mask(e.get("recipient") or ""),
            "recipient": (e.get("recipient") or "").lower(),
            "code": code,
            "email_campaign_id": str(e.get("emailCampaignId") or ""),
            "device": e.get("deviceType") or "",
        })
    return pd.DataFrame(rows)


def _recipient_events(token: str, email_addr: str, start_ms: int) -> list[dict]:
    return _paged_events(token, {"recipient": email_addr,
                                 "startTimestamp": start_ms, "limit": 1000})


# ── code → original-email campaign ids ───────────────────────────────────────

def build_code_campaign_map(raw_v3_emails: list[dict]) -> dict[str, list[tuple[date, set]]]:
    """{normalized_code: [(send_date, {v1 campaign ids}), ...]}"""
    result: dict[str, list[tuple[date, set]]] = {}
    for e in raw_v3_emails:
        p = parse_campaign_name(e.get("name", ""))
        if not p.discount_code or not p.parsed_send_date:
            continue
        key = _normalize_code(p.discount_code)
        ids = {str(i) for i in e.get("allEmailCampaignIds", [])}
        result.setdefault(key, []).append((p.parsed_send_date, ids))
    return result


# ── Classification core ──────────────────────────────────────────────────────

def _classify_one_order(order: dict, token: str,
                        code_campaigns: dict) -> dict | None:
    """Classify a single order. Returns a cache-row dict, or None if the
    order has no campaign code / no customer email."""
    codes = [c.get("code") for c in order.get("discount_codes", []) if c.get("code")]
    camp_codes = [c for c in codes if not _NON_CAMPAIGN_CODE_RE.match(c)]
    if not camp_codes:
        return None
    em = ((order.get("customer") or {}).get("email") or order.get("email") or "").lower()
    if not em:
        return None

    order_ts = datetime.fromisoformat(order["created_at"]).astimezone(timezone.utc)
    order_ms = int(order_ts.timestamp() * 1000)
    start_ms = int((order_ts - timedelta(days=JOURNEY_LOOKBACK_DAYS)).timestamp() * 1000)
    evs = _recipient_events(token, em, start_ms)

    per_code = []
    for code in camp_codes:
        nc = _normalize_code(code)
        sec_ts = orig_ts = None
        for ev in evs:
            if ev.get("type") != "CLICK" or ev.get("created", 0) >= order_ms:
                continue
            u = ev.get("url") or ""
            if f"/discount/{code}".lower() not in u.lower():
                continue
            ts = ev.get("created", 0)
            if "offer_recs" in u:
                sec_ts = max(sec_ts or 0, ts)
            else:
                orig_ts = max(orig_ts or 0, ts)
        received = False
        for sd, ids in code_campaigns.get(nc, []):
            if not (order_ts.date() - timedelta(days=JOURNEY_LOOKBACK_DAYS)
                    <= sd <= order_ts.date()):
                continue
            if any(ev.get("type") == "DELIVERED"
                   and str(ev.get("emailCampaignId")) in ids for ev in evs):
                received = True
                break
        if sec_ts and not received:
            bucket = "A"
        elif sec_ts:
            bucket = "B"
        elif orig_ts or received:
            bucket = "C"
        else:
            bucket = "D"
        lag_h = round((order_ms - sec_ts) / 3.6e6, 1) if sec_ts else None
        per_code.append({"code": code, "bucket": bucket,
                         "section_click": bool(sec_ts),
                         "original_click": bool(orig_ts),
                         "received_original": received,
                         "section_click_lag_hours": lag_h})

    order_bucket = min((pc["bucket"] for pc in per_code),
                       key=lambda b: _BUCKET_RANK[b])
    return {
        "cache_version": CACHE_VERSION,
        "order_id": str(order.get("id")),
        "order_name": order.get("name", ""),
        "order_created_at": order["created_at"],
        "customer_masked": _mask(em),
        "codes": ", ".join(camp_codes),
        "bucket": order_bucket,
        "total_price": float(order.get("total_price", 0)),
        "evidence_json": json.dumps(per_code),
    }


def classify_section_orders(token: str, shopify_auth, raw_v3_emails: list[dict],
                            end_date: date | None = None) -> pd.DataFrame:
    """Classify every campaign-coded order since SECTION_LAUNCH_DATE.

    Cached by order id in output/journey_cache.csv — each order is fetched
    and classified exactly once, ever.
    """
    end_date = end_date or date.today()
    cache = pd.DataFrame()
    if os.path.isfile(CACHE_FILE):
        try:
            cache = pd.read_csv(CACHE_FILE, dtype={"order_id": str})
            cache = cache[cache.get("cache_version", 0) == CACHE_VERSION]
        except Exception:
            cache = pd.DataFrame()
    known_ids = set(cache["order_id"]) if not cache.empty else set()

    orders = _fetch_orders_in_window(shopify_auth, SECTION_LAUNCH_DATE, end_date)
    code_campaigns = build_code_campaign_map(raw_v3_emails)

    new_rows = []
    for o in orders:
        if str(o.get("id")) in known_ids:
            continue
        row = _classify_one_order(o, token, code_campaigns)
        if row:
            new_rows.append(row)

    result = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True) \
        if new_rows else cache
    if not result.empty:
        result.sort_values("order_created_at", ascending=False, inplace=True)
        result.reset_index(drop=True, inplace=True)
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            result.to_csv(CACHE_FILE, index=False)
        except Exception as exc:
            print(f"  [journey] WARNING: could not persist cache: {exc}")
    return result


def bucket_summary(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Orders + revenue by evidence bucket (one row per bucket, A→D)."""
    rows = []
    for b in ["A", "B", "C", "D"]:
        sub = orders_df[orders_df["bucket"] == b] if not orders_df.empty \
            else pd.DataFrame()
        rows.append({"Bucket": BUCKET_LABELS[b],
                     "Orders": len(sub),
                     "Revenue": round(sub["total_price"].sum(), 2)
                     if not sub.empty else 0.0})
    return pd.DataFrame(rows)
