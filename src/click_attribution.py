"""
click_attribution.py — Click-confirmed secondary attribution.

The UTM rule (shopify_orders._order_utm_matches) only sees the FIRST page of
the buying session. A buyer who clicks the email, leaves, and comes back later
through Google, a typed URL or another device is invisible to it. This pass
closes that gap with HubSpot's recipient-level CLICK events:

An order is **click-influenced** by a campaign when, inside the campaign's
attribution window:
    1. the order's customer email CLICKED that campaign's email before the
       order was placed (HubSpot v1 events API), AND
    2. the order contains the campaign's offered wine (same line match as
       Influenced Offer Revenue — shopify_orders._offer_line_revenue), AND
    3. the order used NO known campaign code / automatic discount title, AND
    4. the order is NOT already UTM-influenced by any campaign.

Rules 3 and 4 make the new columns ADDITIVE: Influenced Offer Revenue +
Click-Influenced Offer Revenue never counts the same order twice. When one
order qualifies for several campaigns, it goes to the campaign whose email the
buyer clicked most recently before ordering (single attribution).

Never blended into the locked primary metrics. Null = not computed (codeless
campaign, family/BIN campaign, or the click fetch failed); 0 = computed, none.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from .journey_attribution import _paged_events
from .shopify_orders import (
    CampaignAttribution,
    _offer_line_revenue,
    _order_uses_campaign_identifier,
)


def fetch_campaign_clicks(token: str, campaign_ids: set[str]) -> dict[str, dict[str, list[int]]]:
    """
    {v1 campaign id: {recipient email (lowercase): [click epoch ms, ...]}}.
    Campaign ids whose fetch fails are omitted, so callers can tell
    "no clicks" (empty dict) from "not fetched" (missing key).
    """
    def _one(cid: str):
        try:
            events = _paged_events(token, {"campaignId": cid, "eventType": "CLICK", "limit": 1000})
        except Exception as exc:
            print(f"    click fetch failed for campaign {cid}: {exc}")
            return cid, None
        clicks: dict[str, list[int]] = {}
        for e in events:
            em = (e.get("recipient") or "").lower()
            if em:
                clicks.setdefault(em, []).append(int(e.get("created", 0)))
        return cid, clicks

    result: dict[str, dict[str, list[int]]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for cid, clicks in pool.map(_one, sorted(campaign_ids)):
            if clicks is not None:
                result[cid] = clicks
    return result


def apply_click_attribution(
    token: str,
    main_records: list,
    attributions: dict[str, CampaignAttribution],
    orders: list[dict],
    all_campaign_identifiers: set[str],
) -> None:
    """
    Populate click_influenced_* on each standard (non-family) attribution in
    `attributions` (keyed "code|send_date", as built by the orchestrators).
    """
    # One task per attribution key; A/B versions share a key and one email can
    # span several campaign ids, so union them all.
    tasks: dict[str, dict] = {}
    for rec in main_records:
        p = rec.parsed
        if not p.discount_code or p.is_family_key or not rec.hubspot_v1_campaign_ids:
            continue
        key = f"{p.discount_code.lower()}|{p.parsed_send_date}"
        if key not in attributions:
            continue
        t = tasks.setdefault(key, {
            "send_date": p.parsed_send_date,
            "window": p.attribution_window_days,
            "topic": p.producer_topic or "",
            "ids": set(),
        })
        t["ids"].update(rec.hubspot_v1_campaign_ids)

    all_ids = set().union(*(t["ids"] for t in tasks.values())) if tasks else set()
    print(f"  Fetching HubSpot click events for {len(all_ids)} campaign email(s)...")
    clicks = fetch_campaign_clicks(token, all_ids)

    # email -> [(task key, [click ms...])] for tasks whose clicks were all fetched
    by_email: dict[str, list[tuple[str, list[int]]]] = {}
    for key, t in tasks.items():
        if not all(cid in clicks for cid in t["ids"]):
            continue  # stays None — not computed
        attr = attributions[key]
        attr.click_influenced_orders = 0
        attr.click_influenced_offer_revenue = 0.0
        merged: dict[str, list[int]] = {}
        for cid in t["ids"]:
            for em, ts in clicks[cid].items():
                merged.setdefault(em, []).extend(ts)
        for em, ts in merged.items():
            by_email.setdefault(em, []).append((key, ts))

    utm_names = {n for a in attributions.values() for n in a.influenced_order_names}

    for order in orders:
        em = ((order.get("customer") or {}).get("email") or order.get("email") or "").lower()
        if not em or em not in by_email:
            continue
        if order.get("name", "") in utm_names:
            continue
        if _order_uses_campaign_identifier(order, all_campaign_identifiers):
            continue
        created = datetime.fromisoformat(order["created_at"]).astimezone(timezone.utc)
        order_ms = int(created.timestamp() * 1000)

        best = None  # (latest click before order, task key, offer revenue)
        for key, ts in by_email[em]:
            t = tasks[key]
            # Same bounds as shopify_orders._order_in_window
            lo = datetime(t["send_date"].year, t["send_date"].month, t["send_date"].day, tzinfo=timezone.utc)
            hi_d = t["send_date"] + timedelta(days=t["window"] + 1)
            hi = datetime(hi_d.year, hi_d.month, hi_d.day, 10, tzinfo=timezone.utc)
            if not lo <= created <= hi:
                continue
            prior = [c for c in ts if c <= order_ms]
            if not prior:
                continue
            offer_rev = _offer_line_revenue(order, t["topic"])
            if offer_rev <= 0:
                continue
            if best is None or max(prior) > best[0]:
                best = (max(prior), key, offer_rev)

        if best:
            attr = attributions[best[1]]
            attr.click_influenced_orders += 1
            attr.click_influenced_offer_revenue += best[2]
            attr.click_influenced_order_names.append(order.get("name", ""))
