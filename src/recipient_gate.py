"""
recipient_gate.py — Email-recipient rule for automatic discounts (logic_spec 5.7).

An automatic discount applies itself to anyone who reaches the product page,
whether or not they got the email. A typed code implies the buyer saw the
offer somewhere; an automatic discount does not. So an order matched through
an AUTOMATIC discount (title == campaign code / family identifier) stays in
Attributed Revenue only when the order's customer email was DELIVERED that
offer's email before the order was placed (HubSpot v1 DELIVERED events).

"That offer's email" = any send of the same code / family key in the same
chain of sends no more than EVENT_MAX_GAP_DAYS apart, up to and including the
send that owns the order — so a BIN Sale buyer who got email 1 but bought
after reminder 2 still counts.

Typed-code orders are never gated.

Before gating, every attribution's totals are copied to all_buyer_orders /
all_buyer_revenue so the dashboard can still show what the offer sold to
everyone. If the delivered-events fetch fails for an attribution that has
automatic orders, that attribution is left ungated and listed in
apply_recipient_gate.last_failures.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from .config import EVENT_MAX_GAP_DAYS
from .journey_attribution import _paged_events
from .shopify_orders import CampaignAttribution


def fetch_campaign_deliveries(token: str, campaign_ids: set[str]) -> dict[str, dict[str, int]]:
    """
    {v1 campaign id: {recipient email (lowercase): first delivery epoch ms}}.
    Ids whose fetch fails are omitted.
    """
    def _one(cid: str):
        try:
            events = _paged_events(token, {"campaignId": cid, "eventType": "DELIVERED", "limit": 1000})
        except Exception as exc:
            print(f"    delivered-events fetch failed for campaign {cid}: {exc}")
            return cid, None
        out: dict[str, int] = {}
        for e in events:
            em = (e.get("recipient") or "").lower()
            if not em:
                continue
            ts = int(e.get("created", 0))
            if em not in out or ts < out[em]:
                out[em] = ts
        return cid, out

    result: dict[str, dict[str, int]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for cid, d in pool.map(_one, sorted(campaign_ids)):
            if d is not None:
                result[cid] = d
    return result


def _chain_dates(send_date, all_dates: list) -> list:
    """Send dates in the same <= EVENT_MAX_GAP_DAYS chain, up to send_date."""
    dates = sorted(d for d in set(all_dates) if d <= send_date)
    chain = [send_date]
    for d in reversed(dates):
        if d >= chain[-1]:
            continue
        if (chain[-1] - d).days > EVENT_MAX_GAP_DAYS:
            break
        chain.append(d)
    return chain


def _order_ms(created_at: str) -> int:
    return int(datetime.fromisoformat(created_at).astimezone(timezone.utc).timestamp() * 1000)


def _recompute(attr: CampaignAttribution) -> None:
    m = attr.matched_orders
    attr.attributed_revenue = sum(o.attributed_revenue for o in m)
    attr.discount_value = sum(o.discount_value for o in m)
    attr.total_order_value = sum(o.order_total_price for o in m)
    attr.total_order_subtotal = sum(o.order_subtotal for o in m)
    attr.discounted_orders = len(m)


def apply_recipient_gate(
    token: str,
    main_records: list,
    attributions: dict[str, CampaignAttribution],
) -> None:
    """Gate automatic-discount orders on email delivery, in place."""
    apply_recipient_gate.last_failures = []

    for attr in attributions.values():
        attr.all_buyer_orders = attr.discounted_orders
        attr.all_buyer_revenue = attr.attributed_revenue

    # v1 ids per attribution key ("code|send_date"); A/B versions share a key.
    ids_by_key: dict[str, set[str]] = {}
    dates_by_code: dict[str, list] = {}
    for rec in main_records:
        p = rec.parsed
        if not p.discount_code:
            continue
        code = p.discount_code.lower()
        ids_by_key.setdefault(f"{code}|{p.parsed_send_date}", set()).update(rec.hubspot_v1_campaign_ids)
        dates_by_code.setdefault(code, []).append(p.parsed_send_date)

    needed: dict[str, set[str]] = {}
    for key, attr in attributions.items():
        if not any(o.is_automatic for o in attr.matched_orders):
            continue
        code, sd = key.split("|", 1)
        send_date = datetime.strptime(sd, "%Y-%m-%d").date()
        ids: set[str] = set()
        for d in _chain_dates(send_date, dates_by_code.get(code, [send_date])):
            ids |= ids_by_key.get(f"{code}|{d}", set())
        needed[key] = ids
    if not needed:
        return

    all_ids = set().union(*needed.values())
    print(f"  Fetching HubSpot delivery records for {len(all_ids)} email(s) "
          f"behind {len(needed)} automatic-discount offer(s)...")
    deliveries = fetch_campaign_deliveries(token, all_ids)

    for key, ids in needed.items():
        attr = attributions[key]
        if not ids or not all(cid in deliveries for cid in ids):
            apply_recipient_gate.last_failures.append(key)
            continue
        first: dict[str, int] = {}
        for cid in ids:
            for em, ts in deliveries[cid].items():
                if em not in first or ts < first[em]:
                    first[em] = ts
        kept = []
        for o in attr.matched_orders:
            if not o.is_automatic:
                kept.append(o)
                continue
            ts = first.get(o.customer_email)
            if ts is not None and ts <= _order_ms(o.order_created_at):
                kept.append(o)
        dropped = len(attr.matched_orders) - len(kept)
        attr.matched_orders = kept
        _recompute(attr)
        if dropped:
            print(f"    {key}: {dropped} automatic-discount order(s) from non-recipients "
                  f"excluded (${attr.all_buyer_revenue - attr.attributed_revenue:,.0f})")


apply_recipient_gate.last_failures = []
