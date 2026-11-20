"""
customer_history.py — New vs existing customers (logic_spec 1.7, added 2026-11-20).

A campaign order is from a NEW customer when it is the buyer's first-ever
Shopify order: no earlier order (not cancelled, not a test order) exists for
the same Shopify customer id or the same email address. Every other order is
from an EXISTING customer, including orders with no customer id and no email
(they cannot be shown to be first orders).

The split covers Attributed Revenue only, so for every campaign
    New Customer Revenue + Existing Customer Revenue = Attributed Revenue.

Order history comes from two places:
  * orders since DATA_START_DATE — the bulk list the pipeline already fetches;
  * orders before DATA_START_DATE (back to the store's first order) — fetched
    once with only id/date/customer fields and cached in
    output/pre_floor_customers.json. Those orders never change, so the cache
    is reused until DATA_START_DATE moves. The cache stores hashed customer
    keys only, never raw emails.

Pre-floor shoppers who come back count as existing. People who bought only
outside Shopify (phone, wholesale, an older site) count as new on their first
Shopify order — agreed with the user 2026-11-20.
"""

import hashlib
import json
import os
import time
from datetime import date, datetime, timezone

import requests

from .auth import ShopifyAuth
from .config import OUTPUT_DIR, SHOPIFY_API_VERSION
from .shopify_orders import CampaignAttribution

PRE_FLOOR_CACHE = os.path.join(OUTPUT_DIR, "pre_floor_customers.json")
CACHE_VERSION = 2


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]


def _keys(customer_id: str, email: str) -> list[str]:
    keys = []
    if customer_id:
        keys.append(_hash(f"id:{customer_id}"))
    if email:
        keys.append(_hash(f"email:{email.strip().lower()}"))
    return keys


def _order_keys(order: dict) -> list[str]:
    cust = order.get("customer") or {}
    cid = str(cust.get("id") or "")
    email = (cust.get("email") or order.get("email") or "")
    return _keys(cid, email)


def _counts_as_purchase(order: dict) -> bool:
    return not order.get("cancelled_at") and not order.get("test")


def _ts(created_at: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(created_at)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_pre_floor_customers(auth: ShopifyAuth, floor: date) -> set[str]:
    """Hashed customer keys of everyone with a purchase before `floor`."""
    if os.path.isfile(PRE_FLOOR_CACHE):
        try:
            with open(PRE_FLOOR_CACHE) as f:
                cached = json.load(f)
            if cached.get("floor") == floor.isoformat() and cached.get("version") == CACHE_VERSION:
                return set(cached["keys"])
        except Exception:
            pass

    # GraphQL: ~0.5 s per 250 orders with only the fields needed (the REST
    # list took ~4 min for ~10K orders). Same boundary as the bulk fetch
    # (created_at_min = floor 00:00:00Z), so the two sets meet exactly.
    url = f"https://{auth.store_domain}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    search = f"created_at:<'{floor.isoformat()}T00:00:00Z'"
    query = """
    query($cursor: String, $search: String!) {
      orders(first: 250, after: $cursor, query: $search) {
        edges { node { createdAt email cancelledAt test customer { id email } } }
        pageInfo { hasNextPage endCursor }
      }
    }"""
    keys: set[str] = set()
    n_orders = 0
    cursor = None
    while True:
        payload = None
        for attempt in range(6):
            resp = requests.post(url, headers=auth.headers(), timeout=60,
                                 json={"query": query, "variables": {"cursor": cursor, "search": search}})
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            payload = resp.json()
            errors = payload.get("errors") or []
            if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors):
                time.sleep(2 * (attempt + 1))
                payload = None
                continue
            if errors:
                raise RuntimeError(f"Shopify GraphQL error: {errors[0].get('message')}")
            break
        if payload is None:
            raise RuntimeError("Shopify GraphQL kept throttling the order-history fetch")
        orders = payload["data"]["orders"]
        for edge in orders["edges"]:
            node = edge["node"]
            n_orders += 1
            if node.get("cancelledAt") or node.get("test"):
                continue
            cust = node.get("customer") or {}
            cid = (cust.get("id") or "").rsplit("/", 1)[-1]   # gid://shopify/Customer/123 -> 123
            keys.update(_keys(cid, cust.get("email") or node.get("email") or ""))
        if not orders["pageInfo"]["hasNextPage"]:
            break
        cursor = orders["pageInfo"]["endCursor"]

    print(f"  [customers] {n_orders} orders before {floor} -> {len(keys)} customer keys")
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(PRE_FLOOR_CACHE, "w") as f:
            json.dump({"version": CACHE_VERSION, "floor": floor.isoformat(),
                       "fetched_at": datetime.now(timezone.utc).isoformat(),
                       "orders": n_orders, "keys": sorted(keys)}, f)
    except Exception as exc:
        print(f"  [customers] WARNING: could not cache pre-floor customers: {exc}")
    return keys


def apply_new_customer_split(
    auth: ShopifyAuth,
    attributions: dict[str, CampaignAttribution],
    all_orders: list[dict],
    floor: date,
) -> None:
    """
    Mark each matched order new/existing and fill each attribution's
    new_customer_orders / new_customer_revenue / existing_customer_revenue,
    in place. Run AFTER the recipient gate so only credited orders count.
    Raises if the pre-floor history cannot be fetched; the caller leaves the
    split blank in that case.
    """
    prior = load_pre_floor_customers(auth, floor)

    # Earliest purchase per customer key since the floor
    earliest: dict[str, datetime] = {}
    for order in all_orders:
        if not _counts_as_purchase(order):
            continue
        ts = _ts(order.get("created_at") or "")
        if ts is None:
            continue
        for k in _order_keys(order):
            if k not in earliest or ts < earliest[k]:
                earliest[k] = ts

    for attr in attributions.values():
        new_orders, new_rev, existing_rev = 0, 0.0, 0.0
        for o in attr.matched_orders:
            keys = _keys(o.customer_id, o.customer_email)
            ts = _ts(o.order_created_at)
            is_new = bool(keys) and ts is not None and all(
                k not in prior and not (k in earliest and earliest[k] < ts)
                for k in keys
            )
            o.is_new_customer = is_new
            if is_new:
                new_orders += 1
                new_rev += o.attributed_revenue
            else:
                existing_rev += o.attributed_revenue
        attr.new_customer_orders = new_orders
        attr.new_customer_revenue = new_rev
        attr.existing_customer_revenue = existing_rev
