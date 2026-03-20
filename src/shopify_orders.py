"""
shopify_orders.py — Fetch Shopify orders and compute line-item attribution.
"""

import requests
from datetime import date, timedelta
from dataclasses import dataclass, field

from .auth import ShopifyAuth
from .config import SHOPIFY_API_VERSION


@dataclass
class OrderAttribution:
    """Attribution results for a single order matched to a campaign."""
    order_id: int = 0
    order_name: str = ""
    order_created_at: str = ""
    attributed_revenue: float = 0.0      # gross value of discounted items
    discount_value: float = 0.0          # dollar discount on those items
    order_total_price: float = 0.0       # full order total_price
    discounted_line_items: int = 0
    total_line_items: int = 0
    financial_status: str = ""


@dataclass
class CampaignAttribution:
    """Aggregated attribution for one campaign across all matched orders."""
    discount_code: str = ""
    attributed_revenue: float = 0.0
    discount_value: float = 0.0
    total_order_value: float = 0.0
    discounted_orders: int = 0
    matched_orders: list[OrderAttribution] = field(default_factory=list)


def _fetch_orders_in_window(
    auth: ShopifyAuth,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Fetch all Shopify orders in [start_date, end_date) with pagination."""
    base_url = f"https://{auth.store_domain}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
    all_orders: list[dict] = []
    params = {
        "status": "any",
        "created_at_min": f"{start_date.isoformat()}T00:00:00Z",
        "created_at_max": f"{end_date.isoformat()}T00:00:00Z",
        "limit": 250,
    }

    while True:
        resp = requests.get(base_url, headers=auth.headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders", [])
        all_orders.extend(orders)

        if len(orders) < 250:
            break

        # Paginate via since_id
        last_id = orders[-1]["id"]
        params["since_id"] = last_id

    return all_orders


def _attribute_order(order: dict, campaign_code: str) -> OrderAttribution | None:
    """
    Compute line-item level attribution for a single order.
    Returns None if the campaign code is not found in this order's discount codes.
    """
    # Check if the order used this campaign's code
    order_codes = [c.get("code", "").lower() for c in order.get("discount_codes", [])]
    if campaign_code.lower() not in order_codes:
        return None

    # Find the discount_application_index for the campaign code
    campaign_app_index = None
    for i, app in enumerate(order.get("discount_applications", [])):
        if (app.get("type") == "discount_code"
                and app.get("code", "").lower() == campaign_code.lower()):
            campaign_app_index = i
            break

    attr = OrderAttribution(
        order_id=order.get("id", 0),
        order_name=order.get("name", ""),
        order_created_at=order.get("created_at", ""),
        order_total_price=float(order.get("total_price", 0)),
        financial_status=order.get("financial_status", ""),
    )

    line_items = order.get("line_items", [])
    attr.total_line_items = len(line_items)

    if campaign_app_index is not None:
        # Line-item level attribution
        for item in line_items:
            for alloc in item.get("discount_allocations", []):
                if alloc.get("discount_application_index") == campaign_app_index:
                    item_gross = float(item.get("price", 0)) * int(item.get("quantity", 1))
                    alloc_amount = float(alloc.get("amount", 0))
                    attr.attributed_revenue += item_gross
                    attr.discount_value += alloc_amount
                    attr.discounted_line_items += 1
                    break  # only count each item once
    else:
        # Fallback: discount_application entry not found (shouldn't happen, but safe)
        # Use order-level discount as a proxy
        attr.attributed_revenue = float(order.get("total_line_items_price", 0))
        attr.discount_value = float(order.get("total_discounts", 0))
        attr.discounted_line_items = len(line_items)

    return attr


def compute_attribution(
    auth: ShopifyAuth,
    discount_code: str,
    send_date: date,
    window_days: int,
) -> CampaignAttribution:
    """
    Fetch orders in the attribution window and compute aggregated metrics
    for the given discount code.
    """
    end_date = send_date + timedelta(days=window_days)
    orders = _fetch_orders_in_window(auth, send_date, end_date)

    result = CampaignAttribution(discount_code=discount_code)

    for order in orders:
        attr = _attribute_order(order, discount_code)
        if attr is None:
            continue
        result.attributed_revenue += attr.attributed_revenue
        result.discount_value += attr.discount_value
        result.total_order_value += attr.order_total_price
        result.discounted_orders += 1
        result.matched_orders.append(attr)

    return result


def fetch_all_discount_codes_in_range(
    auth: ShopifyAuth,
    start_date: date,
    end_date: date,
) -> dict[str, list[dict]]:
    """
    Fetch ALL orders in a date range and return a dict mapping
    discount_code (lowered) -> list of order summary dicts.
    Used for the unmatched discount codes QA report.
    """
    orders = _fetch_orders_in_window(auth, start_date, end_date)
    code_map: dict[str, list[dict]] = {}

    for order in orders:
        for dc in order.get("discount_codes", []):
            code = dc.get("code", "")
            if not code:
                continue
            key = code.lower()
            if key not in code_map:
                code_map[key] = []
            code_map[key].append({
                "code_original": code,
                "order_name": order.get("name", ""),
                "order_id": order.get("id", 0),
                "created_at": order.get("created_at", ""),
                "total_price": float(order.get("total_price", 0)),
                "discount_amount": float(dc.get("amount", 0)),
            })

    return code_map
