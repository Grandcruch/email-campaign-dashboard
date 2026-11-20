"""
products.py — Attributed line items as a table, for the Producers tab's
"which products earn the revenue" view (logic_spec 6.6, added 2026-11-20).

Only lines the campaign discount applied to are included, with the same net
revenue (price × quantity − campaign discount) that makes up Attributed
Revenue, so a producer's product rows add up to its attributed revenue.

Vintage and bottle size come from the SKU (e.g. BDX:MAR:MARG-0750-2017-F0L0C0
→ 750ml, 2017), falling back to the product title ("2017 Chateau Margaux,
Margaux, Bottle (750ml)").

"Featured" compares the product's vintage with the vintages named in the
campaign name's Producer/Topic ("2016 Mouton" → 2016): True = a featured
vintage, False = another vintage of a campaign that named vintages,
None = the campaign named no vintage or the product has none.
"""

import re
from datetime import date

import pandas as pd

from .shopify_orders import CampaignAttribution

_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
_LEADING_YEAR_RE = re.compile(r"^\s*((?:19|20)\d{2})\b")
_TITLE_SIZE_RE = re.compile(r"\((\d+(?:\.\d+)?)\s*(ml|l)\)", re.IGNORECASE)
_TITLE_CASE_RE = re.compile(r"\bCase of (\d+)", re.IGNORECASE)

LINE_COLUMNS = [
    "code_key", "send_date", "window_end", "topic", "order_id", "is_new_customer",
    "product_id", "title", "sku", "quantity", "net_revenue", "discount",
    "vintage", "size", "featured",
]


def _size_label(code: str) -> str:
    code = code.upper()
    if code.startswith("PK") and code[2:].isdigit():
        return f"Case of {int(code[2:])}"
    if code.isdigit():
        ml = int(code)
        return f"{ml}ml" if ml < 1000 else f"{ml / 1000:g}L"
    return ""


def parse_vintage_size(title: str, sku: str) -> tuple[str, str]:
    """(vintage, size) — vintage is 'YYYY', 'NV' or ''; size like '750ml'."""
    vintage = size = ""
    parts = (sku or "").split(":")[-1].split("-")
    if len(parts) >= 3:
        size = _size_label(parts[1])
        v = parts[2].upper()
        if re.fullmatch(r"(?:19|20)\d{2}", v) or v == "NV":
            vintage = v
    if not vintage:
        m = _LEADING_YEAR_RE.match(title or "")
        if m:
            vintage = m.group(1)
        elif (title or "").strip().upper().startswith("NV"):
            vintage = "NV"
    if not size:
        m = _TITLE_SIZE_RE.search(title or "")
        if m:
            qty, unit = float(m.group(1)), m.group(2).lower()
            ml = qty if unit == "ml" else qty * 1000
            size = f"{ml:g}ml" if ml < 1000 else f"{ml / 1000:g}L"
        else:
            m = _TITLE_CASE_RE.search(title or "")
            if m:
                size = f"Case of {int(m.group(1))}"
    return vintage, size


def topic_years(topic: str) -> set[str]:
    return set(_YEAR_RE.findall(topic or ""))


def attribution_lines_frame(
    attributions: dict[str, CampaignAttribution],
    topics_by_key: dict[str, str],
    window_end_by_key: dict[str, date],
) -> pd.DataFrame:
    """One row per attributed line item across all campaigns.

    topics_by_key / window_end_by_key are keyed like `attributions`
    ("code|YYYY-MM-DD"); topics of A/B versions sharing a key are joined.
    """
    rows = []
    for key, attr in attributions.items():
        code_key, _, send_date = key.partition("|")
        topic = topics_by_key.get(key, "")
        years = topic_years(topic)
        for o in attr.matched_orders:
            for ln in getattr(o, "lines", []) or []:
                vintage, size = parse_vintage_size(ln.get("title", ""), ln.get("sku", ""))
                featured = (vintage in years) if (years and vintage and vintage != "NV") else None
                rows.append({
                    "code_key": code_key,
                    "send_date": send_date,
                    "window_end": window_end_by_key.get(key),
                    "topic": topic,
                    "order_id": o.order_id,
                    "is_new_customer": o.is_new_customer,
                    "product_id": ln.get("product_id"),
                    "title": ln.get("title", ""),
                    "sku": ln.get("sku", ""),
                    "quantity": ln.get("quantity", 0),
                    "net_revenue": ln.get("net_revenue", 0.0),
                    "discount": ln.get("discount", 0.0),
                    "vintage": vintage,
                    "size": size,
                    "featured": featured,
                })
    return pd.DataFrame(rows, columns=LINE_COLUMNS)
