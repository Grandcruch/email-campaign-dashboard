"""
producer_mapping.py — Load producer/tier/region lookup tables from Excel.

All loaders return empty dicts/tuples on FileNotFoundError or parse failure
so the dashboard degrades gracefully (tier/region shows 'Unmapped') rather
than crashing.
"""

import os
import re

import pandas as pd


EXCLUDED_OFFER_TYPES = {
    "bin sale", "holiday", "format promotion", "category offer", "automation",
}
INCLUDED_OFFER_TYPES = {"standalone producer", "combo offer"}
UNMAPPED_LABEL = "Unmapped — review in QA"


# Column → (top-level region, sub-region) for the Producer - Region sheet.
# Producer data starts at row index 3 (after 3 header rows).
_REGION_COL_MAP = {
    0: ("France",    "Bordeaux Left Bank"),
    1: ("France",    "Bordeaux Right Bank"),
    2: ("France",    "Burgundy"),
    3: ("France",    "Other France"),
    4: ("US",        "California"),
    5: ("US",        "Other US"),
    6: ("Australia", "Australia"),
    7: ("Spain",     "Spain"),
    8: ("Italy",     "Italy"),
}


def _norm(value) -> str:
    """Lowercase + collapse whitespace + strip curly/straight quotes."""
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"[''`]", "", s)          # strip apostrophes/curly quotes
    s = re.sub(r"\s+", " ", s)           # collapse internal whitespace
    return s.lower()


def _split_codes(raw) -> list[str]:
    """Split a comma-separated codes cell into individual lowercased codes."""
    if not raw or (isinstance(raw, float)):
        return []
    return [c.strip().lower() for c in str(raw).split(",") if c.strip()]


def load_producer_mapping(excel_path: str) -> dict[str, str]:
    """
    Returns {discount_code_lower: producer_name}.
    All rows including 'Other -' buckets are included so BINSALE_GROUP etc.
    resolve to their bucket names. Exclusion from the producer drill-down
    is handled downstream via offer-type filtering.
    """
    if not os.path.isfile(excel_path):
        print(f"  [producer_mapping] WARNING: {excel_path} not found; producer map empty.")
        return {}
    try:
        df = pd.read_excel(excel_path, sheet_name="Sheet1")
        result: dict[str, str] = {}
        for _, row in df.iterrows():
            producer = str(row.get("Producer") or "").strip()
            if not producer:
                continue
            for code in _split_codes(row.get("Discount Codes Included")):
                result[code] = producer
        print(f"  [producer_mapping] Loaded {len(result)} code->producer entries "
              f"({len(df)} rows)")
        return result
    except Exception as exc:
        print(f"  [producer_mapping] ERROR loading producer map: {exc}")
        return {}


def load_offer_type_mapping(excel_path: str) -> dict[str, str]:
    """
    Returns {discount_code_lower: offer_type_lowercase}.
    NaN / empty Offer Type treated as 'standalone producer'.
    """
    if not os.path.isfile(excel_path):
        return {}
    try:
        df = pd.read_excel(excel_path, sheet_name="Sheet1")
        result: dict[str, str] = {}
        for _, row in df.iterrows():
            raw_type = row.get("Offer Type")
            offer_type = str(raw_type).strip().lower() if raw_type and str(raw_type) != "nan" else "standalone producer"
            for code in _split_codes(row.get("Discount Codes Included")):
                result[code] = offer_type
        return result
    except Exception as exc:
        print(f"  [producer_mapping] ERROR loading offer type map: {exc}")
        return {}


def load_tier_mapping(excel_path: str) -> dict[str, str]:
    """
    Returns {producer_name_lower: tier_label}.
    Reads 'Producer - Tier' sheet; each column header is the tier name.
    """
    if not os.path.isfile(excel_path):
        print(f"  [producer_mapping] WARNING: {excel_path} not found; tier map empty.")
        return {}
    try:
        df = pd.read_excel(excel_path, sheet_name="Producer - Tier", header=0)
        result: dict[str, str] = {}
        for tier_label in df.columns:
            tier_label = str(tier_label).strip()
            for val in df[tier_label].dropna():
                key = _norm(val)
                if key:
                    result[key] = tier_label
        print(f"  [producer_mapping] Loaded {len(result)} producer->tier entries")
        return result
    except Exception as exc:
        print(f"  [producer_mapping] ERROR loading tier map: {exc}")
        return {}


def load_region_mapping(excel_path: str) -> tuple[dict[str, str], dict[str, str]]:
    """
    Returns (top_region_map, sub_region_map).
    top_region_map: {producer_lower: 'France'|'US'|'Australia'|'Spain'|'Italy'}
    sub_region_map: {producer_lower: sub_region_label}
    """
    if not os.path.isfile(excel_path):
        print(f"  [producer_mapping] WARNING: {excel_path} not found; region map empty.")
        return {}, {}
    try:
        df = pd.read_excel(excel_path, sheet_name="Producer - Region", header=None)
        top_map: dict[str, str] = {}
        sub_map: dict[str, str] = {}
        # Producer data starts at row index 3
        for _, row in df.iloc[3:].iterrows():
            for col_idx, (top_region, sub_region) in _REGION_COL_MAP.items():
                if col_idx >= len(row):
                    continue
                val = row.iloc[col_idx]
                if val and str(val).strip() and str(val) != "nan":
                    key = _norm(val)
                    if key:
                        top_map[key] = top_region
                        sub_map[key] = sub_region
        print(f"  [producer_mapping] Loaded {len(top_map)} producer->region entries")
        return top_map, sub_map
    except Exception as exc:
        print(f"  [producer_mapping] ERROR loading region map: {exc}")
        return {}, {}


def resolve_producer(
    discount_code: str,
    producer_map: dict[str, str],
) -> str | None:
    """
    Resolve a discount_code to a producer name.
    Returns None if the code is not in the mapping.
    The code 'None' (string) always returns None.
    """
    if not discount_code or discount_code == "None":
        return None
    return producer_map.get(discount_code.lower())


def get_offer_type(
    discount_code: str,
    offer_type_map: dict[str, str],
) -> str:
    """Return offer type for a code, defaulting to 'standalone producer'."""
    if not discount_code or discount_code == "None":
        return "standalone producer"
    return offer_type_map.get(discount_code.lower(), "standalone producer")


def resolve_tier(producer_name: str, tier_map: dict[str, str]) -> str:
    """Resolve producer name to tier, with substring fallback. Returns 'Untiered' on miss."""
    if not producer_name or not tier_map:
        return "Untiered"
    key = _norm(producer_name)
    if key in tier_map:
        return tier_map[key]
    # Substring fallback: check if any tier key is contained in the producer name or vice versa
    for tier_key, tier_label in tier_map.items():
        if tier_key in key or key in tier_key:
            return tier_label
    return "Untiered"


def resolve_region(
    producer_name: str,
    top_map: dict[str, str],
    sub_map: dict[str, str],
) -> tuple[str, str]:
    """
    Returns (top_region, sub_region). Both 'Unmapped Region' on miss.
    """
    if not producer_name or not top_map:
        return "Unmapped Region", "Unmapped Region"
    key = _norm(producer_name)
    if key in top_map:
        return top_map[key], sub_map.get(key, top_map[key])
    # Substring fallback
    for region_key in top_map:
        if region_key in key or key in region_key:
            return top_map[region_key], sub_map.get(region_key, top_map[region_key])
    return "Unmapped Region", "Unmapped Region"
