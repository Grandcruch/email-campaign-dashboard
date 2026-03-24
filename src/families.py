"""
families.py — Load discount family mappings for multi-code campaigns.

A "discount family" maps a single campaign identifier (family_key) to
multiple Shopify discount identifiers.  This handles cases like BIN Sale
campaigns where automatic discounts (not discount codes) are used, and
multiple discount titles map to one campaign.

The mapping is stored in discount_family_mapping.csv with columns:
    family_key, shopify_identifier_type, shopify_identifier
"""

import csv
import os
from dataclasses import dataclass

from .config import PROJECT_ROOT

FAMILY_MAPPING_FILE = os.path.join(PROJECT_ROOT, "discount_family_mapping.csv")


@dataclass
class FamilyMember:
    """One Shopify discount identifier within a family."""
    identifier_type: str   # e.g. "Discount name", "Discount code"
    identifier: str        # e.g. "BinSale10"


def load_family_mapping() -> dict[str, list[FamilyMember]]:
    """
    Load discount_family_mapping.csv.
    Returns dict: family_key (uppercased) -> list of FamilyMember.
    """
    if not os.path.isfile(FAMILY_MAPPING_FILE):
        return {}

    families: dict[str, list[FamilyMember]] = {}

    with open(FAMILY_MAPPING_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row.get("family_key") or "").strip().upper()
            id_type = (row.get("shopify_identifier_type") or "").strip()
            identifier = (row.get("shopify_identifier") or "").strip()

            if not key or not identifier:
                continue

            if key not in families:
                families[key] = []
            families[key].append(FamilyMember(
                identifier_type=id_type,
                identifier=identifier,
            ))

    print(f"  [families] Loaded {len(families)} family mapping(s): "
          f"{', '.join(f'{k} ({len(v)} codes)' for k, v in families.items())}")
    return families


def is_family_key(code: str, families: dict[str, list[FamilyMember]]) -> bool:
    """Check if a discount code is actually a family key."""
    return code.upper() in families


def get_family_identifiers(
    family_key: str,
    families: dict[str, list[FamilyMember]],
) -> list[FamilyMember]:
    """Get all Shopify identifiers for a family key."""
    return families.get(family_key.upper(), [])
