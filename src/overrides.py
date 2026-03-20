"""
overrides.py — Load and apply manual campaign overrides from campaign_overrides.csv.
"""

import csv
import os
from dataclasses import dataclass

from .config import OVERRIDES_FILE
from .hubspot import CampaignRecord


@dataclass
class Override:
    hubspot_email_name: str
    override_send_date: str = ""
    override_producer_topic: str = ""
    override_campaign_type: str = ""
    override_offer_value: str = ""
    override_discount_code: str = ""
    override_window_days: str = ""
    force_include: bool = False
    force_exclude: bool = False
    notes: str = ""


def load_overrides() -> dict[str, Override]:
    """Load campaign_overrides.csv if it exists. Returns dict keyed by hubspot_email_name."""
    if not os.path.isfile(OVERRIDES_FILE):
        return {}

    overrides: dict[str, Override] = {}
    with open(OVERRIDES_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("hubspot_email_name", "").strip()
            if not name:
                continue
            ov = Override(
                hubspot_email_name=name,
                override_send_date=row.get("override_send_date", "").strip(),
                override_producer_topic=row.get("override_producer_topic", "").strip(),
                override_campaign_type=row.get("override_campaign_type", "").strip(),
                override_offer_value=row.get("override_offer_value", "").strip(),
                override_discount_code=row.get("override_discount_code", "").strip(),
                override_window_days=row.get("override_window_days", "").strip(),
                force_include=row.get("force_include", "").strip().lower() == "true",
                force_exclude=row.get("force_exclude", "").strip().lower() == "true",
                notes=row.get("notes", "").strip(),
            )
            overrides[name] = ov

    print(f"  [overrides] Loaded {len(overrides)} override(s)")
    return overrides


def apply_overrides(records: list[CampaignRecord], overrides: dict[str, Override]) -> None:
    """
    Apply overrides to campaign records IN PLACE.
    Overrides are matched by exact raw_name.
    """
    if not overrides:
        return

    from datetime import date, timedelta
    from .config import DEFAULT_ATTRIBUTION_WINDOW_DAYS

    for rec in records:
        ov = overrides.get(rec.parsed.raw_name)
        if ov is None:
            continue

        original_bucket = rec.parsed.qa_bucket

        # force_exclude takes highest precedence
        if ov.force_exclude:
            rec.parsed.qa_bucket = "FORCE_EXCLUDED"
            continue

        # Apply field overrides
        if ov.override_send_date:
            try:
                parts = ov.override_send_date.split("-")
                rec.parsed.parsed_send_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
            except (ValueError, IndexError):
                pass

        if ov.override_producer_topic:
            rec.parsed.producer_topic = ov.override_producer_topic

        if ov.override_campaign_type:
            rec.parsed.campaign_type = ov.override_campaign_type.upper()

        if ov.override_offer_value:
            rec.parsed.offer_value = ov.override_offer_value

        if ov.override_discount_code:
            if ov.override_discount_code.lower() == "none":
                rec.parsed.discount_code = None
            else:
                rec.parsed.discount_code = ov.override_discount_code

        if ov.override_window_days:
            try:
                rec.parsed.attribution_window_days = int(ov.override_window_days)
            except ValueError:
                pass

        # Recompute window end
        if rec.parsed.parsed_send_date:
            rec.parsed.attribution_window_end = (
                rec.parsed.parsed_send_date + timedelta(days=rec.parsed.attribution_window_days)
            )

        # force_include promotes to main table
        if ov.force_include:
            rec.parsed.qa_bucket = "OK_OVERRIDE"
        elif original_bucket in ("PARSE_ERROR", "LEGACY_FORMAT") and ov.override_campaign_type:
            # Override provided enough info to promote
            rec.parsed.qa_bucket = "OK_OVERRIDE"
