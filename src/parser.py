"""
parser.py — Campaign name parser, date normaliser, and BIN/holiday detector.
"""

import re
from dataclasses import dataclass
from datetime import date, timedelta

from .config import (
    BIN_HOLIDAY_KEYWORDS,
    DATA_START_DATE,
    DEFAULT_ATTRIBUTION_WINDOW_DAYS,
    BIN_HOLIDAY_ATTRIBUTION_WINDOW_DAYS,
)

# Regex to capture date portion:  2026-03-20 | 2026-0323 | 2026_0317
DATE_RE = re.compile(r"^(\d{4})[-_]?(\d{2})-?(\d{2})")


@dataclass
class ParsedCampaign:
    """Result of parsing a campaign name."""
    raw_name: str
    parsed_send_date: date | None = None
    producer_topic: str | None = None
    campaign_type: str | None = None
    offer_value: str | None = None
    discount_code: str | None = None          # None means Code = "None" or unparseable
    is_bin_holiday: bool = False
    attribution_window_days: int = DEFAULT_ATTRIBUTION_WINDOW_DAYS
    attribution_window_end: date | None = None
    qa_bucket: str = "PARSE_ERROR"


def normalise_date(raw: str) -> date | None:
    """
    Normalise date string from segment[0] of campaign name.
    Handles: 2026-03-20, 2026-0323, 2026_0317
    """
    m = DATE_RE.match(raw.strip())
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def is_bin_or_holiday(campaign_name: str, producer_topic: str) -> bool:
    combined = (campaign_name + " " + producer_topic).lower()
    return any(kw in combined for kw in BIN_HOLIDAY_KEYWORDS)


VALID_TYPES = {"PROD", "EDU", "CONTENT"}


def parse_campaign_name(raw_name: str) -> ParsedCampaign:
    """
    Parse a campaign name following the convention:
        YYYY-MM-DD - Producer/Topic - Type - OfferValue - Code
    Returns a ParsedCampaign with qa_bucket reflecting parse status.
    """
    result = ParsedCampaign(raw_name=raw_name)

    segments = [s.strip() for s in raw_name.split(" - ")]

    # ── Date ──
    if not segments:
        return result
    result.parsed_send_date = normalise_date(segments[0])
    if result.parsed_send_date is None:
        return result

    # Date out of range?
    if result.parsed_send_date < DATA_START_DATE:
        result.qa_bucket = "DATE_OUT_OF_RANGE"
        return result

    # ── EDU / CONTENT: accept 3-segment format: Date - Producer/Topic - Type ──
    if len(segments) == 3 and segments[2].upper().strip() in {"EDU", "CONTENT"}:
        result.producer_topic = segments[1]
        result.campaign_type = segments[2].upper().strip()
        result.offer_value = None
        result.discount_code = None  # null — attribution not applicable
        result.is_bin_holiday = is_bin_or_holiday(raw_name, result.producer_topic)
        result.attribution_window_days = (
            BIN_HOLIDAY_ATTRIBUTION_WINDOW_DAYS if result.is_bin_holiday
            else DEFAULT_ATTRIBUTION_WINDOW_DAYS
        )
        result.attribution_window_end = (
            result.parsed_send_date + timedelta(days=result.attribution_window_days)
        )
        result.qa_bucket = "OK_NO_CODE"
        return result

    # ── Need exactly 5 segments for PROD convention ──
    if len(segments) < 5:
        result.qa_bucket = "LEGACY_FORMAT"
        # Best-effort: grab producer/topic if available
        if len(segments) >= 2:
            result.producer_topic = segments[1]
        return result

    result.producer_topic = segments[1]
    result.campaign_type = segments[2].upper().strip()
    result.offer_value = segments[3].strip()
    code_raw = segments[4].strip()

    # Validate campaign type
    if result.campaign_type not in VALID_TYPES:
        result.qa_bucket = "PARSE_ERROR"
        return result

    # Discount code
    if code_raw.lower() == "none" or code_raw == "":
        result.discount_code = None
    else:
        result.discount_code = code_raw

    # BIN / holiday detection
    result.is_bin_holiday = is_bin_or_holiday(raw_name, result.producer_topic)
    result.attribution_window_days = (
        BIN_HOLIDAY_ATTRIBUTION_WINDOW_DAYS if result.is_bin_holiday
        else DEFAULT_ATTRIBUTION_WINDOW_DAYS
    )
    result.attribution_window_end = (
        result.parsed_send_date + timedelta(days=result.attribution_window_days)
    )

    # QA bucket — initial assignment (may be refined later)
    if result.discount_code is None:
        result.qa_bucket = "OK_NO_CODE"
    else:
        result.qa_bucket = "OK"

    return result
