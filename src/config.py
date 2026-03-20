"""
config.py — Load credentials from .env.txt and define project constants.
"""

import os
from datetime import date

# ─── Project paths ───────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(PROJECT_ROOT, ".env.txt")
OVERRIDES_FILE = os.path.join(PROJECT_ROOT, "campaign_overrides.csv")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# ─── Business constants ──────────────────────────────────────────────────────
DATA_START_DATE = date(2026, 3, 9)
DEFAULT_ATTRIBUTION_WINDOW_DAYS = 7
BIN_HOLIDAY_ATTRIBUTION_WINDOW_DAYS = 3
SHOPIFY_API_VERSION = "2025-01"

BIN_HOLIDAY_KEYWORDS = [
    "bin sale", "bin sale reminder",
    "holiday sale", "holiday sale reminder",
    "flash sale", "clearance",
]

KNOWN_NON_CAMPAIGN_CODE_PATTERNS = [
    "grandcru",       # new-user offer (exact, case-insensitive)
]
KNOWN_NON_CAMPAIGN_CODE_PREFIXES = [
    "gcla-",          # auto-generated new-user codes
    "thankyou",       # holiday/thank-you offers
]


def _load_from_streamlit_secrets() -> dict | None:
    """
    Try loading credentials from Streamlit's secrets manager.
    Returns None if not running inside Streamlit or secrets not configured.
    """
    try:
        import streamlit as st
        secrets = st.secrets
        env = {
            "HUBSPOT_PRIVATE_APP_TOKEN": secrets["HUBSPOT_PRIVATE_APP_TOKEN"],
            "SHOPIFY_CLIENT_ID": secrets["SHOPIFY_CLIENT_ID"],
            "SHOPIFY_CLIENT_SECRET": secrets["SHOPIFY_CLIENT_SECRET"],
            "SHOPIFY_STORE_DOMAIN": secrets["SHOPIFY_STORE_DOMAIN"],
        }
        return env
    except Exception:
        return None


def _load_from_env_file(filepath: str) -> dict:
    """
    Parse .env.txt which mixes KEY=VALUE and 'Label: value' formats.
    Returns a dict with normalised keys.
    """
    env: dict[str, str] = {}
    key_map = {
        "HUBSPOT_PRIVATE_APP_TOKEN": "HUBSPOT_PRIVATE_APP_TOKEN",
        "Shopify API Client ID": "SHOPIFY_CLIENT_ID",
        "Secret": "SHOPIFY_CLIENT_SECRET",
        "Store": "SHOPIFY_STORE_DOMAIN",
        "SHOPIFY_API_VERSION": "SHOPIFY_API_VERSION",
    }

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Credential file not found: {filepath}")

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            for file_key, env_key in key_map.items():
                if line.startswith(file_key):
                    if "=" in line and file_key == line.split("=")[0]:
                        env[env_key] = line.split("=", 1)[1].strip()
                    elif ":" in line:
                        env[env_key] = line.split(":", 1)[1].strip()

    return env


def load_env(filepath: str = ENV_FILE) -> dict:
    """
    Load credentials from Streamlit secrets (cloud) or .env.txt (local).
    Streamlit secrets take priority when available.
    """
    # Try Streamlit secrets first (works on Streamlit Community Cloud)
    env = _load_from_streamlit_secrets()
    if env:
        return env

    # Fall back to .env.txt for local development
    env = _load_from_env_file(filepath)

    # Validate required keys
    required = ["HUBSPOT_PRIVATE_APP_TOKEN", "SHOPIFY_CLIENT_ID",
                "SHOPIFY_CLIENT_SECRET", "SHOPIFY_STORE_DOMAIN"]
    missing = [k for k in required if k not in env]
    if missing:
        raise EnvironmentError(f"Missing required credentials: {missing}")

    return env
