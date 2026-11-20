#!/usr/bin/env python3
"""
dashboard.py — Streamlit dashboard for the Email Campaign Performance Dashboard.

Re-fetches fresh data from HubSpot + Shopify on each run/refresh,
then displays weekly, monthly, producer, and QA reports in tabbed views.

Usage:
    streamlit run dashboard.py
"""

import sys
import os
import io
import csv
import html
import json
from datetime import date, datetime, timedelta

import requests
import streamlit as st
import pandas as pd
import altair as alt

# Pipeline code version — bump on every deploy-relevant change. Displayed in
# the sidebar so a stale-module deploy (Streamlit Cloud soft-reload keeping
# old src/ modules) is immediately visible instead of masquerading as bad data.
PIPELINE_VERSION = "2026-11-20.3"

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PRODUCER_CODE_EXCEL = os.path.join(PROJECT_ROOT, "Producer - Discount Code Mapping - Copy.xlsx")
PRODUCER_PRODUCT_EXCEL = os.path.join(PROJECT_ROOT, "Producer - Product Mapping - Copy.xlsx")
sys.path.insert(0, PROJECT_ROOT)

from src.config import load_env, DATA_START_DATE, OUTPUT_DIR
from src.auth import ShopifyAuth, hubspot_headers
from src.hubspot import fetch_campaigns, _fetch_v3_emails
from src.journey_attribution import (
    classify_section_orders,
    bucket_summary,
    fetch_section_clicks,
    SECTION_LAUNCH_DATE,
    JOURNEY_LOOKBACK_DAYS,
    BUCKET_LABELS,
)
from src.overrides import load_overrides, apply_overrides
from src.shopify_orders import (
    compute_attribution,
    compute_family_attribution,
    fetch_all_discount_codes_in_range,
    _normalize_code as _normalize_discount_code,
    _fetch_orders_in_window as _fetch_all_shopify_orders,
    event_window_days,
)
from src.click_attribution import apply_click_attribution
from src.recipient_gate import apply_recipient_gate
from src.customer_history import apply_new_customer_split
from src.products import attribution_lines_frame
from src.offer_slots import parse_offer_log, assign_offer_slots
from src.families import load_family_mapping, is_family_key, get_family_identifiers, event_codes
from src.reports import (
    assemble_dashboard_rows,
    rows_to_dataframe,
    apply_ab_grouping,
    apply_event_grouping,
    event_mask,
    generate_weekly_report,
    generate_weekly_insights,
    generate_monthly_report,
    generate_period_report,
    generate_producer_report,
    generate_producer_analytics,
    generate_excluded_campaigns,
    generate_unmatched_codes_report,
    update_history,
    generate_qa_summary,
    write_all_outputs,
    MIN_DELIVERED_THRESHOLD,
)
from src.producer_mapping import (
    load_producer_mapping,
    load_offer_type_mapping,
    load_tier_mapping,
    load_region_mapping,
    resolve_producer,
    UNMAPPED_LABEL,
)


# ─── Design System ──────────────────────────────────────────────────────────

# Color tokens — Red scale palette
# Full scale: "#fef2f2","#fee2e2","#fecaca","#fca5a5","#f87171","#ef4444","#dc2626","#b91c1c","#991b1b","#7f1d1d","#450a0a"
CLR_BG_PAGE = "#ffffff"         # White — page background
CLR_SURFACE = "#ffffff"
CLR_BORDER = "#fecaca"          # Light red border
CLR_ACCENT = "#b91c1c"          # Deep red — brand accent
CLR_WEEKLY = "#991b1b"          # Dark red — Weekly charts
CLR_MONTHLY = "#dc2626"         # Medium red — Monthly charts
CLR_MONTHLY_LINE = "#7f1d1d"    # Very dark red — Monthly combo line
CLR_PRODUCER = "#450a0a"        # Darkest red — Producer charts
CLR_TEAL = "#ef4444"            # Bright red — scatter secondary
CLR_TEXT_PRIMARY = "#450a0a"    # Darkest red — primary text
CLR_TEXT_SECONDARY = "#7f1d1d"  # Dark red — secondary text
CLR_TEXT_MUTED = "#fca5a5"      # Soft red — muted text
CLR_POSITIVE = "#991b1b"        # Dark red — positive indicator
CLR_NEGATIVE = "#ef4444"        # Bright red — negative indicator

# Multi-series palette (dark to light for visual distinction)
PALETTE_MULTI = ["#450a0a", "#991b1b", "#dc2626", "#f87171", "#fca5a5"]

GLOBAL_CSS = f"""
<style>
    /* ── Page background ── */
    .stApp {{
        background-color: {CLR_BG_PAGE};
    }}

    /* ── Content container ── */
    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}

    /* ── Typography ── */
    h1 {{
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        color: {CLR_TEXT_PRIMARY} !important;
        letter-spacing: -0.02em;
    }}
    h2 {{
        font-size: 1.25rem !important;
        font-weight: 600 !important;
        color: {CLR_TEXT_PRIMARY} !important;
    }}
    h3 {{
        font-size: 1.05rem !important;
        font-weight: 600 !important;
        color: {CLR_TEXT_PRIMARY} !important;
    }}

    /* ── KPI Cards: 4 per row (2 when the content area is narrow, e.g.
          sidebar open on a laptop); every card the same size ── */
    .kpi-wrap {{
        container-type: inline-size;
    }}
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.75rem;
    }}
    @container (max-width: 760px) {{
        .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    .kpi-card {{
        background: {CLR_SURFACE};
        border: 1px solid {CLR_BORDER};
        border-radius: 8px;
        padding: 1rem 1.25rem;
        text-align: left;
        height: 108px;
        box-sizing: border-box;
        overflow: hidden;
        min-width: 0;
    }}
    .kpi-card .kpi-label {{
        font-size: 0.75rem;
        font-weight: 500;
        color: {CLR_TEXT_MUTED};
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}
    .kpi-card .kpi-value {{
        font-size: clamp(1.15rem, 1.8vw, 1.5rem);
        font-weight: 700;
        color: {CLR_TEXT_PRIMARY};
        line-height: 1.25;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}
    .kpi-card .kpi-sub {{
        font-size: 0.78rem;
        color: {CLR_TEXT_SECONDARY};
        margin-top: 0.3rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }}

    /* ── Insight Card — all bordered containers get the accent ── */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        border-left: 4px solid {CLR_ACCENT} !important;
        border-radius: 4px !important;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] h4 {{
        font-size: 0.95rem !important;
        font-weight: 600 !important;
        color: {CLR_TEXT_PRIMARY} !important;
        margin-bottom: 0.75rem !important;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] p,
    [data-testid="stVerticalBlockBorderWrapper"] li {{
        font-size: 0.875rem;
        color: {CLR_TEXT_SECONDARY};
        line-height: 1.6;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] strong {{
        color: {CLR_TEXT_PRIMARY};
    }}

    /* ── Section title ── */
    .section-title {{
        font-size: 0.95rem;
        font-weight: 600;
        color: {CLR_TEXT_PRIMARY};
        margin-bottom: 0.75rem;
        margin-top: 1.5rem;
    }}

    /* ── Page header ── */
    .page-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding-bottom: 1.25rem;
        border-bottom: 1px solid {CLR_BORDER};
        margin-bottom: 1.25rem;
    }}
    .page-header .page-title {{
        font-size: 1.5rem;
        font-weight: 700;
        color: {CLR_TEXT_PRIMARY};
    }}
    .page-header .page-subtitle {{
        font-size: 0.8rem;
        color: {CLR_TEXT_MUTED};
        margin-top: 0.25rem;
    }}

    /* ── Tab styling ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        border-bottom: 1px solid {CLR_BORDER};
    }}
    .stTabs [data-baseweb="tab"] {{
        font-size: 0.875rem;
        font-weight: 500;
        color: {CLR_TEXT_SECONDARY};
        padding: 0.75rem 1.5rem;
        border-bottom: 2px solid transparent;
    }}
    .stTabs [aria-selected="true"] {{
        color: {CLR_ACCENT} !important;
        border-bottom-color: {CLR_ACCENT} !important;
        font-weight: 600;
    }}

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {{
        background-color: {CLR_SURFACE};
        border-right: 1px solid {CLR_BORDER};
    }}

    /* ── Hide Streamlit footer ── */
    footer {{visibility: hidden;}}

    /* ── Spacing utility ── */
    .spacer-lg {{ margin-top: 2rem; }}
    .spacer-md {{ margin-top: 1.25rem; }}
    .spacer-sm {{ margin-top: 0.75rem; }}

    /* ── Context line ── */
    .context-line {{
        font-size: 0.8rem;
        color: {CLR_TEXT_MUTED};
        margin-bottom: 1rem;
    }}

    /* ── QA status badges ── */
    .qa-ok {{
        color: {CLR_POSITIVE};
        font-weight: 600;
    }}
    .qa-warn {{
        color: {CLR_NEGATIVE};
        font-weight: 600;
    }}
</style>
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _is_missing(v) -> bool:
    try:
        return v is None or bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def fmt_money(v) -> str:
    """Short dollars for KPI cards: $9,412 · $48.2K · $1.24M (no overflow at any range)."""
    if _is_missing(v):
        return "—"
    v = float(v)
    sign, a = ("-" if v < 0 else ""), abs(v)
    if a >= 999_500:
        return f"{sign}${a / 1_000_000:.2f}M"
    if a >= 10_000:
        return f"{sign}${a / 1_000:.1f}K"
    return f"{sign}${a:,.0f}"


def fmt_count(v) -> str:
    """Short counts for KPI cards: 41,312 · 342.1K · 3.42M."""
    if _is_missing(v):
        return "—"
    v = float(v)
    if abs(v) >= 999_500:
        return f"{v / 1_000_000:.2f}M"
    if abs(v) >= 100_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:,.0f}"


def exact_money(v) -> str:
    return "—" if _is_missing(v) else f"${float(v):,.2f}"


def exact_count(v) -> str:
    return "—" if _is_missing(v) else f"{float(v):,.0f}"


def render_kpi_row(kpis: list[dict]):
    """Render KPI cards in a fixed 4-column grid (wraps to more rows).

    Each dict: {label, value, sub (optional), exact (optional)}. Every card has
    the same height whether or not it has a sub line, values are shortened by
    the caller (fmt_money / fmt_count) so they never overflow, and `exact`
    (the full figure) shows on hover.
    """
    cards = []
    for kpi in kpis:
        tip = kpi.get("exact") or ""
        title_attr = f' title="{html.escape(str(tip), quote=True)}"' if tip else ""
        sub = kpi.get("sub") or "&nbsp;"
        cards.append(
            f'<div class="kpi-card"{title_attr}>'
            f'<div class="kpi-label">{kpi["label"]}</div>'
            f'<div class="kpi-value">{kpi["value"]}</div>'
            f'<div class="kpi-sub">{sub}</div>'
            f'</div>'
        )
    st.markdown(f'<div class="kpi-wrap"><div class="kpi-grid">{"".join(cards)}</div></div>',
                unsafe_allow_html=True)


TOP_N = 10


def split_top_n(df: pd.DataFrame, value_col: str, n: int = TOP_N,
                show_all: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(top n rows by value_col, the rest). With show_all, everything is 'top'.
    Bar charts draw only the top rows; the rest is summed in a caption rather
    than drawn as one 'Other' bar, which would dwarf the bars that matter."""
    d = df.sort_values(value_col, ascending=False).reset_index(drop=True)
    if show_all or len(d) <= n:
        return d, d.iloc[0:0]
    return d.head(n), d.iloc[n:]


def rest_caption(rest: pd.DataFrame, value_col: str, total: float, noun: str,
                 unit: str = ""):
    """'Plus 152 more producers with $1.65M combined (72% of the total).'
    unit='' means dollars; otherwise a count with that unit ('clicks')."""
    if rest.empty:
        return
    val = rest[value_col].sum()
    share = f" ({val / total:.0%} of the total)" if total else ""
    amount = f"{fmt_count(val)} {unit}" if unit else fmt_money(val)
    st.caption(
        f"Plus {len(rest)} more {noun} with {amount} combined{share}, "
        f"not drawn. Turn on “Show all” to list them."
    )


def show_all_toggle(total: int, key: str, n: int = TOP_N, noun: str = "items") -> bool:
    """'Show all N' switch, rendered only when there is more than n to show."""
    if total <= n:
        return False
    return st.toggle(f"Show all {total} {noun}", value=False, key=key)


# ─── Campaign Details CSV (other projects load this file — keep it stable) ───
# Same columns, same order and same value format as the table download the
# team used before 2026-11-20 (Streamlit's table download: original column
# names, raw numbers, YYYY-MM-DD dates, UTF-8 with BOM). New columns are only
# ever APPENDED at the end so position- and name-based loaders keep working.
CAMPAIGN_CSV_COLUMNS = [
    "Parsed Send Date", "Producer", "Discount Code", "Campaign Name",
    "Discounted Orders", "Delivered", "Attributed Revenue",
    "Total Sales", "Revenue per Delivered", "Sales per Delivered",
    "Influenced Orders", "Influenced Offer Revenue", "Influenced Total Sales",
    "Click-Influenced Orders", "Click-Influenced Offer Revenue",
    "All-Buyer Orders", "All-Buyer Revenue",
]
# (source column, CSV header) — appended after the columns above, in order.
CAMPAIGN_CSV_APPENDED = [
    ("Clicked", "Clicks"),                                       # 2026-11-20 (Phase 1)
    ("New Customer Orders", "New Customer Orders"),              # 2026-11-20 (Phase 2)
    ("New Customer Revenue", "New Customer Revenue"),
    ("Existing Customer Revenue", "Existing Customer Revenue"),
]


def _csv_cell(v) -> str:
    """Format one value the way the old table download did: blank for null,
    whole numbers without '.0', other numbers in full precision, dates ISO."""
    if _is_missing(v):
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (int, float)) or hasattr(v, "dtype"):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return str(v)
        if f.is_integer():
            return str(int(f))
        import numpy as np
        return np.format_float_positional(f, trim="-")
    return str(v)


def build_campaign_csv(details_df: pd.DataFrame) -> bytes:
    """Campaign Details CSV: legacy columns first, appended columns after."""
    cols = [c for c in CAMPAIGN_CSV_COLUMNS if c in details_df.columns]
    appended = [(src, hdr) for src, hdr in CAMPAIGN_CSV_APPENDED if src in details_df.columns]
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(cols + [hdr for _, hdr in appended])
    sources = cols + [src for src, _ in appended]
    for row in details_df[sources].itertuples(index=False, name=None):
        writer.writerow([_csv_cell(v) for v in row])
    return ("﻿" + buf.getvalue()).encode("utf-8")


# ─── Fixed benchmark (Revenue per 1,000 delivered) ──────────────────────────
# Frozen reference year so "good or bad" doesn't move with the date picker.
# Median offer, split by day type because Fri/Sat full-list sends and weekday
# segment sends earn very different amounts per email. Reset once a year.
BENCHMARK_START = date(2025, 6, 1)
BENCHMARK_END = date(2026, 5, 31)
BENCHMARK_LABEL = "Jun 2025–May 2026"


def _day_type(d) -> str:
    return "Fri/Sat" if pd.Timestamp(d).weekday() in (4, 5) else "Weekday"


def offer_rates(df: pd.DataFrame, offer_type_map: dict) -> pd.DataFrame:
    """Finalized, non-event offers with a discount code and at least the
    minimum delivered, with their revenue per 1,000 delivered and day type."""
    if df.empty:
        return df.assign(rate_per_1000=pd.Series(dtype=float), day_type=pd.Series(dtype=str))
    m = (
        (df["Discount Code"] != "None")
        & df["Attributed Revenue"].notna()
        & (df["Delivered"] >= MIN_DELIVERED_THRESHOLD)
        & (df["is_final_snapshot"] == True)
        & df["Parsed Send Date"].notna()
        & ~event_mask(df, offer_type_map)
    )
    o = df[m].copy()
    o["rate_per_1000"] = o["Attributed Revenue"] / o["Delivered"] * 1000
    o["day_type"] = o["Parsed Send Date"].apply(_day_type)
    return o


def compute_benchmarks(full: pd.DataFrame, offer_type_map: dict) -> dict:
    """Median revenue per 1,000 delivered per day type over the frozen year."""
    o = offer_rates(full, offer_type_map)
    if o.empty:
        return {}
    d = pd.to_datetime(o["Parsed Send Date"]).dt.date
    o = o[(d >= BENCHMARK_START) & (d <= BENCHMARK_END)]
    return {k: float(g["rate_per_1000"].median()) for k, g in o.groupby("day_type") if len(g) >= 10}


# ─── Producer products (Producers tab) ──────────────────────────────────────

_FEATURED_LABELS = {True: "Yes", False: "No"}


def render_producer_products(producer: str, data: dict, run_date: date):
    """Which wines earned a producer's attributed revenue (finalized campaigns).

    Built from the attributed line items, so it adds up to the producer's
    Attributed Revenue. 'Featured' = the wine's vintage is named in the
    campaign name (logic_spec 6.6)."""
    lines = data.get("product_lines")
    if lines is None or lines.empty:
        st.info("Product detail appears after the next data refresh.")
        return

    pmap = data.get("producer_map", {})
    code_to_producer = {c: (resolve_producer(c, pmap) or UNMAPPED_LABEL) for c in lines["code_key"].unique()}
    sel = lines[lines["code_key"].map(code_to_producer) == producer].copy()
    sel = sel[sel["window_end"].notna() & (sel["window_end"].apply(lambda d: d <= run_date))]
    section_title("Products", f"Wines sold through {producer}'s campaign discounts · finalized campaigns")
    if sel.empty:
        st.info(f"No attributed sales for {producer} in finalized campaigns.")
        return

    sel["campaign_key"] = sel["code_key"] + "|" + sel["send_date"]
    total = sel["net_revenue"].sum()
    feat = sel.loc[sel["featured"] == True, "net_revenue"].sum()
    other = sel.loc[sel["featured"] == False, "net_revenue"].sum()
    named = sel["featured"].notna().any()
    new_rev = sel.loc[sel["is_new_customer"] == True, "net_revenue"].sum()

    prod = sel.groupby(["title", "vintage", "size"], dropna=False).agg(
        Units=("quantity", "sum"),
        Orders=("order_id", "nunique"),
        Revenue=("net_revenue", "sum"),
        Campaigns=("campaign_key", "nunique"),
        _feat_yes=("featured", lambda s: bool((s == True).any())),
        _feat_known=("featured", lambda s: bool(s.notna().any())),
    ).reset_index()
    prod["Featured in Email"] = [
        "Yes" if y else ("No" if k else "—") for y, k in zip(prod["_feat_yes"], prod["_feat_known"])
    ]
    prod = prod.drop(columns=["_feat_yes", "_feat_known"]).sort_values("Revenue", ascending=False)

    render_kpi_row([
        {"label": "Attributed Revenue", "value": fmt_money(total), "exact": exact_money(total),
         "sub": f"{len(prod)} products · {fmt_count(sel['quantity'].sum())} units"},
        {"label": "Featured Vintages", "value": fmt_money(feat) if named else "—",
         "exact": exact_money(feat) if named else "",
         "sub": (f"{feat / total:.0%} · vintage named in the email" if named and total
                 else "Emails didn't name a vintage")},
        {"label": "Other Vintages", "value": fmt_money(other) if named else "—",
         "exact": exact_money(other) if named else "",
         "sub": (f"{other / total:.0%} · same producer, not named" if named and total else "")},
        {"label": "New-Customer Revenue", "value": fmt_money(new_rev), "exact": exact_money(new_rev),
         "sub": f"{new_rev / total:.0%} of this producer" if total else ""},
    ])
    spacer("sm")

    show_all = show_all_toggle(len(prod), f"producer_products_show_all_{producer}", noun="products")
    top, rest = split_top_n(prod, "Revenue", show_all=show_all)
    bar = alt.Chart(top).mark_bar(cornerRadiusEnd=3).encode(
        y=alt.Y("title:N", sort="-x", title=None, axis=alt.Axis(labelLimit=420, labelOverlap=False)),
        x=alt.X("Revenue:Q", title="Attributed Revenue ($)", axis=alt.Axis(format="$,.0f")),
        color=alt.Color(
            "Featured in Email:N",
            scale=alt.Scale(domain=["Yes", "No", "—"], range=[CLR_PRODUCER, CLR_TEAL, CLR_TEXT_MUTED]),
            legend=alt.Legend(title="Featured in email", orient="top"),
        ),
        tooltip=[
            alt.Tooltip("title:N", title="Product"),
            alt.Tooltip("Revenue:Q", title="Attributed Revenue", format="$,.2f"),
            alt.Tooltip("Units:Q", format=",.0f"),
            alt.Tooltip("Orders:Q", format=",.0f"),
            alt.Tooltip("Campaigns:Q", format=",.0f"),
            alt.Tooltip("Featured in Email:N"),
        ],
    ).properties(height=max(len(top) * 30, 120) + 40)   # +40: legend on top
    st.altair_chart(styled_chart(bar), width="stretch")
    rest_caption(rest, "Revenue", total, "products")

    st.dataframe(
        prod.rename(columns={"title": "Product", "vintage": "Vintage", "size": "Size"}),
        column_config={
            "Product": st.column_config.TextColumn("Product", width="large"),
            "Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
            "Units": st.column_config.NumberColumn("Units", format="%d"),
        },
        height=min(38 * (len(prod) + 1), 360),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Revenue here counts only the bottles the campaign discount applied to, net of the discount, so "
        "it adds up to the producer's Attributed Revenue. **Featured in email** = the wine's vintage "
        "appears in the campaign name (Yes), a campaign named other vintages (No), or no vintage was "
        "named (—). Codes work across the producer, so buyers often pick a different vintage."
    )


# ─── Offer Log (A/B labels) ─────────────────────────────────────────────────
# The live app reads the log through a view-only OneDrive share link stored
# as the OFFER_LOG_URL secret; local runs read the OneDrive file directly.
OFFER_LOG_LOCAL = os.path.join(
    os.path.expanduser("~"), "OneDrive - Grand Cru Liquid Assets LLC", "Desktop",
    "Everett's email offers recommendation system", "Offer Evaluation Standard - Agent4.xlsx",
)
B_WEEKLY_TARGET = 5000      # suggested in the 2026-11-12 cadence review
B_ZERO_TARGET = 0.25        # suggested: at most 25% of B offers with $0


def _setting(name: str) -> str:
    """A setting from Streamlit secrets, the environment, or .env.txt."""
    try:
        v = st.secrets.get(name)
        if v:
            return str(v).strip()
    except Exception:
        pass
    if os.environ.get(name):
        return os.environ[name].strip()
    env_path = os.path.join(PROJECT_ROOT, ".env.txt")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                if line.strip().startswith(f"{name}="):
                    return line.split("=", 1)[1].strip()
    return ""


def _direct_download(url: str) -> str:
    """OneDrive/SharePoint share link -> direct file download."""
    if "download=1" in url:
        return url
    return url + ("&" if "?" in url else "?") + "download=1"


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def load_offer_log(today: date) -> tuple[pd.DataFrame, str, bool]:
    """(log rows, note for the page, connected?)."""
    empty = pd.DataFrame(columns=["log_send_date", "log_offer", "log_rank"])
    url = _setting("OFFER_LOG_URL")
    try:
        if url:
            resp = requests.get(_direct_download(url), timeout=30)
            resp.raise_for_status()
            if not resp.content.startswith(b"PK"):
                return empty, ("The Offer Log link did not return the workbook (it may need "
                               "“Anyone with the link can view”). A/B is estimated from "
                               "audience size until it does."), False
            return parse_offer_log(resp.content, today), "A/B labels from the Offer Log.", True
        path = _setting("OFFER_LOG_PATH") or OFFER_LOG_LOCAL
        if os.path.isfile(path):
            with open(path, "rb") as f:
                return parse_offer_log(f.read(), today), "A/B labels from the Offer Log (local file).", True
        return empty, ("The Offer Log isn't connected yet, so A/B is estimated from audience size "
                       "(the bigger send that day = A)."), False
    except Exception as exc:
        return empty, (f"The Offer Log could not be read ({type(exc).__name__}); A/B is estimated "
                       "from audience size."), False


def slot_summary(offers: pd.DataFrame) -> dict:
    """Totals for one group of offers (A, B or Single)."""
    n = len(offers)
    rev = offers["Attributed Revenue"].sum() if n else 0.0
    deliv = offers["Delivered"].sum() if n else 0
    return {
        "offers": n,
        "revenue": rev,
        "avg": rev / n if n else None,
        "per_1000": rev / deliv * 1000 if deliv else None,
        "zero_share": float((offers["Attributed Revenue"] == 0).mean()) if n else None,
        "audience": offers["Delivered"].mean() if n else None,
    }


def _week_monday(d) -> date:
    d = pd.Timestamp(d).date()
    return d - timedelta(days=d.weekday())


def month_weeks(year: int, month: int) -> list[date]:
    """Mondays of the Mon-Sun weeks that belong to a month: a week belongs to
    the month holding most of its days (its Thursday), so every month has 4
    or 5 whole weeks and no week is split."""
    first = date(year, month, 1)
    mon = first - timedelta(days=first.weekday())
    weeks = []
    while mon <= first + timedelta(days=37):
        thu = mon + timedelta(days=3)
        if thu.year == year and thu.month == month:
            weeks.append(mon)
        mon += timedelta(days=7)
    return weeks


def week_label(mon: date, with_year: bool = False) -> str:
    sun = mon + timedelta(days=6)
    left = f"{mon.strftime('%b')} {mon.day}"
    right = f"{sun.day}" if sun.month == mon.month else f"{sun.strftime('%b')} {sun.day}"
    return f"{left} – {right}" + (f", {sun.year}" if with_year else "")


def coded_offers(frame: pd.DataFrame) -> pd.DataFrame:
    """Campaigns with a code and attribution (events included)."""
    if frame.empty:
        return frame
    return frame[(frame["Discount Code"] != "None") & frame["Attributed Revenue"].notna()]


def _in_range(frame: pd.DataFrame, start: date, end: date, finalized: bool = True) -> pd.DataFrame:
    if frame.empty:
        return frame
    send = pd.to_datetime(frame["Parsed Send Date"]).dt.date
    out = frame[frame["Parsed Send Date"].notna() & (send >= start) & (send <= end)]
    if finalized:
        out = out[out["is_final_snapshot"] == True]
    return out


def _mini_bars(df: pd.DataFrame, label: str, value: str, fmt: str, color_col: str | None = None,
               height: int | None = None) -> alt.Chart:
    """Small horizontal bar chart for the insight cards."""
    enc = dict(
        y=alt.Y(f"{label}:N", sort=None, title=None, axis=alt.Axis(labelLimit=220, labelOverlap=False)),
        x=alt.X(f"{value}:Q", title=None, axis=alt.Axis(format=fmt, tickCount=3)),
        tooltip=[alt.Tooltip(f"{label}:N"), alt.Tooltip(f"{value}:Q", format=fmt)],
    )
    chart = alt.Chart(df).mark_bar(cornerRadiusEnd=3, color=CLR_WEEKLY)
    if color_col:
        enc["color"] = alt.Color(f"{color_col}:N", legend=None,
                                 scale=alt.Scale(range=[CLR_PRODUCER, CLR_TEAL, CLR_TEXT_MUTED]))
    return chart.encode(**enc).properties(height=height or max(len(df) * 30, 84))


def render_key_insights(range_df: pd.DataFrame, wk_start: date, wk_end: date,
                        finalized_only: bool, pool: pd.DataFrame, offer_type_map: dict,
                        benchmarks: dict, slots: pd.DataFrame):
    """Four short cards for leadership: revenue vs last year, best offer and
    $0 offers, Offer A vs B, new customers. Each = one-line headline + a few
    numbers + a small chart. Plain language, no statistics jargon."""
    n_days = (wk_end - wk_start).days + 1
    coded = coded_offers(range_df)
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)

    # 1 ── Revenue vs last year (and vs the previous period) ───────────────
    with c1, st.container(border=True):
        ty_rev = coded["Attributed Revenue"].sum()
        ly_start, ly_end = wk_start - timedelta(days=364), wk_end - timedelta(days=364)
        pr_start, pr_end = wk_start - timedelta(days=n_days), wk_start - timedelta(days=1)
        ly = coded_offers(_in_range(pool, ly_start, ly_end))
        pr = coded_offers(_in_range(pool, pr_start, pr_end))
        # A comparison period is only usable when it lies inside the data
        ly_ok = ly_start >= DATA_START_DATE
        pr_ok = pr_start >= DATA_START_DATE
        if ly_ok and ly["Attributed Revenue"].sum() > 0:
            ch = ty_rev / ly["Attributed Revenue"].sum() - 1
            head = f"Email revenue is {'up' if ch >= 0 else 'down'} {abs(ch):.0%} on the same {n_days} days last year"
        elif pr_ok and pr["Attributed Revenue"].sum() > 0:
            ch = ty_rev / pr["Attributed Revenue"].sum() - 1
            head = f"Email revenue is {'up' if ch >= 0 else 'down'} {abs(ch):.0%} on the previous {n_days} days"
        else:
            head = f"Email revenue: {fmt_money(ty_rev)} in this period"
        st.markdown(f"#### {head}")
        rows = [("This period", ty_rev, len(coded))]
        if pr_ok:
            rows.append((f"Previous {n_days} days", pr["Attributed Revenue"].sum(), len(pr)))
        if ly_ok:
            rows.append(("Same period last year", ly["Attributed Revenue"].sum(), len(ly)))
        st.markdown(" · ".join(f"**{lbl}** {fmt_money(v)} ({n} offers)" for lbl, v, n in rows))
        if len(rows) > 1:
            st.altair_chart(_mini_bars(pd.DataFrame([{"Period": r[0], "Revenue": r[1]} for r in rows]),
                                       "Period", "Revenue", "$,.0f"), width="stretch")
        notes = []
        if not ly_ok or not pr_ok:
            notes.append("Comparisons need data from before this period; the data starts May 1, 2025.")
        if not finalized_only:
            notes.append("This period includes offers still collecting orders.")
        if notes:
            st.caption(" ".join(notes))

    # 2 ── Best offer and offers that made $0 ──────────────────────────────
    with c2, st.container(border=True):
        o = offer_rates(range_df, offer_type_map)
        if o.empty:
            st.markdown("#### No finalized offers in this range yet")
        else:
            best = o.loc[o["Attributed Revenue"].idxmax()]
            zero = o[o["Attributed Revenue"] == 0]
            bench = benchmarks.get(best["day_type"])
            _dt = "weekday" if best["day_type"] == "Weekday" else "Fri/Sat"
            mult = (f", {best['rate_per_1000'] / bench:.1f}× the {_dt} benchmark" if bench else "")
            st.markdown(f"#### {best['Producer / Topic']} was the best offer: {fmt_money(best['Attributed Revenue'])}")
            st.markdown(
                f"**{int(best['Discounted Orders'] or 0)} orders**, ${best['rate_per_1000']:,.0f} per 1,000 "
                f"emails delivered{mult}. **{len(zero)} of {len(o)} offers** made no sales"
                + (": " + ", ".join(zero.sort_values("Delivered", ascending=False)["Producer / Topic"].head(3))
                   + ("…" if len(zero) > 3 else "") if len(zero) else "") + "."
            )
            top5 = o.nlargest(5, "Attributed Revenue")[["Producer / Topic", "Attributed Revenue"]]
            st.altair_chart(_mini_bars(top5.rename(columns={"Producer / Topic": "Offer"}),
                                       "Offer", "Attributed Revenue", "$,.0f"), width="stretch")
            st.caption("Top 5 offers by attributed revenue. Events are left out.")

    # 3 ── Offer A vs Offer B ───────────────────────────────────────────────
    with c3, st.container(border=True):
        ab = range_df.join(slots, how="inner") if not slots.empty else pd.DataFrame()
        a = ab[ab["Offer Slot"] == "A"] if not ab.empty else ab
        b = ab[ab["Offer Slot"] == "B"] if not ab.empty else ab
        if a.empty or b.empty:
            st.markdown("#### No two-offer days in this range")
            st.caption("Offer A vs B compares the main and second offer on days with two offers.")
        else:
            sa, sb = slot_summary(a), slot_summary(b)
            weeks = b["Parsed Send Date"].apply(_week_monday).nunique()
            b_week = sb["revenue"] / weeks if weeks else 0
            if sa["per_1000"] and sb["per_1000"] is not None:
                diff = sb["per_1000"] / sa["per_1000"] - 1
                head = (f"Second offers earned {abs(diff):.0%} {'less' if diff < 0 else 'more'} per email "
                        f"than main offers")
            else:
                head = "Offer A vs Offer B"
            st.markdown(f"#### {head}")
            st.markdown(
                f"**A** ${sa['per_1000'] or 0:,.0f} vs **B** ${sb['per_1000'] or 0:,.0f} per 1,000 delivered. "
                f"B brought **{fmt_money(b_week)} a week** (suggested target {fmt_money(B_WEEKLY_TARGET)}); "
                f"**{sb['zero_share']:.0%} of B offers** made $0 (target ≤ {B_ZERO_TARGET:.0%})."
            )
            st.altair_chart(_mini_bars(pd.DataFrame([
                {"Offer": "A (main)", "Per 1,000 delivered": sa["per_1000"] or 0},
                {"Offer": "B (second)", "Per 1,000 delivered": sb["per_1000"] or 0},
            ]), "Offer", "Per 1,000 delivered", "$,.0f"), width="stretch")
            st.caption("Details in the Offer A vs B tab.")

    # 4 ── New customers ─────────────────────────────────────────────────────
    with c4, st.container(border=True):
        if "New Customer Revenue" not in coded.columns or coded["New Customer Revenue"].notna().sum() == 0:
            st.markdown("#### New-customer revenue appears after the next data refresh")
        else:
            nr = coded["New Customer Revenue"].sum()
            nn = int(coded["New Customer Orders"].sum())
            share = nr / ty_rev if ty_rev else 0
            st.markdown(f"#### First-time buyers brought {share:.0%} of email revenue")
            with_new = coded[coded["New Customer Orders"] > 0]
            top = with_new.nlargest(5, "New Customer Revenue")
            lead = ""
            if not with_new.empty:
                most = with_new.loc[with_new["New Customer Orders"].idxmax()]
                k = int(most["New Customer Orders"])
                lead = (f" The most new buyers came from **{most['Producer / Topic']}** "
                        f"({k} new buyer{'s' if k != 1 else ''}).")
            st.markdown(f"**{fmt_money(nr)}** from **{nn} new customers** (first-ever Shopify order).{lead}")
            if not top.empty:
                st.altair_chart(_mini_bars(top[["Producer / Topic", "New Customer Revenue"]]
                                           .rename(columns={"Producer / Topic": "Offer"}),
                                           "Offer", "New Customer Revenue", "$,.0f"), width="stretch")
                st.caption("Offers that brought the most new-customer revenue.")


def section_title(title: str, subtitle: str = ""):
    """Render a section title with optional subtitle."""
    sub = f'<div style="font-size:0.8rem; color:{CLR_TEXT_MUTED}; margin-top:-0.5rem; margin-bottom:0.75rem;">{subtitle}</div>' if subtitle else ""
    st.markdown(f'<div class="section-title">{title}</div>{sub}', unsafe_allow_html=True)


def styled_chart(chart: alt.Chart) -> alt.Chart:
    """Apply consistent styling to all Altair charts."""
    return chart.configure_view(
        strokeWidth=0,
    ).configure_axis(
        labelFontSize=11,
        labelColor=CLR_TEXT_SECONDARY,
        titleFontSize=12,
        titleColor=CLR_TEXT_PRIMARY,
        titleFontWeight="normal",
        gridColor=CLR_BORDER,
        gridOpacity=0.5,
        domainColor=CLR_BORDER,
        tickColor=CLR_BORDER,
    ).configure_legend(
        labelFontSize=11,
        labelColor=CLR_TEXT_SECONDARY,
        titleFontSize=12,
        titleColor=CLR_TEXT_PRIMARY,
        titleFontWeight="normal",
    )


def render_insight_card(insights_md: str):
    """Render insight markdown inside a styled card container.
    Uses st.container(border=True) — CSS overrides give it the burgundy left accent.
    """
    with st.container(border=True):
        st.markdown(insights_md)


def spacer(size: str = "md"):
    """Add vertical spacing. size: sm, md, lg."""
    st.markdown(f'<div class="spacer-{size}"></div>', unsafe_allow_html=True)




def _generate_analytical_insights(cdf: pd.DataFrame, week_start: date, week_end: date) -> str:
    """
    Generate rich analytical insights from completed-week campaigns.
    Returns markdown text.
    """
    lines = []
    coded = cdf[
        (cdf["Discount Code"] != "None") &
        (cdf["Attributed Revenue"].notna())
    ].copy()

    if coded.empty:
        return "_No coded campaigns with attribution data in this completed week._"

    # ── CAMPAIGN ANALYSIS ─────────────────────────────────────────────────
    lines.append("#### Campaign Performance Analysis")
    lines.append(f"**Completed week: {week_start} to {week_end}**\n")

    # Best/worst by efficiency
    eff_pool = coded[coded["Revenue per Delivered"].notna()].copy()
    eff_above = eff_pool[eff_pool["Delivered"] >= MIN_DELIVERED_THRESHOLD]
    eff_src = eff_above if len(eff_above) >= 2 else eff_pool

    if not eff_src.empty:
        best_eff = eff_src.loc[eff_src["Revenue per Delivered"].idxmax()]
        worst_eff = eff_src.loc[eff_src["Revenue per Delivered"].idxmin()]
        best_scale = coded.loc[coded["Attributed Revenue"].idxmax()]
        worst_scale = coded.loc[coded["Attributed Revenue"].idxmin()]

        # Efficiency leader
        lines.append(
            f"**Efficiency leader**: {best_eff['Campaign Name']} converted at "
            f"**\\${best_eff['Revenue per Delivered']:.4f}/delivered** with "
            f"\\${best_eff['Attributed Revenue']:,.2f} attributed revenue "
            f"(\\${best_eff['Total Sales']:,.2f} total sales) from "
            f"{int(best_eff['Delivered']):,} deliveries."
        )

        # Scale leader
        if best_scale['Campaign Name'] != best_eff['Campaign Name']:
            lines.append(
                f"**Scale leader**: {best_scale['Campaign Name']} generated the highest "
                f"attributed revenue at **\\${best_scale['Attributed Revenue']:,.2f}** "
                f"(\\${best_scale['Total Sales']:,.2f} total sales) from "
                f"{int(best_scale['Delivered']):,} deliveries "
                f"(\\${best_scale['Revenue per Delivered']:.4f}/delivered)."
            )
            lines.append(
                f"This suggests {best_scale['Campaign Name']}'s revenue was driven "
                f"more by **delivery volume** than conversion efficiency, while "
                f"{best_eff['Campaign Name']} achieved its results through "
                f"**stronger per-email monetization**."
            )
        else:
            lines.append(
                f"{best_eff['Campaign Name']} led in **both efficiency and scale** "
                f"this week \u2014 a standout performer across the board."
            )

        # Worst performers
        lines.append("")
        if worst_eff['Revenue per Delivered'] == 0:
            lines.append(
                f"**Weakest efficiency**: {worst_eff['Campaign Name']} had "
                f"{int(worst_eff['Delivered']):,} deliveries but generated "
                f"**\\$0 in attributed revenue** \u2014 the discount code saw no redemptions."
            )
        else:
            lines.append(
                f"**Weakest efficiency**: {worst_eff['Campaign Name']} converted at just "
                f"\\${worst_eff['Revenue per Delivered']:.4f}/delivered despite "
                f"{int(worst_eff['Delivered']):,} deliveries."
            )

        # Underperformers: high delivery, low revenue
        if len(coded) >= 3:
            med_del = coded["Delivered"].median()
            med_rev = coded["Attributed Revenue"].median()
            underperformers = coded[
                (coded["Delivered"] > med_del) & (coded["Attributed Revenue"] <= med_rev)
            ]
            if not underperformers.empty:
                lines.append("")
                lines.append("**Campaigns with strong delivery but weak monetization:**")
                for _, r in underperformers.iterrows():
                    lines.append(
                        f"- {r['Campaign Name']}: {int(r['Delivered']):,} delivered "
                        f"but only \\${r['Attributed Revenue']:,.2f} attributed revenue "
                        f"(\\${r['Total Sales']:,.2f} total sales). "
                        f"This audience was reached effectively but didn't convert \u2014 "
                        f"consider whether the offer, timing, or product resonated."
                    )

            # Efficient dark horses: low delivery, high revenue
            dark_horses = coded[
                (coded["Delivered"] <= med_del) & (coded["Attributed Revenue"] > med_rev)
            ]
            if not dark_horses.empty:
                lines.append("")
                lines.append("**Campaigns with smaller reach but strong conversion:**")
                for _, r in dark_horses.iterrows():
                    lines.append(
                        f"- {r['Campaign Name']}: only {int(r['Delivered']):,} delivered "
                        f"but \\${r['Attributed Revenue']:,.2f} attributed revenue "
                        f"(\\${r['Total Sales']:,.2f} total sales, "
                        f"\\${r['Revenue per Delivered']:.4f}/delivered). "
                        f"This campaign punched above its weight \u2014 "
                        f"scaling its delivery could unlock significant upside."
                    )

    # ── PRODUCER ANALYSIS ─────────────────────────────────────────────────
    lines.append("")
    lines.append("#### Producer Performance Analysis")

    prod_group = coded.groupby("Producer / Topic").agg(
        Revenue=("Attributed Revenue", "sum"),
        TotalSales=("Total Sales", "sum"),
        Delivered=("Delivered", "sum"),
        Campaigns=("Campaign Name", "count"),
    ).reset_index()
    prod_group["Efficiency"] = (prod_group["Revenue"] / prod_group["Delivered"].replace(0, float("nan"))).round(4)
    prod_group.sort_values("Revenue", ascending=False, inplace=True)

    if len(prod_group) >= 2:
        best_p = prod_group.iloc[0]
        worst_coded = prod_group[prod_group["Revenue"] > 0]
        lines.append(
            f"**Top producer by scale**: {best_p['Producer / Topic']} "
            f"(\\${best_p['Revenue']:,.2f} attributed revenue, "
            f"\\${best_p['TotalSales']:,.2f} total sales from "
            f"{int(best_p['Campaigns'])} campaign(s))."
        )

        eff_producers = prod_group[prod_group["Efficiency"].notna() & (prod_group["Delivered"] >= MIN_DELIVERED_THRESHOLD)]
        if not eff_producers.empty:
            best_pe = eff_producers.loc[eff_producers["Efficiency"].idxmax()]
            worst_pe = eff_producers.loc[eff_producers["Efficiency"].idxmin()]
            if best_pe["Producer / Topic"] != best_p["Producer / Topic"]:
                lines.append(
                    f"**Most efficient producer**: {best_pe['Producer / Topic']} "
                    f"(\\${best_pe['Efficiency']:.4f}/delivered), outperforming the "
                    f"scale leader on per-email monetization."
                )
            if worst_pe["Efficiency"] == 0:
                lines.append(
                    f"**Least efficient producer**: {worst_pe['Producer / Topic']} "
                    f"had {int(worst_pe['Delivered']):,} deliveries but zero revenue. "
                    f"Investigate whether the offer or audience segment needs adjustment."
                )
    elif len(prod_group) == 1:
        p = prod_group.iloc[0]
        lines.append(
            f"Only one producer this week: {p['Producer / Topic']} "
            f"(\\${p['Revenue']:,.2f} revenue, {int(p['Delivered']):,} delivered)."
        )

    return "\n\n".join(lines)


def _generate_monthly_insights(mdf: pd.DataFrame, all_month_df: pd.DataFrame) -> str:
    """Generate analytical insights for the monthly report."""
    lines = []

    if mdf.empty:
        return "_No finalized campaign data for this month yet._"

    total_rev = mdf["Total_Attributed_Revenue"].sum()
    top = mdf.head(3)

    # Top contributors
    lines.append("**Top discount code contributors:**")
    for _, r in top.iterrows():
        pct = (r["Total_Attributed_Revenue"] / total_rev * 100) if total_rev > 0 else 0
        camp_list = r.get("Campaign_Names", "")
        n_camps = r.get("Campaign_Count", 1)
        total_sales_val = r.get("Total_Sales", 0) or 0
        lines.append(
            f"- **{r['Discount Code']}**: \\${r['Total_Attributed_Revenue']:,.2f} attributed revenue "
            f"/ \\${total_sales_val:,.2f} total sales "
            f"({pct:.1f}% of monthly total) from {int(n_camps)} campaign(s)."
        )
        if n_camps > 1:
            lines.append(
                f"  This code's revenue was spread across multiple campaigns, "
                f"suggesting consistent demand for this promotion."
            )
        elif r["Total_Attributed_Revenue"] > total_rev * 0.3:
            lines.append(
                f"  A single campaign drove over 30% of the month's revenue \u2014 "
                f"strong standalone performance."
            )

    # Concentration analysis
    if len(mdf) >= 3:
        top3_rev = top["Total_Attributed_Revenue"].sum()
        top3_pct = (top3_rev / total_rev * 100) if total_rev > 0 else 0
        lines.append("")
        if top3_pct > 80:
            lines.append(
                f"Revenue is **heavily concentrated**: top 3 codes account for "
                f"{top3_pct:.0f}% of monthly revenue. Diversifying successful "
                f"promotions could reduce risk."
            )
        else:
            lines.append(
                f"Revenue is **well-distributed**: top 3 codes account for "
                f"{top3_pct:.0f}% of monthly revenue, indicating a balanced portfolio."
            )

    return "\n\n".join(lines)


def _generate_producer_insights(display_df: pd.DataFrame, view_label: str) -> str:
    """Generate analytical insights for producer performance."""
    lines = []
    if display_df.empty:
        return f"_No producer data for the {view_label} view._"

    lines.append(f"#### Producer Insights ({view_label})")

    coded = display_df[display_df["Total_Attributed_Revenue"] > 0].copy()
    zero_rev = display_df[display_df["Total_Attributed_Revenue"] == 0].copy()

    if not coded.empty:
        best = coded.loc[coded["Total_Attributed_Revenue"].idxmax()]
        best_total_sales = best.get("Total_Sales", 0) or 0
        lines.append(
            f"**Scale leader**: {best['Producer']} dominates with "
            f"\\${best['Total_Attributed_Revenue']:,.2f} attributed revenue "
            f"(\\${best_total_sales:,.2f} total sales) "
            f"across {int(best['Campaign_Count'])} campaign(s)."
        )

        eff_pool = coded[coded["Revenue per Delivered"].notna()]
        if len(eff_pool) >= 2:
            best_e = eff_pool.loc[eff_pool["Revenue per Delivered"].idxmax()]
            worst_e = eff_pool.loc[eff_pool["Revenue per Delivered"].idxmin()]
            if best_e["Producer"] != best["Producer"]:
                lines.append(
                    f"**Efficiency leader**: {best_e['Producer']} achieves the "
                    f"highest per-email conversion at \\${best_e['Revenue per Delivered']:.4f}/delivered \u2014 "
                    f"scaling delivery for this producer could unlock significant revenue."
                )
            lines.append(
                f"**Lowest efficiency**: {worst_e['Producer']} at "
                f"\\${worst_e['Revenue per Delivered']:.4f}/delivered. "
                f"Consider testing different subject lines, offers, or audience segments."
            )

    if not zero_rev.empty:
        names = zero_rev["Producer"].tolist()
        lines.append(
            f"**Zero-revenue producers** ({len(names)}): {', '.join(names[:5])}. "
            f"These producers had campaigns sent but no attributed orders \u2014 "
            f"investigate whether discount codes were used or the attribution window was too narrow."
        )

    return "\n\n".join(lines)


# ─── Pipeline runner ─────────────────────────────────────────────────────────

@st.cache_resource(ttl=6 * 3600, show_spinner=False)
def run_pipeline() -> dict:
    """
    Execute the full data pipeline (same logic as run_dashboard.py).
    Returns a dict of all computed artifacts for display.

    Cached globally across sessions for 6 hours — only the first visitor
    after a deploy (or after Refresh Data clears the cache) pays the
    multi-minute pipeline cost; everyone else loads instantly.
    """
    run_date = date.today()

    with st.status("Fetching fresh data from HubSpot and Shopify...", expanded=True) as status:

        # Step 1: Credentials
        status.update(label="Loading credentials...")
        env = load_env()
        hubspot_token = env["HUBSPOT_PRIVATE_APP_TOKEN"]
        shopify_auth = ShopifyAuth(
            store_domain=env["SHOPIFY_STORE_DOMAIN"],
            client_id=env["SHOPIFY_CLIENT_ID"],
            client_secret=env["SHOPIFY_CLIENT_SECRET"],
        )

        # Step 2: Overrides + family mappings
        status.update(label="Loading campaign overrides...")
        overrides = load_overrides()
        families = load_family_mapping()

        # Step 3: HubSpot campaigns
        status.update(label="Fetching HubSpot campaigns...")
        records = fetch_campaigns(hubspot_token)
        hubspot_stats_failures = list(getattr(fetch_campaigns, "last_stats_failures", []))
        apply_overrides(records, overrides)

        # Tag family keys on parsed campaigns
        for rec in records:
            p = rec.parsed
            if p.discount_code and is_family_key(p.discount_code, families):
                p.is_family_key = True

        main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                        "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}
        main_records = [r for r in records if r.parsed.qa_bucket in main_buckets]

        # Step 4: Shopify attribution
        status.update(label="Computing Shopify attribution...")
        attributions: dict = {}
        attribution_tasks = []
        family_tasks = []

        for rec in main_records:
            p = rec.parsed
            if p.discount_code:
                if p.is_family_key:
                    family_tasks.append((
                        p.discount_code,
                        p.parsed_send_date,
                        p.attribution_window_days,
                    ))
                else:
                    attribution_tasks.append((
                        p.discount_code,
                        p.parsed_send_date,
                        p.attribution_window_days,
                        p.producer_topic or "",
                    ))

        # All campaign codes/titles (normalized) — used by the UTM-influenced
        # pass to preserve single attribution across campaigns.
        all_campaign_identifiers: set[str] = set()
        for rec in main_records:
            p = rec.parsed
            if not p.discount_code:
                continue
            if p.is_family_key:
                for m in get_family_identifiers(p.discount_code, families):
                    all_campaign_identifiers.add(_normalize_discount_code(m.identifier))
            else:
                all_campaign_identifiers.add(_normalize_discount_code(p.discount_code))

        # Bulk-fetch ALL orders once; each campaign filters its window
        # in-memory. Cuts hundreds of overlapping per-window Shopify
        # fetches down to one paginated pass.
        status.update(label="Bulk-fetching all Shopify orders...")
        all_orders = _fetch_all_shopify_orders(shopify_auth, DATA_START_DATE, run_date)

        # Attribution dict is keyed by "code|send_date" so each campaign
        # gets its own attribution result for its own window, even if
        # multiple campaigns share the same discount code / family key.
        # logic_spec 5.3 — map each code/family key to every send_date that
        # uses it, so overlapping windows resolve one owner per order.
        sibling_dates: dict[str, list] = {}
        for _c, _sd, _w, _p in attribution_tasks:
            sibling_dates.setdefault(_c.lower(), []).append(_sd)
        for _fk, _sd, _w in family_tasks:
            sibling_dates.setdefault(_fk.lower(), []).append(_sd)
        for _k in sibling_dates:
            sibling_dates[_k] = sorted(set(sibling_dates[_k]))

        offer_type_map = load_offer_type_mapping(PRODUCER_CODE_EXCEL)
        event_code_set = event_codes(main_records, offer_type_map)

        seen = set()

        # Standard code attribution
        attribution_failures: list[tuple[str, str]] = []
        for code, send_date, window, producer_topic in attribution_tasks:
            dedup_key = f"{code.lower()}|{send_date}|{window}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            status.update(label=f"Attributing: {code}...")
            try:
                attr = compute_attribution(
                    shopify_auth, code, send_date,
                    event_window_days(code, send_date, window, sibling_dates, event_code_set),
                    producer_topic=producer_topic,
                    all_campaign_identifiers=all_campaign_identifiers,
                    orders=all_orders,
                    sibling_send_dates=sibling_dates.get(code.lower()),
                )
            except Exception as exc:
                attribution_failures.append((f"{code} @ {send_date}", str(exc)))
                continue
            storage_key = f"{code.lower()}|{send_date}"
            attributions[storage_key] = attr

        # Family / multi-code attribution
        for family_key, send_date, window in family_tasks:
            dedup_key = f"{family_key.lower()}|{send_date}|{window}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            members = get_family_identifiers(family_key, families)
            title_ids = [m.identifier for m in members]
            status.update(label=f"Attributing family: {family_key} ({len(title_ids)} codes)...")
            try:
                attr = compute_family_attribution(
                    shopify_auth, family_key, title_ids, send_date,
                    event_window_days(family_key, send_date, window, sibling_dates, event_code_set),
                    orders=all_orders,
                    sibling_send_dates=sibling_dates.get(family_key.lower()),
                )
            except Exception as exc:
                attribution_failures.append((f"{family_key} @ {send_date}", str(exc)))
                continue
            storage_key = f"{family_key.lower()}|{send_date}"
            attributions[storage_key] = attr

        # Flag offers that used an automatic discount, before the recipient
        # rule can drop those orders (Campaigns tab, automatic-discount view)
        for _attr in attributions.values():
            _attr.automatic_discount = any(o.is_automatic for o in _attr.matched_orders)

        # Email-recipient rule for automatic discounts (logic_spec 5.7)
        status.update(label="Checking automatic-discount buyers against email recipients...")
        try:
            apply_recipient_gate(hubspot_token, main_records, attributions)
            for key in apply_recipient_gate.last_failures:
                attribution_failures.append(
                    (f"Recipient check ({key})",
                     "HubSpot delivery records unavailable - every buyer counted"))
        except Exception as exc:
            attribution_failures.append(
                ("Recipient check", f"{exc} - every buyer counted for automatic discounts"))

        # New vs existing customers (logic_spec 1.7) — after the recipient
        # rule, so only orders credited to the email are split
        status.update(label="Splitting revenue into new and existing customers...")
        try:
            apply_new_customer_split(shopify_auth, attributions, all_orders, DATA_START_DATE)
        except Exception as exc:
            attribution_failures.append(
                ("New vs existing customer split", f"{exc} - those columns are left blank"))

        # Attributed line items for the Producers tab product view
        _topics_by_key: dict[str, str] = {}
        _window_end_by_key: dict = {}
        for rec in main_records:
            p = rec.parsed
            if not p.discount_code:
                continue
            _k = f"{p.discount_code.lower()}|{p.parsed_send_date}"
            if p.producer_topic and p.producer_topic not in _topics_by_key.get(_k, ""):
                _topics_by_key[_k] = " / ".join(filter(None, [_topics_by_key.get(_k), p.producer_topic]))
            if p.attribution_window_end:
                _window_end_by_key[_k] = max(p.attribution_window_end,
                                             _window_end_by_key.get(_k, p.attribution_window_end))
        product_lines = attribution_lines_frame(attributions, _topics_by_key, _window_end_by_key)

        # Click-influenced secondary attribution (HubSpot click events → orders)
        status.update(label="Matching email clicks to orders...")
        try:
            apply_click_attribution(hubspot_token, main_records, attributions,
                                    all_orders, all_campaign_identifiers)
        except Exception as exc:
            attribution_failures.append(("Click-influenced attribution", str(exc)))

        if attribution_failures:
            st.warning(
                "Some campaigns could not be attributed and were skipped:\n\n"
                + "\n".join(f"- {label}: {msg}" for label, msg in attribution_failures)
            )

        # Step 5: Assemble dashboard
        status.update(label="Assembling dashboard rows...")
        dashboard_rows = assemble_dashboard_rows(records, attributions, run_date)
        df = rows_to_dataframe(dashboard_rows)

        # Step 5b: Merge A/B test versions
        status.update(label="Merging A/B test campaigns...")
        df = apply_ab_grouping(df)
        df = apply_event_grouping(df, offer_type_map)

        # Step 6: Reports
        status.update(label="Generating reports...")
        weekly_df = generate_weekly_report(df, run_date)
        weekly_insights = generate_weekly_insights(df)
        monthly_df = generate_monthly_report(df, run_date.year, run_date.month)
        producer_current_df, producer_final_df = generate_producer_report(df)

        # Step 6b: Producer analytics (new)
        status.update(label="Loading producer mapping data...")
        producer_map = load_producer_mapping(PRODUCER_CODE_EXCEL)
        tier_map = load_tier_mapping(PRODUCER_PRODUCT_EXCEL)
        top_region_map, sub_region_map = load_region_mapping(PRODUCER_PRODUCT_EXCEL)
        producer_analytics = generate_producer_analytics(
            df, producer_map, offer_type_map, tier_map, top_region_map, sub_region_map
        )

        # Step 7: QA
        status.update(label="Fetching Shopify orders for unmatched-code analysis...")
        excluded_df = generate_excluded_campaigns(records)
        shopify_code_map = fetch_all_discount_codes_in_range(
            shopify_auth, DATA_START_DATE, run_date + timedelta(days=1),
            orders=all_orders,
        )
        campaign_codes = {
            r.parsed.discount_code.lower()
            for r in records
            if r.parsed.discount_code and r.parsed.qa_bucket in main_buckets
        }
        # Also add family member identifiers so they don't show as unmatched
        for fkey, members in families.items():
            if fkey.lower() in campaign_codes:
                for m in members:
                    campaign_codes.add(m.identifier.lower())
        unmatched_df = generate_unmatched_codes_report(shopify_code_map, campaign_codes)
        qa_summary = generate_qa_summary(records, dashboard_rows, unmatched_df)

        # Step 8: Write files
        status.update(label="Writing output files...")
        history_df = update_history(df)
        write_all_outputs(
            df=df,
            weekly_df=weekly_df,
            weekly_insights=weekly_insights,
            monthly_df=monthly_df,
            producer_current_df=producer_current_df,
            producer_final_df=producer_final_df,
            history_df=history_df,
            excluded_df=excluded_df,
            unmatched_df=unmatched_df,
            qa_summary=qa_summary,
        )

        status.update(label="Data loaded successfully!", state="complete", expanded=False)

    return {
        "run_date": run_date,
        "df": df,
        "weekly_df": weekly_df,
        "weekly_insights": weekly_insights,
        "monthly_df": monthly_df,
        "producer_current_df": producer_current_df,
        "producer_final_df": producer_final_df,
        "producer_analytics": producer_analytics,
        "product_lines": product_lines,
        "producer_map": producer_map,
        "offer_type_map": offer_type_map,
        "attribution_failures": attribution_failures,
        "hubspot_stats_failures": hubspot_stats_failures,
        "excluded_df": excluded_df,
        "unmatched_df": unmatched_df,
        "qa_summary": qa_summary,
        "main_count": len(main_records),
        "excluded_count": len(records) - len(main_records),
        "total_records": len(records),
    }


# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Grand Cru \u2014 Campaign Dashboard",
    page_icon="\U0001F377",
    layout="wide",
)

# Inject global CSS
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


# ─── Password Gate ───────────────────────────────────────────────────────────

def _check_password() -> bool:
    """Show a login form and return True only if the correct password is entered."""
    if st.session_state.get("authenticated"):
        return True

    # Retrieve password from Streamlit secrets (cloud) or .env.txt fallback
    try:
        correct_pw = st.secrets["DASHBOARD_PASSWORD"]
    except Exception:
        # Fallback: read from .env.txt
        correct_pw = None
        env_path = os.path.join(PROJECT_ROOT, ".env.txt")
        if os.path.isfile(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("DASHBOARD_PASSWORD"):
                        if "=" in line:
                            correct_pw = line.split("=", 1)[1].strip()
                        elif ":" in line:
                            correct_pw = line.split(":", 1)[1].strip()

    if not correct_pw:
        # No password configured — skip gate (local dev without password set)
        return True

    # ── Login UI ──
    st.markdown(f"""
    <div style="max-width: 400px; margin: 8rem auto; text-align: center;">
        <div style="font-size: 1.5rem; font-weight: 700; color: {CLR_TEXT_PRIMARY}; margin-bottom: 0.25rem;">
            Grand Cru Liquid Assets
        </div>
        <div style="font-size: 0.85rem; color: {CLR_TEXT_MUTED}; margin-bottom: 2rem;">
            Campaign Performance Dashboard
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        password = st.text_input("Password", type="password", placeholder="Enter dashboard password")
        if st.button("Sign in", type="primary", width="stretch"):
            if password == correct_pw:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    st.stop()
    return False


if not _check_password():
    st.stop()


# ─── Load data ───────────────────────────────────────────────────────────────

if "data" not in st.session_state:
    try:
        st.session_state["data"] = run_pipeline()
    except Exception as e:
        st.error(f"Pipeline failed: {e}")
        st.stop()

data = st.session_state["data"]
run_date = data["run_date"]
full_df = data["df"]

# Persistent alert: if any campaigns were skipped due to Shopify errors during
# the pipeline run, their metrics show as $0. This banner survives reruns
# (unlike the in-run warning) so the failure state is never invisible.
_attr_failures = data.get("attribution_failures") or []
if _attr_failures:
    st.error(
        f"⚠️ **{len(_attr_failures)} campaign(s) could not be attributed** during the "
        f"last data load (Shopify errors) — their revenue shows as $0 even though "
        f"orders may exist. Click **Refresh Data** in the sidebar to re-run.\n\n"
        + "\n".join(f"- {label}" for label, _msg in _attr_failures[:15])
        + ("\n- …" if len(_attr_failures) > 15 else "")
    )

# Stale-module guard: if Streamlit Cloud soft-reloaded dashboard.py without
# restarting the process, old src/ modules stay loaded and silently corrupt
# the pipeline (rate-limit drops, missing kwargs). Make that state loud.
from src import hubspot as _hubspot_mod
_module_version = getattr(_hubspot_mod, "MODULE_VERSION", "pre-2026-08-04")
if _module_version != PIPELINE_VERSION:
    st.error(
        f"🔴 **Stale deployment detected** — the app is running mixed code versions "
        f"(dashboard {PIPELINE_VERSION}, modules {_module_version}). Data may be wrong. "
        f"Reboot the app from the Streamlit Cloud console (Manage app → Reboot)."
    )

_stats_failures = data.get("hubspot_stats_failures") or []
if _stats_failures:
    st.error(
        f"⚠️ **HubSpot delivery stats were unreachable for {len(_stats_failures)} "
        f"campaign(s)** during the last data load (rate limit or API error). They show "
        f"0 delivered and their revenue may be understated. Click **Refresh Data** in "
        f"the sidebar to re-run.\n\n"
        + "\n".join(f"- {name}" for name in _stats_failures[:15])
        + ("\n- …" if len(_stats_failures) > 15 else "")
    )


# ─── Page Header ─────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="page-header">
    <div>
        <div class="page-title">Grand Cru Liquid Assets</div>
        <div class="page-subtitle">Campaign Performance Dashboard</div>
    </div>
    <div style="text-align: right;">
        <div style="font-size: 0.75rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em;">Run Date</div>
        <div style="font-size: 1rem; font-weight: 600; color: {CLR_TEXT_PRIMARY};">{run_date.strftime('%B %d, %Y')}</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ─── Sidebar ─────────────────────────────────────────────────────────────────

excl_df = data["excluded_df"]
unmatched_df = data["unmatched_df"]

with st.sidebar:
    if st.button("Refresh Data", type="primary", width="stretch"):
        run_pipeline.clear()  # clear the cross-session cache, not just this session
        load_offer_log.clear()
        st.session_state.pop("data", None)
        st.rerun()

    st.markdown(f"""
    <div style="margin-top: 1.5rem; font-size: 0.8rem; color: {CLR_TEXT_MUTED};">
        Data from <strong style="color:{CLR_TEXT_SECONDARY};">{DATA_START_DATE}</strong><br>
        <strong style="color:{CLR_TEXT_SECONDARY};">{data['main_count']}</strong> campaigns tracked<br>
        <span style="font-size:0.7rem;">Pipeline v{PIPELINE_VERSION}
        ({'modules OK' if _module_version == PIPELINE_VERSION else 'STALE MODULES'})</span>
    </div>
    """, unsafe_allow_html=True)

    # QA health summary
    n_excluded = len(excl_df) if not excl_df.empty else 0
    n_unmatched = len(unmatched_df) if not unmatched_df.empty else 0
    qa_clean = n_excluded == 0 and n_unmatched == 0
    qa_class = "qa-ok" if qa_clean else "qa-warn"
    qa_icon = "\u2713" if qa_clean else "\u26A0"

    st.markdown(f"""
    <div style="margin-top: 1rem; padding: 0.75rem; background: {CLR_BG_PAGE}; border-radius: 6px; border: 1px solid {CLR_BORDER};">
        <div style="font-size: 0.7rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.35rem;">Data Quality</div>
        <div class="{qa_class}" style="font-size: 0.85rem;">
            {qa_icon} {n_excluded} excluded &middot; {n_unmatched} unmatched
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Date Range (shared by the Weekly and Campaigns tabs) ────────────
    st.markdown(f"""
    <div style="margin-top: 1.25rem; font-size: 0.7rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em;">
        Date Range &middot; Weekly, Campaigns &amp; A vs B tabs
    </div>
    """, unsafe_allow_html=True)
    _all_send_dates = pd.to_datetime(full_df["Parsed Send Date"].dropna()).dt.date
    _min_date = _all_send_dates.min() if not _all_send_dates.empty else DATA_START_DATE
    date_range = st.date_input(
        "Send dates",
        value=(run_date - timedelta(days=6), run_date),
        min_value=_min_date,
        max_value=run_date,
        key="wk_date_range",
    )
    finalized_only = st.toggle(
        "Finalized only", value=True, key="wk_finalized_toggle",
        help="Only campaigns whose attribution window has closed, so their numbers won't change.",
    )

    # ── Campaign Filter ─────────────────────────────────────────────────
    st.markdown(f"""
    <div style="margin-top: 1.25rem; font-size: 0.7rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em;">
        Campaign Filter
    </div>
    """, unsafe_allow_html=True)
    exclude_bin_holiday = st.toggle(
        "Exclude BIN Sale, Large Format & Holiday campaigns",
        value=False,
        key="exclude_bin_holiday",
        help=(
            "Hides event campaigns so standalone producer offers can be compared "
            "on their own. A campaign counts as an event when ANY of these hold: "
            "its code is a discount family (BIN Sale, GoBig, BigDaddy, July4th, "
            "LaborDay25…); its name contains BIN Sale / holiday sale / flash sale "
            "/ clearance; or its code's Offer Type in the Producer - Discount Code "
            "mapping sheet is 'holiday' or 'format promotion'. Producer offers that "
            "merely feature large formats or a holiday theme under the producer's "
            "own code are kept."
        ),
    )

    # ── Metric Definitions ──────────────────────────────────────────────
    st.markdown(f"""
    <div style="margin-top: 1.5rem; padding: 0.75rem; background: {CLR_BG_PAGE}; border-radius: 6px; border: 1px solid {CLR_BORDER};">
        <div style="font-size: 0.7rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem;">Metric Definitions</div>
        <div style="font-size: 0.78rem; color: {CLR_TEXT_SECONDARY}; line-height: 1.65;">
            <strong style="color:{CLR_TEXT_PRIMARY};">Attributed Revenue</strong><br>
            Net sales from the item(s) that received the campaign discount (price &times; qty &minus; discount).<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Total Sales</strong><br>
            Full order revenue from matched orders, including discounted and non-discounted items.<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Revenue / Delivered</strong><br>
            Attributed Revenue &divide; Delivered. Measures net revenue earned per email delivered.<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Delivered</strong><br>
            Number of emails successfully delivered (from HubSpot).<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Clicks</strong><br>
            People who clicked at least one link in the email (HubSpot&rsquo;s &ldquo;Clicked&rdquo;). Click rate = Clicks &divide; Delivered.<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Discounted Orders</strong><br>
            Number of Shopify orders that used the campaign discount code within the attribution window.<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Discount Value</strong><br>
            Total dollar amount discounted across all matched line items.
        </div>
    </div>
    """, unsafe_allow_html=True)


# ─── Apply event-campaign filter ──────────────────────────────────────────────
# Keep raw full_df for QA tab; filtered version used by Weekly/Monthly/Producer.
# Display-only: is_bin_holiday also drives the 3-day attribution window, so it
# must not be widened — the event flag is derived here instead.
_offer_type_map = data.get("offer_type_map") or load_offer_type_mapping(PRODUCER_CODE_EXCEL)
_event_mask = event_mask(full_df, _offer_type_map)
if exclude_bin_holiday:
    display_df = full_df[~_event_mask].copy()
else:
    display_df = full_df

# Recompute monthly and producer reports from filtered data
from src.reports import generate_monthly_report, generate_producer_report
_monthly_df = generate_monthly_report(display_df, run_date.year, run_date.month)
_producer_current_df, _producer_final_df = generate_producer_report(display_df)

# ─── Apply the sidebar date range (Weekly + Campaigns tabs) ──────────────────
completed_week_df = pd.DataFrame()
coded_week = pd.DataFrame()
wk_start = wk_end = None
_weekly_has_data = False
_range_label = ""

# date_input returns a tuple only when both dates are picked
if isinstance(date_range, tuple) and len(date_range) == 2:
    wk_start, wk_end = date_range[0], date_range[1]
    _send = pd.to_datetime(display_df["Parsed Send Date"]).dt.date
    completed_week_df = display_df[
        display_df["Parsed Send Date"].notna() & (_send >= wk_start) & (_send <= wk_end)
    ].copy()
    if finalized_only:
        completed_week_df = completed_week_df[
            completed_week_df["is_final_snapshot"] == True
        ].copy()
    # Coded campaigns for charts and revenue KPIs
    coded_week = completed_week_df[
        (completed_week_df["Discount Code"] != "None") &
        (completed_week_df["Attributed Revenue"].notna())
    ].copy()
    _weekly_has_data = not completed_week_df.empty
    _fin_label = ("Finalized campaigns only" if finalized_only
                  else "All campaigns (includes open windows)")
    _range_label = (f'{_fin_label} &middot; {wk_start.strftime("%b %d")} '
                    f'– {wk_end.strftime("%b %d, %Y")}')

_benchmarks = compute_benchmarks(full_df, _offer_type_map)

# Offer A / B slots for every offer (logic_spec 6.8)
_offer_log, _offer_log_note, _offer_log_ok = load_offer_log(run_date)
_slots = assign_offer_slots(full_df, _offer_log, _offer_type_map)

# ─── Tabs ────────────────────────────────────────────────────────────────────

(tab_weekly, tab_campaigns, tab_monthly, tab_ab, tab_producer, tab_section,
 tab_qa, tab_method) = st.tabs([
    "Weekly",
    "Campaigns",
    "Monthly",
    "Offer A vs B",
    "Producers",
    "Offer Section",
    "QA",
    "Methodology",
])


@st.cache_resource(ttl=6 * 3600, show_spinner=False)
def load_section_impact() -> dict:
    """Journey-classify post-launch orders + fetch section clicks.

    Separate from run_pipeline so the main dashboard load is unaffected;
    computed on demand from the Offer Section tab, then shared across all
    sessions for 6 hours. Per-order classifications are additionally cached
    on disk (journey_cache.csv) so only new orders ever cost API calls.
    """
    env = load_env()
    token = env["HUBSPOT_PRIVATE_APP_TOKEN"]
    auth = ShopifyAuth(
        store_domain=env["SHOPIFY_STORE_DOMAIN"],
        client_id=env["SHOPIFY_CLIENT_ID"],
        client_secret=env["SHOPIFY_CLIENT_SECRET"],
    )
    raw_v3 = _fetch_v3_emails(token)
    orders_df = classify_section_orders(token, auth, raw_v3)
    clicks_df = fetch_section_clicks(token, SECTION_LAUNCH_DATE)
    return {
        "orders": orders_df,
        "clicks": clicks_df,
        "loaded_at": datetime.now(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 1: Weekly — summary cards, top-10 ranking, detailed notes
# (date range + Finalized toggle live in the sidebar, shared with Campaigns)
# ═══════════════════════════════════════════════════════════════════════════════

with tab_weekly:

    if wk_start is None:
        st.info("Pick a start and an end date under Date Range in the sidebar.")
    elif not _weekly_has_data:
        st.markdown(f'<div class="context-line">{_range_label}</div>', unsafe_allow_html=True)
        st.info("No campaigns in the selected date range match the current filter.")
    else:
        st.markdown(f'<div class="context-line">{_range_label}</div>', unsafe_allow_html=True)

        # ── KPI cards (4 per row, same size, short numbers; hover = exact) ──
        _deliv = completed_week_df["Delivered"].sum()
        _clicks = completed_week_df["Clicked"].sum() if "Clicked" in completed_week_df.columns else None
        _delivered_card = {
            "label": "Delivered", "value": fmt_count(_deliv), "exact": exact_count(_deliv),
            "sub": f"{len(completed_week_df):,} campaigns",
        }
        _clicks_card = {
            "label": "Clicks", "value": fmt_count(_clicks), "exact": exact_count(_clicks),
            "sub": (f"{_clicks / _deliv:.1%} of delivered" if _clicks is not None and _deliv else ""),
        }
        kpis = []
        if not coded_week.empty:
            _attr = coded_week["Attributed Revenue"].sum()
            _sales = coded_week["Total Sales"].sum()
            _orders = coded_week["Discounted Orders"].dropna().sum()
            kpis += [
                {"label": "Attributed Revenue", "value": fmt_money(_attr), "exact": exact_money(_attr),
                 "sub": f"{fmt_count(_orders)} orders · net of discount"},
            ]
            if ("New Customer Revenue" in coded_week.columns
                    and coded_week["New Customer Revenue"].notna().any()):
                _new_rev = coded_week["New Customer Revenue"].sum()
                _new_orders = coded_week["New Customer Orders"].sum()
                kpis.append({
                    "label": "New-Customer Revenue", "value": fmt_money(_new_rev),
                    "exact": (f"{exact_money(_new_rev)} from {exact_count(_new_orders)} first-ever "
                              f"Shopify orders; existing customers {exact_money(_attr - _new_rev)}"),
                    "sub": (f"{_new_rev / _attr:.0%} of attributed · {fmt_count(_new_orders)} buyers"
                            if _attr else f"{fmt_count(_new_orders)} buyers"),
                })
            kpis += [
                {"label": "Total Sales", "value": fmt_money(_sales), "exact": exact_money(_sales),
                 "sub": "Full matched orders"},
            ]
            # Fixed benchmark: each finalized offer vs the typical offer of
            # the reference year sent on the same kind of day.
            if _benchmarks:
                _rates = offer_rates(completed_week_df, _offer_type_map)
                _rates = _rates[_rates["day_type"].isin(list(_benchmarks))]
                _bench_txt = " · ".join(
                    f"{k} ${v:,.0f}" for k, v in sorted(_benchmarks.items(), key=lambda kv: kv[0] != "Weekday")
                )
                _bench_tip = (
                    f"Benchmark = revenue per 1,000 emails delivered of the typical (median) offer, "
                    f"{BENCHMARK_LABEL}: {_bench_txt}. Each finalized offer in this range is compared "
                    f"with the benchmark for its day type (Fri/Sat or weekday). Events and sends under "
                    f"{MIN_DELIVERED_THRESHOLD} delivered are left out."
                )
                if _rates.empty:
                    kpis.append({"label": "Offers beating benchmark", "value": "—",
                                 "sub": "No finalized offers in range", "exact": _bench_tip})
                else:
                    _above = int((_rates["rate_per_1000"] > _rates["day_type"].map(_benchmarks)).sum())
                    kpis.append({"label": "Offers beating benchmark",
                                 "value": f"{_above} of {len(_rates)}",
                                 "sub": "Typical period: about half", "exact": _bench_tip})
            kpis += [_delivered_card, _clicks_card]
            if "Influenced Total Sales" in coded_week.columns:
                _inf_sales = coded_week["Influenced Total Sales"].dropna().sum()
                _inf_orders = coded_week["Influenced Orders"].dropna().sum()
                if _inf_orders > 0:
                    kpis.append({
                        "label": "Influenced Sales", "value": fmt_money(_inf_sales),
                        "exact": exact_money(_inf_sales),
                        "sub": f"{_inf_orders:.0f} email-driven orders w/o code",
                    })
            if "Click-Influenced Offer Revenue" in coded_week.columns:
                _clk_rev = coded_week["Click-Influenced Offer Revenue"].dropna().sum()
                _clk_orders = coded_week["Click-Influenced Orders"].dropna().sum()
                if _clk_orders > 0:
                    kpis.append({
                        "label": "Click-Influenced", "value": fmt_money(_clk_rev),
                        "exact": exact_money(_clk_rev),
                        "sub": f"{_clk_orders:.0f} clicked, returned later, bought w/o code",
                    })
        else:
            kpis += [_delivered_card, _clicks_card]
        render_kpi_row(kpis)

        spacer("lg")

        # ── Key insights for leadership: four short cards ────────────────
        section_title("Key Insights", "Short findings with the numbers behind them, ready for slides")
        render_key_insights(completed_week_df, wk_start, wk_end, finalized_only,
                            display_df, _offer_type_map, _benchmarks, _slots)

        spacer("lg")

        # ── Campaign Performance Ranking (top 10 by default) ─────────────
        section_title(
            "Campaign Performance Ranking",
            "Top 10 by the selected metric · every campaign is listed in the Campaigns tab",
        )

        if not coded_week.empty:
            _c_metric, _c_view = st.columns([3, 2])
            with _c_metric:
                metric_choice = st.radio(
                    "Rank by",
                    ["Attributed Revenue", "Total Sales", "Revenue per Delivered"],
                    horizontal=True,
                    key="weekly_bar_metric",
                    label_visibility="collapsed",
                )
            with _c_view:
                rank_view = st.radio(
                    "Show",
                    ["Top 10", "Bottom 10", "All"],
                    horizontal=True,
                    key="weekly_bar_view",
                    label_visibility="collapsed",
                )

            _rank_src = coded_week
            if metric_choice == "Revenue per Delivered":
                # Efficiency rankings skip tiny sends (business rule)
                _rank_src = coded_week[coded_week["Delivered"] >= MIN_DELIVERED_THRESHOLD]
            chart_data = _rank_src[["Campaign Name", "Discount Code", "Delivered", metric_choice]] \
                .dropna(subset=[metric_choice]).copy()
            if rank_view == "Top 10":
                chart_data = chart_data.nlargest(TOP_N, metric_choice)
            elif rank_view == "Bottom 10":
                chart_data = chart_data.nsmallest(TOP_N, metric_choice)

            fmt = "$,.4f" if metric_choice == "Revenue per Delivered" else "$,.2f"
            bar_chart = alt.Chart(chart_data).mark_bar(
                color=CLR_WEEKLY,
                cornerRadiusEnd=3,
            ).encode(
                y=alt.Y("Campaign Name:N", sort=("x" if rank_view == "Bottom 10" else "-x"),
                        title=None, axis=alt.Axis(labelLimit=380)),
                x=alt.X(f"{metric_choice}:Q", title=metric_choice,
                        axis=alt.Axis(format=("$,.2f" if metric_choice == "Revenue per Delivered" else "$,.0f"))),
                tooltip=[
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip("Delivered:Q", format=","),
                    alt.Tooltip(f"{metric_choice}:Q", format=fmt),
                ],
            ).properties(height=max(len(chart_data) * 30, 160))

            st.altair_chart(styled_chart(bar_chart), width="stretch")
            _notes = []
            if metric_choice == "Revenue per Delivered":
                _notes.append(f"Campaigns with fewer than {MIN_DELIVERED_THRESHOLD} delivered are left out.")
            if rank_view == "All" and len(chart_data) > 50:
                _notes.append("Long list: the Campaigns tab shows the same campaigns as a scrollable table.")
            if _notes:
                st.caption(" ".join(_notes))
        else:
            st.info("No coded campaigns with revenue data to chart.")

        spacer("lg")

        # ── Detailed notes (collapsed; a short leadership summary replaces
        #    this in a later update) ─────────────────────────────────────
        with st.expander("Detailed campaign notes", expanded=False):
            st.markdown(_generate_analytical_insights(completed_week_df, wk_start, wk_end))


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 2: Campaigns — the campaign table + the CSV other projects load
# ═══════════════════════════════════════════════════════════════════════════════

with tab_campaigns:

    if wk_start is None:
        st.info("Pick a start and an end date under Date Range in the sidebar.")
    elif not _weekly_has_data:
        st.markdown(f'<div class="context-line">{_range_label}</div>', unsafe_allow_html=True)
        st.info("No campaigns in the selected date range match the current filter.")
    else:
        st.markdown(f'<div class="context-line">{_range_label}</div>', unsafe_allow_html=True)

        # Derive Sales per Delivered = Total Sales / Delivered.
        # NaN-safe: null Total Sales (EDU/CONTENT) propagates; 0-Delivered
        # avoids divide-by-zero by becoming NaN.
        details_df = completed_week_df.copy()
        if "Total Sales" in details_df.columns and "Delivered" in details_df.columns:
            details_df["Sales per Delivered"] = (
                details_df["Total Sales"]
                / details_df["Delivered"].replace(0, float("nan"))
            )

        # Add Producer column by resolving discount codes
        _wk_producer_map = data.get("producer_map", {})
        details_df["Producer"] = details_df["Discount Code"].apply(
            lambda c: resolve_producer(c, _wk_producer_map) or ""
        )

        _c_view, _c_dl = st.columns([3, 1])
        with _c_view:
            campaigns_view = st.radio(
                "View",
                ["Campaign results", "Automatic-discount offers"],
                horizontal=True,
                key="campaigns_view",
                label_visibility="collapsed",
            )
        with _c_dl:
            st.download_button(
                "Download CSV (all columns)",
                data=build_campaign_csv(details_df),
                file_name=f"campaign_details_{wk_start.isoformat()}_to_{wk_end.isoformat()}.csv",
                mime="text/csv",
                key="campaign_csv_download",
                width="stretch",
            )

        if campaigns_view == "Campaign results":
            section_title(
                "Campaign Results",
                f"{len(details_df):,} campaigns · newest first · click a column header to sort",
            )
            # Send Date + Campaign are pinned so they stay in view while the
            # numbers scroll sideways; revenue columns come first.
            table_cols = [
                "Parsed Send Date", "Campaign Name",
                "Attributed Revenue", "New Customer Revenue", "Existing Customer Revenue",
                "Discounted Orders", "Delivered", "Clicked",
                "Total Sales", "Revenue per Delivered", "Sales per Delivered",
                "Producer", "Discount Code",
            ]
            available = [c for c in table_cols if c in details_df.columns]
            st.dataframe(
                details_df[available],
                column_config={
                    "Parsed Send Date": st.column_config.DateColumn("Send Date", width="small", pinned=True),
                    "Producer": st.column_config.TextColumn("Producer", width="medium"),
                    "Campaign Name": st.column_config.TextColumn("Campaign", width="large", pinned=True),
                    "Discount Code": st.column_config.TextColumn("Code", width="small"),
                    "Delivered": st.column_config.NumberColumn("Delivered", format="%d"),
                    "Clicked": st.column_config.NumberColumn(
                        "Clicks", format="%d",
                        help="People who clicked at least one link in the email (HubSpot's 'Clicked')"),
                    "Discounted Orders": st.column_config.NumberColumn(
                        "Orders", help="Orders that used the campaign discount"),
                    "Attributed Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                    "New Customer Revenue": st.column_config.NumberColumn(
                        "New Cust. Rev", format="$%.2f",
                        help="Attributed Revenue from first-ever Shopify orders"),
                    "Existing Customer Revenue": st.column_config.NumberColumn(
                        "Existing Cust. Rev", format="$%.2f",
                        help="Attributed Revenue from customers who had ordered before. "
                             "New + Existing = Attr. Revenue"),
                    "Total Sales": st.column_config.NumberColumn("Total Sales", format="$%.2f"),
                    "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                    "Sales per Delivered": st.column_config.NumberColumn("Sales/Delivered", format="$%.4f"),
                },
                height=460,
                width="stretch",
                hide_index=True,
            )
        else:
            # ── Automatic-discount offers: what the email is credited with vs
            #    what the discount sold to people who weren't sent the email ──
            section_title(
                "Automatic-Discount Offers",
                "Offers set up as automatic discounts apply to every shopper, not only email "
                "readers · only buyers who were sent the email are credited to it",
            )
            _auto = (details_df[details_df["Automatic Discount"] == True].copy()
                     if "Automatic Discount" in details_df.columns else pd.DataFrame())
            if _auto.empty:
                st.info("No offers in this date range used an automatic discount.")
            else:
                _auto["Not-Sent Orders"] = (_auto["All-Buyer Orders"] - _auto["Discounted Orders"]).clip(lower=0)
                _auto["Not-Sent Revenue"] = (_auto["All-Buyer Revenue"] - _auto["Attributed Revenue"]).clip(lower=0)
                _cred = _auto["Attributed Revenue"].sum()
                _cred_orders = _auto["Discounted Orders"].sum()
                _ns = _auto["Not-Sent Revenue"].sum()
                _ns_orders = _auto["Not-Sent Orders"].sum()
                _all = _auto["All-Buyer Revenue"].sum()
                render_kpi_row([
                    {"label": "Automatic-Discount Offers", "value": fmt_count(len(_auto)),
                     "sub": f"of {len(details_df):,} campaigns in range"},
                    {"label": "Credited to Email", "value": fmt_money(_cred), "exact": exact_money(_cred),
                     "sub": f"{fmt_count(_cred_orders)} orders from people sent the email"},
                    {"label": "Buyers Not Sent the Email", "value": fmt_money(_ns), "exact": exact_money(_ns),
                     "sub": f"{fmt_count(_ns_orders)} orders · context only"},
                    {"label": "Share Credited to Email", "value": (f"{_cred / _all:.0%}" if _all else "—"),
                     "sub": f"of {fmt_money(_all)} sold under these discounts",
                     "exact": exact_money(_all) + " total under these discounts"},
                ])
                spacer("sm")
                st.dataframe(
                    _auto[[
                        "Parsed Send Date", "Campaign Name", "Discount Code", "Delivered",
                        "Discounted Orders", "Attributed Revenue",
                        "Not-Sent Orders", "Not-Sent Revenue", "All-Buyer Revenue",
                    ]],
                    column_config={
                        "Parsed Send Date": st.column_config.DateColumn("Send Date", width="small", pinned=True),
                        "Campaign Name": st.column_config.TextColumn("Campaign", width="large", pinned=True),
                        "Discount Code": st.column_config.TextColumn("Code", width="small"),
                        "Delivered": st.column_config.NumberColumn("Delivered", format="%d"),
                        "Discounted Orders": st.column_config.NumberColumn(
                            "Credited Orders", help="Orders from people who were sent the email before ordering"),
                        "Attributed Revenue": st.column_config.NumberColumn(
                            "Credited Revenue", format="$%.2f",
                            help="Attributed Revenue: what the email is credited with"),
                        "Not-Sent Orders": st.column_config.NumberColumn(
                            "Not-Sent Orders", help="Orders under the discount from people not sent the email"),
                        "Not-Sent Revenue": st.column_config.NumberColumn(
                            "Not-Sent Revenue", format="$%.2f",
                            help="Revenue under the discount from people not sent the email. Context only, "
                                 "not credited to the email"),
                        "All-Buyer Revenue": st.column_config.NumberColumn(
                            "All Buyers", format="$%.2f", help="Credited + Not-Sent"),
                    },
                    height=420,
                    width="stretch",
                    hide_index=True,
                )
                st.caption(
                    "**How to read this:** an automatic discount works for anyone who reaches the wine's page "
                    "(search, the website, word of mouth), so part of what it sells would have happened "
                    "without the email. The dashboard credits the email only with orders from people it was "
                    "delivered to before they ordered. **Not-Sent** is the rest: real sales under the offer, "
                    "shown for context and never added to email revenue."
                )

        st.caption(
            "**Download CSV (all columns)** is the file for other projects: the same columns in "
            "the same order as before (including Influenced, Click-Influenced and All-Buyer), "
            "with Clicks, New Customer Orders, New Customer Revenue and Existing Customer Revenue "
            "added at the end. The small download icon on a table saves only the columns shown. "
            "Blank = attribution not attempted (no code, EDU/CONTENT); 0 = attempted, no orders."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 2: Monthly Report
# ═══════════════════════════════════════════════════════════════════════════════

with tab_monthly:

    # ── Month Selector ───────────────────────────────────────────────────
    # Build the list of selectable months: every month from DATA_START_DATE
    # through the current run month, newest first. This ensures the user
    # can always pick any month (even ones with zero finalized campaigns).
    from dateutil.relativedelta import relativedelta  # local import; keeps top clean

    _floor = pd.Timestamp(DATA_START_DATE).to_pydatetime().date().replace(day=1)
    _ceiling = run_date.replace(day=1)
    _month_options: list[tuple[int, int]] = []
    _cursor = _ceiling
    while _cursor >= _floor:
        _month_options.append((_cursor.year, _cursor.month))
        _cursor = (_cursor - relativedelta(months=1))

    def _fmt_month(ym: tuple[int, int]) -> str:
        return date(ym[0], ym[1], 1).strftime("%B %Y")

    # Default to the current run month (first option).
    sel_year, sel_month = st.selectbox(
        "Month",
        _month_options,
        index=0,
        format_func=_fmt_month,
        key="monthly_month_picker",
    )

    # ── Weeks of the month ───────────────────────────────────────────────
    # Whole Monday-Sunday weeks; a week belongs to the month that holds most
    # of its days (its Thursday). Month totals are the sum of these weeks.
    _weeks = month_weeks(sel_year, sel_month)
    _p_start, _p_end = _weeks[0], _weeks[-1] + timedelta(days=6)
    st.markdown(
        f'<div class="context-line">{_fmt_month((sel_year, sel_month))} &middot; '
        f'{len(_weeks)} weeks, {week_label(_weeks[0])} to {week_label(_weeks[-1])} &middot; '
        f'Finalized campaigns with closed attribution windows</div>',
        unsafe_allow_html=True,
    )

    monthly_df = generate_period_report(display_df, _p_start, _p_end)
    _fin_coded = coded_offers(display_df[display_df["is_final_snapshot"] == True])
    all_month = _in_range(_fin_coded, _p_start, _p_end).copy()

    # Same week last year = 52 weeks (364 days) earlier: weekdays and
    # holidays line up (Thanksgiving week vs Thanksgiving week).
    _wk_rows = []
    for _mon in _weeks:
        _sun = _mon + timedelta(days=6)
        _ty = _in_range(_fin_coded, _mon, _sun)
        _ly_mon = _mon - timedelta(days=364)
        _ly_ok = _ly_mon >= DATA_START_DATE
        _ly = _in_range(_fin_coded, _ly_mon, _ly_mon + timedelta(days=6)) if _ly_ok else None
        _ty_rev = _ty["Attributed Revenue"].sum()
        _ly_rev = _ly["Attributed Revenue"].sum() if _ly_ok else None
        # A week is finished once its last send's 7-day window has closed
        _future = _mon > run_date
        _done = _sun + timedelta(days=7) <= run_date
        _wk_rows.append({
            "Week": week_label(_mon) + (" (upcoming)" if _future else "" if _done else " (still open)"),
            "_mon": _mon,
            "_done": _done,
            "Offers": len(_ty),
            "Revenue": _ty_rev,
            "Same Week Last Year": week_label(_ly_mon, with_year=True),
            "LY Offers": len(_ly) if _ly_ok else None,
            "LY Revenue": _ly_rev,
            "Change": (_ty_rev / _ly_rev - 1) if (_ly_ok and _ly_rev) else None,
        })
    _wk_df = pd.DataFrame(_wk_rows)
    _ly_any = _wk_df["LY Revenue"].notna().any()

    if all_month.empty:
        render_kpi_row([
            {"label": "Attributed Revenue", "value": "$0", "sub": "Net sales, discounted items"},
            {"label": "Total Sales", "value": "$0", "sub": "Full matched orders"},
            {"label": "Total Orders", "value": "0"},
            {"label": "Offers Sent", "value": "0"},
        ])
        spacer("md")
        st.info(
            f"No finalized campaigns in {_fmt_month((sel_year, sel_month))} yet. "
            "Campaigns appear here once their attribution window closes. "
            "Pick a different month above to compare."
        )
    else:
        # ── KPI Row (this month's weeks, vs the same weeks last year) ─────
        total_rev = monthly_df["Total_Attributed_Revenue"].sum() if not monthly_df.empty else 0
        total_sales = monthly_df["Total_Sales"].sum() if (not monthly_df.empty and "Total_Sales" in monthly_df.columns) else 0
        total_orders = monthly_df["Total_Discounted_Orders"].sum() if not monthly_df.empty else 0
        # Compare with last year only over FINISHED weeks, so a month in
        # progress isn't measured against last year's complete month.
        _cmp = _wk_df[_wk_df["_done"] & _wk_df["LY Revenue"].notna()]
        _all_done = bool(_wk_df["_done"].all())
        _prefix = "" if _all_done else "Finished weeks: "
        _rev_sub, _off_sub = "Net sales, discounted items", f"{len(monthly_df)} discount codes"
        if not _cmp.empty and _cmp["LY Revenue"].sum() > 0:
            _rev_sub = (f"{_prefix}{_cmp['Revenue'].sum() / _cmp['LY Revenue'].sum() - 1:+.0%} "
                        f"vs same weeks last year")
            _off_sub = f"{_prefix}{int(_cmp['Offers'].sum())} vs {int(_cmp['LY Offers'].sum())} last year"
        elif not _ly_any:
            _rev_sub = "No last-year data (starts May 2025)"
        elif _cmp.empty:
            _rev_sub = "No finished weeks to compare yet"
        render_kpi_row([
            {"label": "Attributed Revenue", "value": fmt_money(total_rev), "exact": exact_money(total_rev),
             "sub": _rev_sub},
            {"label": "Total Sales", "value": fmt_money(total_sales), "exact": exact_money(total_sales),
             "sub": "Full matched orders"},
            {"label": "Total Orders", "value": fmt_count(total_orders), "exact": exact_count(total_orders)},
            {"label": "Offers Sent", "value": fmt_count(len(all_month)), "sub": _off_sub},
        ])

        spacer("lg")

        # ── Week by week vs the same week last year ──────────────────────
        section_title(
            "Week by Week vs Last Year",
            "Monday–Sunday weeks · bars = attributed revenue · labels = offers sent",
        )
        _chart_rows = []
        for _, _w in _wk_df.iterrows():
            _chart_rows.append({"Week": _w["Week"], "Year": "This year", "Revenue": _w["Revenue"],
                                "Offers": f"{_w['Offers']} offers", "Dates": _w["Week"]})
            if pd.notna(_w["LY Revenue"]):
                _chart_rows.append({"Week": _w["Week"], "Year": "Last year", "Revenue": _w["LY Revenue"],
                                    "Offers": f"{int(_w['LY Offers'])} offers",
                                    "Dates": _w["Same Week Last Year"]})
        _cdf = pd.DataFrame(_chart_rows)
        _week_order = _wk_df["Week"].tolist()
        _base = alt.Chart(_cdf).encode(
            x=alt.X("Week:N", sort=_week_order, title=None, axis=alt.Axis(labelAngle=0)),
            xOffset=alt.XOffset("Year:N", sort=["Last year", "This year"]),
        )
        _bars = _base.mark_bar(cornerRadiusEnd=3).encode(
            y=alt.Y("Revenue:Q", title="Attributed Revenue", axis=alt.Axis(format="$,.0f")),
            color=alt.Color("Year:N", sort=["Last year", "This year"],
                            scale=alt.Scale(domain=["Last year", "This year"], range=[CLR_TEXT_MUTED, CLR_MONTHLY]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("Year:N"), alt.Tooltip("Dates:N", title="Week"),
                     alt.Tooltip("Revenue:Q", format="$,.2f"), alt.Tooltip("Offers:N")],
        )
        _labels = _base.mark_text(dy=-7, fontSize=10, color=CLR_TEXT_SECONDARY).encode(
            y=alt.Y("Revenue:Q"), text="Offers:N",
        )
        st.altair_chart(styled_chart((_bars + _labels).properties(height=320)), width="stretch")

        st.dataframe(
            _wk_df.drop(columns=["_mon", "_done"]),
            column_config={
                "Week": st.column_config.TextColumn("Week (Mon–Sun)", width="medium"),
                "Revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                "Same Week Last Year": st.column_config.TextColumn("Same Week Last Year", width="medium"),
                "LY Offers": st.column_config.NumberColumn("LY Offers", format="%d"),
                "LY Revenue": st.column_config.NumberColumn("LY Revenue", format="$%.2f"),
                "Change": st.column_config.NumberColumn("Change", format="percent"),
            },
            width="stretch",
            hide_index=True,
        )
        st.caption(
            f"**How weeks work:** every week runs Monday to Sunday and belongs to the month that holds "
            f"most of its days, so {_fmt_month((sel_year, sel_month))} is {week_label(_weeks[0])} to "
            f"{week_label(_weeks[-1])} and no week is split between months. The month's totals above "
            f"are these weeks added up. **Same week last year** is exactly 52 weeks earlier, so weekdays "
            f"and holidays line up. Revenue = attributed revenue of finalized campaigns; offers = "
            f"campaigns with a discount code. Last-year data starts May 2025."
        )

        spacer("lg")

        # ── Top Discount Codes ───────────────────────────────────────────
        section_title("Top Discount Codes", "Highest revenue-generating codes this month")

        if not monthly_df.empty:
            top3 = monthly_df.head(3).copy()

            top3_chart = alt.Chart(top3).mark_bar(
                color=CLR_MONTHLY,
                cornerRadiusEnd=3,
            ).encode(
                x=alt.X("Total_Attributed_Revenue:Q", title="Attributed Revenue"),
                y=alt.Y("Discount Code:N", sort=None, title=None),
                tooltip=[
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", format="$,.2f", title="Revenue"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns"),
                    alt.Tooltip("Total_Discounted_Orders:Q", title="Orders"),
                ],
            ).properties(height=max(len(top3) * 50, 120))

            st.altair_chart(styled_chart(top3_chart), width="stretch")

        spacer("lg")

        # ── Revenue by Campaign Breakdown ────────────────────────────────
        section_title(
            "Revenue by Campaign",
            "Top 10 discount codes · each segment is one campaign (hover for its name)",
        )

        contrib = all_month[["Discount Code", "Campaign Name", "Attributed Revenue"]].copy()
        contrib = contrib.sort_values("Attributed Revenue", ascending=False)

        _code_tot = contrib.groupby("Discount Code")["Attributed Revenue"].sum() \
            .sort_values(ascending=False).rename("Attributed Revenue").reset_index()
        _show_all_codes = show_all_toggle(len(_code_tot), "monthly_codes_show_all", noun="codes")
        _code_top, _code_rest = split_top_n(_code_tot, "Attributed Revenue", show_all=_show_all_codes)
        code_order = _code_top["Discount Code"].tolist()
        contrib = contrib[contrib["Discount Code"].isin(code_order)]

        # One color, white gaps between campaigns: no legend that grows
        # with the number of campaigns.
        stacked = alt.Chart(contrib).mark_bar(
            color=CLR_MONTHLY, stroke="white", strokeWidth=1.5,
        ).encode(
            y=alt.Y("Discount Code:N", sort=code_order, title=None),
            x=alt.X("Attributed Revenue:Q", title="Attributed Revenue", stack="zero"),
            tooltip=[
                alt.Tooltip("Discount Code:N"),
                alt.Tooltip("Campaign Name:N"),
                alt.Tooltip("Attributed Revenue:Q", format="$,.2f"),
            ],
        ).properties(height=max(len(code_order) * 34, 150))

        st.altair_chart(styled_chart(stacked), width="stretch")
        rest_caption(_code_rest, "Attributed Revenue", _code_tot["Attributed Revenue"].sum(), "codes")

        spacer("lg")

        # ── Monthly Insights ─────────────────────────────────────────────
        section_title("Monthly Insights")
        monthly_insights_md = _generate_monthly_insights(monthly_df, all_month)
        render_insight_card(monthly_insights_md)

        spacer("lg")

        # ── Supporting Detail Table ──────────────────────────────────────
        section_title("Discount Code Summary")
        if not monthly_df.empty:
            st.dataframe(
                monthly_df,
                column_config={
                    "Total_Attributed_Revenue": st.column_config.NumberColumn(
                        "Attr. Revenue", format="$%.2f"
                    ),
                    "Total_Sales": st.column_config.NumberColumn(
                        "Total Sales", format="$%.2f"
                    ),
                    "Total_Discount_Value": st.column_config.NumberColumn(
                        "Discount Value", format="$%.2f"
                    ),
                    "Avg Revenue per Campaign": st.column_config.NumberColumn(
                        "Avg Rev/Campaign", format="$%.2f"
                    ),
                    "Avg Revenue per Delivered": st.column_config.NumberColumn(
                        "Avg Rev/Delivered", format="$%.4f"
                    ),
                },
                width="stretch",
                hide_index=True,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Tab: Offer A vs Offer B (2-1-1 cadence) — uses the sidebar date range
# ═══════════════════════════════════════════════════════════════════════════════

with tab_ab:

    if wk_start is None:
        st.info("Pick a start and an end date under Date Range in the sidebar.")
    else:
        st.markdown(f'<div class="context-line">{_range_label}</div>', unsafe_allow_html=True)
        _ab = completed_week_df.join(_slots, how="inner") if not _slots.empty else pd.DataFrame()
        _a = _ab[_ab["Offer Slot"] == "A"] if not _ab.empty else _ab
        _b = _ab[_ab["Offer Slot"] == "B"] if not _ab.empty else _ab
        _single = _ab[_ab["Offer Slot"] == "Single"] if not _ab.empty else _ab

        if _a.empty or _b.empty:
            st.info(
                "No two-offer days in this date range. Offer A vs B compares the main offer (A) and "
                "the second offer (B) on days that sent two offers. Widen the date range in the sidebar."
            )
            st.caption(_offer_log_note)
        else:
            _sa, _sb, _ss = slot_summary(_a), slot_summary(_b), slot_summary(_single)
            _two_days = _ab[_ab["Offer Slot"].isin(["A", "B"])]
            _n_days = _two_days["Parsed Send Date"].nunique()
            _n_log = _two_days[_two_days["Slot Source"] == "Offer Log"]["Parsed Send Date"].nunique()
            _weeks_b = _b["Parsed Send Date"].apply(_week_monday).nunique()
            _b_week = _sb["revenue"] / _weeks_b if _weeks_b else 0
            _gap = (_sb["per_1000"] / _sa["per_1000"] - 1) if _sa["per_1000"] else None

            render_kpi_row([
                {"label": "A (Main) per 1,000 Delivered", "value": f"${_sa['per_1000'] or 0:,.0f}",
                 "sub": f"{_sa['offers']} offers · avg {fmt_money(_sa['avg'])} each"},
                {"label": "B (Second) per 1,000 Delivered", "value": f"${_sb['per_1000'] or 0:,.0f}",
                 "sub": (f"{_gap:+.0%} vs A · {_sb['offers']} offers" if _gap is not None
                         else f"{_sb['offers']} offers")},
                {"label": "B Revenue per Week", "value": fmt_money(_b_week), "exact": exact_money(_b_week),
                 "sub": f"suggested target {fmt_money(B_WEEKLY_TARGET)} · {_weeks_b} weeks"},
                {"label": "B Offers With $0", "value": f"{_sb['zero_share']:.0%}",
                 "sub": f"suggested target ≤ {B_ZERO_TARGET:.0%} · A: {_sa['zero_share']:.0%}"},
            ])
            st.caption(
                f"{_n_days} two-offer days in this range: {_n_log} labelled A/B in the Offer Log, "
                f"{_n_days - _n_log} estimated (the bigger send that day = A). {_offer_log_note} "
                f"Targets are the ones suggested in the November 12 cadence review."
            )

            spacer("md")
            section_title("Side by Side", "Main offer, second offer, and days with a single offer")

            def _fmt_row(s):
                return [
                    f"{s['offers']:,}", fmt_money(s["revenue"]),
                    fmt_money(s["avg"]) if s["avg"] is not None else "—",
                    f"${s['per_1000']:,.0f}" if s["per_1000"] is not None else "—",
                    f"{s['zero_share']:.0%}" if s["zero_share"] is not None else "—",
                    f"{s['audience']:,.0f}" if s["audience"] is not None else "—",
                ]

            st.dataframe(
                pd.DataFrame({
                    "Measure": ["Offers sent", "Attributed revenue", "Average revenue per offer",
                                "Revenue per 1,000 delivered", "Offers with $0", "Average audience (delivered)"],
                    "Offer A (main)": _fmt_row(_sa),
                    "Offer B (second)": _fmt_row(_sb),
                    "Single-offer days": _fmt_row(_ss),
                }),
                width="stretch",
                hide_index=True,
            )

            spacer("md")
            section_title("Week by Week", "Revenue from main and second offers on two-offer days")
            _wk = _two_days.copy()
            _wk["Week"] = _wk["Parsed Send Date"].apply(_week_monday)
            _wk_sum = _wk.groupby(["Week", "Offer Slot"])["Attributed Revenue"].sum().reset_index()
            _wk_sum["Week Label"] = _wk_sum["Week"].apply(week_label)
            _wk_sum["Offer"] = _wk_sum["Offer Slot"].map({"A": "A (main)", "B": "B (second)"})
            _order = [week_label(w) for w in sorted(_wk_sum["Week"].unique())]
            _bars = alt.Chart(_wk_sum).mark_bar(cornerRadiusEnd=2).encode(
                x=alt.X("Week Label:N", sort=_order, title=None, axis=alt.Axis(labelAngle=-40)),
                xOffset=alt.XOffset("Offer:N"),
                y=alt.Y("Attributed Revenue:Q", title="Attributed Revenue", axis=alt.Axis(format="$,.0f")),
                color=alt.Color("Offer:N", scale=alt.Scale(domain=["A (main)", "B (second)"],
                                                           range=[CLR_WEEKLY, CLR_TEAL]),
                                legend=alt.Legend(title=None, orient="top")),
                tooltip=[alt.Tooltip("Week Label:N", title="Week"), alt.Tooltip("Offer:N"),
                         alt.Tooltip("Attributed Revenue:Q", format="$,.2f")],
            )
            _rule = alt.Chart(pd.DataFrame({"y": [B_WEEKLY_TARGET]})).mark_rule(
                strokeDash=[5, 4], color=CLR_TEAL,
            ).encode(y="y:Q")
            st.altair_chart(styled_chart((_bars + _rule).properties(height=320)), width="stretch")
            st.caption(f"Dashed line = suggested B target of {fmt_money(B_WEEKLY_TARGET)} a week.")

            spacer("md")
            section_title("Day by Day", "Each two-offer day with its main and second offer")
            _days = []
            for _d, _g in _two_days.groupby("Parsed Send Date"):
                _ga, _gb = _g[_g["Offer Slot"] == "A"], _g[_g["Offer Slot"] == "B"]
                _days.append({
                    "Date": _d,
                    "Offer A": " | ".join(_ga["Producer / Topic"]),
                    "A Revenue": _ga["Attributed Revenue"].sum(),
                    "A Delivered": _ga["Delivered"].sum(),
                    "Offer B": " | ".join(_gb["Producer / Topic"]),
                    "B Revenue": _gb["Attributed Revenue"].sum(),
                    "B Delivered": _gb["Delivered"].sum(),
                    "A/B From": "Offer Log" if (_g["Slot Source"] == "Offer Log").all() else "Estimated",
                })
            st.dataframe(
                pd.DataFrame(_days).sort_values("Date", ascending=False),
                column_config={
                    "Date": st.column_config.DateColumn("Date", width="small", pinned=True),
                    "A Revenue": st.column_config.NumberColumn("A Revenue", format="$%.2f"),
                    "B Revenue": st.column_config.NumberColumn("B Revenue", format="$%.2f"),
                    "A Delivered": st.column_config.NumberColumn("A Delivered", format="%d"),
                    "B Delivered": st.column_config.NumberColumn("B Delivered", format="%d"),
                },
                height=420,
                width="stretch",
                hide_index=True,
            )
            st.caption(
                "**How to read this:** on two-offer days (Monday to Thursday under 2-1-1) the main offer "
                "(A) goes to the bigger list and the second offer (B) to a smaller segment, so compare "
                "**revenue per 1,000 delivered** rather than totals. Events (BIN Sale, Large Format, "
                "Holiday) are left out. A/B comes from the Offer Log's Rank column (“A” = main); "
                "days without a single “A” use the bigger send as A and are marked Estimated."
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 3: Producer Performance
# ═══════════════════════════════════════════════════════════════════════════════

with tab_producer:

    st.markdown(
        '<div class="context-line">Producer analytics &middot; Finalized campaigns only (attribution window closed) &middot; Excludes BIN Sale, Holiday, Large Format, Category, and Automation offers</div>',
        unsafe_allow_html=True,
    )

    _pa = data["producer_analytics"]
    _pa_detail = _pa.get("producer_detail_df", pd.DataFrame())
    _pa_overview = _pa.get("overview_df", pd.DataFrame())
    _pa_campaigns = _pa.get("campaign_detail_df", pd.DataFrame())
    _pa_unmapped = _pa.get("unmapped_df", pd.DataFrame())
    _pa_tier = _pa.get("tier_detail_df", pd.DataFrame())
    _pa_region = _pa.get("region_detail_df", pd.DataFrame())

    if _pa_overview.empty:
        st.info("No finalized producer data available yet.")
    else:
        # ── KPI Row ──────────────────────────────────────────────────────
        _pa_total_rev = _pa_detail["Total_Attributed_Revenue"].sum() if not _pa_detail.empty else 0
        _pa_total_orders = _pa_detail["Total_Discounted_Orders"].sum() if not _pa_detail.empty else 0
        _pa_n_producers = _pa_detail["Producer"].nunique() if not _pa_detail.empty else 0
        _pa_n_campaigns = _pa_detail["Campaign_Count"].sum() if not _pa_detail.empty else 0
        render_kpi_row([
            {"label": "Attributed Revenue", "value": fmt_money(_pa_total_rev), "exact": exact_money(_pa_total_rev),
             "sub": "Standalone & combo offers"},
            {"label": "Producers", "value": fmt_count(_pa_n_producers)},
            {"label": "Campaigns", "value": fmt_count(_pa_n_campaigns), "exact": exact_count(_pa_n_campaigns)},
            {"label": "Discounted Orders", "value": fmt_count(_pa_total_orders), "exact": exact_count(_pa_total_orders)},
        ])

        spacer("lg")

        # ── E.1: Top producers (ranked bars; replaces the all-producer pie) ──
        section_title(
            "Top Producers",
            "Attributed revenue by producer, top 10 (finalized, standalone & combo offers)",
        )

        _prod_data = _pa_overview[_pa_overview["Total_Attributed_Revenue"] > 0][
            ["Producer", "Total_Attributed_Revenue", "Campaign_Count", "Total_Discounted_Orders"]
        ].copy()

        if not _prod_data.empty:
            _show_all_prod = show_all_toggle(len(_prod_data), "producer_bar_show_all", noun="producers")
            _prod_total = _prod_data["Total_Attributed_Revenue"].sum()
            _prod_data["Share"] = _prod_data["Total_Attributed_Revenue"] / _prod_total if _prod_total else 0.0
            _prod_data, _prod_rest = split_top_n(_prod_data, "Total_Attributed_Revenue", show_all=_show_all_prod)
            _prod_bar = alt.Chart(_prod_data).mark_bar(cornerRadiusEnd=3, color=CLR_PRODUCER).encode(
                y=alt.Y("Producer:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
                x=alt.X("Total_Attributed_Revenue:Q", title="Attributed Revenue ($)"),
                tooltip=[
                    alt.Tooltip("Producer:N"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", title="Attributed Revenue", format="$,.2f"),
                    alt.Tooltip("Share:Q", title="Share of revenue", format=".1%"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns", format=",.0f"),
                    alt.Tooltip("Total_Discounted_Orders:Q", title="Orders", format=",.0f"),
                ],
            ).properties(height=max(len(_prod_data) * 30, 160))
            st.altair_chart(styled_chart(_prod_bar), width="stretch")
            rest_caption(_prod_rest, "Total_Attributed_Revenue", _prod_total, "producers")

        spacer("lg")

        # ── E.2: Producer Drill-Down — products, then campaigns ───────────
        section_title(
            "Producer Drill-Down",
            "Pick a producer to see which wines earned its attributed revenue, then its campaigns",
        )

        if not _pa_overview.empty:
            _all_producers = sorted(_pa_overview["Producer"].dropna().unique().tolist())
            _selected_producer = st.selectbox(
                "Producer",
                options=["— Select a producer —"] + _all_producers,
                key="producer_drilldown_select",
                label_visibility="collapsed",
            )

            if _selected_producer != "— Select a producer —":
                render_producer_products(_selected_producer, data, run_date)

            if _selected_producer != "— Select a producer —" and not _pa_campaigns.empty:
                _drill = _pa_campaigns[_pa_campaigns["Producer"] == _selected_producer].copy()
                spacer("md")
                section_title("Campaigns", f"Finalized campaigns for {_selected_producer}")
                if _drill.empty:
                    st.info(f"No finalized campaigns found for {_selected_producer}.")
                else:
                    _drill_cols = [c for c in [
                        "Campaign Name", "Parsed Send Date", "Discount Code",
                        "Attributed Revenue", "Discount Value", "Discounted Orders",
                        "Delivered", "Revenue per Delivered", "Offer Type",
                    ] if c in _drill.columns]
                    st.dataframe(
                        _drill[_drill_cols],
                        column_config={
                            "Attributed Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                            "Discount Value": st.column_config.NumberColumn("Discount Value", format="$%.2f"),
                            "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                        },
                        width="stretch",
                        hide_index=True,
                    )

        spacer("lg")

        # ── E.3: Region Performance ───────────────────────────────────────
        section_title("Region Performance", "Attributed revenue by top region and sub-region")

        if not _pa_region.empty:
            _show_all_reg = show_all_toggle(len(_pa_region), "region_bar_show_all", noun="sub-regions")
            _region_chart_df, _region_rest = split_top_n(
                _pa_region, "Total_Attributed_Revenue", show_all=_show_all_reg,
            )
            _region_bar = alt.Chart(_region_chart_df).mark_bar(cornerRadiusEnd=3).encode(
                y=alt.Y("Sub-Region:N", sort="-x", title="Sub-Region"),
                x=alt.X("Total_Attributed_Revenue:Q", title="Attributed Revenue ($)"),
                color=alt.Color("Top Region:N", scale=alt.Scale(scheme="reds")),
                tooltip=[
                    alt.Tooltip("Top Region:N"),
                    alt.Tooltip("Sub-Region:N"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", title="Attr. Revenue", format="$,.2f"),
                    alt.Tooltip("Total_Discounted_Orders:Q", title="Orders", format=",.0f"),
                    alt.Tooltip("Producer_Count:Q", title="Producers", format=",.0f"),
                ],
            ).properties(height=max(len(_region_chart_df) * 30, 160))
            st.altair_chart(styled_chart(_region_bar), width="stretch")
            rest_caption(_region_rest, "Total_Attributed_Revenue",
                         _pa_region["Total_Attributed_Revenue"].sum(), "sub-regions")

            st.dataframe(
                _pa_region,
                column_config={
                    "Total_Attributed_Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                    "Total_Discount_Value": st.column_config.NumberColumn("Discount Value", format="$%.2f"),
                    "Total_Discounted_Orders": st.column_config.NumberColumn("Orders", format="%.0f"),
                    "Total_Delivered": st.column_config.NumberColumn("Delivered", format="%.0f"),
                    "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                    "Producer_Count": st.column_config.NumberColumn("Producers", format="%.0f"),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No region data available.")

        spacer("lg")

        # ── E.6: Tier Performance ─────────────────────────────────────────
        section_title("Tier Performance", "Attributed revenue by producer tier")

        if not _pa_tier.empty:
            _tier_bar = alt.Chart(_pa_tier).mark_bar(color=CLR_PRODUCER, cornerRadiusEnd=3).encode(
                y=alt.Y("Tier:N", sort="-x", title="Tier"),
                x=alt.X("Total_Attributed_Revenue:Q", title="Attributed Revenue ($)"),
                tooltip=[
                    alt.Tooltip("Tier:N"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", title="Attr. Revenue", format="$,.2f"),
                    alt.Tooltip("Total_Discounted_Orders:Q", title="Orders", format=",.0f"),
                    alt.Tooltip("Producer_Count:Q", title="Producers", format=",.0f"),
                ],
            ).properties(height=max(len(_pa_tier) * 50, 150))
            st.altair_chart(styled_chart(_tier_bar), width="stretch")

            st.dataframe(
                _pa_tier,
                column_config={
                    "Total_Attributed_Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                    "Total_Discount_Value": st.column_config.NumberColumn("Discount Value", format="$%.2f"),
                    "Total_Discounted_Orders": st.column_config.NumberColumn("Orders", format="%.0f"),
                    "Total_Delivered": st.column_config.NumberColumn("Delivered", format="%.0f"),
                    "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                    "Producer_Count": st.column_config.NumberColumn("Producers", format="%.0f"),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No tier data available.")

        spacer("lg")

        # ── Unmapped QA Expander ──────────────────────────────────────────
        _n_unmapped = len(_pa_unmapped) if not _pa_unmapped.empty else 0
        with st.expander(f"Unmapped Campaigns — review in QA ({_n_unmapped})", expanded=_n_unmapped > 0):
            if not _pa_unmapped.empty:
                st.markdown(
                    f'<p style="font-size:0.85rem; color:{CLR_TEXT_SECONDARY};">These finalized campaigns have a discount code that does not resolve to any producer in the mapping Excel. Add the code to <strong>Producer - Discount Code Mapping - Copy.xlsx</strong> and refresh.</p>',
                    unsafe_allow_html=True,
                )
                st.dataframe(
                    _pa_unmapped,
                    column_config={
                        "Attributed Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                        "Delivered": st.column_config.NumberColumn("Delivered", format="%.0f"),
                    },
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.markdown(
                    f'<p style="color:{CLR_TEXT_MUTED}; font-size:0.85rem;">All finalized campaigns are mapped. No review needed.</p>',
                    unsafe_allow_html=True,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 4: Offer Section — "Current Offers" email section impact
# ═══════════════════════════════════════════════════════════════════════════════

with tab_section:
    section_title(
        "Current Offers Section — Impact",
        f"Evidence-based journey attribution · live since {SECTION_LAUNCH_DATE} · "
        "separate from Attributed Revenue, never blended",
    )

    st.warning(
        "**Read this before the numbers — what this CAN and CANNOT tell you**\n\n"
        "- **Evidence, not proof.** Each order is classified by the buyer's observable "
        "HubSpot click/delivery trail. Bucket A (section click, never received the "
        "original offer email) is very strong evidence; Bucket B (clicked section but "
        "also received the original email) is directional only — we cannot know which "
        "touch caused the purchase.\n"
        "- **Bucket D is permanently unattributable.** Buyers who only opened emails, "
        "typed a code by hand, checked out under a different email address, or switched "
        "devices leave no click trail. No method except separate section-only discount "
        "codes could ever claim these.\n"
        "- **Same-session tracking chronically understates** — ~40% of redemptions land "
        "1–8 days after a send — which is why this report joins clicks to later orders "
        "per person instead of relying on session UTMs.\n"
        f"- **Lookback is {JOURNEY_LOOKBACK_DAYS} days** before each order; clicks are "
        "matched per code with a last-click rule.\n"
        "- **Gmail clipping dilutes exposure**: emails over ~102KB hide the section (it "
        "sits at the bottom) for some recipients until the template slim-down ships.\n"
        f"- **Clean data starts {SECTION_LAUNCH_DATE}** (first send carrying the "
        "section). Pre-launch orders cannot be classified.\n"
        "- **Incrementality is out of scope**: whether these buyers would have purchased "
        "anyway can only be answered with a holdout test (randomly omitting the section "
        "from some sends)."
    )

    if not st.session_state.get("section_impact_requested"):
        st.info(
            "Journey data is loaded on demand so the main dashboard stays fast. "
            "First load fetches each buyer's HubSpot activity (~1–3 min); results "
            "are then cached for 6 hours for all viewers, and each order is only "
            "ever classified once."
        )
        if st.button("Load section impact data", type="primary", key="load_section_impact_btn"):
            st.session_state["section_impact_requested"] = True
            st.rerun()
    else:
        with st.spinner("Reconstructing buyer journeys from HubSpot + Shopify..."):
            _si = load_section_impact()
        _orders = _si["orders"]
        _clicks = _si["clicks"]

        _sec_touched = _orders[_orders["bucket"].isin(["A", "B"])] if not _orders.empty else pd.DataFrame()
        _d_rev = _orders[_orders["bucket"] == "D"]["total_price"].sum() if not _orders.empty else 0
        _tot_rev = _orders["total_price"].sum() if not _orders.empty else 0
        render_kpi_row([
            {"label": "Section Clicks", "value": f"{len(_clicks):,}",
             "sub": f"{_clicks['recipient'].nunique() if not _clicks.empty else 0} unique clickers"},
            {"label": "Section-Touched Orders", "value": str(len(_sec_touched)),
             "sub": "Buckets A + B"},
            {"label": "Section-Touched Revenue",
             "value": fmt_money(_sec_touched['total_price'].sum() if not _sec_touched.empty else 0),
             "exact": exact_money(_sec_touched['total_price'].sum() if not _sec_touched.empty else 0),
             "sub": "Evidence-based, not causal"},
            {"label": "Unattributable Share", "value": f"{(100 * _d_rev / _tot_rev) if _tot_rev else 0:.0f}%",
             "sub": "Bucket D revenue share"},
        ])
        st.caption(
            f"Data loaded {_si['loaded_at'].strftime('%Y-%m-%d %H:%M')} · "
            f"{len(_orders)} code-redeeming orders classified since {SECTION_LAUNCH_DATE}."
        )

        spacer("md")
        section_title("Evidence buckets", "Every code-redeeming order since launch, classified once")
        st.dataframe(
            bucket_summary(_orders),
            column_config={
                "Revenue": st.column_config.NumberColumn(format="$%.2f"),
            },
            width="stretch",
            hide_index=True,
        )

        spacer("md")
        section_title("Section clicks by offer code", "Which offers the section is driving interest toward")
        if not _clicks.empty:
            _by_code = _clicks.groupby("code").size().reset_index(name="Clicks")
            _show_all_sc = show_all_toggle(len(_by_code), "section_clicks_show_all", noun="codes")
            _sc_total = _by_code["Clicks"].sum()
            _by_code, _sc_rest = split_top_n(_by_code, "Clicks", show_all=_show_all_sc)
            _chart = alt.Chart(_by_code).mark_bar(color=CLR_PRODUCER).encode(
                y=alt.Y("code:N", sort="-x", title=None),
                x=alt.X("Clicks:Q"),
                tooltip=["code:N", "Clicks:Q"],
            ).properties(height=max(len(_by_code) * 30, 160))
            st.altair_chart(styled_chart(_chart), width="stretch")
            rest_caption(_sc_rest, "Clicks", _sc_total, "codes", unit="clicks")
        else:
            st.info("No section clicks recorded yet.")

        spacer("md")
        section_title("Order drill-down", "Per-order journey evidence")
        if not _orders.empty:
            _disp = _orders.copy()
            _disp["Bucket"] = _disp["bucket"].map(BUCKET_LABELS)
            _disp["Order Date"] = pd.to_datetime(
                _disp["order_created_at"], utc=True, format="ISO8601"
            ).dt.strftime("%Y-%m-%d")

            def _fmt_evidence(js):
                try:
                    parts = []
                    for pc in json.loads(js):
                        bits = []
                        if pc.get("section_click"):
                            lag = pc.get("section_click_lag_hours")
                            bits.append(f"section click{f' ({lag:.0f}h before order)' if lag is not None else ''}")
                        if pc.get("original_click"):
                            bits.append("original-email click")
                        if pc.get("received_original"):
                            bits.append("received original email")
                        parts.append(f"{pc['code']}: " + (", ".join(bits) or "no trail"))
                    return " | ".join(parts)
                except Exception:
                    return ""

            _disp["Evidence"] = _disp["evidence_json"].apply(_fmt_evidence)
            st.dataframe(
                _disp[["Order Date", "order_name", "customer_masked", "codes",
                       "Bucket", "total_price", "Evidence"]],
                column_config={
                    "order_name": st.column_config.TextColumn("Order", width="small"),
                    "customer_masked": st.column_config.TextColumn("Customer", width="small"),
                    "codes": st.column_config.TextColumn("Code(s)", width="small"),
                    "total_price": st.column_config.NumberColumn("Order Total", format="$%.2f"),
                    "Evidence": st.column_config.TextColumn("Journey evidence", width="large"),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No code-redeeming orders since launch yet.")

        if st.button("Refresh journey data", key="refresh_section_impact"):
            load_section_impact.clear()
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 5: QA — Data Quality & Diagnostics
# ═══════════════════════════════════════════════════════════════════════════════

with tab_qa:

    # ── QA Health Summary ────────────────────────────────────────────────
    st.markdown(
        f'<div class="context-line">Data quality checks and diagnostic information</div>',
        unsafe_allow_html=True,
    )

    n_excluded = len(excl_df) if not excl_df.empty else 0
    n_unmatched = len(unmatched_df) if not unmatched_df.empty else 0
    qa_status = "Clean" if (n_excluded == 0 and n_unmatched == 0) else "Needs Review"

    render_kpi_row([
        {"label": "Excluded Campaigns", "value": str(n_excluded)},
        {"label": "Unmatched Codes", "value": str(n_unmatched)},
        {"label": "QA Status", "value": qa_status},
    ])

    spacer("lg")

    # ── Excluded Campaigns ───────────────────────────────────────────────
    with st.expander(f"Excluded Campaigns ({n_excluded})", expanded=False):
        if not excl_df.empty:
            st.dataframe(
                excl_df,
                width="stretch",
                hide_index=True,
            )
        else:
            st.markdown(
                f'<p style="color:{CLR_TEXT_MUTED}; font-size:0.85rem;">No excluded campaigns.</p>',
                unsafe_allow_html=True,
            )

    # ── Unmatched Discount Codes ─────────────────────────────────────────
    with st.expander(f"Unmatched Shopify Discount Codes ({n_unmatched})", expanded=False):
        if not unmatched_df.empty:
            reasons = ["All"] + sorted(unmatched_df["Possible Reason"].unique().tolist())
            selected = st.selectbox("Filter by reason", reasons, key="qa_reason_filter")

            display_um = unmatched_df
            if selected != "All":
                display_um = unmatched_df[unmatched_df["Possible Reason"] == selected]

            st.caption(f"{len(display_um)} unmatched code(s)")
            st.dataframe(
                display_um,
                column_config={
                    "Total Discount Amount": st.column_config.NumberColumn(format="$%.2f"),
                    "Total Order Value": st.column_config.NumberColumn(format="$%.2f"),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.markdown(
                f'<p style="color:{CLR_TEXT_MUTED}; font-size:0.85rem;">No unmatched discount codes.</p>',
                unsafe_allow_html=True,
            )

    # ── QA Summary ───────────────────────────────────────────────────────
    with st.expander("Full QA Summary", expanded=False):
        st.code(data["qa_summary"], language=None)


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 5: Methodology — how every metric is computed
# ═══════════════════════════════════════════════════════════════════════════════

with tab_method:
    section_title(
        "Methodology",
        "How campaigns are matched to Shopify orders and what each metric means",
    )

    st.markdown("""
#### Data sources

| Source | What it provides |
|---|---|
| **HubSpot** | Campaign name, send date, and delivery stats (Delivered, Opened, Clicked). A campaign counts as "sent" only if HubSpot reports at least 1 delivery. |
| **Shopify** | Orders — including line items, discount codes applied, and the landing page URL that started the buyer's session. |
| **Mapping spreadsheets** | Producer, offer type, tier, and region for each discount code (maintained manually in Excel). |

#### Campaign naming convention

Every campaign name follows: `YYYY-MM-DD - Producer/Topic - Type - OfferValue - Code`

- **Type** is one of `PROD` (sellable offer), `EDU` (educational), `CONTENT`, or `HYBR` (hybrid — EDU-style content to part of the audience, offer framing to the rest).
- Campaigns **with a real discount code** are matched to Shopify orders; campaigns without one show delivery stats only.
- A/B test versions and EDU/HYBR sibling sends of the same campaign (same code, same send date) are automatically **merged into one row** — delivery stats are summed, and the shared Shopify attribution is counted once.
- **Events** (BIN Sale, large format and holiday campaigns) appear as **one row per event**, such as "BIN Sale (3 emails)". Emails with the same code join an event when each is sent within 7 days of the previous one. Each order counts once, including orders placed between emails. Delivered adds up all the emails, so someone who received three emails counts three times, and Rev/Delivered will look lower than a single offer.

#### Attribution window

Orders are matched to a campaign if they were created within **7 days** of the send date (**3 days** for BIN Sale / holiday / flash campaigns). A campaign is **Finalized** once its window has closed — finalized numbers never change afterward.
""")

    spacer("md")
    section_title("Primary attribution — discount code", "The buyer used the campaign's code at checkout")

    st.markdown("""
An order belongs to a campaign when the campaign's **discount code appears on the order**, either typed at checkout or as an automatic discount with the same name. Within a matched order, only the line items that the code was applied to count toward Attributed Revenue.

| Metric | Definition |
|---|---|
| **Attributed Revenue** | For each discounted line item: `price × quantity − discount amount`. Net revenue of the wines the code was applied to. |
| **Discount Value** | Total dollar discount given on those line items. |
| **Total Sales** | Full order value (`total_price`) of every matched order — includes non-discounted items in the same cart, tax, and shipping. |
| **Discounted Orders** | Number of orders that used the code. |
| **Revenue per Delivered** | Attributed Revenue ÷ Delivered. The efficiency yardstick across campaigns. |
| **Sales per Delivered** | Total Sales ÷ Delivered. |

**BIN Sale campaigns** apply automatic discounts (e.g. `BinSale10` + `BinSale12`) instead of typed codes, so they're matched by discount *title* across the whole family and de-duplicated so no order is counted twice.

**Automatic discounts count email recipients only (added 2026-11-17).** An automatic discount applies itself to anyone who visits the page, including shoppers who never got the email. So an order that used an **automatic** discount counts toward the campaign only if the buyer's checkout email was **sent that offer's email before ordering** (any email of the same offer, for BIN Sale / Large Format events with reminders). Orders with a typed code always count. **All-Buyer Orders / All-Buyer Revenue** show the offer's results across every buyer, for context; they are not credited to the email.

**Single attribution:** each order is credited to exactly **one** campaign.
""")

    spacer("md")
    section_title("Secondary attribution — UTM- and click-influenced", "The email drove the visit, but the code wasn't used")

    st.markdown("""
Some buyers click a campaign email, land on the store, and complete a purchase **without using the campaign code** — for example, checking out with the generic `GrandCru` loyalty code instead. Code-based attribution alone misses these orders.

Shopify records the **first page of each buyer's session** (`landing_site`), and HubSpot email links carry two fingerprints in that URL:

1. The **`/discount/<Code>` path** — email links route through the campaign's own discount URL
2. The **`utm_campaign` parameter** — contains the full campaign name

An order counts as **Influenced** when, inside the campaign's attribution window:

- its landing URL matches the campaign by either fingerprint, **and**
- it was **not** already attributed to any campaign by code (single attribution is preserved — an order that used another campaign's code is never double-counted).

| Metric | Definition |
|---|---|
| **Influenced Orders** | Orders matching the rules above. |
| **Influenced Offer Revenue** | Revenue from **the offered wine's line items only** on those orders (net of any discounts applied). E.g. an influenced order containing \\$473 of the offered wine plus \\$1,500 of other wine contributes \\$473 here. |
| **Influenced Total Sales** | Full order value of influenced orders — the analog of Total Sales. |

**These metrics are reported separately and never blended into Attributed Revenue.** Code usage is proof of conversion; a UTM match is strong evidence. Keeping them apart preserves comparability with all historical reporting.

**Known limitations:**
- `landing_site` only captures the **first session**. A buyer who clicks the email on their phone but orders later from a laptop won't be captured — Influenced numbers are a *floor*, not a ceiling.
- Offer-wine line matching compares the campaign's Producer/Topic text against Shopify product titles; unusual naming mismatches can miss a line (the order still counts in Influenced Total Sales). Run-together names match hyphenated titles (`PontetCanet` = `Pontet-Canet`) and accents are ignored.

**Click-influenced (added 2026-11-03)** closes the first-session gap with HubSpot's own click records. An order counts as **Click-Influenced** when, inside the campaign's attribution window:

- the buyer's checkout email **clicked this campaign's email before ordering**, **and**
- the order contains **the offered wine**, **and**
- it used no campaign code and was not already counted as UTM-influenced.

| Metric | Definition |
|---|---|
| **Click-Influenced Orders** | Orders matching the rules above. If an order qualifies for several campaigns, it goes to the email clicked most recently before the order. |
| **Click-Influenced Offer Revenue** | Revenue from the offered wine's line items on those orders (net of any discounts). |

These are **additive** to the UTM-influenced columns (no order is in both) and are never blended into Attributed Revenue. Buyers who check out with a different email address than the one that received the campaign, or who only opened (never clicked), are still not captured.
""")

    spacer("md")
    section_title("Offer Section impact — journey attribution", "The Current Offers email section (live since 2026-09-11)")

    st.markdown("""
Every daily offer email carries a **Current Offers** section listing all open offers. Its links
are the only ones tagged `utm_term=offer_recs`, and each offer's featured email is excluded from
its own section — so a section click is by definition a cross-offer click.

Because the same discount code works from both the original email and the section, a redemption
alone cannot reveal its source. Instead, each code-redeeming order since launch is classified by
the buyer's **HubSpot click and delivery trail** (30-day lookback):

| Bucket | Evidence | Strength |
|---|---|---|
| **A — Section only** | Clicked the section's link for that code; never *received* the code's original email | Very strong section attribution |
| **B — Section + original** | Clicked the section's link but also received the original email | Directional — either touch could have converted |
| **C — Original only** | Clicked or received the original email; no section click | Normal path |
| **D — No evidence** | No click trail (open-only, typed code, different device/email) | Unattributable — reported honestly, credited to neither side |

**These figures are evidence, not causal proof, and are never blended into Attributed Revenue.**
Known limitations: Bucket D is permanently blind to any click-based method; Gmail clipping hides
the section for some recipients (emails ~113–117KB vs the ~102KB clip limit) until the template
slim-down; and incrementality would require a holdout test.
""")

    spacer("md")
    section_title("Reading the numbers", "Null vs. zero, and other conventions")

    st.markdown("""
| Value | Meaning |
|---|---|
| **blank / null** | Attribution was **not attempted** — EDU or CONTENT campaign, or no discount code. |
| **0** | Attribution **was attempted** and no matching orders were found. |
| **> 0** | Normal attributed result. |

- Campaigns with fewer than **50 deliveries** are excluded from efficiency rankings.
- **Offers beating benchmark** (Weekly tab) compares each finalized offer's revenue per 1,000 emails delivered with a **fixed benchmark**: the typical (median) offer from **Jun 2025 – May 2026**, worked out separately for Fri/Sat sends and weekday sends because they earn very different amounts per email. Events and sends under 50 delivered are left out. Because the benchmark is the middle offer of that year, about half of offers beat it in a typical period. The benchmark does not change with the date picker; it is reset once a year.
- **Clicks** is HubSpot's "Clicked": people who clicked at least one link. HubSpot's click rate is Clicks ÷ Delivered.
- **New vs existing customers** splits Attributed Revenue. An order is from a **new customer** when it is the buyer's first-ever Shopify order (checked by Shopify customer and email address against every order since the store opened in March 2020, ignoring cancelled and test orders). Everything else is **existing**, so New + Existing = Attributed Revenue. People who bought only by phone, wholesale or on an older site count as new on their first Shopify order.
- **Automatic-discount offers** (Campaigns tab) shows, for offers set up as automatic discounts, what the email is credited with next to what the same discount sold to people who were **not sent** the email. The not-sent part is context only and is never added to email revenue.
- **Products** (Producers tab) lists the wines that carried each producer's campaign discount, valued the same way as Attributed Revenue, so they add up to it. **Featured in email** compares the wine's vintage with the vintage(s) in the campaign name.
- **Monthly tab weeks** run Monday to Sunday. A week belongs to the month that holds most of its days (4 or 5 whole weeks a month, never split), and the month's totals are those weeks added up. **Same week last year** is exactly 52 weeks earlier, so weekdays and holidays line up. Comparisons with last year use finished weeks only.
- **Offer A vs B**: on days with two offers, the main offer is **A** and the other **B**; days with one offer are **Single**. Which offer is A comes from the Offer Log's Rank column ("A"); days the log doesn't label use the bigger send as A and are marked **Estimated**. Compare **revenue per 1,000 delivered**, because B goes to smaller segments. The $5,000-a-week and 25%-at-$0 lines are suggested targets from the November 2026 cadence review.
- **Key Insights** (Weekly tab) are four short findings for the selected dates: revenue vs the same dates last year and the previous period, the best offer and the offers that made $0, Offer A vs B, and new customers.
- The sidebar **event-campaign filter** hides BIN Sale, Large Format and Holiday campaigns: any campaign whose code is a discount family (BIN Sale, GoBig, BigDaddy, July4th, LaborDay25…), whose name contains BIN Sale / holiday sale / flash sale / clearance, or whose code has Offer Type `holiday` or `format promotion` in the mapping sheet. Producer offers using the producer's own code are kept even if themed (e.g. a large-format Las Cases offer). This filter changes what is shown only — never how revenue is attributed.
- The **Producers tab** only includes finalized campaigns, resolves each discount code to a producer via the mapping spreadsheet, and excludes non-producer buckets (BIN Sale, Holiday, etc.) from producer rankings.
- History rows for finalized campaigns are **frozen** — later pipeline runs never overwrite them.
""")
