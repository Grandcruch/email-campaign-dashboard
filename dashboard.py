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

import streamlit as st
import pandas as pd
import altair as alt

# Pipeline code version — bump on every deploy-relevant change. Displayed in
# the sidebar so a stale-module deploy (Streamlit Cloud soft-reload keeping
# old src/ modules) is immediately visible instead of masquerading as bad data.
PIPELINE_VERSION = "2026-11-20.1"

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
    ("Clicked", "Clicks"),
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
        if st.button("Sign in", type="primary", use_container_width=True):
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
    if st.button("Refresh Data", type="primary", use_container_width=True):
        run_pipeline.clear()  # clear the cross-session cache, not just this session
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
        Date Range &middot; Weekly &amp; Campaigns tabs
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

# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_weekly, tab_campaigns, tab_monthly, tab_producer, tab_section, tab_qa, tab_method = st.tabs([
    "Weekly",
    "Campaigns",
    "Monthly",
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
                 "sub": "Net sales, discounted items"},
                {"label": "Total Sales", "value": fmt_money(_sales), "exact": exact_money(_sales),
                 "sub": "Full matched orders"},
                {"label": "Orders", "value": fmt_count(_orders), "exact": exact_count(_orders),
                 "sub": "Used the campaign discount"},
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

            st.altair_chart(styled_chart(bar_chart), use_container_width=True)
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

        _c_info, _c_dl = st.columns([3, 1])
        with _c_info:
            section_title(
                "Campaign Results",
                f"{len(details_df):,} campaigns · newest first · click a column header to sort",
            )
        with _c_dl:
            st.download_button(
                "Download CSV (all columns)",
                data=build_campaign_csv(details_df),
                file_name=f"campaign_details_{wk_start.isoformat()}_to_{wk_end.isoformat()}.csv",
                mime="text/csv",
                key="campaign_csv_download",
                use_container_width=True,
            )

        table_cols = [
            "Parsed Send Date", "Producer", "Campaign Name", "Discount Code",
            "Delivered", "Clicked", "Discounted Orders", "Attributed Revenue",
            "Total Sales", "Revenue per Delivered", "Sales per Delivered",
        ]
        available = [c for c in table_cols if c in details_df.columns]
        st.dataframe(
            details_df[available],
            column_config={
                "Parsed Send Date": st.column_config.DateColumn("Send Date", width="small"),
                "Producer": st.column_config.TextColumn("Producer", width="medium"),
                "Campaign Name": st.column_config.TextColumn("Campaign", width="large"),
                "Discount Code": st.column_config.TextColumn("Code", width="small"),
                "Delivered": st.column_config.NumberColumn("Delivered", format="%d"),
                "Clicked": st.column_config.NumberColumn(
                    "Clicks", format="%d",
                    help="People who clicked at least one link in the email (HubSpot's 'Clicked')"),
                "Discounted Orders": st.column_config.NumberColumn(
                    "Orders", help="Orders that used the campaign discount"),
                "Attributed Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                "Total Sales": st.column_config.NumberColumn("Total Sales", format="$%.2f"),
                "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                "Sales per Delivered": st.column_config.NumberColumn("Sales/Delivered", format="$%.4f"),
            },
            height=460,
            use_container_width=True,
            hide_index=True,
        )
        st.caption(
            "**Download CSV (all columns)** is the file for other projects: the same columns in "
            "the same order as before (including Influenced, Click-Influenced and All-Buyer), "
            "with Clicks added at the end. The small download icon on the table saves only the "
            "columns shown here. Blank = attribution not attempted (no code, EDU/CONTENT); "
            "0 = attempted, no orders."
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

    # ── Scope / Context ──────────────────────────────────────────────────
    st.markdown(
        f'<div class="context-line">{_fmt_month((sel_year, sel_month))} &middot; '
        f'Finalized campaigns with closed attribution windows</div>',
        unsafe_allow_html=True,
    )

    # Recompute monthly report for the selected month (not run month).
    monthly_df = generate_monthly_report(display_df, sel_year, sel_month)

    # Get all finalized campaigns in the selected month for weekly breakdown
    all_month = display_df[
        (display_df["Parsed Send Date"].notna()) &
        (display_df["is_final_snapshot"] == True) &
        (display_df["Discount Code"] != "None") &
        (display_df["Attributed Revenue"].notna())
    ].copy()
    if not all_month.empty:
        all_month["_send_dt"] = pd.to_datetime(all_month["Parsed Send Date"])
        all_month = all_month[
            (all_month["_send_dt"].dt.year == sel_year) &
            (all_month["_send_dt"].dt.month == sel_month)
        ]

    _month_is_empty = monthly_df.empty and all_month.empty

    # Always render the KPI row — zero values for empty months so the
    # layout does not disappear when comparing across months.
    if _month_is_empty:
        render_kpi_row([
            {"label": "Attributed Revenue", "value": "$0", "sub": "Net sales, discounted items"},
            {"label": "Total Sales", "value": "$0", "sub": "Full matched orders"},
            {"label": "Total Orders", "value": "0"},
            {"label": "Discount Codes", "value": "0"},
        ])
        spacer("md")
        st.info(
            f"No finalized campaigns in {_fmt_month((sel_year, sel_month))} yet. "
            "Campaigns appear here once their attribution window closes. "
            "Pick a different month above to compare."
        )
    else:
        # ── KPI Row ──────────────────────────────────────────────────────
        total_rev = monthly_df["Total_Attributed_Revenue"].sum() if not monthly_df.empty else 0
        total_sales = monthly_df["Total_Sales"].sum() if (not monthly_df.empty and "Total_Sales" in monthly_df.columns) else 0
        total_orders = monthly_df["Total_Discounted_Orders"].sum() if not monthly_df.empty else 0
        render_kpi_row([
            {"label": "Attributed Revenue", "value": fmt_money(total_rev), "exact": exact_money(total_rev),
             "sub": "Net sales, discounted items"},
            {"label": "Total Sales", "value": fmt_money(total_sales), "exact": exact_money(total_sales),
             "sub": "Full matched orders"},
            {"label": "Total Orders", "value": fmt_count(total_orders), "exact": exact_count(total_orders)},
            {"label": "Discount Codes", "value": str(len(monthly_df) if not monthly_df.empty else 0)},
        ])

        spacer("lg")

        # ── Weekly Revenue Trend ─────────────────────────────────────────
        section_title("Weekly Revenue Trend", "Total and average revenue by week within the month")

        if not all_month.empty:
            all_month["_send_dt"] = pd.to_datetime(all_month["_send_dt"])
            all_month["Week"] = all_month["_send_dt"].dt.isocalendar().week.astype(str)
            _monday = all_month["_send_dt"] - pd.to_timedelta(all_month["_send_dt"].dt.weekday, unit="D")
            all_month["Week_Start"] = _monday.dt.strftime("%m/%d")

            weekly_agg = all_month.groupby("Week_Start").agg(
                Attributed_Revenue=("Attributed Revenue", "sum"),
                Total_Sales=("Total Sales", "sum"),
                Campaign_Count=("Campaign Name", "count"),
            ).reset_index().sort_values("Week_Start")

            base = alt.Chart(weekly_agg).encode(
                x=alt.X("Week_Start:N", title="Week Starting", sort=None),
            )

            bars = base.mark_bar(color=CLR_MONTHLY, opacity=0.7, cornerRadiusEnd=3).encode(
                y=alt.Y("Total_Sales:Q", title="Total Sales"),
                tooltip=[
                    alt.Tooltip("Week_Start:N", title="Week of"),
                    alt.Tooltip("Total_Sales:Q", format="$,.2f", title="Total Sales"),
                    alt.Tooltip("Attributed_Revenue:Q", format="$,.2f", title="Attributed Revenue"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns"),
                ],
            )

            line = base.mark_line(color=CLR_MONTHLY_LINE, strokeWidth=3, point=True).encode(
                y=alt.Y("Attributed_Revenue:Q", title="Attributed Revenue"),
                tooltip=[
                    alt.Tooltip("Attributed_Revenue:Q", format="$,.2f", title="Attributed Revenue"),
                ],
            )

            combo = alt.layer(bars, line).resolve_scale(y="independent").properties(height=350)
            st.altair_chart(styled_chart(combo), use_container_width=True)
            st.caption(
                f"Bars = total sales (full orders) \u00b7 Line = attributed revenue (net sales, discounted items)"
            )
        else:
            st.info("No weekly data to chart.")

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

            st.altair_chart(styled_chart(top3_chart), use_container_width=True)

        spacer("lg")

        # ── Revenue by Campaign Breakdown ────────────────────────────────
        section_title(
            "Revenue by Campaign",
            "Top 10 discount codes · each segment is one campaign (hover for its name)",
        )

        if not all_month.empty:
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

            st.altair_chart(styled_chart(stacked), use_container_width=True)
            rest_caption(_code_rest, "Attributed Revenue", _code_tot["Attributed Revenue"].sum(), "codes")
        else:
            st.info("No data for discount code breakdown.")

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
                use_container_width=True,
                hide_index=True,
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
            st.altair_chart(styled_chart(_prod_bar), use_container_width=True)
            rest_caption(_prod_rest, "Total_Attributed_Revenue", _prod_total, "producers")

        spacer("lg")

        # ── E.2: Campaign Drill-Down ──────────────────────────────────────
        section_title("Campaign Drill-Down", "Select a producer to see individual campaign results")

        if not _pa_overview.empty:
            _all_producers = sorted(_pa_overview["Producer"].dropna().unique().tolist())
            _selected_producer = st.selectbox(
                "Producer",
                options=["— Select a producer —"] + _all_producers,
                key="producer_drilldown_select",
                label_visibility="collapsed",
            )

            if _selected_producer != "— Select a producer —" and not _pa_campaigns.empty:
                _drill = _pa_campaigns[_pa_campaigns["Producer"] == _selected_producer].copy()
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
                        use_container_width=True,
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
            st.altair_chart(styled_chart(_region_bar), use_container_width=True)
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
                use_container_width=True,
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
            st.altair_chart(styled_chart(_tier_bar), use_container_width=True)

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
                use_container_width=True,
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
                    use_container_width=True,
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
            use_container_width=True,
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
            st.altair_chart(styled_chart(_chart), use_container_width=True)
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
                use_container_width=True,
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
                use_container_width=True,
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
                use_container_width=True,
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
- The sidebar **event-campaign filter** hides BIN Sale, Large Format and Holiday campaigns: any campaign whose code is a discount family (BIN Sale, GoBig, BigDaddy, July4th, LaborDay25…), whose name contains BIN Sale / holiday sale / flash sale / clearance, or whose code has Offer Type `holiday` or `format promotion` in the mapping sheet. Producer offers using the producer's own code are kept even if themed (e.g. a large-format Las Cases offer). This filter changes what is shown only — never how revenue is attributed.
- The **Producers tab** only includes finalized campaigns, resolves each discount code to a producer via the mapping spreadsheet, and excludes non-producer buckets (BIN Sale, Holiday, etc.) from producer rankings.
- History rows for finalized campaigns are **frozen** — later pipeline runs never overwrite them.
""")
