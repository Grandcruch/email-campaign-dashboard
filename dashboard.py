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
from datetime import date, timedelta

import streamlit as st
import pandas as pd
import altair as alt

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PRODUCER_CODE_EXCEL = os.path.join(PROJECT_ROOT, "Producer - Discount Code Mapping - Copy.xlsx")
PRODUCER_PRODUCT_EXCEL = os.path.join(PROJECT_ROOT, "Producer - Product Mapping - Copy.xlsx")
sys.path.insert(0, PROJECT_ROOT)

from src.config import load_env, DATA_START_DATE, OUTPUT_DIR
from src.auth import ShopifyAuth, hubspot_headers
from src.hubspot import fetch_campaigns
from src.overrides import load_overrides, apply_overrides
from src.shopify_orders import (
    compute_attribution,
    compute_family_attribution,
    fetch_all_discount_codes_in_range,
    _normalize_code as _normalize_discount_code,
)
from src.families import load_family_mapping, is_family_key, get_family_identifiers
from src.reports import (
    assemble_dashboard_rows,
    rows_to_dataframe,
    apply_ab_grouping,
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

    /* ── KPI Card ── */
    .kpi-card {{
        background: {CLR_SURFACE};
        border: 1px solid {CLR_BORDER};
        border-radius: 8px;
        padding: 1.25rem 1.5rem;
        text-align: left;
    }}
    .kpi-card .kpi-label {{
        font-size: 0.75rem;
        font-weight: 500;
        color: {CLR_TEXT_MUTED};
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
    }}
    .kpi-card .kpi-value {{
        font-size: 1.5rem;
        font-weight: 700;
        color: {CLR_TEXT_PRIMARY};
        line-height: 1.2;
        white-space: nowrap;
    }}
    .kpi-card .kpi-sub {{
        font-size: 0.8rem;
        color: {CLR_TEXT_SECONDARY};
        margin-top: 0.25rem;
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

def render_kpi_row(kpis: list[dict]):
    """Render a row of styled KPI cards. Each dict: {label, value, sub (optional)}."""
    cols = st.columns(len(kpis))
    for col, kpi in zip(cols, kpis):
        sub_html = f'<div class="kpi-sub">{kpi.get("sub", "")}</div>' if kpi.get("sub") else ""
        col.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{kpi['label']}</div>
            <div class="kpi-value">{kpi['value']}</div>
            {sub_html}
        </div>
        """, unsafe_allow_html=True)


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

def run_pipeline() -> dict:
    """
    Execute the full data pipeline (same logic as run_dashboard.py).
    Returns a dict of all computed artifacts for display.
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

        # Attribution dict is keyed by "code|send_date" so each campaign
        # gets its own attribution result for its own window, even if
        # multiple campaigns share the same discount code / family key.
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
                    shopify_auth, code, send_date, window,
                    producer_topic=producer_topic,
                    all_campaign_identifiers=all_campaign_identifiers,
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
                    shopify_auth, family_key, title_ids, send_date, window,
                )
            except Exception as exc:
                attribution_failures.append((f"{family_key} @ {send_date}", str(exc)))
                continue
            storage_key = f"{family_key.lower()}|{send_date}"
            attributions[storage_key] = attr

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

        # Step 6: Reports
        status.update(label="Generating reports...")
        weekly_df = generate_weekly_report(df, run_date)
        weekly_insights = generate_weekly_insights(df)
        monthly_df = generate_monthly_report(df, run_date.year, run_date.month)
        producer_current_df, producer_final_df = generate_producer_report(df)

        # Step 6b: Producer analytics (new)
        status.update(label="Loading producer mapping data...")
        producer_map = load_producer_mapping(PRODUCER_CODE_EXCEL)
        offer_type_map = load_offer_type_mapping(PRODUCER_CODE_EXCEL)
        tier_map = load_tier_mapping(PRODUCER_PRODUCT_EXCEL)
        top_region_map, sub_region_map = load_region_mapping(PRODUCER_PRODUCT_EXCEL)
        producer_analytics = generate_producer_analytics(
            df, producer_map, offer_type_map, tier_map, top_region_map, sub_region_map
        )

        # Step 7: QA
        status.update(label="Fetching Shopify orders for unmatched-code analysis...")
        excluded_df = generate_excluded_campaigns(records)
        shopify_code_map = fetch_all_discount_codes_in_range(
            shopify_auth, DATA_START_DATE, run_date + timedelta(days=1)
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
        st.session_state.pop("data", None)
        st.rerun()

    st.markdown(f"""
    <div style="margin-top: 1.5rem; font-size: 0.8rem; color: {CLR_TEXT_MUTED};">
        Data from <strong style="color:{CLR_TEXT_SECONDARY};">{DATA_START_DATE}</strong><br>
        <strong style="color:{CLR_TEXT_SECONDARY};">{data['main_count']}</strong> campaigns tracked
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

    # ── Campaign Filter ─────────────────────────────────────────────────
    st.markdown(f"""
    <div style="margin-top: 1.25rem; font-size: 0.7rem; color: {CLR_TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.05em;">
        Campaign Filter
    </div>
    """, unsafe_allow_html=True)
    exclude_bin_holiday = st.toggle(
        "Exclude BIN Sale & Holiday campaigns",
        value=False,
        key="exclude_bin_holiday",
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
            <strong style="color:{CLR_TEXT_PRIMARY};">Discounted Orders</strong><br>
            Number of Shopify orders that used the campaign discount code within the attribution window.<br><br>
            <strong style="color:{CLR_TEXT_PRIMARY};">Discount Value</strong><br>
            Total dollar amount discounted across all matched line items.
        </div>
    </div>
    """, unsafe_allow_html=True)


# ─── Apply BIN/Holiday filter ─────────────────────────────────────────────────
# Keep raw full_df for QA tab; filtered version used by Weekly/Monthly/Producer
if exclude_bin_holiday:
    display_df = full_df[full_df["is_bin_holiday"] != True].copy()
else:
    display_df = full_df

# Recompute monthly and producer reports from filtered data
from src.reports import generate_monthly_report, generate_producer_report
_monthly_df = generate_monthly_report(display_df, run_date.year, run_date.month)
_producer_current_df, _producer_final_df = generate_producer_report(display_df)

# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_weekly, tab_monthly, tab_producer, tab_qa, tab_method = st.tabs([
    "Weekly",
    "Monthly",
    "Producers",
    "QA",
    "Methodology",
])


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 1: Weekly Report — Completed Campaigns Only
# ═══════════════════════════════════════════════════════════════════════════════

with tab_weekly:

    # ── Date Range Selector ──────────────────────────────────────────────
    col_toggle, col_dates = st.columns([1, 3])

    with col_toggle:
        finalized_only = st.toggle("Finalized only", value=True, key="wk_finalized_toggle")

    # Determine date bounds from available data
    _all_send_dates = pd.to_datetime(display_df["Parsed Send Date"].dropna()).dt.date
    _min_date = _all_send_dates.min() if not _all_send_dates.empty else DATA_START_DATE
    _max_date = _all_send_dates.max() if not _all_send_dates.empty else run_date

    # Default: most recent full week (Mon-Sun)
    _default_end = run_date
    _default_start = _default_end - timedelta(days=6)

    with col_dates:
        date_range = st.date_input(
            "Date range",
            value=(_default_start, _default_end),
            min_value=_min_date,
            max_value=run_date,
            key="wk_date_range",
        )

    # Initialize to empty so references below are always safe
    completed_week_df = pd.DataFrame()
    coded_week = pd.DataFrame()
    wk_start = wk_end = None
    _weekly_has_data = False

    # date_input returns a tuple only when both dates are picked
    if isinstance(date_range, tuple) and len(date_range) == 2:
        wk_start, wk_end = date_range[0], date_range[1]

        # Filter DataFrame to selected date range
        mask = (
            display_df["Parsed Send Date"].notna() &
            (pd.to_datetime(display_df["Parsed Send Date"]).dt.date >= wk_start) &
            (pd.to_datetime(display_df["Parsed Send Date"]).dt.date <= wk_end)
        )
        completed_week_df = display_df[mask].copy()

        if finalized_only:
            completed_week_df = completed_week_df[
                completed_week_df["is_final_snapshot"] == True
            ].copy()

        # Context label
        fin_label = "Finalized campaigns only" if finalized_only else "All campaigns (includes open windows)"
        st.markdown(
            f'<div class="context-line">{fin_label} &middot; '
            f'{wk_start.strftime("%b %d")} \u2013 {wk_end.strftime("%b %d, %Y")}</div>',
            unsafe_allow_html=True,
        )

        # Coded campaigns for charts
        coded_week = completed_week_df[
            (completed_week_df["Discount Code"] != "None") &
            (completed_week_df["Attributed Revenue"].notna())
        ].copy()

        _weekly_has_data = not completed_week_df.empty

    if not _weekly_has_data:
        if wk_start is not None:
            st.info("No campaigns in the selected date range match the current filter.")
    else:
        # ── KPI Row ──────────────────────────────────────────────────────
        kpis = [
            {"label": "Campaigns", "value": str(len(completed_week_df))},
            {"label": "Total Delivered", "value": f"{completed_week_df['Delivered'].sum():,}"},
        ]
        if not coded_week.empty:
            kpis.append({"label": "Attributed Revenue", "value": f"${coded_week['Attributed Revenue'].sum():,.2f}",
                         "sub": "Net sales, discounted items"})
            kpis.append({"label": "Total Sales", "value": f"${coded_week['Total Sales'].sum():,.2f}",
                         "sub": "Full matched orders"})
            kpis.append({"label": "Orders", "value": f"{coded_week['Discounted Orders'].dropna().sum():.0f}"})
            if "Influenced Total Sales" in coded_week.columns:
                _inf_sales = coded_week["Influenced Total Sales"].dropna().sum()
                _inf_orders = coded_week["Influenced Orders"].dropna().sum()
                if _inf_orders > 0:
                    kpis.append({
                        "label": "Influenced Sales",
                        "value": f"${_inf_sales:,.2f}",
                        "sub": f"{_inf_orders:.0f} email-driven orders w/o code",
                    })
        render_kpi_row(kpis)

        spacer("lg")

        # ── Campaign Performance Ranking ─────────────────────────────────
        section_title("Campaign Performance Ranking", "Ranked by selected metric")

        if not coded_week.empty:
            metric_choice = st.radio(
                "Rank by",
                ["Attributed Revenue", "Total Sales", "Revenue per Delivered"],
                horizontal=True,
                key="weekly_bar_metric",
                label_visibility="collapsed",
            )

            chart_data = coded_week[["Campaign Name", "Discount Code", metric_choice]].copy()
            chart_data = chart_data.sort_values(metric_choice, ascending=True)

            fmt = "$,.4f" if metric_choice == "Revenue per Delivered" else "$,.2f"

            bar_chart = alt.Chart(chart_data).mark_bar(
                color=CLR_WEEKLY,
                cornerRadiusEnd=3,
            ).encode(
                y=alt.Y("Campaign Name:N", sort=None, title=None),
                x=alt.X(f"{metric_choice}:Q", title=metric_choice),
                tooltip=[
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip(f"{metric_choice}:Q", format=fmt),
                ],
            ).properties(height=max(len(chart_data) * 40, 200))

            st.altair_chart(styled_chart(bar_chart), use_container_width=True)
        else:
            st.info("No coded campaigns with revenue data to chart.")

        spacer("lg")

        # ── Delivery vs Revenue ──────────────────────────────────────────
        section_title("Delivery vs Revenue", "Each dot represents one campaign")

        if not coded_week.empty and len(coded_week) >= 2:
            scatter_metric = st.radio(
                "Scatter Y-axis",
                ["Attributed Revenue", "Total Sales"],
                horizontal=True,
                key="weekly_scatter_metric",
                label_visibility="collapsed",
            )

            scatter_data = coded_week[[
                "Campaign Name", "Producer / Topic", "Delivered",
                "Attributed Revenue", "Total Sales", "Revenue per Delivered"
            ]].copy()

            med_del = scatter_data["Delivered"].median()
            med_rev = scatter_data[scatter_metric].median()

            scatter = alt.Chart(scatter_data).mark_circle(size=120, opacity=0.8).encode(
                x=alt.X("Delivered:Q", title="Delivered"),
                y=alt.Y(f"{scatter_metric}:Q", title=scatter_metric),
                color=alt.Color(
                    "Producer / Topic:N",
                    scale=alt.Scale(range=PALETTE_MULTI),
                    legend=alt.Legend(title="Producer"),
                ),
                tooltip=[
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Producer / Topic:N"),
                    alt.Tooltip("Delivered:Q", format=","),
                    alt.Tooltip("Attributed Revenue:Q", format="$,.2f"),
                    alt.Tooltip("Total Sales:Q", format="$,.2f"),
                    alt.Tooltip("Revenue per Delivered:Q", format="$,.4f"),
                ],
            )

            # Quadrant reference lines
            hline = alt.Chart(pd.DataFrame({"y": [med_rev]})).mark_rule(
                strokeDash=[4, 4], color=CLR_BORDER, opacity=0.7
            ).encode(y="y:Q")

            vline = alt.Chart(pd.DataFrame({"x": [med_del]})).mark_rule(
                strokeDash=[4, 4], color=CLR_BORDER, opacity=0.7
            ).encode(x="x:Q")

            # Quadrant text annotations
            max_del = scatter_data["Delivered"].max()
            min_del = scatter_data["Delivered"].min()
            max_rev = scatter_data[scatter_metric].max()
            min_rev = scatter_data[scatter_metric].min()
            range_del = max_del - min_del if max_del != min_del else 1
            range_rev = max_rev - min_rev if max_rev != min_rev else 1

            quad_labels = pd.DataFrame([
                {"x": max_del - range_del * 0.02, "y": max_rev - range_rev * 0.02, "text": "High reach, High revenue"},
                {"x": min_del + range_del * 0.02, "y": max_rev - range_rev * 0.02, "text": "Low reach, High revenue"},
                {"x": max_del - range_del * 0.02, "y": min_rev + range_rev * 0.02, "text": "High reach, Low revenue"},
                {"x": min_del + range_del * 0.02, "y": min_rev + range_rev * 0.02, "text": "Low reach, Low revenue"},
            ])
            quad_text = alt.Chart(quad_labels).mark_text(
                fontSize=10, color=CLR_TEXT_MUTED, fontWeight="normal",
            ).encode(
                x=alt.X("x:Q"),
                y=alt.Y("y:Q"),
                text="text:N",
            )

            scatter_combined = (scatter + hline + vline + quad_text).properties(height=400)
            st.altair_chart(styled_chart(scatter_combined), use_container_width=True)
        elif not coded_week.empty:
            st.info("Need at least 2 campaigns for the scatter plot.")

        spacer("lg")

        # ── Weekly Insights ──────────────────────────────────────────────
        section_title("Weekly Performance Insights", "Analysis based on completed-week campaigns only")
        insights_md = _generate_analytical_insights(completed_week_df, wk_start, wk_end)
        render_insight_card(insights_md)

        spacer("lg")

        # ── Supporting Detail Table ──────────────────────────────────────
        section_title("Campaign Details")

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

        display_cols = [
            "Parsed Send Date", "Producer", "Discount Code", "Campaign Name",
            "Discounted Orders", "Delivered", "Attributed Revenue",
            "Total Sales", "Revenue per Delivered", "Sales per Delivered",
            "Influenced Orders", "Influenced Offer Revenue", "Influenced Total Sales",
        ]
        available = [c for c in display_cols if c in details_df.columns]
        st.dataframe(
            details_df[available],
            column_config={
                "Parsed Send Date": st.column_config.DateColumn("Send Date", width="small"),
                "Producer": st.column_config.TextColumn("Producer", width="medium"),
                "Campaign Name": st.column_config.TextColumn("Campaign", width="large"),
                "Discount Code": st.column_config.TextColumn("Code", width="medium"),
                "Attributed Revenue": st.column_config.NumberColumn("Attr. Revenue", format="$%.2f"),
                "Total Sales": st.column_config.NumberColumn("Total Sales", format="$%.2f"),
                "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
                "Sales per Delivered": st.column_config.NumberColumn("Sales/Delivered", format="$%.4f"),
                "Influenced Orders": st.column_config.NumberColumn(
                    "Infl. Orders", help="Email-driven orders that did not use the campaign code (UTM-matched)"),
                "Influenced Offer Revenue": st.column_config.NumberColumn(
                    "Infl. Offer Rev", format="$%.2f",
                    help="Revenue from the offered wine's line items on influenced orders"),
                "Influenced Total Sales": st.column_config.NumberColumn(
                    "Infl. Total Sales", format="$%.2f",
                    help="Full order value of influenced orders"),
            },
            use_container_width=True,
            hide_index=True,
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
            {"label": "Attributed Revenue", "value": "$0.00", "sub": "Net sales, discounted items"},
            {"label": "Total Sales", "value": "$0.00", "sub": "Full matched orders"},
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
            {"label": "Attributed Revenue", "value": f"${total_rev:,.2f}", "sub": "Net sales, discounted items"},
            {"label": "Total Sales", "value": f"${total_sales:,.2f}", "sub": "Full matched orders"},
            {"label": "Total Orders", "value": f"{total_orders:.0f}"},
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
        section_title("Revenue by Campaign", "How each discount code's revenue splits across campaigns")

        if not all_month.empty:
            contrib = all_month[["Discount Code", "Campaign Name", "Attributed Revenue"]].copy()
            contrib = contrib.sort_values("Attributed Revenue", ascending=False)

            code_order = contrib.groupby("Discount Code")["Attributed Revenue"].sum().sort_values(ascending=False).index.tolist()

            stacked = alt.Chart(contrib).mark_bar(cornerRadiusEnd=2).encode(
                y=alt.Y("Discount Code:N", sort=code_order, title=None),
                x=alt.X("Attributed Revenue:Q", title="Attributed Revenue", stack="zero"),
                color=alt.Color(
                    "Campaign Name:N",
                    scale=alt.Scale(range=PALETTE_MULTI),
                    legend=alt.Legend(title="Campaign", orient="bottom", columns=2),
                ),
                tooltip=[
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Attributed Revenue:Q", format="$,.2f"),
                ],
            ).properties(height=max(len(code_order) * 45, 150))

            st.altair_chart(styled_chart(stacked), use_container_width=True)
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
            {"label": "Attributed Revenue", "value": f"${_pa_total_rev:,.2f}", "sub": "Standalone & combo offers"},
            {"label": "Producers", "value": str(int(_pa_n_producers))},
            {"label": "Campaigns", "value": str(int(_pa_n_campaigns))},
            {"label": "Discounted Orders", "value": f"{int(_pa_total_orders):,}"},
        ])

        spacer("lg")

        # ── E.1: Producer Revenue Pie Chart ──────────────────────────────
        section_title("Producer Revenue", "Share of attributed revenue by producer (finalized, standalone & combo offers)")

        _pie_data = _pa_overview[_pa_overview["Total_Attributed_Revenue"] > 0][
            ["Producer", "Total_Attributed_Revenue"]
        ].copy()
        _pie_data = _pie_data.sort_values("Total_Attributed_Revenue", ascending=False)

        if not _pie_data.empty:
            _pie_chart = alt.Chart(_pie_data).mark_arc(innerRadius=70, outerRadius=160).encode(
                theta=alt.Theta("Total_Attributed_Revenue:Q", stack=True),
                color=alt.Color(
                    "Producer:N",
                    scale=alt.Scale(scheme="reds"),
                    legend=alt.Legend(orient="right", title="Producer"),
                ),
                tooltip=[
                    alt.Tooltip("Producer:N", title="Producer"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", title="Attributed Revenue", format="$,.2f"),
                ],
            ).properties(height=380)
            st.altair_chart(styled_chart(_pie_chart), use_container_width=True)

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
            _region_bar = alt.Chart(_pa_region).mark_bar(cornerRadiusEnd=3).encode(
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
            ).properties(height=max(len(_pa_region) * 38, 200))
            st.altair_chart(styled_chart(_region_bar), use_container_width=True)

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
# Tab 4: QA — Data Quality & Diagnostics
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

- **Type** is one of `PROD` (sellable offer), `EDU` (educational), or `CONTENT`.
- Only **PROD campaigns with a real discount code** are matched to Shopify orders. EDU/CONTENT campaigns show delivery stats only.
- A/B test versions of the same campaign (same code, same send date) are automatically **merged into one row** — delivery stats are summed, and the shared Shopify attribution is counted once.

#### Attribution window

Orders are matched to a campaign if they were created within **7 days** of the send date (**3 days** for BIN Sale / holiday / flash campaigns). A campaign is **Finalized** once its window has closed — finalized numbers never change afterward.
""")

    spacer("md")
    section_title("Primary attribution — discount code", "The buyer used the campaign's code at checkout")

    st.markdown("""
An order belongs to a campaign when the campaign's **discount code appears on the order**. Within a matched order, only the line items that the code was applied to count toward Attributed Revenue.

| Metric | Definition |
|---|---|
| **Attributed Revenue** | For each discounted line item: `price × quantity − discount amount`. Net revenue of the wines the code was applied to. |
| **Discount Value** | Total dollar discount given on those line items. |
| **Total Sales** | Full order value (`total_price`) of every matched order — includes non-discounted items in the same cart, tax, and shipping. |
| **Discounted Orders** | Number of orders that used the code. |
| **Revenue per Delivered** | Attributed Revenue ÷ Delivered. The efficiency yardstick across campaigns. |
| **Sales per Delivered** | Total Sales ÷ Delivered. |

**BIN Sale campaigns** apply automatic discounts (e.g. `BinSale10` + `BinSale12`) instead of typed codes, so they're matched by discount *title* across the whole family and de-duplicated so no order is counted twice.

**Single attribution:** each order is credited to exactly **one** campaign.
""")

    spacer("md")
    section_title("Secondary attribution — UTM-influenced", "The email drove the visit, but the code wasn't used")

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
- Offer-wine line matching compares the campaign's Producer/Topic text against Shopify product titles; unusual naming mismatches can miss a line (the order still counts in Influenced Total Sales).
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
- The **Producers tab** only includes finalized campaigns, resolves each discount code to a producer via the mapping spreadsheet, and excludes non-producer buckets (BIN Sale, Holiday, etc.) from producer rankings.
- History rows for finalized campaigns are **frozen** — later pipeline runs never overwrite them.
""")
