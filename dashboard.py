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
sys.path.insert(0, PROJECT_ROOT)

from src.config import load_env, DATA_START_DATE, OUTPUT_DIR
from src.auth import ShopifyAuth, hubspot_headers
from src.hubspot import fetch_campaigns
from src.overrides import load_overrides, apply_overrides
from src.shopify_orders import compute_attribution, fetch_all_discount_codes_in_range
from src.reports import (
    assemble_dashboard_rows,
    rows_to_dataframe,
    generate_weekly_report,
    generate_weekly_insights,
    generate_monthly_report,
    generate_producer_report,
    generate_excluded_campaigns,
    generate_unmatched_codes_report,
    update_history,
    generate_qa_summary,
    write_all_outputs,
    MIN_DELIVERED_THRESHOLD,
)


# ─── Design System ──────────────────────────────────────────────────────────

# Color tokens — Red scale palette
# Full scale: "#fef2f2","#fee2e2","#fecaca","#fca5a5","#f87171","#ef4444","#dc2626","#b91c1c","#991b1b","#7f1d1d","#450a0a"
CLR_BG_PAGE = "#fef2f2"         # Lightest red — page background
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


def _get_completed_week_range(run_date: date) -> tuple[date, date]:
    """
    Return the most recent COMPLETED week range for campaign analysis.
    A campaign is 'completed' when its attribution window has fully closed.
    The longest window is 7 days, so the latest completed send date is
    run_date - 8 days (sent 8 days ago => 7-day window closed yesterday).
    We show a full Mon-Sun week that falls within the completed range.
    """
    latest_completed = run_date - timedelta(days=8)
    week_end = latest_completed
    week_start = week_end - timedelta(days=week_end.weekday())  # Monday
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def _filter_completed_campaigns(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """
    Filter to only campaigns whose attribution window has fully closed.
    is_final_snapshot == True is the definitive marker.
    """
    return df[df["is_final_snapshot"] == True].copy()


def _filter_completed_week(df: pd.DataFrame, run_date: date) -> tuple[pd.DataFrame, date, date]:
    """
    Filter to completed campaigns within the most recent completed week.
    Returns (filtered_df, week_start, week_end).
    """
    completed = _filter_completed_campaigns(df, run_date)
    week_start, week_end = _get_completed_week_range(run_date)
    mask = (
        completed["Parsed Send Date"].notna() &
        (pd.to_datetime(completed["Parsed Send Date"]).dt.date >= week_start) &
        (pd.to_datetime(completed["Parsed Send Date"]).dt.date <= week_end)
    )
    return completed[mask].copy(), week_start, week_end


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
            f"\\${best_eff['Attributed Revenue']:,.2f} total revenue from "
            f"{int(best_eff['Delivered']):,} deliveries."
        )

        # Scale leader
        if best_scale['Campaign Name'] != best_eff['Campaign Name']:
            lines.append(
                f"**Scale leader**: {best_scale['Campaign Name']} generated the highest "
                f"revenue at **\\${best_scale['Attributed Revenue']:,.2f}** from "
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
                        f"but only \\${r['Attributed Revenue']:,.2f} revenue. "
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
                        f"but \\${r['Attributed Revenue']:,.2f} revenue "
                        f"(\\${r['Revenue per Delivered']:.4f}/delivered). "
                        f"This campaign punched above its weight \u2014 "
                        f"scaling its delivery could unlock significant upside."
                    )

    # ── PRODUCER ANALYSIS ─────────────────────────────────────────────────
    lines.append("")
    lines.append("#### Producer Performance Analysis")

    prod_group = coded.groupby("Producer / Topic").agg(
        Revenue=("Attributed Revenue", "sum"),
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
            f"(\\${best_p['Revenue']:,.2f} from {int(best_p['Campaigns'])} campaign(s))."
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
        lines.append(
            f"- **{r['Discount Code']}**: \\${r['Total_Attributed_Revenue']:,.2f} "
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
        lines.append(
            f"**Scale leader**: {best['Producer / Topic']} dominates with "
            f"\\${best['Total_Attributed_Revenue']:,.2f} total attributed revenue "
            f"across {int(best['Campaign_Count'])} campaign(s)."
        )

        eff_pool = coded[coded["Revenue per Delivered"].notna()]
        if len(eff_pool) >= 2:
            best_e = eff_pool.loc[eff_pool["Revenue per Delivered"].idxmax()]
            worst_e = eff_pool.loc[eff_pool["Revenue per Delivered"].idxmin()]
            if best_e["Producer / Topic"] != best["Producer / Topic"]:
                lines.append(
                    f"**Efficiency leader**: {best_e['Producer / Topic']} achieves the "
                    f"highest per-email conversion at \\${best_e['Revenue per Delivered']:.4f}/delivered \u2014 "
                    f"scaling delivery for this producer could unlock significant revenue."
                )
            lines.append(
                f"**Lowest efficiency**: {worst_e['Producer / Topic']} at "
                f"\\${worst_e['Revenue per Delivered']:.4f}/delivered. "
                f"Consider testing different subject lines, offers, or audience segments."
            )

    if not zero_rev.empty:
        names = zero_rev["Producer / Topic"].tolist()
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

        # Step 2: Overrides
        status.update(label="Loading campaign overrides...")
        overrides = load_overrides()

        # Step 3: HubSpot campaigns
        status.update(label="Fetching HubSpot campaigns...")
        records = fetch_campaigns(hubspot_token)
        apply_overrides(records, overrides)

        main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                        "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}
        main_records = [r for r in records if r.parsed.qa_bucket in main_buckets]

        # Step 4: Shopify attribution
        status.update(label="Computing Shopify attribution...")
        attributions: dict = {}
        attribution_tasks = []
        for rec in main_records:
            p = rec.parsed
            if p.discount_code:
                attribution_tasks.append((
                    p.discount_code,
                    p.parsed_send_date,
                    p.attribution_window_days,
                ))

        seen = set()
        for code, send_date, window in attribution_tasks:
            key = f"{code.lower()}|{send_date}|{window}"
            if key in seen:
                continue
            seen.add(key)
            status.update(label=f"Attributing: {code}...")
            attr = compute_attribution(shopify_auth, code, send_date, window)
            code_lower = code.lower()
            if code_lower not in attributions or (
                send_date and send_date > (
                    attributions[code_lower]._send_date
                    if hasattr(attributions[code_lower], '_send_date')
                    else date.min
                )
            ):
                attr._send_date = send_date  # type: ignore
                attributions[code_lower] = attr

        # Step 5: Assemble dashboard
        status.update(label="Assembling dashboard rows...")
        dashboard_rows = assemble_dashboard_rows(records, attributions, run_date)
        df = rows_to_dataframe(dashboard_rows)

        # Step 6: Reports
        status.update(label="Generating reports...")
        weekly_df = generate_weekly_report(df, run_date)
        weekly_insights = generate_weekly_insights(df)
        monthly_df = generate_monthly_report(df, run_date.year, run_date.month)
        producer_current_df, producer_final_df = generate_producer_report(df)

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


# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_weekly, tab_monthly, tab_producer, tab_qa = st.tabs([
    "Weekly",
    "Monthly",
    "Producers",
    "QA",
])


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 1: Weekly Report — Completed Campaigns Only
# ═══════════════════════════════════════════════════════════════════════════════

with tab_weekly:

    # ── Scope / Context ──────────────────────────────────────────────────
    completed_week_df, wk_start, wk_end = _filter_completed_week(full_df, run_date)

    st.markdown(
        f'<div class="context-line">Week of {wk_start.strftime("%b %d")} \u2013 '
        f'{wk_end.strftime("%b %d, %Y")} &middot; Finalized campaigns only</div>',
        unsafe_allow_html=True,
    )

    # Coded campaigns for charts
    coded_week = completed_week_df[
        (completed_week_df["Discount Code"] != "None") &
        (completed_week_df["Attributed Revenue"].notna())
    ].copy()

    if completed_week_df.empty:
        st.info(
            f"No completed campaigns for the week of {wk_start} to {wk_end}. "
            f"This may mean all campaigns in that window are still open."
        )
    else:
        # ── KPI Row ──────────────────────────────────────────────────────
        kpis = [
            {"label": "Campaigns", "value": str(len(completed_week_df))},
            {"label": "Total Delivered", "value": f"{completed_week_df['Delivered'].sum():,}"},
        ]
        if not coded_week.empty:
            kpis.append({"label": "Attributed Revenue", "value": f"${coded_week['Attributed Revenue'].sum():,.2f}"})
            kpis.append({"label": "Orders", "value": f"{coded_week['Discounted Orders'].dropna().sum():.0f}"})
        render_kpi_row(kpis)

        spacer("lg")

        # ── Campaign Performance Ranking ─────────────────────────────────
        section_title("Campaign Performance Ranking", "Ranked by selected metric")

        if not coded_week.empty:
            metric_choice = st.radio(
                "Rank by",
                ["Attributed Revenue", "Revenue per Delivered"],
                horizontal=True,
                key="weekly_bar_metric",
                label_visibility="collapsed",
            )

            chart_data = coded_week[["Campaign Name", "Discount Code", metric_choice]].copy()
            chart_data = chart_data.sort_values(metric_choice, ascending=True)

            fmt = "$,.2f" if metric_choice == "Attributed Revenue" else "$,.4f"

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
            scatter_data = coded_week[[
                "Campaign Name", "Producer / Topic", "Delivered",
                "Attributed Revenue", "Revenue per Delivered"
            ]].copy()

            med_del = scatter_data["Delivered"].median()
            med_rev = scatter_data["Attributed Revenue"].median()

            scatter = alt.Chart(scatter_data).mark_circle(size=120, opacity=0.8).encode(
                x=alt.X("Delivered:Q", title="Delivered"),
                y=alt.Y("Attributed Revenue:Q", title="Attributed Revenue"),
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
            max_rev = scatter_data["Attributed Revenue"].max()
            min_rev = scatter_data["Attributed Revenue"].min()
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
        display_cols = [
            "Parsed Send Date", "Discount Code", "Campaign Name",
            "Discounted Orders", "Delivered", "Attributed Revenue",
            "Revenue per Delivered",
        ]
        available = [c for c in display_cols if c in completed_week_df.columns]
        st.dataframe(
            completed_week_df[available],
            column_config={
                "Parsed Send Date": st.column_config.DateColumn("Send Date", width="small"),
                "Campaign Name": st.column_config.TextColumn("Campaign", width="large"),
                "Discount Code": st.column_config.TextColumn("Code", width="medium"),
                "Attributed Revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                "Revenue per Delivered": st.column_config.NumberColumn("Rev/Delivered", format="$%.4f"),
            },
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 2: Monthly Report
# ═══════════════════════════════════════════════════════════════════════════════

with tab_monthly:

    # ── Scope / Context ──────────────────────────────────────────────────
    st.markdown(
        f'<div class="context-line">{run_date.strftime("%B %Y")} &middot; '
        f'Finalized campaigns with closed attribution windows</div>',
        unsafe_allow_html=True,
    )

    monthly_df = data["monthly_df"]

    # Get all finalized campaigns this month for weekly breakdown
    all_month = full_df[
        (full_df["Parsed Send Date"].notna()) &
        (full_df["is_final_snapshot"] == True) &
        (full_df["Discount Code"] != "None") &
        (full_df["Attributed Revenue"].notna())
    ].copy()
    all_month["_send_dt"] = pd.to_datetime(all_month["Parsed Send Date"])
    all_month = all_month[
        (all_month["_send_dt"].dt.year == run_date.year) &
        (all_month["_send_dt"].dt.month == run_date.month)
    ]

    if monthly_df.empty and all_month.empty:
        st.info(
            "No finalized campaigns for the current month yet. "
            "Campaigns appear here once their attribution window closes."
        )
    else:
        # ── KPI Row ──────────────────────────────────────────────────────
        total_rev = monthly_df["Total_Attributed_Revenue"].sum() if not monthly_df.empty else 0
        total_orders = monthly_df["Total_Discounted_Orders"].sum() if not monthly_df.empty else 0
        render_kpi_row([
            {"label": "Total Revenue", "value": f"${total_rev:,.2f}"},
            {"label": "Total Orders", "value": f"{total_orders:.0f}"},
            {"label": "Discount Codes", "value": str(len(monthly_df) if not monthly_df.empty else 0)},
        ])

        spacer("lg")

        # ── Weekly Revenue Trend ─────────────────────────────────────────
        section_title("Weekly Revenue Trend", "Total and average revenue by week within the month")

        if not all_month.empty:
            all_month["Week"] = all_month["_send_dt"].dt.isocalendar().week.astype(str)
            all_month["Week_Start"] = all_month["_send_dt"].apply(
                lambda d: (d - timedelta(days=d.weekday())).strftime("%m/%d")
            )

            weekly_agg = all_month.groupby("Week_Start").agg(
                Total_Revenue=("Attributed Revenue", "sum"),
                Avg_Revenue=("Attributed Revenue", "mean"),
                Campaign_Count=("Campaign Name", "count"),
            ).reset_index().sort_values("Week_Start")

            base = alt.Chart(weekly_agg).encode(
                x=alt.X("Week_Start:N", title="Week Starting", sort=None),
            )

            bars = base.mark_bar(color=CLR_MONTHLY, opacity=0.7, cornerRadiusEnd=3).encode(
                y=alt.Y("Total_Revenue:Q", title="Total Attributed Revenue"),
                tooltip=[
                    alt.Tooltip("Week_Start:N", title="Week of"),
                    alt.Tooltip("Total_Revenue:Q", format="$,.2f", title="Total Revenue"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns"),
                ],
            )

            line = base.mark_line(color=CLR_MONTHLY_LINE, strokeWidth=3, point=True).encode(
                y=alt.Y("Avg_Revenue:Q", title="Avg Campaign Revenue"),
                tooltip=[
                    alt.Tooltip("Avg_Revenue:Q", format="$,.2f", title="Avg Revenue/Campaign"),
                ],
            )

            combo = alt.layer(bars, line).resolve_scale(y="independent").properties(height=350)
            st.altair_chart(styled_chart(combo), use_container_width=True)
            st.caption(
                f"Bars = total weekly revenue \u00b7 Line = average revenue per campaign"
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
                        "Total Revenue", format="$%.2f"
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

    # ── Scope / Context + View Toggle ────────────────────────────────────
    st.markdown(
        f'<div class="context-line">Producer-level aggregation &middot; All tracked campaigns</div>',
        unsafe_allow_html=True,
    )

    view = st.radio(
        "View",
        ["current-to-date", "finalized-only"],
        horizontal=True,
        help="Current-to-date includes all main-table campaigns. "
             "Finalized-only includes only campaigns whose attribution window has closed.",
        label_visibility="collapsed",
    )

    if view == "current-to-date":
        prod_df = data["producer_current_df"]
    else:
        prod_df = data["producer_final_df"]

    if prod_df.empty:
        st.info(f"No data for the {view} view.")
    else:
        display_df = prod_df.drop(columns=["View"], errors="ignore")

        # ── KPI Row ──────────────────────────────────────────────────────
        render_kpi_row([
            {"label": "Total Revenue", "value": f"${display_df['Total_Attributed_Revenue'].sum():,.2f}"},
            {"label": "Producers", "value": str(len(display_df))},
            {"label": "Total Campaigns", "value": f"{display_df['Campaign_Count'].sum():.0f}"},
        ])

        spacer("lg")

        # ── Producer Revenue Ranking ─────────────────────────────────────
        section_title("Producer Revenue Ranking", "Ranked by selected metric")

        prod_metric = st.radio(
            "Rank by",
            ["Total_Attributed_Revenue", "Revenue per Delivered", "Total_Discounted_Orders"],
            format_func=lambda x: {
                "Total_Attributed_Revenue": "Attributed Revenue",
                "Revenue per Delivered": "Revenue per Delivered",
                "Total_Discounted_Orders": "Discounted Orders",
            }.get(x, x),
            horizontal=True,
            key="producer_bar_metric",
            label_visibility="collapsed",
        )

        chart_prod = display_df[["Producer / Topic", prod_metric]].copy()
        chart_prod = chart_prod[chart_prod[prod_metric].notna()]
        chart_prod = chart_prod.sort_values(prod_metric, ascending=True)

        if prod_metric == "Revenue per Delivered":
            fmt = "$,.4f"
        elif prod_metric == "Total_Attributed_Revenue":
            fmt = "$,.2f"
        else:
            fmt = ",.0f"

        prod_bar = alt.Chart(chart_prod).mark_bar(
            color=CLR_PRODUCER,
            cornerRadiusEnd=3,
        ).encode(
            y=alt.Y("Producer / Topic:N", sort=None, title=None),
            x=alt.X(f"{prod_metric}:Q", title=prod_metric.replace("_", " ")),
            tooltip=[
                alt.Tooltip("Producer / Topic:N"),
                alt.Tooltip(f"{prod_metric}:Q", format=fmt),
            ],
        ).properties(height=max(len(chart_prod) * 35, 200))

        st.altair_chart(styled_chart(prod_bar), use_container_width=True)

        spacer("lg")

        # ── Producer Insights ────────────────────────────────────────────
        section_title("Producer Insights")
        render_insight_card(_generate_producer_insights(display_df, view))

        spacer("lg")

        # ── Supporting Detail Table ──────────────────────────────────────
        section_title("Producer Details")
        st.dataframe(
            display_df,
            column_config={
                "Total_Attributed_Revenue": st.column_config.NumberColumn(
                    "Total Revenue", format="$%.2f"
                ),
                "Revenue per Delivered": st.column_config.NumberColumn(
                    "Rev/Delivered", format="$%.4f"
                ),
                "Avg Revenue per Campaign": st.column_config.NumberColumn(
                    "Avg Rev/Campaign", format="$%.2f"
                ),
            },
            use_container_width=True,
            hide_index=True,
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
