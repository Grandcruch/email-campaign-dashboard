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


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_completed_week_range(run_date: date) -> tuple[date, date]:
    """
    Return the most recent COMPLETED week range for campaign analysis.
    A campaign is 'completed' when its attribution window has fully closed.
    The longest window is 7 days, so the latest completed send date is
    run_date - 8 days (sent 8 days ago => 7-day window closed yesterday).
    We show a full Mon-Sun week that falls within the completed range.
    """
    latest_completed = run_date - timedelta(days=8)
    # Find the Monday of that week
    week_end = latest_completed
    week_start = week_end - timedelta(days=week_end.weekday())  # Monday
    # Ensure the week_end is the Sunday of that week
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
    lines.append("### Campaign Performance Analysis")
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
    lines.append("### Producer Performance Analysis")

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

    lines.append("### Monthly Insights")

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

    lines.append(f"### Producer Insights ({view_label})")

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

st.title("\U0001F377 Grand Cru Liquid Assets \u2014 Campaign Dashboard")


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


# ─── Sidebar (rendered AFTER data is loaded) ─────────────────────────────────

with st.sidebar:
    st.header("Controls")
    if st.button("\U0001F504 Refresh Data", type="primary", use_container_width=True):
        st.session_state.pop("data", None)
        st.rerun()

    st.divider()
    st.caption(f"Data scope: **{DATA_START_DATE}** onward")

    st.metric("Run Date", str(data["run_date"]))
    col1, col2 = st.columns(2)
    col1.metric("Main", data["main_count"])
    col2.metric("Excluded", data["excluded_count"])

    # Excluded campaigns — expandable section
    excl_df = data["excluded_df"]
    if not excl_df.empty:
        with st.expander(f"\u26A0\uFE0F Excluded Campaigns ({len(excl_df)})"):
            for _, row in excl_df.iterrows():
                name = row.get("Campaign Name", "Unknown")
                bucket = row.get("QA Bucket", "")
                st.markdown(f"- **{bucket}**: {name}")


# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_weekly, tab_monthly, tab_producer, tab_qa = st.tabs([
    "\U0001F4C5 Weekly Report",
    "\U0001F4CA Monthly Report",
    "\U0001F3AD Producer Performance",
    "\U0001F50D QA",
])


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 1: Weekly Report — Completed Campaigns Only
# ═══════════════════════════════════════════════════════════════════════════════

with tab_weekly:

    # ── Filter to completed week ──────────────────────────────────────────
    completed_week_df, wk_start, wk_end = _filter_completed_week(full_df, run_date)

    st.subheader("Completed Weekly Campaign View")
    st.caption(
        f"Showing campaigns from **{wk_start}** to **{wk_end}** whose attribution "
        f"windows have fully closed (standard: +7d, BIN/holiday: +3d). "
        f"Only finalized results are included."
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
        # Summary metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Completed Campaigns", len(completed_week_df))
        c2.metric("Total Delivered", f"{completed_week_df['Delivered'].sum():,}")
        if not coded_week.empty:
            c3.metric("Total Attr. Revenue", f"${coded_week['Attributed Revenue'].sum():,.2f}")
            c4.metric("Total Orders", f"{coded_week['Discounted Orders'].dropna().sum():.0f}")

        # ── Chart A: Performance ranking bar chart ────────────────────────
        st.divider()
        st.markdown("#### Chart A: Campaign Performance Ranking")

        if not coded_week.empty:
            metric_choice = st.radio(
                "Rank by",
                ["Attributed Revenue", "Revenue per Delivered"],
                horizontal=True,
                key="weekly_bar_metric",
            )

            chart_data = coded_week[["Campaign Name", "Discount Code", metric_choice]].copy()
            chart_data = chart_data.sort_values(metric_choice, ascending=True)

            fmt = "$,.2f" if metric_choice == "Attributed Revenue" else "$,.4f"

            bar_chart = alt.Chart(chart_data).mark_bar(color="#4e79a7").encode(
                y=alt.Y("Campaign Name:N", sort=None, title=None),
                x=alt.X(f"{metric_choice}:Q", title=metric_choice),
                tooltip=[
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip(f"{metric_choice}:Q", format=fmt),
                ],
            ).properties(height=max(len(chart_data) * 40, 200))

            st.altair_chart(bar_chart, use_container_width=True)
        else:
            st.info("No coded campaigns with revenue data to chart.")

        # ── Chart B: Delivered vs Revenue scatter ─────────────────────────
        st.markdown("#### Chart B: Delivered vs Attributed Revenue")

        if not coded_week.empty and len(coded_week) >= 2:
            scatter_data = coded_week[[
                "Campaign Name", "Producer / Topic", "Delivered",
                "Attributed Revenue", "Revenue per Delivered"
            ]].copy()

            # Quadrant reference lines
            med_del = scatter_data["Delivered"].median()
            med_rev = scatter_data["Attributed Revenue"].median()

            scatter = alt.Chart(scatter_data).mark_circle(size=120, opacity=0.8).encode(
                x=alt.X("Delivered:Q", title="Delivered"),
                y=alt.Y("Attributed Revenue:Q", title="Attributed Revenue"),
                color=alt.Color("Producer / Topic:N", legend=alt.Legend(title="Producer")),
                tooltip=[
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Producer / Topic:N"),
                    alt.Tooltip("Delivered:Q", format=","),
                    alt.Tooltip("Attributed Revenue:Q", format="$,.2f"),
                    alt.Tooltip("Revenue per Delivered:Q", format="$,.4f"),
                ],
            )

            # Quadrant lines
            hline = alt.Chart(pd.DataFrame({"y": [med_rev]})).mark_rule(
                strokeDash=[4, 4], color="gray", opacity=0.5
            ).encode(y="y:Q")

            vline = alt.Chart(pd.DataFrame({"x": [med_del]})).mark_rule(
                strokeDash=[4, 4], color="gray", opacity=0.5
            ).encode(x="x:Q")

            st.altair_chart(
                (scatter + hline + vline).properties(height=400),
                use_container_width=True,
            )

            # Quadrant labels
            q1, q2, q3, q4 = st.columns(4)
            q1.caption("\u2197\uFE0F High delivery, High revenue")
            q2.caption("\u2196\uFE0F Low delivery, High revenue")
            q3.caption("\u2198\uFE0F High delivery, Low revenue")
            q4.caption("\u2199\uFE0F Low delivery, Low revenue")
        elif not coded_week.empty:
            st.info("Need at least 2 campaigns for the scatter plot.")

        # ── Weekly data table ─────────────────────────────────────────────
        st.divider()
        st.markdown("#### Campaign Detail Table")
        display_cols = [
            "Parsed Send Date", "Discount Code", "Campaign Name",
            "Discounted Orders", "Delivered", "Attributed Revenue",
            "Revenue per Delivered",
        ]
        available = [c for c in display_cols if c in completed_week_df.columns]
        st.dataframe(
            completed_week_df[available],
            column_config={
                "Attributed Revenue": st.column_config.NumberColumn(format="$%.2f"),
                "Revenue per Delivered": st.column_config.NumberColumn(format="$%.4f"),
            },
            use_container_width=True,
            hide_index=True,
        )

    # ── Analytical insights ───────────────────────────────────────────────
    st.divider()
    st.subheader("Weekly Performance Insights")
    st.caption("_Analysis based on completed-week campaigns only_")
    insights_md = _generate_analytical_insights(completed_week_df, wk_start, wk_end)
    st.markdown(insights_md)


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 2: Monthly Report — All Campaigns in Month
# ═══════════════════════════════════════════════════════════════════════════════

with tab_monthly:
    st.subheader("Monthly Campaign Report")
    st.caption(
        f"All finalized campaigns in **{run_date.strftime('%B %Y')}**. "
        f"Only campaigns with closed attribution windows are included."
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
        # Summary metrics
        total_rev = monthly_df["Total_Attributed_Revenue"].sum() if not monthly_df.empty else 0
        total_orders = monthly_df["Total_Discounted_Orders"].sum() if not monthly_df.empty else 0
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue", f"${total_rev:,.2f}")
        c2.metric("Total Orders", f"{total_orders:.0f}")
        c3.metric("Discount Codes", len(monthly_df) if not monthly_df.empty else 0)

        # ── Chart C: Weekly comparison within month ───────────────────────
        st.divider()
        st.markdown("#### Chart C: Weekly Performance Comparison")

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

            bars = base.mark_bar(color="#4e79a7", opacity=0.7).encode(
                y=alt.Y("Total_Revenue:Q", title="Total Attributed Revenue"),
                tooltip=[
                    alt.Tooltip("Week_Start:N", title="Week of"),
                    alt.Tooltip("Total_Revenue:Q", format="$,.2f", title="Total Revenue"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns"),
                ],
            )

            line = base.mark_line(color="#e15759", strokeWidth=3, point=True).encode(
                y=alt.Y("Avg_Revenue:Q", title="Avg Campaign Revenue"),
                tooltip=[
                    alt.Tooltip("Avg_Revenue:Q", format="$,.2f", title="Avg Revenue/Campaign"),
                ],
            )

            combo = alt.layer(bars, line).resolve_scale(y="independent").properties(height=350)
            st.altair_chart(combo, use_container_width=True)
            st.caption(
                "\U0001F7E6 Bars = total weekly revenue | "
                "\U0001F534 Line = average revenue per campaign"
            )
        else:
            st.info("No weekly data to chart.")

        # ── Chart D: Top 3 discount codes ─────────────────────────────────
        st.divider()
        st.markdown("#### Chart D: Top 3 Discount Codes This Month")

        if not monthly_df.empty:
            top3 = monthly_df.head(3).copy()

            top3_chart = alt.Chart(top3).mark_bar(color="#59a14f").encode(
                x=alt.X("Total_Attributed_Revenue:Q", title="Attributed Revenue"),
                y=alt.Y("Discount Code:N", sort=None, title=None),
                tooltip=[
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip("Total_Attributed_Revenue:Q", format="$,.2f", title="Revenue"),
                    alt.Tooltip("Campaign_Count:Q", title="Campaigns"),
                    alt.Tooltip("Total_Discounted_Orders:Q", title="Orders"),
                ],
            ).properties(height=max(len(top3) * 50, 120))

            st.altair_chart(top3_chart, use_container_width=True)

        # ── Chart E: Discount code contribution (stacked) ────────────────
        st.divider()
        st.markdown("#### Chart E: Discount Code Revenue Breakdown by Campaign")

        if not all_month.empty:
            contrib = all_month[["Discount Code", "Campaign Name", "Attributed Revenue"]].copy()
            contrib = contrib.sort_values("Attributed Revenue", ascending=False)

            # Order discount codes by total revenue
            code_order = contrib.groupby("Discount Code")["Attributed Revenue"].sum().sort_values(ascending=False).index.tolist()

            stacked = alt.Chart(contrib).mark_bar().encode(
                y=alt.Y("Discount Code:N", sort=code_order, title=None),
                x=alt.X("Attributed Revenue:Q", title="Attributed Revenue", stack="zero"),
                color=alt.Color(
                    "Campaign Name:N",
                    legend=alt.Legend(title="Campaign", orient="bottom", columns=2),
                ),
                tooltip=[
                    alt.Tooltip("Discount Code:N"),
                    alt.Tooltip("Campaign Name:N"),
                    alt.Tooltip("Attributed Revenue:Q", format="$,.2f"),
                ],
            ).properties(height=max(len(code_order) * 45, 150))

            st.altair_chart(stacked, use_container_width=True)
        else:
            st.info("No data for discount code breakdown.")

        # ── Monthly data table ────────────────────────────────────────────
        st.divider()
        st.markdown("#### Discount Code Summary Table")
        if not monthly_df.empty:
            st.dataframe(
                monthly_df,
                column_config={
                    "Total_Attributed_Revenue": st.column_config.NumberColumn(
                        "Total Attributed Revenue", format="$%.2f"
                    ),
                    "Total_Discount_Value": st.column_config.NumberColumn(
                        "Total Discount Value", format="$%.2f"
                    ),
                    "Avg Revenue per Campaign": st.column_config.NumberColumn(format="$%.2f"),
                    "Avg Revenue per Delivered": st.column_config.NumberColumn(format="$%.4f"),
                },
                use_container_width=True,
                hide_index=True,
            )

        # ── Monthly insights ──────────────────────────────────────────────
        st.divider()
        st.subheader("Monthly Insights")
        monthly_insights_md = _generate_monthly_insights(monthly_df, all_month)
        st.markdown(monthly_insights_md)


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 3: Producer Performance
# ═══════════════════════════════════════════════════════════════════════════════

with tab_producer:
    st.subheader("Producer Performance")

    view = st.radio(
        "View",
        ["current-to-date", "finalized-only"],
        horizontal=True,
        help="Current-to-date includes all main-table campaigns. "
             "Finalized-only includes only campaigns whose attribution window has closed.",
    )

    if view == "current-to-date":
        prod_df = data["producer_current_df"]
    else:
        prod_df = data["producer_final_df"]

    if prod_df.empty:
        st.info(f"No data for the {view} view.")
    else:
        display_df = prod_df.drop(columns=["View"], errors="ignore")

        # Summary metrics
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue", f"${display_df['Total_Attributed_Revenue'].sum():,.2f}")
        c2.metric("Producers", len(display_df))
        c3.metric("Total Campaigns", f"{display_df['Campaign_Count'].sum():.0f}")

        # ── Chart F: Producer ranking bar chart ───────────────────────────
        st.divider()
        st.markdown("#### Chart F: Producer Performance Ranking")

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

        prod_bar = alt.Chart(chart_prod).mark_bar(color="#e15759").encode(
            y=alt.Y("Producer / Topic:N", sort=None, title=None),
            x=alt.X(f"{prod_metric}:Q", title=prod_metric.replace("_", " ")),
            tooltip=[
                alt.Tooltip("Producer / Topic:N"),
                alt.Tooltip(f"{prod_metric}:Q", format=fmt),
            ],
        ).properties(height=max(len(chart_prod) * 35, 200))

        st.altair_chart(prod_bar, use_container_width=True)

        # ── Producer data table ───────────────────────────────────────────
        st.divider()
        st.markdown("#### Producer Detail Table")
        st.dataframe(
            display_df,
            column_config={
                "Total_Attributed_Revenue": st.column_config.NumberColumn(
                    "Total Attributed Revenue", format="$%.2f"
                ),
                "Revenue per Delivered": st.column_config.NumberColumn(format="$%.4f"),
                "Avg Revenue per Campaign": st.column_config.NumberColumn(format="$%.2f"),
            },
            use_container_width=True,
            hide_index=True,
        )

        # ── Producer analytical insights ──────────────────────────────────
        st.divider()
        st.markdown(_generate_producer_insights(display_df, view))


# ═══════════════════════════════════════════════════════════════════════════════
# Tab 4: QA
# ═══════════════════════════════════════════════════════════════════════════════

with tab_qa:
    st.subheader("Unmatched Shopify Discount Codes")

    unmatched_df = data["unmatched_df"]
    if unmatched_df.empty:
        st.success("No unmatched discount codes.")
    else:
        reasons = ["All"] + sorted(unmatched_df["Possible Reason"].unique().tolist())
        selected = st.selectbox("Filter by reason", reasons)

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

    st.divider()
    st.subheader("QA Summary")
    st.code(data["qa_summary"], language=None)
