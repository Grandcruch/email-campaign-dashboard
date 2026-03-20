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
)


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
    page_title="Grand Cru — Campaign Dashboard",
    page_icon="\U0001F377",
    layout="wide",
)

st.title("\U0001F377 Grand Cru Liquid Assets — Campaign Dashboard")

# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Controls")
    if st.button("\U0001F504 Refresh Data", type="primary", use_container_width=True):
        st.session_state.pop("data", None)

    st.divider()
    st.caption(f"Data scope: **{DATA_START_DATE}** onward")

    if "data" in st.session_state:
        d = st.session_state["data"]
        st.metric("Run Date", str(d["run_date"]))
        col1, col2 = st.columns(2)
        col1.metric("Main", d["main_count"])
        col2.metric("Excluded", d["excluded_count"])


# ─── Load data ───────────────────────────────────────────────────────────────

if "data" not in st.session_state:
    try:
        st.session_state["data"] = run_pipeline()
    except Exception as e:
        st.error(f"Pipeline failed: {e}")
        st.stop()

data = st.session_state["data"]


# ─── Tabs ────────────────────────────────────────────────────────────────────

tab_weekly, tab_monthly, tab_producer, tab_qa = st.tabs([
    "\U0001F4C5 Weekly Report",
    "\U0001F4CA Monthly Report",
    "\U0001F3AD Producer Performance",
    "\U0001F50D QA",
])


# ─── Tab 1: Weekly Report ────────────────────────────────────────────────────

with tab_weekly:
    st.subheader("Weekly Campaign Report")

    weekly_df = data["weekly_df"]

    if weekly_df.empty:
        st.info("No campaign data for this period.")
    else:
        # Summary metrics
        coded = weekly_df[weekly_df["Discount Code"] != "None"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Campaigns", len(weekly_df))
        c2.metric("Total Delivered", f"{weekly_df['Delivered'].sum():,}")
        if not coded.empty and coded["Attributed Revenue"].notna().any():
            c3.metric("Total Attr. Revenue", f"${coded['Attributed Revenue'].sum():,.2f}")
            c4.metric("Total Orders", f"{coded['Discounted Orders'].dropna().sum():.0f}")

        st.dataframe(
            weekly_df,
            column_config={
                "Attributed Revenue": st.column_config.NumberColumn(format="$%.2f"),
                "Revenue per Delivered": st.column_config.NumberColumn(format="$%.4f"),
            },
            use_container_width=True,
            hide_index=True,
        )

    # Insights
    st.divider()
    st.subheader("Weekly Performance Insights")

    insights = data["weekly_insights"]
    for line in insights:
        if line.startswith("==="):
            st.markdown(f"**{line.strip('= ')}**")
        elif line.strip() == "":
            continue
        else:
            st.markdown(f"- {line}")


# ─── Tab 2: Monthly Report ───────────────────────────────────────────────────

with tab_monthly:
    st.subheader("Monthly Discount Code Report")

    monthly_df = data["monthly_df"]

    if monthly_df.empty:
        st.info(
            "No finalized campaigns for the current month yet. "
            "Campaigns appear here once their attribution window closes."
        )
    else:
        # Summary metrics
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Revenue", f"${monthly_df['Total_Attributed_Revenue'].sum():,.2f}")
        c2.metric("Total Orders", f"{monthly_df['Total_Discounted_Orders'].sum():.0f}")
        c3.metric("Discount Codes", len(monthly_df))

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


# ─── Tab 3: Producer Performance ─────────────────────────────────────────────

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

        # Best / worst highlights
        st.divider()
        coded_producers = display_df[display_df["Total_Attributed_Revenue"] > 0]
        if not coded_producers.empty:
            best_scale = coded_producers.loc[coded_producers["Total_Attributed_Revenue"].idxmax()]
            worst_scale = coded_producers.loc[coded_producers["Total_Attributed_Revenue"].idxmin()]

            col_best, col_worst = st.columns(2)
            with col_best:
                st.markdown("**\U0001F3C6 Best Producer (Scale)**")
                st.markdown(
                    f"**{best_scale['Producer / Topic']}** — "
                    f"${best_scale['Total_Attributed_Revenue']:,.2f} attributed revenue"
                )
            with col_worst:
                st.markdown("**\u26A0\uFE0F Worst Producer (Scale)**")
                st.markdown(
                    f"**{worst_scale['Producer / Topic']}** — "
                    f"${worst_scale['Total_Attributed_Revenue']:,.2f} attributed revenue"
                )

            eff = coded_producers[coded_producers["Revenue per Delivered"].notna()]
            if not eff.empty:
                best_eff = eff.loc[eff["Revenue per Delivered"].idxmax()]
                worst_eff = eff.loc[eff["Revenue per Delivered"].idxmin()]
                col_be, col_we = st.columns(2)
                with col_be:
                    st.markdown("**\U0001F3AF Best Producer (Efficiency)**")
                    st.markdown(
                        f"**{best_eff['Producer / Topic']}** — "
                        f"${best_eff['Revenue per Delivered']:.4f}/delivered"
                    )
                with col_we:
                    st.markdown("**\U0001F4C9 Worst Producer (Efficiency)**")
                    st.markdown(
                        f"**{worst_eff['Producer / Topic']}** — "
                        f"${worst_eff['Revenue per Delivered']:.4f}/delivered"
                    )


# ─── Tab 4: QA ───────────────────────────────────────────────────────────────

with tab_qa:
    st.subheader("Excluded Campaigns")

    excluded_df = data["excluded_df"]
    if excluded_df.empty:
        st.success("No excluded campaigns.")
    else:
        st.caption(f"{len(excluded_df)} campaign(s) excluded from the main table")
        st.dataframe(excluded_df, use_container_width=True, hide_index=True)

    st.divider()

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
