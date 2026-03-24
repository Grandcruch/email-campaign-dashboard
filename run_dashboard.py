#!/usr/bin/env python3
"""
run_dashboard.py — Main orchestrator for the Email Campaign Dashboard.

Fetches fresh data from HubSpot + Shopify, computes attribution,
generates all reports, and writes outputs to ./output/.

Usage:
    cd "Email campaign dashboard"
    python run_dashboard.py
"""

import sys
import os
from datetime import date

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import load_env, DATA_START_DATE, OUTPUT_DIR
from src.auth import ShopifyAuth, hubspot_headers
from src.hubspot import fetch_campaigns, CampaignRecord
from src.parser import ParsedCampaign
from src.overrides import load_overrides, apply_overrides
from src.shopify_orders import compute_attribution, compute_family_attribution, fetch_all_discount_codes_in_range
from src.families import load_family_mapping, is_family_key, get_family_identifiers
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


def main():
    run_date = date.today()
    print(f"{'='*60}")
    print(f"  Email Campaign Dashboard — Run Date: {run_date}")
    print(f"  Data scope: {DATA_START_DATE} onward")
    print(f"{'='*60}")

    # ── Step 1: Load credentials ─────────────────────────────────────────
    print("\n[1/8] Loading credentials...")
    env = load_env()
    hubspot_token = env["HUBSPOT_PRIVATE_APP_TOKEN"]

    shopify_auth = ShopifyAuth(
        store_domain=env["SHOPIFY_STORE_DOMAIN"],
        client_id=env["SHOPIFY_CLIENT_ID"],
        client_secret=env["SHOPIFY_CLIENT_SECRET"],
    )

    # ── Step 2: Load overrides + family mappings ─────────────────────────
    print("\n[2/8] Loading campaign overrides...")
    overrides = load_overrides()
    families = load_family_mapping()

    # ── Step 3: Fetch HubSpot campaigns ──────────────────────────────────
    print("\n[3/8] Fetching HubSpot campaigns...")
    records = fetch_campaigns(hubspot_token)

    # Apply overrides
    apply_overrides(records, overrides)

    # Tag family keys on parsed campaigns
    for rec in records:
        p = rec.parsed
        if p.discount_code and is_family_key(p.discount_code, families):
            p.is_family_key = True

    # Separate main-table vs excluded
    main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                    "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}
    main_records = [r for r in records if r.parsed.qa_bucket in main_buckets]
    print(f"  Main table campaigns: {len(main_records)}")
    print(f"  Excluded campaigns: {len(records) - len(main_records)}")

    # ── Step 4: Shopify attribution ──────────────────────────────────────
    print("\n[4/8] Computing Shopify attribution...")
    attributions: dict = {}

    # Collect unique (code, send_date, window) combinations
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
                ))

    # Standard code attribution
    seen = set()
    for code, send_date, window in attribution_tasks:
        key = f"{code.lower()}|{send_date}|{window}"
        if key in seen:
            continue
        seen.add(key)
        print(f"  Attributing: {code} (window: {send_date} + {window}d)...")
        attr = compute_attribution(shopify_auth, code, send_date, window)
        code_lower = code.lower()
        if code_lower not in attributions or (
            send_date and send_date > (attributions[code_lower]._send_date if hasattr(attributions[code_lower], '_send_date') else date.min)
        ):
            attr._send_date = send_date  # type: ignore
            attributions[code_lower] = attr

    # Family / multi-code attribution
    for family_key, send_date, window in family_tasks:
        key = f"{family_key.lower()}|{send_date}|{window}"
        if key in seen:
            continue
        seen.add(key)
        members = get_family_identifiers(family_key, families)
        title_ids = [m.identifier for m in members]
        print(f"  Attributing family: {family_key} -> {title_ids} (window: {send_date} + {window}d)...")
        attr = compute_family_attribution(
            shopify_auth, family_key, title_ids, send_date, window,
        )
        code_lower = family_key.lower()
        if code_lower not in attributions or (
            send_date and send_date > (attributions[code_lower]._send_date if hasattr(attributions[code_lower], '_send_date') else date.min)
        ):
            attr._send_date = send_date  # type: ignore
            attributions[code_lower] = attr

    print(f"  Attribution computed for {len(attributions)} discount code(s)")

    # ── Step 5: Assemble dashboard rows ──────────────────────────────────
    print("\n[5/8] Assembling dashboard...")
    dashboard_rows = assemble_dashboard_rows(records, attributions, run_date)
    df = rows_to_dataframe(dashboard_rows)
    print(f"  Dashboard rows: {len(df)}")

    # ── Step 6: Generate reports ─────────────────────────────────────────
    print("\n[6/8] Generating reports...")

    weekly_df = generate_weekly_report(df, run_date)
    weekly_insights = generate_weekly_insights(df)

    monthly_df = generate_monthly_report(df, run_date.year, run_date.month)

    producer_current_df, producer_final_df = generate_producer_report(df)
    print(f"  Producer report (current-to-date): {len(producer_current_df)} producer(s)")
    print(f"  Producer report (finalized-only): {len(producer_final_df)} producer(s)")

    # ── Step 7: QA outputs ───────────────────────────────────────────────
    print("\n[7/8] Generating QA outputs...")

    excluded_df = generate_excluded_campaigns(records)

    # Unmatched Shopify discount codes — fetch all orders in scope
    print("  Fetching all Shopify orders for unmatched-code analysis...")
    shopify_code_map = fetch_all_discount_codes_in_range(
        shopify_auth, DATA_START_DATE, run_date + __import__("datetime").timedelta(days=1)
    )
    campaign_codes = {
        r.parsed.discount_code.lower()
        for r in records
        if r.parsed.discount_code and r.parsed.qa_bucket in main_buckets
    }
    # Add family member identifiers so they don't show as unmatched
    for fkey, members in families.items():
        if fkey.lower() in campaign_codes:
            for m in members:
                campaign_codes.add(m.identifier.lower())
    unmatched_df = generate_unmatched_codes_report(shopify_code_map, campaign_codes)

    qa_summary = generate_qa_summary(records, dashboard_rows, unmatched_df)
    print(qa_summary)

    # ── Step 8: Update history and write outputs ─────────────────────────
    print("\n[8/8] Writing outputs...")

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

    print(f"\n{'='*60}")
    print(f"  Run complete. Output files in: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
