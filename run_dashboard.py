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
    apply_ab_grouping,
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
    records = fetch_campaigns(hubspot_token, always_resolve_names=set(overrides))

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
                    p.producer_topic or "",
                ))

    # All campaign codes/titles (normalized) — used by the UTM-influenced
    # pass to keep single attribution: orders converting through any other
    # campaign's code are never counted as influenced.
    from src.shopify_orders import _normalize_code
    all_campaign_identifiers: set[str] = set()
    for rec in main_records:
        p = rec.parsed
        if not p.discount_code:
            continue
        if p.is_family_key:
            for m in get_family_identifiers(p.discount_code, families):
                all_campaign_identifiers.add(_normalize_code(m.identifier))
        else:
            all_campaign_identifiers.add(_normalize_code(p.discount_code))

    # Attribution dict keyed by "code|send_date" so each campaign gets
    # its own result for its own window.
    # Bulk-fetch ALL orders once; each campaign filters its window in-memory.
    # Cuts hundreds of overlapping per-window Shopify fetches down to one
    # paginated pass (~20-30 requests total).
    from src.shopify_orders import _fetch_orders_in_window
    print(f"  Bulk-fetching all Shopify orders {DATA_START_DATE} -> {run_date}...")
    all_orders = _fetch_orders_in_window(shopify_auth, DATA_START_DATE, run_date)
    print(f"  {len(all_orders)} orders fetched")

    # logic_spec 5.3 — map each code/family key to every send_date that uses
    # it, so overlapping windows can resolve a single owner per order instead
    # of each send counting the same order again.
    sibling_dates: dict[str, list] = {}
    for code, send_date, _w, _p in attribution_tasks:
        sibling_dates.setdefault(code.lower(), []).append(send_date)
    for family_key, send_date, _w in family_tasks:
        sibling_dates.setdefault(family_key.lower(), []).append(send_date)
    for k in sibling_dates:
        sibling_dates[k] = sorted(set(sibling_dates[k]))
    dupes = {k: v for k, v in sibling_dates.items() if len(v) > 1}
    if dupes:
        print(f"  [dedup] {len(dupes)} code(s) reused across sends — "
              f"resolving to most recent preceding send (logic_spec 5.3)")

    seen = set()

    # Standard code attribution
    attribution_failures: list[str] = []
    for code, send_date, window, producer_topic in attribution_tasks:
        dedup_key = f"{code.lower()}|{send_date}|{window}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        print(f"  Attributing: {code} (window: {send_date} + {window}d)...")
        try:
            attr = compute_attribution(
                shopify_auth, code, send_date, window,
                producer_topic=producer_topic,
                all_campaign_identifiers=all_campaign_identifiers,
                orders=all_orders,
                sibling_send_dates=sibling_dates.get(code.lower()),
            )
        except Exception as exc:
            print(f"    SKIPPED ({type(exc).__name__}): {exc}")
            attribution_failures.append(f"{code} @ {send_date}")
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
        print(f"  Attributing family: {family_key} -> {title_ids} (window: {send_date} + {window}d)...")
        try:
            attr = compute_family_attribution(
                shopify_auth, family_key, title_ids, send_date, window,
                orders=all_orders,
                sibling_send_dates=sibling_dates.get(family_key.lower()),
            )
        except Exception as exc:
            print(f"    SKIPPED ({type(exc).__name__}): {exc}")
            attribution_failures.append(f"{family_key} @ {send_date}")
            continue
        storage_key = f"{family_key.lower()}|{send_date}"
        attributions[storage_key] = attr

    print(f"  Attribution computed for {len(attributions)} discount code(s)")
    if attribution_failures:
        print(f"  WARNING: {len(attribution_failures)} campaign(s) skipped due to Shopify errors:")
        for label in attribution_failures:
            print(f"    - {label}")

    # ── Step 5: Assemble dashboard rows ──────────────────────────────────
    print("\n[5/8] Assembling dashboard...")
    dashboard_rows = assemble_dashboard_rows(records, attributions, run_date)
    df = rows_to_dataframe(dashboard_rows)
    pre_ab = len(df)
    df = apply_ab_grouping(df)
    print(f"  Dashboard rows: {len(df)} ({pre_ab - len(df)} A/B groups merged)")

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
        shopify_auth, DATA_START_DATE, run_date + __import__("datetime").timedelta(days=1),
        orders=all_orders,
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
