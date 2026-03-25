#!/usr/bin/env python3
"""
export_historical_csv.py — Generate the Historical Email Offer Performance CSV.

Runs the full HubSpot + Shopify pipeline, filters to finalized campaigns only,
and exports a CSV with the agreed schema.

Revenue column uses Shopify `current_subtotal_price` (order-level merchandise
sales net of discounts, excluding tax and shipping).

Scheduled to run every Sunday. Can also be run manually at any time.

Usage:
    python export_historical_csv.py
"""

import sys
import os
import csv
from datetime import date, timedelta

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.config import load_env, DATA_START_DATE, OUTPUT_DIR
from src.auth import ShopifyAuth, hubspot_headers
from src.hubspot import fetch_campaigns
from src.overrides import load_overrides, apply_overrides
from src.shopify_orders import compute_attribution, compute_family_attribution
from src.families import load_family_mapping, is_family_key, get_family_identifiers
from src.reports import assemble_dashboard_rows, rows_to_dataframe

# ─── Output file name ────────────────────────────────────────────────────────
OUTPUT_FILENAME = "Historical Email Offer Performance(2026.3.9~).csv"
TARGET_KPI = 1.5  # Fixed Target $/Del KPI value


def run_export():
    """Execute the pipeline and export the CSV."""
    run_date = date.today()
    print(f"{'='*60}")
    print(f"  Historical CSV Export — Run Date: {run_date}")
    print(f"  Data scope: {DATA_START_DATE} onward (finalized only)")
    print(f"{'='*60}")

    # ── Step 1: Load credentials ─────────────────────────────────────────
    print("\n[1/6] Loading credentials...")
    env = load_env()
    hubspot_token = env["HUBSPOT_PRIVATE_APP_TOKEN"]
    shopify_auth = ShopifyAuth(
        store_domain=env["SHOPIFY_STORE_DOMAIN"],
        client_id=env["SHOPIFY_CLIENT_ID"],
        client_secret=env["SHOPIFY_CLIENT_SECRET"],
    )

    # ── Step 2: Load overrides + family mappings ─────────────────────────
    print("\n[2/6] Loading campaign overrides and family mappings...")
    overrides = load_overrides()
    families = load_family_mapping()

    # ── Step 3: Fetch HubSpot campaigns ──────────────────────────────────
    print("\n[3/6] Fetching HubSpot campaigns...")
    records = fetch_campaigns(hubspot_token)
    apply_overrides(records, overrides)

    # Tag family keys
    for rec in records:
        p = rec.parsed
        if p.discount_code and is_family_key(p.discount_code, families):
            p.is_family_key = True

    main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                    "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}
    main_records = [r for r in records if r.parsed.qa_bucket in main_buckets]
    print(f"  Main table campaigns: {len(main_records)}")

    # ── Step 4: Shopify attribution ──────────────────────────────────────
    print("\n[4/6] Computing Shopify attribution...")
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
                ))

    seen = set()

    # Standard code attribution
    for code, send_date, window in attribution_tasks:
        dedup_key = f"{code.lower()}|{send_date}|{window}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        print(f"  Attributing: {code} (window: {send_date} + {window}d)...")
        attr = compute_attribution(shopify_auth, code, send_date, window)
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
        attr = compute_family_attribution(
            shopify_auth, family_key, title_ids, send_date, window,
        )
        storage_key = f"{family_key.lower()}|{send_date}"
        attributions[storage_key] = attr

    print(f"  Attribution computed for {len(attributions)} discount code(s)")

    # ── Step 5: Assemble dashboard rows ──────────────────────────────────
    print("\n[5/6] Assembling dashboard rows...")
    dashboard_rows = assemble_dashboard_rows(records, attributions, run_date)
    df = rows_to_dataframe(dashboard_rows)
    print(f"  Total dashboard rows: {len(df)}")

    # Filter to finalized campaigns only
    finalized_df = df[df["is_final_snapshot"] == True].copy()
    print(f"  Finalized campaigns: {len(finalized_df)}")

    # ── Step 6: Export CSV ───────────────────────────────────────────────
    print("\n[6/6] Exporting CSV...")

    # Log campaigns with missing discount code mappings
    missing_code = finalized_df[
        (finalized_df["Discount Code"] == "None") |
        (finalized_df["Discount Code"].isna())
    ]
    if not missing_code.empty:
        print(f"\n  WARNING: {len(missing_code)} finalized campaign(s) have no discount code mapping:")
        for _, row in missing_code.iterrows():
            print(f"    - {row['Campaign Name']} (send: {row['Parsed Send Date']})")

    # Sort by send date ascending for the export
    finalized_sorted = finalized_df.sort_values("Parsed Send Date", ascending=True)

    # Build export rows
    export_rows_sorted = []
    for _, row in finalized_sorted.iterrows():
        email_name = row["Campaign Name"]
        delivered = int(row["Delivered"]) if row["Delivered"] else 0
        opened = int(row["Opened"]) if row["Opened"] else 0
        clicked = int(row["Clicked"]) if row["Clicked"] else 0
        subject = row.get("Subject", "")

        # Revenue = Order Subtotal (current_subtotal_price — merch net of discounts, excl tax/shipping)
        # This is the full matched order merchandise revenue, not just discounted items.
        revenue = row.get("Order Subtotal")
        if revenue is None or (isinstance(revenue, float) and revenue != revenue):  # NaN check
            revenue = 0.0
        else:
            revenue = float(revenue)

        # Click Rate = Clicked / Delivered
        click_rate = round(clicked / delivered, 6) if delivered > 0 else None

        # $ / Delivered = Revenue / Delivered
        rev_per_delivered = round(revenue / delivered, 4) if (delivered > 0 and revenue > 0) else None

        export_rows_sorted.append({
            "Email Name": email_name,
            "Click Rate": click_rate,
            "Clicked": clicked,
            "Opened": opened,
            "Delivered": delivered,
            "$ / Delivered": rev_per_delivered,
            "Target $/Del KPI": TARGET_KPI,
            "Subject": subject,
            "Revenue": round(revenue, 2),
        })

    # Write CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

    fieldnames = [
        "Email Name",
        "Click Rate",
        "Clicked",
        "Opened",
        "Delivered",
        "$ / Delivered",
        "Target $/Del KPI",
        "Subject",
        "Revenue",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(export_rows_sorted)

    print(f"\n  Exported {len(export_rows_sorted)} rows to:")
    print(f"    {output_path}")

    # ── Summary ──────────────────────────────────────────────────────────
    total_rev = sum(r["Revenue"] for r in export_rows_sorted)
    print(f"\n{'='*60}")
    print(f"  Export complete")
    print(f"  Campaigns: {len(export_rows_sorted)}")
    print(f"  Total Revenue: ${total_rev:,.2f}")
    print(f"  Revenue field: Shopify current_subtotal_price")
    print(f"    (order-level merchandise net of discounts, excl tax/shipping)")
    print(f"{'='*60}")

    return output_path


if __name__ == "__main__":
    run_export()
