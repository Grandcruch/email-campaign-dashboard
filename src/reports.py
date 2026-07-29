"""
reports.py — Generate weekly, monthly, and producer performance reports + QA outputs.

Includes efficiency-vs-scale best/worst logic with Delivered >= 50 threshold,
dual producer report (current-to-date + finalized-only), and Code=None exclusions.
"""

import os
import re
import csv
from datetime import date, timedelta
from dataclasses import dataclass, field

import pandas as pd

from .config import (
    OUTPUT_DIR,
    DATA_START_DATE,
    KNOWN_NON_CAMPAIGN_CODE_PATTERNS,
    KNOWN_NON_CAMPAIGN_CODE_PREFIXES,
)
from .hubspot import CampaignRecord
from .shopify_orders import CampaignAttribution


# Minimum delivered threshold for efficiency rankings
MIN_DELIVERED_THRESHOLD = 50


# ─── Assembled campaign row (HubSpot + Shopify joined) ──────────────────────

@dataclass
class DashboardRow:
    """One row of the campaign detail table."""
    campaign_name: str = ""
    parsed_send_date: date | None = None
    producer_topic: str = ""
    campaign_type: str = ""
    offer_value: str = ""
    discount_code: str | None = None
    attribution_window_days: int = 7
    attribution_window_end: date | None = None
    # HubSpot
    delivered: int = 0
    opened: int = 0
    clicked: int = 0
    subject: str = ""
    # Shopify — None means "not applicable" (null)
    attributed_revenue: float | None = None
    discount_value: float | None = None
    total_order_value: float | None = None
    total_order_subtotal: float | None = None  # current_subtotal_price sum (merch net of discounts, excl tax/shipping)
    discounted_orders: int | None = None
    revenue_per_delivered: float | None = None
    # UTM-influenced (secondary attribution) — same null-vs-zero semantics:
    # None = not attempted (EDU/CONTENT/no code), 0 = attempted with no matches
    influenced_orders: int | None = None
    influenced_offer_revenue: float | None = None
    influenced_total_sales: float | None = None
    # Metadata
    is_final_snapshot: bool = False
    is_family_key: bool = False
    is_bin_holiday: bool = False
    qa_bucket: str = ""
    run_date: date | None = None
    hubspot_v3_email_id: str = ""
    hubspot_v1_campaign_id: str = ""


def assemble_dashboard_rows(
    records: list[CampaignRecord],
    attributions: dict[str, CampaignAttribution],
    run_date: date,
) -> list[DashboardRow]:
    """
    Join HubSpot campaign records with Shopify attribution results.
    Returns list of DashboardRow for all main-table-eligible campaigns.
    """
    rows: list[DashboardRow] = []
    main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                    "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}

    for rec in records:
        p = rec.parsed
        if p.qa_bucket not in main_buckets:
            continue

        row = DashboardRow(
            campaign_name=p.raw_name,
            parsed_send_date=p.parsed_send_date,
            producer_topic=p.producer_topic or "",
            campaign_type=p.campaign_type or "",
            offer_value=p.offer_value or "",
            discount_code=p.discount_code,
            attribution_window_days=p.attribution_window_days,
            attribution_window_end=p.attribution_window_end,
            delivered=rec.delivered,
            opened=rec.opened,
            clicked=rec.clicked,
            subject=rec.subject,
            hubspot_v3_email_id=rec.hubspot_v3_email_id,
            hubspot_v1_campaign_id=rec.hubspot_v1_campaign_id,
            run_date=run_date,
            qa_bucket=p.qa_bucket,
            is_family_key=p.is_family_key,
            is_bin_holiday=p.is_bin_holiday,
        )

        # is_final_snapshot
        if p.attribution_window_end and run_date >= p.attribution_window_end:
            row.is_final_snapshot = True

        # Shopify metrics — null for codeless campaigns
        if p.discount_code is None:
            # null = attribution not applicable (no discount code)
            row.attributed_revenue = None
            row.discount_value = None
            row.total_order_value = None
            row.total_order_subtotal = None
            row.discounted_orders = None
            row.revenue_per_delivered = None
            row.influenced_orders = None
            row.influenced_offer_revenue = None
            row.influenced_total_sales = None
        else:
            # Look up by compound key "code|send_date" first (supports
            # multiple campaigns sharing the same code with different windows).
            # Fall back to code-only key for backward compatibility.
            compound_key = f"{p.discount_code.lower()}|{p.parsed_send_date}"
            attr = attributions.get(compound_key)
            if attr is None:
                attr = attributions.get(p.discount_code.lower())
            if attr and attr.discounted_orders > 0:
                row.attributed_revenue = round(attr.attributed_revenue, 2)
                row.discount_value = round(attr.discount_value, 2)
                row.total_order_value = round(attr.total_order_value, 2)
                row.total_order_subtotal = round(attr.total_order_subtotal, 2)
                row.discounted_orders = attr.discounted_orders
                if rec.delivered > 0:
                    row.revenue_per_delivered = round(attr.attributed_revenue / rec.delivered, 4)
                else:
                    row.revenue_per_delivered = 0.0
            else:
                # Attribution attempted, zero orders
                row.attributed_revenue = 0.0
                row.discount_value = 0.0
                row.total_order_value = 0.0
                row.total_order_subtotal = 0.0
                row.discounted_orders = 0
                row.revenue_per_delivered = 0.0
                if p.qa_bucket == "OK":
                    row.qa_bucket = "OK_NO_ORDERS"
                    p.qa_bucket = "OK_NO_ORDERS"

            # UTM-influenced metrics — populated whenever attribution ran,
            # independent of whether any code-attributed orders were found
            if attr is not None:
                row.influenced_orders = getattr(attr, "influenced_orders", 0)
                row.influenced_offer_revenue = round(
                    getattr(attr, "influenced_offer_revenue", 0.0), 2)
                row.influenced_total_sales = round(
                    getattr(attr, "influenced_total_sales", 0.0), 2)
            else:
                row.influenced_orders = 0
                row.influenced_offer_revenue = 0.0
                row.influenced_total_sales = 0.0

        # WINDOW_OPEN check
        if not row.is_final_snapshot and row.qa_bucket in ("OK", "OK_OVERRIDE"):
            row.qa_bucket = "WINDOW_OPEN"
            p.qa_bucket = "WINDOW_OPEN"

        rows.append(row)

    return rows


# ─── DataFrame helpers ───────────────────────────────────────────────────────

def rows_to_dataframe(rows: list[DashboardRow]) -> pd.DataFrame:
    """Convert DashboardRow list to a pandas DataFrame."""
    data = []
    for r in rows:
        data.append({
            "Campaign Name": r.campaign_name,
            "Parsed Send Date": r.parsed_send_date,
            "Producer / Topic": r.producer_topic,
            "Campaign Type": r.campaign_type,
            "Offer Value": r.offer_value,
            "Discount Code": r.discount_code if r.discount_code else "None",
            "Delivered": r.delivered,
            "Opened": r.opened,
            "Clicked": r.clicked,
            "Subject": r.subject,
            "Attributed Revenue": r.attributed_revenue,
            "Discount Value": r.discount_value,
            "Total Sales": r.total_order_value,
            "Order Subtotal": r.total_order_subtotal,
            "Discounted Orders": r.discounted_orders,
            "Revenue per Delivered": r.revenue_per_delivered,
            "Influenced Orders": r.influenced_orders,
            "Influenced Offer Revenue": r.influenced_offer_revenue,
            "Influenced Total Sales": r.influenced_total_sales,
            "Attribution Window Days": r.attribution_window_days,
            "Attribution Window End": r.attribution_window_end,
            "is_final_snapshot": r.is_final_snapshot,
            "is_family_key": r.is_family_key,
            "is_bin_holiday": r.is_bin_holiday,
            "QA Bucket": r.qa_bucket,
            "Run Date": r.run_date,
            "HubSpot v3 Email ID": r.hubspot_v3_email_id,
            "HubSpot v1 Campaign ID": r.hubspot_v1_campaign_id,
        })
    df = pd.DataFrame(data)
    if not df.empty:
        df.sort_values(["Parsed Send Date", "Campaign Name"], ascending=[False, True], inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


# ─── BIN Sale grouping ────────────────────────────────────────────────────────

def apply_bin_grouping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine BIN Sale + BIN Sale Reminder rows that share the same discount
    family into a single reporting row.

    Grouping key: (Discount Code, send_week_monday) where is_family_key=True.

    For HubSpot metrics (Delivered, Opened, Clicked): SUM across group.
    For Shopify metrics (Attributed Revenue, Total Sales, etc.): take the
    values from the row with the latest Attribution Window End to avoid
    double-counting from overlapping attribution windows.
    Revenue per Delivered is recomputed from combined values.

    Non-family rows pass through unchanged.
    """
    if df.empty:
        return df.copy()

    # If no is_family_key column or no family rows, return as-is
    if "is_family_key" not in df.columns:
        return df.copy()

    family_mask = df["is_family_key"] == True
    if not family_mask.any():
        return df.copy()

    non_family = df[~family_mask].copy()
    family = df[family_mask].copy()

    grouped_rows = []
    for code, grp in family.groupby("Discount Code"):
        if len(grp) == 1:
            # Single row — no grouping needed, pass through
            grouped_rows.append(grp.iloc[0].to_dict())
            continue

        # Sum HubSpot delivery metrics
        total_delivered = grp["Delivered"].sum()
        total_opened = grp["Opened"].sum()
        total_clicked = grp["Clicked"].sum()

        # Sum Shopify metrics across all rows in the group.
        # Each campaign has its own attribution window and its own separate
        # Shopify API call, so order sets do not overlap.
        rev_rows = grp[grp["Attributed Revenue"].notna()].copy()
        if not rev_rows.empty:
            attr_rev = rev_rows["Attributed Revenue"].sum()
            total_sales = rev_rows["Total Sales"].sum()
            discount_val = rev_rows["Discount Value"].sum()
            disc_orders = rev_rows["Discounted Orders"].sum()
        else:
            attr_rev = None
            total_sales = None
            discount_val = None
            disc_orders = None

        # Recompute efficiency
        rpd = None
        if attr_rev is not None and total_delivered > 0:
            rpd = round(attr_rev / total_delivered, 4)

        # Build combined row from the first row as template
        combined = grp.iloc[0].to_dict()
        campaign_names = sorted(grp["Campaign Name"].unique())
        earliest_date = grp["Parsed Send Date"].min()

        combined["Campaign Name"] = f"{earliest_date} BIN Sale Group ({code})"
        combined["Parsed Send Date"] = earliest_date
        combined["Delivered"] = total_delivered
        combined["Opened"] = total_opened
        combined["Clicked"] = total_clicked
        combined["Attributed Revenue"] = attr_rev
        combined["Total Sales"] = total_sales
        combined["Discount Value"] = discount_val
        combined["Discounted Orders"] = disc_orders
        combined["Revenue per Delivered"] = rpd
        combined["Attribution Window End"] = grp["Attribution Window End"].max()
        combined["is_final_snapshot"] = grp["is_final_snapshot"].all()
        combined["_grouped_from"] = " | ".join(campaign_names)

        grouped_rows.append(combined)

    if grouped_rows:
        grouped_df = pd.DataFrame(grouped_rows)
        # Drop temp columns
        for col in ["_send_dt", "_week_monday", "_awe"]:
            if col in grouped_df.columns:
                grouped_df.drop(columns=[col], inplace=True)
        result = pd.concat([non_family, grouped_df], ignore_index=True)
    else:
        result = non_family.copy()

    if not result.empty:
        result.sort_values(["Parsed Send Date", "Campaign Name"], ascending=[False, True], inplace=True)
        result.reset_index(drop=True, inplace=True)

    return result


# ─── A/B test grouping ────────────────────────────────────────────────────────

_VERSION_SUFFIX_RE = re.compile(
    r'\s+(?:Version\s+[A-Z]|V\d+)\s*$', re.IGNORECASE,
)
_VINTAGE_RE = re.compile(r'\b((?:19|20)\d{2})\b')


def _build_ab_combined_name(names: list[str], types: list[str]) -> tuple[str, str]:
    """
    Build a merged campaign name and type from A/B test versions.
    Returns (combined_name, combined_type).
    """
    parts_list = [n.split(" - ", 4) for n in names]

    date_str = parts_list[0][0].strip()

    # Types — keep PROD first when mixed
    unique_types = list(dict.fromkeys(types))
    if "PROD" in unique_types and unique_types[0] != "PROD":
        unique_types.remove("PROD")
        unique_types.insert(0, "PROD")
    combined_type = "/".join(unique_types)

    offer_value = parts_list[0][3].strip() if len(parts_list[0]) >= 5 else ""
    code = parts_list[0][4].strip() if len(parts_list[0]) >= 5 else ""

    # Clean producer/topic: strip version suffixes
    raw_topics = [p[1].strip() if len(p) >= 2 else "" for p in parts_list]
    cleaned = [_VERSION_SUFFIX_RE.sub("", t).strip() for t in raw_topics]
    unique_cleaned = list(dict.fromkeys(cleaned))

    if len(unique_cleaned) == 1:
        combined_topic = unique_cleaned[0]
    else:
        # Extract vintages and compare de-vintaged base names
        all_vintages: list[str] = []
        bases: list[str] = []
        for topic in unique_cleaned:
            vints = _VINTAGE_RE.findall(topic)
            all_vintages.extend(vints)
            base = _VINTAGE_RE.sub("", topic).strip()
            base = re.sub(r'^[/\s]+', '', base)
            base = re.sub(r'\s+', ' ', base)
            bases.append(base)

        unique_vintages = sorted(set(all_vintages))
        unique_bases = list(dict.fromkeys(bases))

        if len(unique_bases) == 1:
            # Same base, different vintages → "1997/2015 Phelps Insignia"
            vint_str = "/".join(unique_vintages)
            combined_topic = f"{vint_str} {unique_bases[0]}".strip()
        else:
            # Different bases → find common word prefix, combine suffixes with "&"
            vint_str = "/".join(unique_vintages) if unique_vintages else ""
            words_list = [b.split() for b in unique_bases]
            min_words = min(len(w) for w in words_list)
            common_words: list[str] = []
            for i in range(min_words):
                if all(w[i] == words_list[0][i] for w in words_list):
                    common_words.append(words_list[0][i])
                else:
                    break
            common = " ".join(common_words)
            suffixes = [b[len(common):].strip() for b in unique_bases]
            suffixes = [s for s in suffixes if s]
            if common and suffixes:
                base_combined = f"{common} {' & '.join(suffixes)}"
            elif common:
                base_combined = common
            else:
                base_combined = " & ".join(unique_bases)
            combined_topic = f"{vint_str} {base_combined}".strip() if vint_str else base_combined

    return f"{date_str} - {combined_topic} - {combined_type} - {offer_value} - {code}", combined_type


def apply_ab_grouping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine A/B test campaigns that share the same discount code and send date
    into single reporting rows.

    Detection: same (Discount Code, Parsed Send Date) with >1 row.
    HubSpot metrics: SUM.  Shopify metrics: SUM (same code → shared orders).
    Revenue per Delivered is recomputed from combined values.
    Non-AB rows pass through unchanged.
    """
    if df.empty:
        return df.copy()

    # Only consider rows with a real discount code
    has_code = (df["Discount Code"] != "None") & df["Discount Code"].notna()
    coded = df[has_code].copy()
    no_code = df[~has_code].copy()

    if coded.empty:
        return df.copy()

    # Find groups with >1 row sharing the same code + date
    coded["_group_key"] = (
        coded["Discount Code"].str.lower() + "|" +
        coded["Parsed Send Date"].astype(str)
    )
    group_sizes = coded.groupby("_group_key").size()
    ab_keys = set(group_sizes[group_sizes > 1].index)

    if not ab_keys:
        return df.copy()

    is_ab = coded["_group_key"].isin(ab_keys)
    non_ab = coded[~is_ab].drop(columns=["_group_key"])
    ab_rows = coded[is_ab]

    merged_rows: list[dict] = []
    for gk, grp in ab_rows.groupby("_group_key"):
        # Sum HubSpot delivery metrics
        total_delivered = grp["Delivered"].sum()
        total_opened = grp["Opened"].sum()
        total_clicked = grp["Clicked"].sum()

        # Sum Shopify metrics (shared code, but each version may have delivered
        # to different audience segments; attribution is code-level so we keep
        # the same Shopify figures — take from the row with attribution data)
        rev_rows = grp[grp["Attributed Revenue"].notna()]
        if not rev_rows.empty:
            attr_rev = rev_rows.iloc[0]["Attributed Revenue"]
            total_sales = rev_rows.iloc[0]["Total Sales"]
            discount_val = rev_rows.iloc[0]["Discount Value"]
            disc_orders = rev_rows.iloc[0]["Discounted Orders"]
        else:
            attr_rev = None
            total_sales = None
            discount_val = None
            disc_orders = None

        rpd = None
        if attr_rev is not None and total_delivered > 0:
            rpd = round(attr_rev / total_delivered, 4)

        # Build combined name
        names = grp["Campaign Name"].tolist()
        types = grp["Campaign Type"].tolist()
        combined_name, combined_type = _build_ab_combined_name(names, types)

        combined = grp.iloc[0].to_dict()
        combined["Campaign Name"] = combined_name
        combined["Campaign Type"] = combined_type
        combined["Parsed Send Date"] = grp["Parsed Send Date"].min()
        combined["Delivered"] = total_delivered
        combined["Opened"] = total_opened
        combined["Clicked"] = total_clicked
        combined["Attributed Revenue"] = attr_rev
        combined["Total Sales"] = total_sales
        combined["Discount Value"] = discount_val
        combined["Discounted Orders"] = disc_orders
        combined["Revenue per Delivered"] = rpd
        combined["Attribution Window End"] = grp["Attribution Window End"].max()
        combined["is_final_snapshot"] = grp["is_final_snapshot"].all()
        combined["_ab_grouped_from"] = " | ".join(sorted(names))
        merged_rows.append(combined)

    merged_df = pd.DataFrame(merged_rows)
    for col in ["_group_key"]:
        if col in merged_df.columns:
            merged_df.drop(columns=[col], inplace=True)

    result = pd.concat([no_code, non_ab, merged_df], ignore_index=True)
    if not result.empty:
        result.sort_values(
            ["Parsed Send Date", "Campaign Name"], ascending=[False, True], inplace=True,
        )
        result.reset_index(drop=True, inplace=True)
    return result


# ─── Weekly report ───────────────────────────────────────────────────────────

def generate_weekly_report(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """
    Weekly campaign report: Date, Discount Code, Campaign Name,
    Discounted Orders, Delivered, Attributed Revenue, Revenue per Delivered.
    """
    cols = [
        "Parsed Send Date", "Discount Code", "Campaign Name",
        "Discounted Orders", "Delivered", "Attributed Revenue",
        "Total Sales", "Revenue per Delivered",
        "Influenced Orders", "Influenced Offer Revenue", "Influenced Total Sales",
    ]
    available = [c for c in cols if c in df.columns]
    weekly = df[available].copy()
    weekly.rename(columns={"Parsed Send Date": "Date"}, inplace=True)
    return weekly


# ─── Best/Worst helpers ──────────────────────────────────────────────────────

def _campaign_insights_efficiency_scale(df: pd.DataFrame) -> list[str]:
    """
    Generate best/worst campaign insights by EFFICIENCY and SCALE.
    - Efficiency = Revenue per Delivered (threshold: Delivered >= 50)
    - Scale = Attributed Revenue
    - Code=None excluded from revenue-based comparisons.
    """
    insights: list[str] = []

    # Eligible: coded campaigns with non-null attributed revenue
    coded = df[
        (df["Discount Code"] != "None") &
        (df["Attributed Revenue"].notna())
    ].copy()

    if coded.empty:
        insights.append("[Campaigns] No coded campaigns with attribution data available.")
        return insights

    # ── EFFICIENCY (Revenue per Delivered) ─────────────────────────────────
    eff_pool = coded[coded["Revenue per Delivered"].notna()].copy()

    # Apply threshold
    eff_above = eff_pool[eff_pool["Delivered"] >= MIN_DELIVERED_THRESHOLD]
    threshold_met = len(eff_above) >= 1
    if threshold_met:
        eff_candidates = eff_above
        threshold_note = ""
    else:
        eff_candidates = eff_pool
        threshold_note = (
            f" (NOTE: fewer than 2 campaigns meet the Delivered >= {MIN_DELIVERED_THRESHOLD} "
            f"threshold; ranking all {len(eff_pool)} campaigns instead)"
        )

    if not eff_candidates.empty:
        best_eff = eff_candidates.loc[eff_candidates["Revenue per Delivered"].idxmax()]
        insights.append(
            f"Best campaign by EFFICIENCY: {best_eff['Campaign Name']} "
            f"(${best_eff['Revenue per Delivered']:.4f}/delivered, "
            f"{int(best_eff['Delivered'])} delivered, "
            f"${best_eff['Attributed Revenue']:,.2f} revenue){threshold_note}"
        )
        worst_eff = eff_candidates.loc[eff_candidates["Revenue per Delivered"].idxmin()]
        insights.append(
            f"Worst campaign by EFFICIENCY: {worst_eff['Campaign Name']} "
            f"(${worst_eff['Revenue per Delivered']:.4f}/delivered, "
            f"{int(worst_eff['Delivered'])} delivered, "
            f"${worst_eff['Attributed Revenue']:,.2f} revenue){threshold_note}"
        )
    else:
        insights.append("Best/Worst campaign by EFFICIENCY: No candidates with Revenue per Delivered data.")

    # ── SCALE (Attributed Revenue) ─────────────────────────────────────────
    best_scale = coded.loc[coded["Attributed Revenue"].idxmax()]
    insights.append(
        f"Best campaign by SCALE: {best_scale['Campaign Name']} "
        f"(${best_scale['Attributed Revenue']:,.2f} attributed revenue, "
        f"{int(best_scale['Delivered'])} delivered)"
    )
    worst_scale = coded.loc[coded["Attributed Revenue"].idxmin()]
    insights.append(
        f"Worst campaign by SCALE: {worst_scale['Campaign Name']} "
        f"(${worst_scale['Attributed Revenue']:,.2f} attributed revenue, "
        f"{int(worst_scale['Delivered'])} delivered)"
    )

    return insights


def _producer_insights(df: pd.DataFrame) -> list[str]:
    """
    Generate best/worst producer insights by EFFICIENCY and SCALE.
    - Efficiency = total Attributed Revenue / total Delivered (threshold: total Delivered >= 50)
    - Scale = total Attributed Revenue
    - Only producers with at least one coded campaign for worst-by-scale.
    """
    insights: list[str] = []

    # Use all main-table campaigns with non-null attributed revenue
    coded = df[
        (df["Discount Code"] != "None") &
        (df["Attributed Revenue"].notna())
    ].copy()

    if coded.empty:
        insights.append("[Producers] No coded campaigns with attribution data available.")
        return insights

    grouped = coded.groupby("Producer / Topic").agg(
        Total_Revenue=("Attributed Revenue", "sum"),
        Total_Delivered=("Delivered", "sum"),
        Campaign_Count=("Campaign Name", "count"),
    ).reset_index()

    grouped["Revenue per Delivered"] = (
        grouped["Total_Revenue"] / grouped["Total_Delivered"].replace(0, float("nan"))
    ).round(4)

    # ── EFFICIENCY ─────────────────────────────────────────────────────────
    eff_pool = grouped[grouped["Revenue per Delivered"].notna()].copy()
    eff_above = eff_pool[eff_pool["Total_Delivered"] >= MIN_DELIVERED_THRESHOLD]
    threshold_met = len(eff_above) >= 1

    if threshold_met:
        eff_candidates = eff_above
        threshold_note = ""
    else:
        eff_candidates = eff_pool
        threshold_note = (
            f" (NOTE: fewer than 2 producers meet Delivered >= {MIN_DELIVERED_THRESHOLD} "
            f"threshold; ranking all {len(eff_pool)} producers)"
        )

    if not eff_candidates.empty:
        best = eff_candidates.loc[eff_candidates["Revenue per Delivered"].idxmax()]
        insights.append(
            f"Best producer by EFFICIENCY: {best['Producer / Topic']} "
            f"(${best['Revenue per Delivered']:.4f}/delivered, "
            f"{int(best['Total_Delivered'])} total delivered, "
            f"${best['Total_Revenue']:,.2f} total revenue){threshold_note}"
        )
        worst = eff_candidates.loc[eff_candidates["Revenue per Delivered"].idxmin()]
        insights.append(
            f"Worst producer by EFFICIENCY: {worst['Producer / Topic']} "
            f"(${worst['Revenue per Delivered']:.4f}/delivered, "
            f"{int(worst['Total_Delivered'])} total delivered, "
            f"${worst['Total_Revenue']:,.2f} total revenue){threshold_note}"
        )
    else:
        insights.append("Best/Worst producer by EFFICIENCY: No candidates with data.")

    # ── SCALE ──────────────────────────────────────────────────────────────
    if not grouped.empty:
        best_s = grouped.loc[grouped["Total_Revenue"].idxmax()]
        insights.append(
            f"Best producer by SCALE: {best_s['Producer / Topic']} "
            f"(${best_s['Total_Revenue']:,.2f} total attributed revenue, "
            f"{int(best_s['Campaign_Count'])} campaigns)"
        )
        worst_s = grouped.loc[grouped["Total_Revenue"].idxmin()]
        insights.append(
            f"Worst producer by SCALE: {worst_s['Producer / Topic']} "
            f"(${worst_s['Total_Revenue']:,.2f} total attributed revenue, "
            f"{int(worst_s['Campaign_Count'])} campaigns)"
        )

    return insights


def generate_weekly_insights(df: pd.DataFrame) -> list[str]:
    """Generate plain-text performance insights from the weekly data."""
    insights: list[str] = []

    # ── CAMPAIGN PERFORMANCE ───────────────────────────────────────────────
    insights.append("")
    insights.append("=== CAMPAIGN PERFORMANCE ===")
    insights.extend(_campaign_insights_efficiency_scale(df))

    # ── PRODUCER PERFORMANCE ───────────────────────────────────────────────
    insights.append("")
    insights.append("=== PRODUCER PERFORMANCE ===")
    insights.extend(_producer_insights(df))

    # ── ADDITIONAL SIGNALS ─────────────────────────────────────────────────
    insights.append("")
    insights.append("=== ADDITIONAL SIGNALS ===")

    prod = df[(df["Campaign Type"] == "PROD") & (df["Discount Code"] != "None")].copy()
    all_prod = prod[prod["Attributed Revenue"].notna()]

    # Strong delivery, weak monetization
    if not all_prod.empty and len(all_prod) >= 2:
        med_del = all_prod["Delivered"].median()
        med_rev = all_prod["Attributed Revenue"].median()
        weak = all_prod[
            (all_prod["Delivered"] > med_del) &
            (all_prod["Attributed Revenue"] <= med_rev)
        ]
        for _, r in weak.iterrows():
            insights.append(
                f"Strong delivery / weak monetization: {r['Campaign Name']} "
                f"({r['Delivered']} delivered, ${r['Attributed Revenue']:,.2f} revenue)"
            )

    # Unused codes
    unused = prod[prod["Discounted Orders"].notna() & (prod["Discounted Orders"] == 0)]
    for _, r in unused.iterrows():
        insights.append(f"Unused code: {r['Discount Code']} ({r['Campaign Name']})")

    # Open windows
    open_w = df[df["QA Bucket"] == "WINDOW_OPEN"]
    if not open_w.empty:
        names = open_w["Campaign Name"].tolist()
        insights.append(f"Open attribution windows ({len(names)}): {', '.join(names[:10])}")

    return insights


# ─── Monthly report ──────────────────────────────────────────────────────────

def generate_monthly_report(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Monthly report by discount code — only finalized campaigns."""
    monthly = df[
        (df["Parsed Send Date"].notna()) &
        (df["is_final_snapshot"] == True) &
        (df["Discount Code"] != "None") &
        (df["Attributed Revenue"].notna())
    ].copy()

    # Filter to target month
    monthly["_month"] = pd.to_datetime(monthly["Parsed Send Date"]).dt.month
    monthly["_year"] = pd.to_datetime(monthly["Parsed Send Date"]).dt.year
    monthly = monthly[(monthly["_year"] == year) & (monthly["_month"] == month)]

    if monthly.empty:
        return pd.DataFrame()

    grouped = monthly.groupby("Discount Code").agg(
        Campaign_Count=("Campaign Name", "count"),
        Campaign_Names=("Campaign Name", lambda x: " | ".join(x)),
        Total_Attributed_Revenue=("Attributed Revenue", "sum"),
        Total_Sales=("Total Sales", "sum"),
        Total_Discounted_Orders=("Discounted Orders", "sum"),
        Total_Discount_Value=("Discount Value", "sum"),
        Total_Delivered=("Delivered", "sum"),
    ).reset_index()

    grouped["Avg Revenue per Campaign"] = (
        grouped["Total_Attributed_Revenue"] / grouped["Campaign_Count"]
    ).round(2)
    grouped["Avg Revenue per Delivered"] = (
        grouped["Total_Attributed_Revenue"] / grouped["Total_Delivered"].replace(0, float("nan"))
    ).round(4)

    grouped.sort_values("Total_Attributed_Revenue", ascending=False, inplace=True)
    grouped.reset_index(drop=True, inplace=True)

    return grouped


# ─── Producer performance report ─────────────────────────────────────────────

def _build_producer_grouped(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build grouped producer performance from a pre-filtered DataFrame.

    Producer identity is determined by Discount Code (the source of truth),
    NOT by the Producer/Topic portion of the email name. One discount code
    == one producer. Codes are expected to be non-null here (caller filters
    on Attributed Revenue notna, which excludes EDU/CONTENT/Code=None).
    """
    if source_df.empty:
        return pd.DataFrame()

    # Drop any rows without a usable discount code (defensive — EDU/CONTENT
    # should already be filtered out by the caller).
    source_df = source_df[
        source_df["Discount Code"].notna() &
        (source_df["Discount Code"].astype(str) != "") &
        (source_df["Discount Code"] != "None")
    ].copy()
    if source_df.empty:
        return pd.DataFrame()

    grouped = source_df.groupby("Discount Code").agg(
        Campaign_Count=("Campaign Name", "count"),
        Total_Attributed_Revenue=("Attributed Revenue", "sum"),
        Total_Sales=("Total Sales", "sum"),
        Total_Discounted_Orders=("Discounted Orders", "sum"),
        Total_Delivered=("Delivered", "sum"),
    ).reset_index().rename(columns={"Discount Code": "Producer"})

    grouped["Revenue per Delivered"] = (
        grouped["Total_Attributed_Revenue"] / grouped["Total_Delivered"].replace(0, float("nan"))
    ).round(4)
    grouped["Avg Revenue per Campaign"] = (
        grouped["Total_Attributed_Revenue"] / grouped["Campaign_Count"]
    ).round(2)

    # Best / worst campaign per producer (grouping by discount code)
    best_map = {}
    worst_map = {}
    for code, grp in source_df.groupby("Discount Code"):
        rev = grp[grp["Attributed Revenue"] > 0]
        if not rev.empty:
            best_map[code] = rev.loc[rev["Attributed Revenue"].idxmax(), "Campaign Name"]
            worst_map[code] = rev.loc[rev["Attributed Revenue"].idxmin(), "Campaign Name"]
        else:
            best_map[code] = "N/A"
            worst_map[code] = "N/A"

    grouped["Best Campaign"] = grouped["Producer"].map(best_map)
    grouped["Worst Campaign"] = grouped["Producer"].map(worst_map)

    grouped.sort_values("Total_Attributed_Revenue", ascending=False, inplace=True)
    grouped.reset_index(drop=True, inplace=True)

    return grouped


def generate_producer_report(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Producer performance — returns TWO DataFrames:
      1. current-to-date: all main-table campaigns with non-null attributed revenue
      2. finalized-only: only is_final_snapshot == True campaigns
    """
    # Current-to-date: include ALL main-table campaigns with attributed revenue
    current_source = df[df["Attributed Revenue"].notna()].copy()
    current_df = _build_producer_grouped(current_source)
    if not current_df.empty:
        current_df.insert(0, "View", "current-to-date")

    # Finalized only
    final_source = df[
        (df["is_final_snapshot"] == True) &
        (df["Attributed Revenue"].notna())
    ].copy()
    final_df = _build_producer_grouped(final_source)
    if not final_df.empty:
        final_df.insert(0, "View", "finalized-only")

    return current_df, final_df


def generate_producer_analytics(
    df: pd.DataFrame,
    producer_map: dict[str, str],
    offer_type_map: dict[str, str],
    tier_map: dict[str, str],
    top_region_map: dict[str, str],
    sub_region_map: dict[str, str],
) -> dict[str, pd.DataFrame]:
    """
    Build producer analytics from finalized campaigns only.

    Returns a dict with keys:
      producer_detail_df  — per-producer revenue, excl. Other- / BIN / Holiday / etc.
      overview_df         — same aggregation but including Other- buckets
      campaign_detail_df  — per-campaign rows with producer/tier/region columns attached
      unmapped_df         — codes that resolved to neither a family key nor any producer row
      tier_detail_df      — revenue rolled up by tier
      region_detail_df    — revenue rolled up by top-region + sub-region
    """
    from .producer_mapping import (
        EXCLUDED_OFFER_TYPES,
        INCLUDED_OFFER_TYPES,
        UNMAPPED_LABEL,
        resolve_producer,
        get_offer_type,
        resolve_tier,
        resolve_region,
    )

    # Finalized campaigns with attribution attempted (non-null Attributed Revenue)
    base = df[(df["is_final_snapshot"] == True) & df["Attributed Revenue"].notna()].copy()

    if base.empty:
        empty = pd.DataFrame()
        return {
            "producer_detail_df": empty,
            "overview_df": empty,
            "campaign_detail_df": empty,
            "unmapped_df": empty,
            "tier_detail_df": empty,
            "region_detail_df": empty,
        }

    def _enrich(row):
        code = str(row.get("Discount Code") or "").strip()
        producer = resolve_producer(code, producer_map)
        offer_type = get_offer_type(code, offer_type_map)
        if producer is None:
            producer = UNMAPPED_LABEL
        tier = resolve_tier(producer, tier_map) if producer != UNMAPPED_LABEL else "Untiered"
        top_region, sub_region = resolve_region(producer, top_region_map, sub_region_map) \
            if producer != UNMAPPED_LABEL else ("Unmapped Region", "Unmapped Region")
        return pd.Series({
            "_producer": producer,
            "_offer_type": offer_type,
            "_tier": tier,
            "_top_region": top_region,
            "_sub_region": sub_region,
        })

    enriched = base.join(base.apply(_enrich, axis=1))

    # campaign_detail_df — all finalized rows with enriched columns
    campaign_detail_df = enriched.rename(columns={
        "_producer": "Producer",
        "_offer_type": "Offer Type",
        "_tier": "Tier",
        "_top_region": "Top Region",
        "_sub_region": "Sub-Region",
    }).copy()

    # unmapped_df — codes that landed on UNMAPPED_LABEL
    unmapped_rows = enriched[enriched["_producer"] == UNMAPPED_LABEL]
    if not unmapped_rows.empty:
        unmapped_df = unmapped_rows[["Campaign Name", "Discount Code", "Attributed Revenue",
                                     "Delivered", "Parsed Send Date"]].copy()
    else:
        unmapped_df = pd.DataFrame(columns=["Campaign Name", "Discount Code",
                                             "Attributed Revenue", "Delivered", "Parsed Send Date"])

    def _agg_by_producer(subset: pd.DataFrame) -> pd.DataFrame:
        rev_col = "Attributed Revenue"
        disc_col = "Discount Value"
        orders_col = "Discounted Orders"
        delivered_col = "Delivered"
        grouped = (
            subset.groupby("_producer", dropna=False)
            .agg(
                Total_Attributed_Revenue=(rev_col, "sum"),
                Total_Discount_Value=(disc_col, lambda s: s.fillna(0).sum()),
                Total_Discounted_Orders=(orders_col, lambda s: s.fillna(0).sum()),
                Total_Delivered=(delivered_col, "sum"),
                Campaign_Count=("Campaign Name", "count"),
            )
            .reset_index()
            .rename(columns={"_producer": "Producer"})
        )
        grouped["Revenue per Delivered"] = (
            grouped["Total_Attributed_Revenue"] / grouped["Total_Delivered"].replace(0, pd.NA)
        )
        grouped.sort_values("Total_Attributed_Revenue", ascending=False, inplace=True)
        grouped.reset_index(drop=True, inplace=True)
        return grouped

    # producer_detail_df — only INCLUDED offer types (excludes Other- buckets)
    included_mask = enriched["_offer_type"].isin(INCLUDED_OFFER_TYPES)
    producer_detail_df = _agg_by_producer(enriched[included_mask])

    # overview_df — all offer types (includes Other- bucket rows)
    overview_df = _agg_by_producer(enriched)

    # tier_detail_df
    rev_col, disc_col, orders_col, delivered_col = (
        "Attributed Revenue", "Discount Value", "Discounted Orders", "Delivered"
    )
    tier_subset = enriched[included_mask].copy()
    if not tier_subset.empty:
        tier_detail_df = (
            tier_subset.groupby("_tier", dropna=False)
            .agg(
                Total_Attributed_Revenue=(rev_col, "sum"),
                Total_Discount_Value=(disc_col, lambda s: s.fillna(0).sum()),
                Total_Discounted_Orders=(orders_col, lambda s: s.fillna(0).sum()),
                Total_Delivered=(delivered_col, "sum"),
                Producer_Count=("_producer", "nunique"),
            )
            .reset_index()
            .rename(columns={"_tier": "Tier"})
        )
        tier_detail_df["Revenue per Delivered"] = (
            tier_detail_df["Total_Attributed_Revenue"]
            / tier_detail_df["Total_Delivered"].replace(0, pd.NA)
        )
        tier_detail_df.sort_values("Total_Attributed_Revenue", ascending=False, inplace=True)
        tier_detail_df.reset_index(drop=True, inplace=True)
    else:
        tier_detail_df = pd.DataFrame()

    # region_detail_df
    if not tier_subset.empty:
        region_detail_df = (
            tier_subset.groupby(["_top_region", "_sub_region"], dropna=False)
            .agg(
                Total_Attributed_Revenue=(rev_col, "sum"),
                Total_Discount_Value=(disc_col, lambda s: s.fillna(0).sum()),
                Total_Discounted_Orders=(orders_col, lambda s: s.fillna(0).sum()),
                Total_Delivered=(delivered_col, "sum"),
                Producer_Count=("_producer", "nunique"),
            )
            .reset_index()
            .rename(columns={"_top_region": "Top Region", "_sub_region": "Sub-Region"})
        )
        region_detail_df["Revenue per Delivered"] = (
            region_detail_df["Total_Attributed_Revenue"]
            / region_detail_df["Total_Delivered"].replace(0, pd.NA)
        )
        region_detail_df.sort_values("Total_Attributed_Revenue", ascending=False, inplace=True)
        region_detail_df.reset_index(drop=True, inplace=True)
    else:
        region_detail_df = pd.DataFrame()

    return {
        "producer_detail_df": producer_detail_df,
        "overview_df": overview_df,
        "campaign_detail_df": campaign_detail_df,
        "unmapped_df": unmapped_df,
        "tier_detail_df": tier_detail_df,
        "region_detail_df": region_detail_df,
    }


# ─── QA: Excluded campaigns ─────────────────────────────────────────────────

def generate_excluded_campaigns(records: list[CampaignRecord]) -> pd.DataFrame:
    """List all campaigns excluded from the main table."""
    excluded_buckets = {"PARSE_ERROR", "LEGACY_FORMAT", "STATS_UNAVAILABLE", "FORCE_EXCLUDED"}
    rows = []
    for rec in records:
        if rec.parsed.qa_bucket not in excluded_buckets:
            continue
        rows.append({
            "Campaign Name": rec.parsed.raw_name,
            "QA Bucket": rec.parsed.qa_bucket,
            "Parsed Date": str(rec.parsed.parsed_send_date) if rec.parsed.parsed_send_date else "",
            "Producer / Topic": rec.parsed.producer_topic or "",
            "Exclusion Reason": _exclusion_reason(rec.parsed.qa_bucket),
            "HubSpot v3 Email ID": rec.hubspot_v3_email_id,
        })
    return pd.DataFrame(rows)


def _exclusion_reason(bucket: str) -> str:
    reasons = {
        "PARSE_ERROR": "Name does not match expected 5-segment format or has invalid campaign type.",
        "LEGACY_FORMAT": "Old naming convention (fewer than 5 segments). Use campaign_overrides.csv to include.",
        "STATS_UNAVAILABLE": "No v1 campaign resolved with delivered > 0. Campaign may not have been sent yet.",
        "FORCE_EXCLUDED": "Excluded via force_exclude in campaign_overrides.csv.",
    }
    return reasons.get(bucket, "Unknown")


# ─── QA: Unmatched Shopify discount codes ────────────────────────────────────

def generate_unmatched_codes_report(
    shopify_code_map: dict[str, list[dict]],
    campaign_codes: set[str],
) -> pd.DataFrame:
    """
    Compare all Shopify discount codes against known campaign codes.
    Report unmatched codes with order counts and classification.
    """
    rows = []
    for code_lower, orders in shopify_code_map.items():
        if code_lower in campaign_codes:
            continue  # matched — not unmatched

        code_original = orders[0]["code_original"] if orders else code_lower
        total_disc = sum(o["discount_amount"] for o in orders)
        total_val = sum(o["total_price"] for o in orders)
        order_names = [o["order_name"] for o in orders[:5]]
        dates = sorted([o["created_at"] for o in orders])

        rows.append({
            "Discount Code": code_original,
            "Order Count": len(orders),
            "Total Discount Amount": round(total_disc, 2),
            "Total Order Value": round(total_val, 2),
            "Earliest Order": dates[0] if dates else "",
            "Latest Order": dates[-1] if dates else "",
            "Sample Order IDs": ", ".join(order_names),
            "Possible Reason": _classify_unmatched_code(code_lower),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values("Order Count", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


def _classify_unmatched_code(code_lower: str) -> str:
    if code_lower in KNOWN_NON_CAMPAIGN_CODE_PATTERNS:
        return "non-campaign code (known pattern)"
    for prefix in KNOWN_NON_CAMPAIGN_CODE_PREFIXES:
        if code_lower.startswith(prefix):
            return "non-campaign code (known pattern)"
    return "unknown - review manually"


# ─── History table ───────────────────────────────────────────────────────────

HISTORY_FILE = os.path.join(OUTPUT_DIR, "campaign_history.csv")
HISTORY_COLUMNS = [
    "Parsed Send Date", "Discount Code", "Campaign Name",
    "Producer / Topic", "Campaign Type", "Offer Value",
    "Attribution Window Days", "Delivered", "Opened", "Clicked",
    "Attributed Revenue", "Discount Value", "Total Sales",
    "Discounted Orders", "Revenue per Delivered",
    "Influenced Orders", "Influenced Offer Revenue", "Influenced Total Sales",
    "is_final_snapshot", "Run Date", "Attribution Window End",
    "QA Bucket", "HubSpot v3 Email ID", "HubSpot v1 Campaign ID",
]


def update_history(new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Load existing history, upsert new rows.
    - Skip rows where is_final_snapshot is already True
    - Update rows where is_final_snapshot is False
    - Insert new rows
    """
    if os.path.isfile(HISTORY_FILE):
        history = pd.read_csv(HISTORY_FILE, dtype=str)
        # Ensure all expected columns exist
        for col in HISTORY_COLUMNS:
            if col not in history.columns:
                history[col] = ""
    else:
        history = pd.DataFrame(columns=HISTORY_COLUMNS)

    # Build composite key
    def make_key(row):
        return f"{row.get('Parsed Send Date', '')}|{row.get('Discount Code', '')}|{row.get('Campaign Name', '')}"

    # Index existing history by key
    existing_keys: dict[str, int] = {}
    for idx, row in history.iterrows():
        key = make_key(row)
        existing_keys[key] = idx

    rows_to_append = []

    for _, new_row in new_df.iterrows():
        key = make_key(new_row)
        new_row_dict = new_row.to_dict()

        if key in existing_keys:
            hist_idx = existing_keys[key]
            # If already final, skip
            if str(history.at[hist_idx, "is_final_snapshot"]).lower() == "true":
                continue
            # Update in place
            for col in HISTORY_COLUMNS:
                if col in new_row_dict and pd.notna(new_row_dict.get(col)):
                    history.at[hist_idx, col] = str(new_row_dict[col]) if new_row_dict[col] is not None else ""
        else:
            # Convert all values to string for CSV consistency
            str_row = {}
            for col in HISTORY_COLUMNS:
                val = new_row_dict.get(col)
                str_row[col] = str(val) if val is not None else ""
            rows_to_append.append(str_row)

    if rows_to_append:
        append_df = pd.DataFrame(rows_to_append, columns=HISTORY_COLUMNS)
        history = pd.concat([history, append_df], ignore_index=True)

    return history


# ─── QA Summary ──────────────────────────────────────────────────────────────

def generate_qa_summary(
    records: list[CampaignRecord],
    rows: list[DashboardRow],
    unmatched_codes_df: pd.DataFrame,
) -> str:
    """Generate a plain-text QA summary."""
    total_fetched = len(records)

    # Count by bucket
    buckets: dict[str, int] = {}
    for rec in records:
        b = rec.parsed.qa_bucket
        buckets[b] = buckets.get(b, 0) + 1

    main_buckets = {"OK", "OK_NO_CODE", "OK_NO_ORDERS", "OK_OVERRIDE",
                    "DUPLICATE_CODE_WARNING", "WINDOW_OPEN"}
    excluded_buckets = {"PARSE_ERROR", "LEGACY_FORMAT", "STATS_UNAVAILABLE",
                        "FORCE_EXCLUDED", "DATE_OUT_OF_RANGE"}

    lines = [
        "=" * 60,
        "QA SUMMARY",
        "=" * 60,
        f"Total v3 emails in scope: {total_fetched}",
        "",
        "Main table:",
    ]
    for b in sorted(main_buckets):
        if buckets.get(b, 0) > 0:
            lines.append(f"  {b}: {buckets[b]}")

    lines.append("")
    lines.append("Excluded:")
    for b in sorted(excluded_buckets):
        if buckets.get(b, 0) > 0:
            names = [r.parsed.raw_name for r in records if r.parsed.qa_bucket == b]
            lines.append(f"  {b}: {buckets[b]}")
            for n in names[:10]:
                lines.append(f"    - {n}")

    if not unmatched_codes_df.empty:
        lines.append("")
        lines.append(f"Unmatched Shopify discount codes: {len(unmatched_codes_df)}")
        for _, r in unmatched_codes_df.head(10).iterrows():
            lines.append(f"  {r['Discount Code']}: {r['Order Count']} orders ({r['Possible Reason']})")

    lines.append("=" * 60)
    return "\n".join(lines)


# ─── Write all outputs ───────────────────────────────────────────────────────

def write_all_outputs(
    df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    weekly_insights: list[str],
    monthly_df: pd.DataFrame,
    producer_current_df: pd.DataFrame,
    producer_final_df: pd.DataFrame,
    history_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    unmatched_df: pd.DataFrame,
    qa_summary: str,
) -> None:
    """Write all CSV outputs to the output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Campaign detail table
    df.to_csv(os.path.join(OUTPUT_DIR, "campaign_detail.csv"), index=False)

    # Weekly report
    weekly_df.to_csv(os.path.join(OUTPUT_DIR, "weekly_campaign_report.csv"), index=False)

    # Weekly insights (with structured sections)
    with open(os.path.join(OUTPUT_DIR, "weekly_insights.txt"), "w") as f:
        f.write("WEEKLY PERFORMANCE INSIGHTS\n")
        f.write("=" * 60 + "\n")
        for insight in weekly_insights:
            if insight.startswith("==="):
                f.write(f"\n{insight}\n")
            elif insight == "":
                f.write("\n")
            else:
                f.write(f"  - {insight}\n")

    # Monthly report
    if not monthly_df.empty:
        monthly_df.to_csv(os.path.join(OUTPUT_DIR, "monthly_discount_report.csv"), index=False)
    else:
        pd.DataFrame(columns=[
            "Discount Code", "Campaign_Count", "Campaign_Names",
            "Total_Attributed_Revenue", "Total_Discounted_Orders",
            "Total_Discount_Value", "Avg Revenue per Campaign",
            "Avg Revenue per Delivered",
        ]).to_csv(os.path.join(OUTPUT_DIR, "monthly_discount_report.csv"), index=False)

    # Producer performance — combined file with both views
    producer_parts = []
    if not producer_current_df.empty:
        producer_parts.append(producer_current_df)
    if not producer_final_df.empty:
        producer_parts.append(producer_final_df)

    if producer_parts:
        combined = pd.concat(producer_parts, ignore_index=True)
        combined.to_csv(os.path.join(OUTPUT_DIR, "producer_performance_report.csv"), index=False)
    else:
        pd.DataFrame(columns=[
            "View", "Producer / Topic", "Campaign_Count", "Total_Attributed_Revenue",
            "Total_Discounted_Orders", "Total_Delivered", "Revenue per Delivered",
            "Avg Revenue per Campaign", "Best Campaign", "Worst Campaign",
        ]).to_csv(os.path.join(OUTPUT_DIR, "producer_performance_report.csv"), index=False)

    # History
    history_df.to_csv(HISTORY_FILE, index=False)

    # QA excluded campaigns
    excluded_df.to_csv(os.path.join(OUTPUT_DIR, "qa_excluded_campaigns.csv"), index=False)

    # QA unmatched codes
    unmatched_df.to_csv(os.path.join(OUTPUT_DIR, "qa_unmatched_shopify_codes.csv"), index=False)

    # QA summary text
    with open(os.path.join(OUTPUT_DIR, "qa_summary.txt"), "w") as f:
        f.write(qa_summary)

    print(f"\n  [output] All files written to {OUTPUT_DIR}/")
