# Email Campaign Dashboard — Logic Specification

This document defines every business rule, metric formula, matching rule, and decision point for the Grand Cru Liquid Assets email campaign performance dashboard. It is the authoritative reference for implementation and validation.

---

## 1. Locked Metric Definitions

These definitions are final and must not be changed without explicit business approval.

### 1.1 Primary Metrics

| Metric | Definition | Formula | Null Condition |
|---|---|---|---|
| **Attributed Revenue** | Pre-discount gross value of line items that actually received the campaign discount. This is the primary revenue metric for all campaign performance analysis. | `SUM(line_item.price * line_item.quantity)` for line items where `discount_allocations` contains an entry matching the campaign's `discount_application_index` | `null` if Code = None, EDU, or CONTENT |
| **Discount Value** | Actual dollar discount amount given to the customer on the discounted items. | `SUM(line_item.discount_allocations[campaign_app_index].amount)` across all matched orders | `null` if Code = None, EDU, or CONTENT |
| **Total Order Value** | Full order value of all orders containing the campaign's discount code. Includes discounted items, non-discounted items, tax, and shipping. Uses `order.total_price`. | `SUM(order.total_price)` across all matched orders | `null` if Code = None, EDU, or CONTENT |
| **Discounted Orders** | Count of orders that used the campaign's discount code within the attribution window. | `COUNT(matched_orders)` | `null` if Code = None, EDU, or CONTENT |
| **Delivered** | Number of emails successfully delivered. | `counters.delivered` from HubSpot v1 campaign detail | Never null for eligible campaigns |
| **Revenue per Delivered** | Revenue efficiency per delivered email. | `Attributed Revenue / Delivered` | `null` if Attributed Revenue is `null` |

### 1.2 Retired Metrics

| Metric | Status | Reason |
|---|---|---|
| **Discounted Sales** | REMOVED | Not a separate business metric. Caused confusion with Attributed Revenue. |
| **Discounted Revenue** | REMOVED | Was defined as `Attributed Revenue - Discount Value`. Not surfaced as a primary column. Can be derived internally if ever needed. |

### 1.3 Null vs Zero Semantics

| Value | Meaning | When Used |
|---|---|---|
| `null` | Attribution was not attempted. No discount code exists for this campaign. | EDU campaigns, CONTENT campaigns, Code = "None" |
| `0` | Attribution was attempted but no matching Shopify orders were found. | PROD campaigns with a valid code but zero orders in the window |
| `> 0` | Attribution succeeded; orders were matched. | Normal PROD campaigns |

This distinction MUST be preserved in all outputs: CSV, Excel, JSON, and report views.

### 1.4 UTM-Influenced Metrics (Secondary Attribution — added 2026-07-28)

Orders driven by a campaign email whose buyer did NOT use the campaign code
(e.g. checked out with the generic `GrandCru` code). Detected via the order's
`landing_site` URL. **Reported in separate columns — never blended into the
locked primary metrics above.** Same null-vs-zero semantics apply.

| Metric | Definition | Formula | Null Condition |
|---|---|---|---|
| **Influenced Orders** | Orders in the window whose `landing_site` matches the campaign but that used no campaign code. | `COUNT(influenced_orders)` | `null` if Code = None, EDU, or CONTENT |
| **Influenced Offer Revenue** | Revenue from the offered wine's line items only, net of any discounts applied to those lines. Line match: every non-year token of the campaign's Producer/Topic appears in the line title, and at least one vintage year matches when the topic names vintages. | `SUM(line.price × qty − line discount allocations)` over matching lines | `null` if Code = None, EDU, or CONTENT |
| **Influenced Total Sales** | Full order value of influenced orders (analog of Total Order Value). | `SUM(order.total_price)` | `null` if Code = None, EDU, or CONTENT |

---

## 2. Line-Item Attribution Logic

### 2.1 Why Line-Item Level

An order may contain items that were NOT discounted by the campaign code (e.g., Order #18039 had one discounted product and one non-discounted product). Using order-level totals would overcount Attributed Revenue.

### 2.2 Attribution Algorithm

```
INPUT: order (Shopify order object), campaign_code (string)

STEP 1: Find the discount application index
    campaign_app_index = null
    FOR i, app IN enumerate(order.discount_applications):
        IF app.type == "discount_code" AND lower(app.code) == lower(campaign_code):
            campaign_app_index = i
            BREAK
    IF campaign_app_index is null:
        RETURN {attributed_revenue: 0, discount_value: 0}  # code not found in this order

STEP 2: Classify each line item
    attributed_revenue = 0
    discount_value = 0
    FOR item IN order.line_items:
        FOR alloc IN item.discount_allocations:
            IF alloc.discount_application_index == campaign_app_index:
                attributed_revenue += float(item.price) * item.quantity
                discount_value += float(alloc.amount)
                BREAK  # only count once per item

STEP 3: Compute order-level total
    total_order_value = float(order.total_price)

RETURN {
    attributed_revenue: attributed_revenue,
    discount_value: discount_value,
    total_order_value: total_order_value
}
```

### 2.3 Confirmed Feasibility

Line-item attribution was confirmed in Phase 1 using live Shopify data:
- Order #18059 (BryantFam): 3 of 4 items received discount; Shipping Insurance did not
- Order #18039 (Duffau): 1 of 2 products received discount; La Fleur de Gay did not
- `discount_allocations[].discount_application_index` reliably links to the correct `discount_applications[]` entry
- `discount_applications[].target_selection = "entitled"` confirms only specific items receive the discount

**This is exact attribution, not a proxy.**

---

## 3. Campaign Name Parsing Rules

### 3.1 Expected Format

```
YYYY-MM-DD - Producer/Topic - CampaignType - OfferValue - Code
```

Split by ` - ` (space-dash-space). Must produce exactly 5 segments.

### 3.2 Segment Extraction

| Segment | Index | Expected Values | Validation |
|---|---|---|---|
| Date | 0 | `2026-03-19`, `2026-0323`, `2026_0317` | Must match date regex; normalized to `YYYY-MM-DD` |
| Producer/Topic | 1 | `Bryant Family`, `2010 Cos d'Estournel`, `BIN Sale` | Free text; used as grouping key for producer reports |
| Campaign Type | 2 | `PROD`, `EDU`, `CONTENT` | Must be one of the three; otherwise `PARSE_ERROR` |
| Offer Value | 3 | `7%`, `10%`, `5%`, `None` | Informational; not used in attribution logic |
| Discount Code | 4 | `BryantFam`, `SQN`, `None` | `"None"` → set to `null`; otherwise used as Shopify matching key |

### 3.3 Date Normalization

| Input Format | Example | Regex | Output |
|---|---|---|---|
| ISO with dashes | `2026-03-20` | `^\d{4}-\d{2}-\d{2}` | `2026-03-20` |
| Dash without middle dash | `2026-0323` | `^\d{4}-\d{4}` | Insert dash → `2026-03-23` |
| Underscore compact | `2026_0317` | `^\d{4}_\d{4}` | Replace _ → -, insert dash → `2026-03-17` |

Master regex: `^(\d{4})[-_]?(\d{2})-?(\d{2})`

### 3.4 BIN Sale / Holiday Sale Detection

```python
BIN_HOLIDAY_KEYWORDS = [
    "bin sale", "bin sale reminder",
    "holiday sale", "holiday sale reminder",
    "flash sale", "clearance"
]

def is_bin_or_holiday(campaign_name: str, producer_topic: str) -> bool:
    combined = (campaign_name + " " + producer_topic).lower()
    return any(kw in combined for kw in BIN_HOLIDAY_KEYWORDS)
```

Effect: If detected, attribution window = 3 days. Otherwise 7 days.

---

## 4. Campaign Eligibility Rules

### 4.1 Main Dashboard Table Eligibility

ALL of the following must be true:

| # | Rule | Implementation |
|---|---|---|
| 1 | Parsed name date >= 2026-03-18 | Filter after parsing |
| 2 | Campaign has been sent | At least one v1 campaign ID from `allEmailCampaignIds` returns `counters.delivered > 0` |
| 3 | Full new naming convention | All 5 segments parsed successfully. `LEGACY_FORMAT` and `PARSE_ERROR` campaigns go to QA excluded output only. |

### 4.2 Shopify Attribution Eligibility

A campaign in the main table is eligible for Shopify attribution only if:
- `discount_code` is not `null` (i.e., Code != "None")
- Campaign type is `PROD` (or BIN/holiday variant which is still PROD-like)

Campaigns with `campaign_type = EDU` or `CONTENT` or `discount_code = null`:
- All Shopify metrics = `null`
- Only HubSpot delivery metrics are populated

---

## 5. Attribution Matching Rules

### 5.1 Core Match

```
MATCH IF:
    lower(order.discount_codes[].code) == lower(campaign.discount_code)
    AND order.created_at >= campaign.parsed_send_date (at 00:00:00)
    AND order.created_at < campaign.parsed_send_date + attribution_window_days (at 00:00:00)
```

### 5.2 Attribution Window

| Campaign Type | Window | Detection |
|---|---|---|
| Standard (PROD) | 7 calendar days | Default |
| BIN Sale / Holiday Sale | 3 calendar days | Keyword match on name/producer_topic |
| EDU / CONTENT | N/A (no attribution) | Campaign type field |

### 5.3 Duplicate Code Resolution

If the same code appears in multiple campaigns with overlapping windows:
1. Attribute to the campaign with the most recent `parsed_send_date` that precedes the order's `created_at`
2. Log `DUPLICATE_CODE_WARNING` for all affected campaigns
3. Flag for manual review

### 5.4 Single Attribution Only

Each order is attributed to exactly one campaign (the one whose code matches). No multi-campaign attribution. No splitting.

### 5.5 UTM-Influenced Secondary Match (added 2026-07-28)

Runs only for orders that did NOT match any campaign by code. An order is
**influenced** by a campaign IF, within the campaign's attribution window:

```
UTM MATCH (either signal suffices):
    order.landing_site contains path "/discount/<campaign_code>"   (case-insensitive)
    OR the last " - " segment of the utm_campaign query value,
       after stripping "Version A/B" / "V1/V2" suffixes, equals campaign_code

AND EXCLUSION:
    the order used NO known campaign discount code or automatic discount
    title (checked against all campaign codes + family member identifiers,
    normalized). Orders converting through another campaign's code are never
    counted as influenced — single attribution is preserved.
```

- An order code-attributed to the same campaign is counted once in primary metrics only.
- Family (BIN Sale) campaigns do not compute influenced metrics — their automatic discounts apply to every qualifying order, so code/title attribution already captures email-driven orders.
- Known limitation: `landing_site` records only the first session; cross-device conversions are missed. Influenced metrics are a floor, not a ceiling.

---

## 6. Report View Specifications

### 6.1 Weekly Campaign Report

**Columns:** Date, Discount Code, Campaign Name, Discounted Orders, Delivered, Attributed Revenue, Revenue per Delivered

**Scope:** Campaigns with `parsed_send_date` in the current week

**Sort:** `parsed_send_date` DESC, then `attributed_revenue` DESC

**Codeless campaigns:** Included; Shopify columns show N/A

**Insights to generate:**

| Insight | Condition |
|---|---|
| Best campaign | MAX(attributed_revenue) among PROD with code |
| Worst campaign | MIN(attributed_revenue) where attributed_revenue > 0, among PROD with code and is_final_snapshot = true |
| Most efficient code | MAX(revenue_per_delivered) among PROD with attributed_revenue > 0 |
| Strong delivery / weak monetization | delivered > median AND (attributed_revenue < median OR attributed_revenue = 0) |
| Low delivery / strong efficiency | delivered < median AND revenue_per_delivered > median |
| Unused codes | qa_bucket = OK_NO_ORDERS |
| Open windows | is_final_snapshot = false |

### 6.2 Monthly Report (by Discount Code)

**Columns:** Discount Code, Campaign Count, Campaign Names, Total Attributed Revenue, Total Discounted Orders, Total Discount Value, Avg Revenue per Campaign, Avg Revenue per Delivered

**Scope:** Campaigns with `parsed_send_date` in the target month AND `is_final_snapshot = true`

**Sort:** Total Attributed Revenue DESC

**Excludes:** Codeless campaigns (EDU/CONTENT)

### 6.3 Producer Performance Report

**Columns:** Producer/Topic, Campaign Count, Total Attributed Revenue, Total Discounted Orders, Total Delivered, Revenue per Delivered, Avg Revenue per Campaign, Best Campaign, Worst Campaign

**Scope:** All `is_final_snapshot = true` campaigns in the requested date range

**Sort:** Total Attributed Revenue DESC

**Codeless producers:** Included for delivery counts; revenue columns = N/A

**Insights:** Top 5 by revenue, top 5 by efficiency, underperformers (high delivery / low revenue)

---

## 7. QA Classification Rules

| QA Bucket | Condition | Destination |
|---|---|---|
| `OK` | Fully parsed, sent (delivered > 0), attribution computed | Main table |
| `OK_NO_CODE` | Fully parsed, sent, Code = "None" or type = EDU/CONTENT | Main table (Shopify metrics = `null`) |
| `OK_NO_ORDERS` | Fully parsed, sent, valid code, zero matching orders | Main table (Shopify metrics = `0`) |
| `OK_OVERRIDE` | Override applied from `campaign_overrides.csv` | Main table |
| `PARSE_ERROR` | Name doesn't match expected format | QA excluded output |
| `LEGACY_FORMAT` | Old naming convention (<5 segments) | QA excluded output |
| `STATS_UNAVAILABLE` | No v1 campaign resolves with delivered > 0 | QA excluded output |
| `DATE_OUT_OF_RANGE` | Parsed date < 2026-03-18 | Excluded silently |
| `DUPLICATE_CODE_WARNING` | Same code in overlapping campaigns | Main table (flagged) |
| `WINDOW_OPEN` | is_final_snapshot = false | Main table (marked incomplete) |

---

## 8. History Table Rules

### 8.1 Unique Key

`(parsed_send_date, discount_code, campaign_name)`

### 8.2 is_final_snapshot Logic

```python
def compute_is_final(parsed_send_date: date, attribution_window_days: int, run_date: date) -> bool:
    window_end = parsed_send_date + timedelta(days=attribution_window_days)
    return run_date >= window_end
```

- `false` → row is updated on each run
- `true` → row is frozen forever

### 8.3 Upsert Rules

| Existing Row? | is_final_snapshot | Action |
|---|---|---|
| No | — | INSERT with current metrics and `is_final_snapshot` based on window status |
| Yes | `true` | SKIP (do not overwrite) |
| Yes | `false` | UPDATE with latest metrics; re-evaluate `is_final_snapshot` |

---

## 9. Shopify Authentication Rules

| Rule | Value |
|---|---|
| Auth method | OAuth2 client_credentials grant |
| Token endpoint | `POST https://{store}/admin/oauth/access_token` |
| Token lifetime | 86,399 seconds (~24 hours) |
| Refresh strategy | Re-call token endpoint with same credentials |
| Pre-expiry buffer | Refresh if within 5 minutes of expiry |
| Retry on 401 | Force-refresh token and retry once |

---

## 10. Known Non-Campaign Discount Code Patterns

These codes appear in Shopify orders but are NOT tied to email campaigns and should be excluded from campaign attribution:

| Pattern | Example | Description |
|---|---|---|
| `GrandCru` | `GrandCru` | New-user offer |
| `GCLA-*` | `GCLA-X510LSQBE0CP` | Auto-generated new-user codes |
| `THANKYOU*` | `THANKYOU5` | Holiday/thank-you offers |

These are reported in the unmatched discount codes QA output with `possible_reason = "non-campaign code (known pattern)"`.

---

## 11. Manual Override Rules

**File:** `campaign_overrides.csv`

**Match key:** Exact match on HubSpot v3 `name` field

**Override precedence:**
1. `force_exclude = true` takes highest precedence (campaign is excluded regardless)
2. `force_include = true` overrides any QA exclusion bucket
3. Individual field overrides (`override_send_date`, `override_discount_code`, etc.) replace parsed values
4. Overrides are applied BEFORE parsing runs
5. Overridden campaigns get `qa_bucket = OK_OVERRIDE`

**File is manually maintained.** The workflow never writes to it.

---

## 12. Data Flow Summary

```
[HubSpot v3 emails]
    |
    v
[Load campaign_overrides.csv] ──> apply overrides
    |
    v
[Campaign Name Parser] ──> extract 5 segments, normalize date
    |
    ├─ PARSE_ERROR / LEGACY_FORMAT ──> QA excluded output
    ├─ DATE_OUT_OF_RANGE ──> silently excluded
    |
    v
[HubSpot v1 campaign stats] ──> resolve delivered, opened, clicked
    |
    ├─ delivered = 0 (STATS_UNAVAILABLE) ──> QA excluded output
    |
    v
[Campaign Detail Table] ──> main dashboard data
    |
    ├─ Code = None / EDU / CONTENT ──> Shopify metrics = null
    |
    v
[Shopify Token Acquisition]
    |
    v
[Shopify Order Fetch] ──> by date window per campaign
    |
    v
[Line-Item Attribution] ──> compute Attributed Revenue, Discount Value, Total Order Value
    |
    v
[Metric Assembly] ──> join HubSpot + Shopify, compute Revenue per Delivered
    |
    v
[QA Bucket Assignment] ──> OK, OK_NO_CODE, OK_NO_ORDERS, WINDOW_OPEN, etc.
    |
    v
[History Table Upsert] ──> insert/update/skip based on is_final_snapshot
    |
    v
[Report Generation]
    ├─ Weekly Report + Insights
    ├─ Monthly Report (by discount code)
    ├─ Producer Performance Report
    |
    v
[QA Outputs]
    ├─ QA Summary
    ├─ Unmatched Shopify Discount Codes
    ├─ Excluded Campaigns
    ├─ Matched Orders Audit
```
