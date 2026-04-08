# Email Campaign Performance Dashboard — Implementation Plan

## Document Purpose
This is the Phase 2 implementation blueprint for the Grand Cru Liquid Assets email campaign dashboard. It defines every extraction rule, parsing rule, attribution rule, metric definition, output schema, QA bucket, history table design, and token refresh strategy needed to build the production workflow.

All decisions below are grounded in Phase 1 API verification against live HubSpot and Shopify endpoints.

---

## 1. Extraction Plan

### 1.1 HubSpot: Campaign Metadata + Delivery Stats

**Step 1 — Fetch email list from v3 endpoint**

```
GET /marketing/v3/emails?limit=100&orderBy=-publishDate
Authorization: Bearer {HUBSPOT_PRIVATE_APP_TOKEN}
```

Fields to extract per email object:
| Field | Purpose |
|---|---|
| `name` | Source of truth for campaign name (parse all fields from this) |
| `publishDate` | Used only as a secondary reference / sanity check — NOT as the send date |
| `state` | Secondary signal only. In this account, sent emails show `AUTOMATED`. The primary "sent" check is `counters.delivered > 0` from the v1 endpoint. |
| `allEmailCampaignIds` | Array of campaign IDs to resolve v1 stats |
| `id` | v3 email object ID (stored for traceability) |

Pagination: use `paging.next.after` cursor if `total > limit`. Continue until all pages are fetched.

**Step 2 — Resolve delivery stats from v1 endpoint**

For each v3 email, iterate through `allEmailCampaignIds` and call:

```
GET /email/public/v1/campaigns/{campaignId}
Authorization: Bearer {HUBSPOT_PRIVATE_APP_TOKEN}
```

Accept the first ID that returns a valid response (HTTP 200 with non-empty `counters`).

Fields to extract from v1 campaign detail:
| Field | Purpose |
|---|---|
| `counters.sent` | Emails sent |
| `counters.delivered` | Emails delivered (primary metric) |
| `counters.open` | Opens |
| `counters.click` | Clicks |
| `counters.bounce` | Bounces |
| `counters.unsubscribed` | Unsubscribes |
| `name` | Cross-check against v3 `name` for validation |

**Filtering rules:**
- **Primary sent check:** Only include emails where at least one ID from `allEmailCampaignIds` resolves in v1 with `counters.delivered > 0`. This is the definitive proof the campaign was sent and received by recipients.
- **Secondary signal:** `state` field is logged for traceability but is NOT the gating criterion. An email with `state = "AUTOMATED"` but `delivered = 0` is treated as unsent.
- Only include emails where the parsed name date is `>= 2026-03-18`

**Why v3 first, then v1:** The v3 endpoint provides the authoritative campaign name with structured naming. The v1 campaign list has no date filter and requires full pagination. By starting with v3 (which supports `orderBy=-publishDate`), we can stop pagination early once we pass our date boundary, then only make targeted v1 calls for relevant campaigns.

### 1.2 Shopify: Orders with Discount Code Usage

**Step 0 — Obtain access token**

```
POST https://grand-cru-liquid-assets.myshopify.com/admin/oauth/access_token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
&client_id={SHOPIFY_CLIENT_ID}
&client_secret={SHOPIFY_CLIENT_SECRET}
```

Response: `{ "access_token": "shpat_...", "scope": "...", "expires_in": 86399 }`

See Section 10 for full token refresh strategy.

**Step 1 — Fetch orders in the attribution window**

For each campaign with a discount code, compute the attribution window:
- `start_date` = parsed send date from campaign name (at 00:00:00 UTC)
- `end_date` = start_date + 7 days (default) or + 3 days (BIN/holiday sale)

```
GET /admin/api/2025-01/orders.json
  ?status=any
  &created_at_min={start_date}
  &created_at_max={end_date}
  &limit=250
X-Shopify-Access-Token: {access_token}
```

For efficiency, if multiple campaigns share overlapping windows, fetch the broadest date range once and filter in memory.

Fields to extract per order:
| Field | Purpose |
|---|---|
| `id` | Order ID |
| `name` | Order number display (e.g., "#18059") |
| `created_at` | Order creation timestamp |
| `total_price` | Total order value (after discount, including tax + shipping) |
| `subtotal_price` | Subtotal after discount, before tax/shipping |
| `total_discounts` | Total discount dollar amount on the order |
| `total_line_items_price` | Sum of all line item prices before any discounts |
| `financial_status` | Payment status ("paid", "refunded", etc.) |
| `discount_codes` | Array: `[{code, amount, type}]` |
| `discount_applications` | Array: `[{type, code, value, value_type, target_type, target_selection}]` |
| `line_items` | Array of line items (see below) |

Fields to extract per line item:
| Field | Purpose |
|---|---|
| `title` | Product name |
| `quantity` | Units ordered |
| `price` | Unit price (before discount) |
| `discount_allocations` | Array: `[{amount, discount_application_index}]` |

**Pagination:** Use Link header or `since_id` pattern for orders exceeding 250 per page.

---

## 2. Parsing Rules

### 2.1 Campaign Name Parser

The campaign name is the single source of truth. The parser extracts five fields from the `name` field of each v3 email object.

**Expected format (new convention, 2026-03-18 onward):**
```
YYYY-MM-DD - Producer/Topic - CampaignType - OfferValue - Code
```

**Parsing steps:**

1. **Split** the name by ` - ` (space-dash-space) delimiter
2. **Extract date** from segment[0]
3. **Extract producer/topic** from segment[1]
4. **Extract campaign type** from segment[2] — expected: `PROD`, `EDU`, `CONTENT`
5. **Extract offer value** from segment[3] — expected: `N%` or `None`
6. **Extract discount code** from segment[4] — expected: string or `None`

### 2.2 Date Normalization

Three date formats have been observed in campaign names. The parser must handle all three:

| Format | Example | Normalization Rule |
|---|---|---|
| `YYYY-MM-DD` | `2026-03-20` | Already ISO — use directly |
| `YYYY-MMDD` | `2026-0323` | Insert dash: `2026-03-23` |
| `YYYY_MMDD` | `2026_0317` | Replace underscore, insert dash: `2026-03-17` |

**Regex pattern:**
```
^(\d{4})[-_]?(\d{2})-?(\d{2})
```
Normalize to `YYYY-MM-DD` after extraction.

### 2.3 Validation Rules

After parsing, apply these validation checks:

| Check | Action on Failure |
|---|---|
| Date segment does not match any known format | Mark campaign as `PARSE_ERROR`, route to QA excluded output only |
| Fewer than 5 segments after split | Mark as `LEGACY_FORMAT`, route to QA excluded output only (not main table) |
| Campaign type not in `{PROD, EDU, CONTENT}` | Mark as `PARSE_ERROR`, route to QA excluded output only |
| Parsed date < 2026-03-18 | Exclude from all outputs silently |
| Code = "None" (string) | Set discount_code = `null`, flag as codeless; still included in main table with Shopify metrics = `null` |

### 2.4 BIN Sale / Holiday Sale Detection

**Keyword matching** on the producer/topic field (segment[1]) or full campaign name:

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

If detected: attribution window = **3 days**. Otherwise: **7 days**.

**Note:** BIN Sale Reminder campaigns are treated identically to BIN Sale campaigns — same attribution window, same categorization.

---

## 3. Campaign Eligibility Rules

A campaign is eligible for the **main dashboard table** if ALL of the following are true:

| Rule | Implementation |
|---|---|
| Parsed name date >= 2026-03-18 | Filter after parsing |
| Campaign has been sent | At least one v1 campaign ID from `allEmailCampaignIds` returns `counters.delivered > 0` (primary check); `state` is used as a secondary signal only |
| Full new naming convention | All 5 segments successfully extracted: date, producer/topic, campaign type, offer value, code. Campaigns with partial/old/legacy naming formats are **excluded from the main table** and routed to the QA excluded-campaigns output instead. |

**Strict naming enforcement:** The main dashboard table contains ONLY campaigns that fully conform to the new `YYYY-MM-DD - Producer - Type - Value - Code` convention. Old-format names (e.g., `2026_0317 2019 Lail Vineyards`) that lack structured segments are never promoted to the main table — they appear only in the QA excluded report for manual review. If needed, they can be re-included via the manual override mechanism (see Section 8.4).

A campaign is eligible for **Shopify attribution** only if:
- Parsed discount code is not `null` / `"None"`
- Campaign type is `PROD` (or BIN/holiday variant)

Campaigns of type `EDU` or `CONTENT`:
- Included in the dashboard for HubSpot delivery metrics only
- All Shopify attribution columns are `null` (displayed as blank / "N/A" in spreadsheet output; never zero, to distinguish "no attribution attempted" from "attribution attempted, zero result")

---

## 4. Attribution Logic

### 4.1 Core Matching Rule

```
Match: Shopify order.discount_codes[].code == campaign.parsed_discount_code
       AND order.created_at >= campaign.parsed_send_date
       AND order.created_at < campaign.parsed_send_date + attribution_window
```

- **Case sensitivity:** Perform case-insensitive matching (normalize both to lowercase)
- **Single attribution:** Each order is attributed to the campaign whose code matches the order's discount code. No multi-campaign attribution.
- **No code = no attribution:** If a campaign has `Code = None`, all Shopify metrics are `null` (not zero). This distinguishes "no attribution attempted" from "zero orders found."

### 4.2 Attribution Window

| Campaign Type | Window |
|---|---|
| Standard (PROD, EDU, CONTENT) | 7 calendar days from parsed send date |
| BIN Sale / Holiday Sale (detected by keyword) | 3 calendar days from parsed send date |

Window is inclusive of start date, exclusive of end date:
```
start: parsed_send_date at 00:00:00 (midnight)
end:   parsed_send_date + N days at 00:00:00 (midnight)
```

### 4.3 Same Code Across Multiple Campaigns

If the same discount code appears in multiple campaigns with overlapping windows:
- Each order is attributed to **the campaign whose window contains the order's created_at**
- If two campaigns have the same code AND overlapping windows: attribute to the **campaign with the most recent send date** (closest preceding send date to the order)
- Log a warning for duplicate-code scenarios for manual review

### 4.4 Line-Item Level Attribution (Confirmed Feasible)

**Phase 1 confirmed:** Shopify's `line_items[].discount_allocations` array identifies exactly which items received the campaign discount, and the `discount_application_index` links back to the specific `discount_applications[]` entry for the campaign code.

**Logic for each matched order:**

```python
# 1. Find the discount_application_index for the campaign's code
campaign_app_index = None
for i, app in enumerate(order["discount_applications"]):
    if app["type"] == "discount_code" and app["code"].lower() == campaign_code.lower():
        campaign_app_index = i
        break

# 2. For each line item, check if it received this discount
for item in order["line_items"]:
    item_discount_amount = 0
    is_discounted = False
    for alloc in item["discount_allocations"]:
        if alloc["discount_application_index"] == campaign_app_index:
            item_discount_amount = float(alloc["amount"])
            is_discounted = True

    item_gross = float(item["price"]) * item["quantity"]

    if is_discounted:
        # This item was discounted by the campaign code
        discounted_item_gross += item_gross
        discount_value_total += item_discount_amount
    else:
        # This item was NOT discounted by the campaign code
        non_discounted_item_gross += item_gross
```

---

## 5. Metric Definitions

### 5.1 Per-Campaign Metrics

| Metric | Definition | Source | Formula |
|---|---|---|---|
| **Campaign Name** | Full v3 email `name` string | HubSpot v3 | Direct |
| **Parsed Send Date** | Date extracted and normalized from campaign name | HubSpot v3 | Derived (parser) |
| **Producer / Topic** | Segment[1] from parsed campaign name | HubSpot v3 | Derived (parser) |
| **Campaign Type** | Segment[2]: PROD, EDU, CONTENT | HubSpot v3 | Derived (parser) |
| **Offer Value** | Segment[3]: e.g., "7%", "10%", "None" | HubSpot v3 | Derived (parser) |
| **Discount Code** | Segment[4]: e.g., "BryantFam", "None" | HubSpot v3 | Derived (parser) |
| **Delivered** | Number of emails delivered | HubSpot v1 | `counters.delivered` |
| **Opened** | Number of email opens | HubSpot v1 | `counters.open` |
| **Clicked** | Number of email clicks | HubSpot v1 | `counters.click` |
| **Attributed Revenue** | Pre-discount gross value of line items that received the campaign discount. **This is the primary revenue metric for campaign performance.** | Shopify | `SUM(line_item.price * line_item.quantity)` for discounted line items only |
| **Discount Value** | Total dollar amount of discounts applied across all matched orders, for discounted line items only | Shopify | `SUM(line_item.discount_allocations[campaign_index].amount)` across all matched orders |
| **Total Order Value** | Full order value for all orders containing the campaign's discount code. **Uses `order.total_price` explicitly** — this includes discounted items, non-discounted items, tax, and shipping. | Shopify | `SUM(order.total_price)` across all matched orders |
| **Discounted Orders** | Count of orders that used the campaign's discount code in the window | Shopify | `COUNT(matched orders)` |
| **Revenue per Delivered** | Revenue efficiency per delivered email | Both | `Attributed Revenue / Delivered` |

### 5.2 Metric Clarifications and Locked Definitions

**The three primary Shopify metrics are locked as follows:**

| Metric | Locked Definition | Derivation |
|---|---|---|
| **Attributed Revenue** | Revenue from the item(s) that actually received the campaign discount. Calculated using Shopify line-item discount allocations to identify exactly which items were discounted. | `SUM(line_item.price * line_item.quantity)` for line items where `discount_allocations` contains an entry matching the campaign's discount application index |
| **Discount Value** | The actual dollar discount amount given to the customer on those items. | `SUM(line_item.discount_allocations[campaign_index].amount)` |
| **Total Order Value** | The full order value including both discounted and non-discounted items, tax, and shipping. Uses `order.total_price`. | `SUM(order.total_price)` across all matched orders |

**Additional clarifications:**
- **Attributed Revenue is the primary revenue metric** for all campaign performance analysis, weekly reports, monthly reports, and producer performance views.
- **Revenue per Delivered** uses Attributed Revenue in the numerator: `Attributed Revenue / Delivered`.
- **"Discounted Sales" / "Discounted Revenue" are NOT separate dashboard metrics.** They were removed to avoid confusion. If the post-discount value of items is ever needed for internal analysis, it can be derived as `Attributed Revenue - Discount Value`, but this is not surfaced as a primary column.
- **Line-item attribution is confirmed feasible** via Shopify's `discount_allocations` array. This is NOT a proxy — it is exact, item-level attribution.
- **EDU / CONTENT / Code=None campaigns:** All Shopify metrics are `null` (not zero). `null` means "attribution not applicable." Zero means "attribution was attempted but no matching orders were found." This distinction is critical for accurate reporting.

### 5.3 Worked Example

Campaign: `2026-03-19 - Bryant Family - PROD - 7% - BryantFam`
Order #18059: code=BryantFam, 4 line items

| Line Item | Qty | Unit Price | Discount Alloc | Discounted? |
|---|---|---|---|---|
| 2006 Bryant Family Cab | 1 | $365.00 | $25.55 | YES |
| 2010 Bryant Family Cab | 2 | $495.00 | $69.30 | YES |
| 2007 Bryant Family Cab | 1 | $495.00 | $34.65 | YES |
| Shipping Insurance | 1 | $18.50 | $0.00 | NO |

- **Attributed Revenue** = (365 * 1) + (495 * 2) + (495 * 1) = $1,850.00
- **Discount Value** = 25.55 + 69.30 + 34.65 = $129.50
- **Total Order Value** = $1,846.08 (order total_price including shipping insurance, tax, shipping)
- **Discounted Orders** = 1
- **Revenue per Delivered** = $1,850.00 / 1,342 = $1.38

---

## 6. Output Schemas

### 6.1 Campaign Dashboard Row

One row per campaign per run:

```json
{
  "campaign_name": "2026-03-19 - Bryant Family - PROD - 7% - BryantFam",
  "parsed_send_date": "2026-03-19",
  "producer_topic": "Bryant Family",
  "campaign_type": "PROD",
  "offer_value": "7%",
  "discount_code": "BryantFam",
  "attribution_window_days": 7,
  "delivered": 1342,
  "opened": 116,
  "clicked": 12,
  "attributed_revenue": 1850.00,
  "discount_value": 129.50,
  "total_order_value": 1846.08,
  "discounted_orders": 1,
  "revenue_per_delivered": 1.38,
  "hubspot_v3_email_id": "325327750878",
  "hubspot_v1_campaign_id": "25341611",
  "run_date": "2026-03-20",
  "attribution_window_end": "2026-03-26",
  "is_final_snapshot": false,
  "qa_bucket": "OK"
}
```

### 6.2 Matched Orders Detail (for audit / debugging)

One row per matched order per campaign:

```json
{
  "campaign_discount_code": "BryantFam",
  "campaign_parsed_send_date": "2026-03-19",
  "order_id": 6842521125101,
  "order_name": "#18059",
  "order_created_at": "2026-03-19T11:19:24-07:00",
  "attributed_revenue": 1850.00,
  "discount_value": 129.50,
  "order_total_price": 1846.08,
  "discounted_line_items": 3,
  "total_line_items": 4,
  "financial_status": "paid"
}
```

### 6.3 Campaign Detail Table (full internal schema, spreadsheet / CSV)

All columns stored per campaign. This is the complete data record.

| Column | Type | Notes |
|---|---|---|
| Campaign Name | string | Full name from HubSpot |
| Parsed Send Date | date | YYYY-MM-DD |
| Producer / Topic | string | Parsed segment |
| Campaign Type | string | PROD / EDU / CONTENT |
| Offer Value | string | "7%", "10%", "None" |
| Discount Code | string | Code or "None" |
| Delivered | integer | HubSpot |
| Opened | integer | HubSpot |
| Clicked | integer | HubSpot |
| Attributed Revenue | currency | $ — gross value of discounted items (primary revenue metric) |
| Discount Value | currency | $ — actual discount amount given |
| Total Order Value | currency | $ — full order totals via `order.total_price` |
| Discounted Orders | integer | Count of orders using the code |
| Revenue per Delivered | currency | Attributed Revenue / Delivered |
| Attribution Window Days | integer | 7 (standard) or 3 (BIN/holiday) |
| Attribution Window End | date | send_date + window |
| is_final_snapshot | boolean | true = frozen, false = may update |
| QA Bucket | string | OK, OK_NO_CODE, WINDOW_OPEN, etc. |
| Run Date | date | When this snapshot was taken |

---

## 7. Report Views

The campaign detail table (Section 6.3) is the master data record. The following report views are derived from it.

### 7.1 Weekly Campaign Report

**Purpose:** Summarize campaign performance for the current week. This is the primary operational report.

**Columns:**

| Column | Source | Notes |
|---|---|---|
| Date | `parsed_send_date` | Campaign send date from name |
| Discount Code | `discount_code` | Parsed from campaign name |
| Campaign Name | `campaign_name` | Full name |
| Discounted Orders | `discounted_orders` | Count of orders using this code in window |
| Delivered | `delivered` | Emails delivered |
| Attributed Revenue | `attributed_revenue` | Revenue from discounted items |
| Revenue per Delivered | `revenue_per_delivered` | Attributed Revenue / Delivered |

**Scope:** All campaigns with `parsed_send_date` in the current reporting week.

**Sort order:** By `parsed_send_date` descending, then by `attributed_revenue` descending.

**Codeless campaigns (EDU/CONTENT):** Included in the weekly report for Delivered visibility. Attributed Revenue and Discounted Orders show as blank/N/A. Revenue per Delivered shows as N/A.

**Performance Insights (generated per weekly report):**

The weekly report should include a performance insights section highlighting:

| Insight | Logic |
|---|---|
| **Best performing campaign** | Highest Attributed Revenue among campaigns with `is_final_snapshot = true` (or all if none finalized yet) |
| **Worst performing campaign** | Lowest Attributed Revenue (> 0) among PROD campaigns with closed windows |
| **Most efficient discount code** | Highest Revenue per Delivered among campaigns with Attributed Revenue > 0 |
| **Strong delivery, weak monetization** | Campaigns where Delivered is above-median but Attributed Revenue is below-median (or zero) |
| **Low delivery, strong efficiency** | Campaigns where Delivered is below-median but Revenue per Delivered is above-median |
| **Unused codes** | Campaigns with `qa_bucket = OK_NO_ORDERS` (valid code, zero orders) |
| **Open windows** | Campaigns with `is_final_snapshot = false` (metrics may still change) |

Insights should compare only PROD campaigns with codes (exclude EDU/CONTENT from performance rankings).

### 7.2 Monthly Report (by Discount Code)

**Purpose:** Show cumulative performance by discount code for the calendar month. Supports monthly review and trend analysis.

**Columns:**

| Column | Source | Notes |
|---|---|---|
| Discount Code | `discount_code` | Grouping key |
| Campaign Count | COUNT of campaigns using this code in the month | How many campaigns used this code |
| Campaign Names | List of `campaign_name` values | Which campaigns contributed |
| Total Attributed Revenue | SUM(`attributed_revenue`) | Cumulative revenue for this code |
| Total Discounted Orders | SUM(`discounted_orders`) | Cumulative orders for this code |
| Total Discount Value | SUM(`discount_value`) | Cumulative discounts given |
| Avg Revenue per Campaign | Total Attributed Revenue / Campaign Count | Average revenue per campaign using this code |
| Avg Revenue per Delivered | SUM(`attributed_revenue`) / SUM(`delivered`) | Blended efficiency across all campaigns with this code |

**Scope:** All campaigns with `parsed_send_date` in the target month AND `is_final_snapshot = true` (only finalized data). Campaigns still within their attribution window are excluded from monthly totals to avoid partial counts.

**Sort order:** By `Total Attributed Revenue` descending.

**Codeless campaigns:** Excluded from the monthly discount-code view (they have no code to group by). A separate line can show total EDU/CONTENT delivery count for the month if desired.

### 7.3 Producer Performance Report

**Purpose:** Show how each producer performs across all their campaigns. Supports producer-level decision making (which producers generate the most revenue, which have the best email-to-revenue conversion).

**Columns:**

| Column | Source | Notes |
|---|---|---|
| Producer / Topic | `producer_topic` | Grouping key |
| Campaign Count | COUNT of campaigns for this producer | |
| Total Attributed Revenue | SUM(`attributed_revenue`) | Cumulative revenue |
| Total Discounted Orders | SUM(`discounted_orders`) | Cumulative orders |
| Total Delivered | SUM(`delivered`) | Cumulative emails delivered |
| Revenue per Delivered | SUM(`attributed_revenue`) / SUM(`delivered`) | Blended efficiency |
| Avg Revenue per Campaign | Total Attributed Revenue / Campaign Count | |
| Best Campaign | Campaign with highest Attributed Revenue for this producer | |
| Worst Campaign | Campaign with lowest Attributed Revenue (> 0) for this producer | |

**Scope:** All campaigns with `is_final_snapshot = true` in the requested date range. Can be filtered to a specific month, quarter, or full history.

**Sort order:** By `Total Attributed Revenue` descending.

**Codeless campaigns (EDU/CONTENT):** Included for delivery counts but excluded from revenue rankings. Revenue per Delivered for EDU/CONTENT producers shows as N/A.

**Performance Insights (generated per producer report):**

| Insight | Logic |
|---|---|
| **Top revenue producers** | Top 5 by Total Attributed Revenue |
| **Most efficient producers** | Top 5 by Revenue per Delivered |
| **Underperforming producers** | Producers with above-average Delivered but below-average Attributed Revenue |
| **Consistent performers** | Producers with 3+ campaigns and low variance in Revenue per Delivered |

---

## 8. QA Buckets

Every campaign processed is tagged with a QA bucket for transparency:

| QA Bucket | Condition | Action |
|---|---|---|
| `OK` | All fields parsed, stats retrieved, attribution computed (if applicable) | Include in dashboard |
| `OK_NO_CODE` | Campaign parsed successfully but Code = "None"; type is EDU or CONTENT | Include in main table with Shopify metrics = `null` |
| `OK_NO_ORDERS` | Campaign has a valid code but zero matching orders found in Shopify | Include in main table with Shopify metrics = `0`; may indicate unused code |
| `PARSE_ERROR` | Campaign name does not match expected format; fewer than 5 segments or unrecognized type | Exclude from main table; route to QA excluded-campaigns output |
| `LEGACY_FORMAT` | Campaign name uses old naming convention (e.g., `2026_0317 ProducerName`); parseable date but no structured segments | Exclude from main table; route to QA excluded-campaigns output |
| `STATS_UNAVAILABLE` | v3 email exists but no v1 campaign ID resolves with `delivered > 0` (campaign not yet sent or processing) | Exclude from main table; retry on next run |
| `DATE_OUT_OF_RANGE` | Parsed send date < 2026-03-18 | Exclude silently |
| `DUPLICATE_CODE_WARNING` | Same discount code appears in another campaign with overlapping window | Include but flag for manual review |
| `WINDOW_OPEN` | Attribution window has not yet closed (`is_final_snapshot = false`; run_date < send_date + window_days) | Include with current data; mark as potentially incomplete. Metrics will be refreshed on next run. |

### 7.1 QA Summary Report

Each run should produce a summary:
```
Total v3 emails fetched: N
Eligible campaigns (date >= 2026-03-18): N
  Main table:
    OK: N
    OK_NO_CODE: N
    OK_NO_ORDERS: N
    DUPLICATE_CODE_WARNING: N (list codes + campaigns)
    WINDOW_OPEN: N (list names)
  Excluded:
    PARSE_ERROR: N (list names)
    LEGACY_FORMAT: N (list names)
    STATS_UNAVAILABLE: N (list names)
  Shopify QA:
    Unmatched discount codes: N (list codes + order counts)
```

### 7.2 Unmatched Shopify Discount Codes QA Output

**Purpose:** Identify discount codes used in Shopify orders during the reporting period that do NOT match any campaign's parsed discount code. This surfaces:
- Non-campaign codes (e.g., `GrandCru`, `THANKYOU5`, `GCLA-...` new-user offers)
- Potential campaign codes that failed to match due to naming issues or typos
- Codes from campaigns that were excluded due to `PARSE_ERROR` or `LEGACY_FORMAT`

**Logic:**
1. Collect ALL unique discount codes from Shopify orders in the reporting date range (2026-03-18 onward)
2. Collect ALL campaign discount codes from the main dashboard table (successfully parsed campaigns)
3. Subtract: `unmatched_codes = shopify_codes - campaign_codes` (case-insensitive)
4. For each unmatched code, report:

```json
{
  "discount_code": "GrandCru",
  "order_count": 3,
  "total_discount_amount": 187.50,
  "total_order_value": 2850.00,
  "earliest_order": "2026-03-18T08:49:00-07:00",
  "latest_order": "2026-03-20T14:22:00-07:00",
  "sample_order_ids": ["#18049", "#18061", "#18075"],
  "possible_reason": "non-campaign code (known new-user offer)"
}
```

**Known non-campaign codes** (pre-classified, not flagged as issues):
- `GrandCru` — new-user offer
- `GCLA-*` (pattern: `GCLA-` prefix followed by alphanumeric) — new-user auto-generated offers
- `THANKYOU*` (pattern: `THANKYOU` prefix) — holiday/thank-you offers

Codes matching these patterns get `possible_reason = "non-campaign code (known pattern)"`. All other unmatched codes get `possible_reason = "unknown — review manually"`.

**Output file:** `qa_unmatched_discount_codes.csv` (or sheet in the Excel workbook)

### 7.3 QA Excluded-Campaigns Output

**Purpose:** List all campaigns that were fetched from HubSpot but excluded from the main dashboard table, with the reason for exclusion.

```json
{
  "campaign_name": "2026_0317 2019 Lail Vineyards",
  "qa_bucket": "LEGACY_FORMAT",
  "parsed_date": "2026-03-17",
  "partial_fields": {"producer_topic": "2019 Lail Vineyards"},
  "exclusion_reason": "Old naming format — fewer than 5 segments. No campaign type, offer value, or code parseable.",
  "override_available": true
}
```

**Output file:** `qa_excluded_campaigns.csv` (or sheet in the Excel workbook)

This output feeds directly into the manual override mechanism (Section 8.4) — if a user sees an excluded campaign that should be included, they can add an override entry.

### 7.4 Manual Override / Mapping Mechanism

**Purpose:** Provide a lightweight way to correct campaign naming issues, supply missing discount codes, or re-map codes without modifying HubSpot data or the core parser logic. This handles:
- Old-format campaigns that should be included in the dashboard
- Typos in campaign names
- Discount codes that need remapping
- Campaigns where the code in the name doesn't match the Shopify code exactly

**Implementation:** A CSV file (`campaign_overrides.csv`) loaded at the start of each run, applied BEFORE parsing and attribution.

**Override file schema:**

| Column | Type | Required | Description |
|---|---|---|---|
| `hubspot_email_name` | string | YES | Exact `name` from HubSpot v3 (match key) |
| `override_send_date` | date | NO | If set, overrides the parsed date from the name |
| `override_producer_topic` | string | NO | If set, overrides the parsed producer/topic |
| `override_campaign_type` | string | NO | If set, overrides the parsed campaign type (PROD/EDU/CONTENT) |
| `override_offer_value` | string | NO | If set, overrides the parsed offer value |
| `override_discount_code` | string | NO | If set, overrides the parsed discount code for Shopify matching |
| `override_window_days` | integer | NO | If set, overrides the default attribution window (7 or 3) |
| `force_include` | boolean | NO | If `true`, forces this campaign into the main table even if it would otherwise be excluded (e.g., LEGACY_FORMAT) |
| `force_exclude` | boolean | NO | If `true`, forces this campaign out of the main table even if it would otherwise be included |
| `notes` | string | NO | Free-text notes for audit trail |

**Processing rules:**
1. Load `campaign_overrides.csv` at the start of each run
2. For each v3 email, check if `name` matches any `hubspot_email_name` in the overrides file (exact match)
3. If matched: apply all non-empty override fields BEFORE the parser runs
4. `force_include = true` overrides any QA bucket exclusion and promotes the campaign to the main table
5. `force_exclude = true` takes precedence over `force_include` (safety mechanism)
6. Override-applied campaigns get `qa_bucket = "OK_OVERRIDE"` with the original bucket noted in the QA report

**Example overrides:**

```csv
hubspot_email_name,override_send_date,override_producer_topic,override_campaign_type,override_offer_value,override_discount_code,override_window_days,force_include,force_exclude,notes
"2026_0317 2019 Lail Vineyards",2026-03-17,2019 Lail Vineyards,PROD,7%,Lail,,true,false,"Legacy name; Lail code confirmed in Shopify"
"2026_0318 2010 Cos d'Estournel",2026-03-18,2010 Cos d'Estournel,PROD,7%,CosD,,true,false,"Legacy name; code confirmed in v3 campaignName"
```

**File management:**
- If `campaign_overrides.csv` does not exist, the workflow proceeds normally with no overrides
- The overrides file is manually maintained — the workflow never modifies it
- Each override entry has a `notes` field for documenting why the override was added

---

## 9. History Table Logic

### 8.1 Purpose

Store one row per campaign per run to enable cumulative tracking over time. This supports weekly snapshots, quarterly analysis, and year-over-year comparisons.

### 8.2 Unique Key

**Composite key:** `(parsed_send_date, discount_code, campaign_name)`

Rationale:
- `parsed_send_date` + `discount_code` is sufficient for most lookups
- `campaign_name` is added to handle edge cases where two campaigns on the same date share the same code (should not happen, but defensive)
- `campaign_name` alone is not a safe unique key because names could theoretically be edited in HubSpot

### 8.3 Schema

```
campaign_history (
    -- Identity
    parsed_send_date       DATE           -- from campaign name
    discount_code          TEXT           -- from campaign name, nullable
    campaign_name          TEXT           -- full name
    producer_topic         TEXT
    campaign_type          TEXT           -- PROD / EDU / CONTENT
    offer_value            TEXT           -- "7%", "None", etc.
    attribution_window_days INTEGER       -- 7 or 3

    -- HubSpot metrics
    delivered              INTEGER
    opened                 INTEGER
    clicked                INTEGER

    -- Shopify metrics (null for EDU/CONTENT/Code=None)
    attributed_revenue     DECIMAL(10,2)  -- primary revenue metric
    discount_value         DECIMAL(10,2)
    total_order_value      DECIMAL(10,2)
    discounted_orders      INTEGER

    -- Derived
    revenue_per_delivered  DECIMAL(10,4)

    -- Snapshot status
    is_final_snapshot      BOOLEAN        -- true = attribution window closed, row is frozen
                                          -- false = window still open, metrics may update on next run

    -- Metadata
    run_date               DATE           -- when this snapshot was taken
    attribution_window_end DATE           -- send_date + window
    qa_bucket              TEXT
    hubspot_v3_email_id    TEXT
    hubspot_v1_campaign_id TEXT
)
```

### 8.4 Update Logic

On each weekly run:
1. Compute all metrics for eligible campaigns
2. For each campaign, check if a row with the same `(parsed_send_date, discount_code, campaign_name)` already exists
3. If exists AND `is_final_snapshot = true`: **do not overwrite** — the data is frozen
4. If exists AND `is_final_snapshot = false`: **update** the row with latest metrics, then re-evaluate `is_final_snapshot`
5. If does not exist: **insert** new row with `is_final_snapshot` set based on current window status

**`is_final_snapshot` determination:**

```python
def compute_is_final(parsed_send_date: date, attribution_window_days: int, run_date: date) -> bool:
    """
    Returns True if the attribution window has fully closed.
    - Standard campaigns (PROD, EDU, CONTENT): 7-day window
    - BIN / Holiday campaigns: 3-day window
    """
    window_end = parsed_send_date + timedelta(days=attribution_window_days)
    return run_date >= window_end
```

| Scenario | `is_final_snapshot` | Behavior |
|---|---|---|
| `run_date < send_date + window_days` | `false` | Row is updated on each run with latest metrics |
| `run_date >= send_date + window_days` | `true` | Row is frozen and never updated again |
| EDU/CONTENT (no Shopify attribution) | Set to `true` immediately if HubSpot stats are stable (delivered count matches previous run); otherwise `false` until window closes |

This ensures:
- Closed campaigns are permanently frozen — `is_final_snapshot = true` rows are never modified
- Open campaigns get refreshed each run until their window closes
- Historical data accumulates over time
- Downstream consumers can filter on `is_final_snapshot = true` for finalized reporting

### 8.5 Storage Format

For the initial implementation, use a CSV or Excel file as the history store. Structure:
- File: `campaign_history.csv` (or `.xlsx`)
- One header row + one data row per campaign snapshot
- Append new campaigns; update open-window campaigns in place
- Later can migrate to a database if needed

---

## 10. Token Refresh Approach

### 9.1 Shopify Token Lifecycle

| Property | Value |
|---|---|
| Token type | OAuth2 client_credentials grant |
| Endpoint | `POST https://grand-cru-liquid-assets.myshopify.com/admin/oauth/access_token` |
| Lifetime | 86,399 seconds (~24 hours) |
| Refresh method | Re-call the same endpoint with client_id + client_secret |
| Refresh token | None — not applicable to this grant type |

### 9.2 Implementation Strategy

```python
import time
import requests

class ShopifyAuth:
    def __init__(self, store_domain, client_id, client_secret):
        self.store_domain = store_domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = 0  # epoch seconds

    def get_token(self) -> str:
        """Return a valid access token, refreshing if expired or near-expiry."""
        # Refresh if within 5 minutes of expiry or no token yet
        if time.time() > (self.token_expires_at - 300):
            self._refresh_token()
        return self.access_token

    def _refresh_token(self):
        resp = requests.post(
            f"https://{self.store_domain}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.token_expires_at = time.time() + data["expires_in"]
```

### 9.3 Usage in Workflow

- Instantiate `ShopifyAuth` once at the start of each run
- Call `auth.get_token()` before every Shopify API request
- The class auto-refreshes if the token is within 5 minutes of expiry
- For a weekly batch run (expected runtime: minutes), a single token should be sufficient
- For longer runs or retry scenarios, the auto-refresh handles it

### 9.4 HubSpot Token

HubSpot uses a private app token (PAT) that does not expire unless revoked. No refresh logic needed. Store as `HUBSPOT_PRIVATE_APP_TOKEN` in `.env.txt`.

---

## 11. Remaining Blockers and Risks

### 10.1 Blockers (must resolve before production)

| # | Blocker | Impact | Proposed Resolution |
|---|---|---|---|
| 1 | **SHOPIFY_API_VERSION not in .env.txt** | Using hardcoded `2025-01`; may break on future deprecation | Add `SHOPIFY_API_VERSION=2025-01` to `.env.txt`; update when Shopify deprecates |
| 2 | **Old-format campaign names in transition window** | "2026_0318 2010 Cos d'Estournel" has old format but v3 `name` is old while its `campaignName` has new format. Two pre-March-18 campaigns (Lail, Cos d'Estournel) have real Shopify order data tied to discount codes that don't appear in the name. | Since scope starts 2026-03-18 and naming convention is being enforced going forward, these are transitional. The parser should log them as `PARSE_ERROR` and they should be manually reviewed. |
| 3 | **v1 campaigns endpoint has no date filter** | Must paginate through all campaigns to find matching IDs; ~1,480 emails exist | Mitigated by v3-first strategy: only look up v1 IDs for campaigns that pass the v3 date filter. Typically 5-10 campaigns per week = 5-10 v1 lookups per run. |

### 10.2 Risks (to monitor)

| # | Risk | Likelihood | Mitigation |
|---|---|---|---|
| 1 | Same discount code reused across campaigns | Low (not observed) | Attribution window + code + date composite match; log `DUPLICATE_CODE_WARNING` |
| 2 | Campaign name typos or non-standard formatting | Medium | Parser returns `PARSE_ERROR`; QA report surfaces these immediately |
| 3 | Shopify API rate limits (40 requests/second for private apps) | Low for weekly batch | Add 0.5s sleep between requests if rate-limited; batch date ranges |
| 4 | HubSpot v1 campaigns API deprecation | Medium-long-term | Monitor HubSpot changelog; v3 may eventually include stats |
| 5 | Refunded orders inflating attributed revenue | Low-medium | Use `current_total_price` / check `financial_status`; optionally exclude refunded orders |
| 6 | Discount code applied to non-campaign items in same order | Confirmed (Shipping Insurance example) | Line-item allocation logic handles this correctly |
| 7 | BIN Sale keyword heuristic misses new sale types | Low | Document keyword list; expand as new patterns emerge |

---

## 12. Implementation Sequence (Recommended Build Order)

```
Step 1: Campaign Name Parser + Unit Tests
         - Date normalization (3 formats)
         - 5-segment extraction
         - BIN/holiday keyword detection
         - Validation and QA bucket assignment

Step 2: HubSpot Data Extraction Module
         - v3 email list fetch with pagination
         - v1 campaign stats resolution via allEmailCampaignIds
         - Date filtering (>= 2026-03-18)
         - Sent-only filtering

Step 3: Shopify Auth Module
         - Token acquisition
         - Auto-refresh logic
         - Credential loading from .env.txt

Step 4: Shopify Order Attribution Module
         - Order fetch by date window
         - Discount code matching (case-insensitive)
         - Line-item discount allocation analysis
         - Metric computation per campaign

Step 5: Manual Override Loader
         - Load campaign_overrides.csv if present
         - Apply overrides before parsing
         - Tag overridden campaigns with OK_OVERRIDE

Step 6: Dashboard Assembly
         - Join HubSpot + Shopify data per campaign
         - Compute derived metrics (Revenue per Delivered, etc.)
         - Assign QA buckets
         - Compute is_final_snapshot per campaign
         - Generate campaign detail table CSV/Excel
         - Enforce: main table = new naming convention only

Step 7: Report View Generator
         - Weekly report: campaign-level summary with insights
         - Monthly report: discount-code-level aggregation
         - Producer performance report: producer-level aggregation
         - Performance insights logic (best/worst/efficient/underperforming)

Step 8: History Table Manager
         - Load existing history
         - Upsert logic (insert new, update if is_final_snapshot=false, skip if is_final_snapshot=true)
         - Save updated history

Step 9: QA Report Generator
         - Summary statistics
         - Error/warning listings
         - Unmatched Shopify discount codes output
         - Excluded campaigns output
         - Audit trail of matched orders

Step 10: End-to-End Integration + Testing
          - Run against live data from 2026-03-18 onward
          - Validate metrics against manual spot-checks
          - Confirm line-item attribution accuracy
          - Verify null vs zero distinction for codeless campaigns
          - Validate weekly/monthly/producer report outputs
```

---

## 13. Environment Configuration

### Required .env.txt format:

```
HUBSPOT_PRIVATE_APP_TOKEN=pat-na2-xxxxx
SHOPIFY_STORE_DOMAIN=grand-cru-liquid-assets.myshopify.com
SHOPIFY_CLIENT_ID=xxxxx
SHOPIFY_CLIENT_SECRET=shpss_xxxxx
SHOPIFY_API_VERSION=2025-01
```

### Reading credentials:

```python
def load_env(filepath=".env.txt"):
    """Parse key=value and 'Key: value' formats from .env.txt."""
    env = {}
    key_map = {
        "HUBSPOT_PRIVATE_APP_TOKEN": "HUBSPOT_PRIVATE_APP_TOKEN",
        "Shopify API Client ID": "SHOPIFY_CLIENT_ID",
        "Secret": "SHOPIFY_CLIENT_SECRET",
        "Store": "SHOPIFY_STORE_DOMAIN",
        "SHOPIFY_API_VERSION": "SHOPIFY_API_VERSION",
    }
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            for file_key, env_key in key_map.items():
                if line.startswith(file_key):
                    # Handle both "KEY=VALUE" and "Key: VALUE"
                    if "=" in line and file_key == line.split("=")[0]:
                        env[env_key] = line.split("=", 1)[1].strip()
                    elif ":" in line:
                        env[env_key] = line.split(":", 1)[1].strip()
    return env
```

---

## Appendix A: Confirmed API Responses (Phase 1 Evidence)

### HubSpot v3 Email Object — Key Fields Confirmed
- `name`: "2026-03-19 - Bryant Family - PROD - 7% - BryantFam" (source of truth)
- `campaignName`: May differ (e.g., Ornellaia has old format in campaignName)
- `allEmailCampaignIds`: Contains v1-resolvable IDs (confirmed for 6+ campaigns)
- `primaryEmailCampaignId`: Does NOT resolve in v1 (confirmed 404 for all tested)
- `state`: All sent emails show `AUTOMATED` (not `PUBLISHED`)

### HubSpot v1 Campaign Detail — Counters Confirmed
- `counters.delivered`: 1,342 (Bryant Family), 9,254 (BIN Sale), etc.
- `counters.open`, `counters.click`, `counters.bounce`, `counters.unsubscribed`: all present

### Shopify Order — Line-Item Discount Allocations Confirmed
- `discount_applications[].code`: exact discount code string
- `discount_applications[].target_selection`: "entitled" (only specific items)
- `line_items[].discount_allocations[].amount`: per-item discount dollar amount
- `line_items[].discount_allocations[].discount_application_index`: links to correct discount_application entry
- Confirmed: Order #18059 had 3 discounted items + 1 non-discounted item (Shipping Insurance)
- Confirmed: Order #18039 had 1 discounted product + 1 non-discounted product in same order

### Shopify Auth — Client Credentials Flow Confirmed
- Endpoint: POST /admin/oauth/access_token
- Response: `access_token`, `scope`, `expires_in: 86399`
- Scopes granted: `read_all_orders, read_price_rules, read_discounts, read_orders`
