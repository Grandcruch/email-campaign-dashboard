# CLAUDE.md — Email Campaign Dashboard

Orientation doc for any Claude session continuing this project. Read this first.

## What this dashboard is for

Grand Cru Liquid Assets sends 5–10 HubSpot email campaigns per week. Some include a wine offer and a Shopify discount code. This project is a **repeatable weekly workflow + Streamlit dashboard** that joins HubSpot delivery stats with Shopify order data to measure per-campaign performance: Attributed Revenue, Discount Value, Discounted Orders, Delivered, and Revenue per Delivered. It also rolls up weekly, monthly, and per-producer views, and maintains a frozen history table for trend analysis.

Repo: https://github.com/Grandcruch/email-campaign-dashboard.git
Deployment: Streamlit Cloud (password-gated).

## Data sources

- **HubSpot v3** (`/marketing/v3/emails`) — authoritative `name`, `allEmailCampaignIds`, pagination by `publishDate`. Source of truth for campaign name.
- **HubSpot v1** (`/email/public/v1/campaigns/{id}`) — delivery counters (`delivered`, `open`, `click`, `bounce`, `unsubscribed`). A campaign is only "sent" if at least one v1 ID resolves with `delivered > 0`.
- **Shopify Admin REST** (`/admin/api/2025-01/orders.json`) — orders with `discount_applications` and `line_items[].discount_allocations`, fetched by date window per campaign.
- **Shopify OAuth** client_credentials — 24h tokens, auto-refreshed in `src/auth.py`.
- **Local files:** `campaign_overrides.csv` (manual overrides, gitignored), `discount_family_mapping.csv` (BIN Sale fan-out), `.env.txt` (secrets).

## How attribution works (locked)

**Line-item level, net of discount.** For each campaign with a valid code and PROD type:

1. Fetch Shopify orders where `created_at` ∈ `[send_date, send_date + window)` (window = 7 days, or 3 days if the name/producer matches BIN / holiday / flash / clearance keywords).
2. For each order, find `i = index of discount_applications[] where type=="discount_code" and code==campaign_code` (case-insensitive).
3. For each `line_item`, if any `discount_allocations[]` entry has `discount_application_index == i`, the line is attributed:
   - **Attributed Revenue** += `line_item.price * quantity - discount_allocations[i].amount` (net of the campaign discount on that line).
   - **Discount Value** += `discount_allocations[i].amount`.
4. **Total Order Value** = sum of `order.total_price` across matched orders (includes non-discounted lines, tax, shipping).
5. **Discounted Orders** = count of matched orders.
6. **Revenue per Delivered** = Attributed Revenue / Delivered.

**Null vs zero semantics (must be preserved everywhere — CSV, Excel, JSON, UI):**
- `null` = attribution not attempted (EDU, CONTENT, or Code=None).
- `0` = attribution attempted, no matching orders.
- `> 0` = normal.

**Discount families:** a campaign "code" may actually be a family key (e.g. `BINSALE_GROUP`) mapped in `discount_family_mapping.csv` to multiple Shopify discount identifiers. Attribution fans out via `compute_family_attribution` in `src/shopify_orders.py`. Used for BIN Sale campaigns that deploy two automatic discounts (`BinSale10` + `BinSale12`).

**Duplicate codes:** attribute to the most recent `send_date` preceding the order; flag `DUPLICATE_CODE_WARNING`.

**Single attribution only** — each order is attributed to exactly one campaign.

## Naming convention

Campaigns must follow: `YYYY-MM-DD - Producer/Topic - Type - OfferValue - Code`

- **Date formats accepted:** `2026-03-20`, `2026-0323`, `2026_0317`. Parser normalises all three via `^(\d{4})[-_]?(\d{2})-?(\d{2})`.
- **Type** ∈ {`PROD`, `EDU`, `CONTENT`, `HYBR`}. Campaigns with a non-None code are attribution-eligible regardless of type; codeless campaigns show delivery metrics only (Shopify columns `null`). `HYBR` (added 2026-08-06) marks a hybrid send paired with an EDU sibling (same code + send date); A/B grouping merges the pair into one reporting row.
- **Split delimiter:** ` - ` (space-dash-space), exactly 5 segments. The parser auto-repairs minor whitespace-around-dash typos (e.g. `Palmer- PROD`); anything not auto-repairable goes to `PARSE_ERROR` / `LEGACY_FORMAT` in the QA excluded output and can be rescued via `campaign_overrides.csv`.
- **Code = "None"** → `discount_code = null`, no attribution attempted.

## Business rules (authoritative)

- `src/config.py:DATA_START_DATE` is the data floor. Currently **2026-03-01**. Do not change without explicit approval — history rows are keyed on this.
- Minimum delivered for efficiency rankings: **50** (`reports.MIN_DELIVERED_THRESHOLD`).
- BIN / holiday keyword list lives in `src/config.py:BIN_HOLIDAY_KEYWORDS`.
- Known non-campaign discount codes (excluded from unmatched-code review): `GrandCru`, `GCLA-*`, `THANKYOU*`.
- History table upsert key: `(parsed_send_date, discount_code, campaign_name)`. Rows with `is_final_snapshot = true` are **frozen forever** — never overwrite.
- `campaign_overrides.csv` is applied **before** parsing. `force_exclude > force_include > field overrides`. Overridden rows get `qa_bucket = OK_OVERRIDE`.
- Shopify auth pre-expiry buffer: 5 minutes. Retry once on 401 with forced refresh.

## File map

```
dashboard.py                # Streamlit app (monolith, 1494 lines)
run_dashboard.py            # CLI orchestrator for weekly runs
export_historical_csv.py    # Weekly historical CSV export (scheduled)
run_weekly_export.bat       # Windows scheduler entry

src/
  config.py                 # env loader, DATA_START_DATE, constants
  auth.py                   # ShopifyAuth + hubspot_headers
  parser.py                 # parse_campaign_name, normalise_date
  hubspot.py                # fetch_campaigns (v3 → v1 resolve)
  overrides.py              # load_overrides, apply_overrides
  families.py               # discount family mapping loader
  shopify_orders.py         # compute_attribution, compute_family_attribution
  reports.py                # DashboardRow, assemble, weekly/monthly/producer, history upsert

plan.md                     # Phase-2 implementation blueprint
logic_spec.md               # Authoritative metric + rule specification
HANDOFF.md                  # Reconstructed project state (2026-04-07)
prompts/phase1_research.txt # Original research brief

campaign_overrides.csv      # Manual overrides (gitignored, not backed up)
discount_family_mapping.csv # BIN Sale multi-code fan-out
.env.txt                    # Secrets (gitignored)

output/                     # Regenerated each run (gitignored)
```

## Current limitations

- `campaign_overrides.csv` lives only on one laptop. No backup.
- `dashboard.py` duplicates fetch/assemble logic from `run_dashboard.py` (1,494 lines, single file).
- No automated tests. Validation is manual spot-checking.
- Shopify API version hardcoded in `src/config.py` (being moved to `.env.txt`).
- `STATS_UNAVAILABLE` campaigns surface in QA output even though they're simply unsent — noisy but not wrong.

## How to safely continue development

1. **Read `HANDOFF.md`, `logic_spec.md`, and `plan.md` before touching code.** Those are the ground truth. If this CLAUDE.md disagrees with `logic_spec.md`, `logic_spec.md` wins and this file should be updated.
2. **Preserve the null-vs-zero distinction** in every new output. It is a correctness invariant.
3. **Do not overwrite `is_final_snapshot = true` rows** in `campaign_history.csv`. The upsert logic in `src/reports.py` handles this; do not bypass it.
4. **Do not change `DATA_START_DATE`** without user approval. History rows depend on it.
5. **Do not commit** `.env.txt`, `.streamlit/secrets.toml`, `campaign_overrides.csv`, or anything in `output/`.
6. **When adding a metric**, update `logic_spec.md` in the same change so the spec and code never diverge again.
7. **Run `python run_dashboard.py` locally** to regenerate `output/` before committing any logic change, and spot-check `weekly_campaign_report.csv` + `qa_summary.txt`.
8. **Parser changes** are especially risky — they cascade into the history table. Prefer additive changes (e.g. new QA bucket) over modifying existing parse logic.
9. **Before any refactor of `dashboard.py` or `reports.py`**, confirm with the user — the project is actively used in production.

## Recent decisions (2026-04-07 handoff session)

- **Attributed Revenue = net line-item:** `line_item.price * quantity - discount_allocations[i].amount`. This is the locked definition. `logic_spec.md` must be updated to match.
- **Data floor = 2026-03-01** (matches current `config.DATA_START_DATE`). May expand to earlier months later, but existing floor is frozen for now; don't touch history rows before 2026-03-01.
- **Design docs (`plan.md`, `logic_spec.md`, `prompts/`, `HANDOFF.md`, `CLAUDE.md`) will be committed to git.**
- **STATS_UNAVAILABLE campaigns are simply unsent** — no investigation needed; can be suppressed or left as-is.
- **Parser auto-repair** for trivial whitespace-around-dash typos (e.g. `Palmer- PROD` → `Palmer - PROD`). Anything not auto-repairable still goes to QA excluded.
- **`SHOPIFY_API_VERSION`** to be moved out of hardcode and into `.env.txt`.
- **Test harness postponed** — not a priority right now.
