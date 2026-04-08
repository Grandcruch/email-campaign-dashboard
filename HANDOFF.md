# Email Campaign Dashboard — Project Handoff

_Reconstructed 2026-04-07 from repo files. Previous conversation memory is not available; everything below is grounded in files in this folder._

## 1. Project Objective

A repeatable weekly workflow + Streamlit dashboard for **Grand Cru Liquid Assets** that measures email campaign performance by joining:

- **HubSpot** (campaign metadata + delivery stats)
- **Shopify** (orders + line-item discount allocations)

The business wants per-campaign Attributed Revenue, Discount Value, Discounted Orders, Delivered, and Revenue per Delivered, plus weekly / monthly / producer rollups and a historical performance CSV. Repo: https://github.com/Grandcruch/email-campaign-dashboard.git

## 2. Current Status

The pipeline runs end-to-end. Most recent outputs in `output/` are from 2026-03-23 → 2026-03-26. Git history shows active iteration on the UI and metric definitions through commit `225ecb5` ("Expand data scope to 2026-03-01, add BIN/holiday campaign filter"). `plan.md`, `logic_spec.md`, and `prompts/` are untracked (not yet committed).

What appears **completed**:
- Campaign name parser with 3 date formats, BIN/holiday keyword detection, QA buckets (`src/parser.py`)
- HubSpot v3 → v1 resolution with pagination and delivered-gating (`src/hubspot.py`)
- Shopify OAuth client-credentials auth with auto-refresh (`src/auth.py`)
- Line-item attribution for single codes + **discount family** attribution for multi-code BIN Sale campaigns (`src/shopify_orders.py`, `src/families.py`, `discount_family_mapping.csv`)
- Manual overrides loader (`src/overrides.py`, `campaign_overrides.csv`)
- Assembled reports: weekly, monthly, producer, QA summary, unmatched codes, excluded campaigns, campaign history upsert with `is_final_snapshot` (`src/reports.py`)
- Streamlit dashboard with red-scale design system, password gate, calendar date picker, 6 charts (`dashboard.py`, 1,494 lines)
- Weekly historical CSV export scheduled via `run_weekly_export.bat` → `export_historical_csv.py`
- Streamlit Cloud deploy (`.streamlit/secrets.toml` referenced in `.gitignore`)

What appears **incomplete / inconsistent / worth attention**:
- **Metric definition drift.** `logic_spec.md` locks Attributed Revenue as *gross pre-discount line-item value*. Git commit `6fe3c1b` switched it to *net*, and `export_historical_csv.py` uses Shopify `current_subtotal_price` (order-level net of discounts, excludes tax/shipping). The spec, the dashboard, and the historical export are no longer definitionally aligned.
- **`plan.md` / `logic_spec.md` / `prompts/` are untracked** — the authoritative design docs are not in git history. Risk of loss.
- **`STATS_UNAVAILABLE` campaigns** in the last QA summary (Rauzan Segla, Trotanoy, Spottswoode) — need to confirm whether they were actually sent or the v1 lookup is missing an ID.
- **`LEGACY_FORMAT` campaign** `2026-0323 - 1983 Palmer- PROD - 7% - Palmer` is excluded because of the missing space after "Palmer" — a parser hardening opportunity or override target.
- **Unmatched Kapcsandy (6 orders) and `lastchance` (2 orders)** are flagged "unknown — review manually" in the QA output.
- **Dashboard.py is 1,494 lines** in a single file — maintainable but getting long.
- **`campaign_overrides.csv` is gitignored** but two BIN Sale family overrides are referenced there. Those overrides are only on Chang's machine.

## 3. Architecture / File Map

```
Email campaign dashboard/
├── .env.txt                          # HubSpot + Shopify credentials (gitignored)
├── .streamlit/secrets.toml           # Streamlit Cloud secrets (gitignored)
├── plan.md                           # Phase-2 implementation blueprint (UNTRACKED)
├── logic_spec.md                     # Authoritative business rules (UNTRACKED)
├── prompts/phase1_research.txt       # Phase-1 research brief (UNTRACKED)
├── campaign_overrides.csv            # Manual overrides (gitignored)
├── discount_family_mapping.csv       # BIN Sale family → Shopify discount titles
├── requirements.txt                  # streamlit, pandas, requests, altair
│
├── run_dashboard.py                  # CLI orchestrator: fetch + compute + write outputs
├── dashboard.py                      # Streamlit app (1494 lines, all-in-one)
├── export_historical_csv.py          # Weekly historical CSV (current_subtotal_price)
├── run_weekly_export.bat             # Scheduler entry point
│
├── src/
│   ├── config.py        # load_env, constants, BIN_HOLIDAY_KEYWORDS, known non-campaign code patterns
│   ├── auth.py          # ShopifyAuth client_credentials + hubspot_headers
│   ├── parser.py        # parse_campaign_name, normalise_date, is_bin_or_holiday
│   ├── hubspot.py       # fetch_campaigns (v3 list → v1 resolve), CampaignRecord
│   ├── overrides.py     # load_overrides + apply_overrides (pre-parse)
│   ├── families.py      # load_family_mapping, is_family_key, FamilyMember
│   ├── shopify_orders.py# compute_attribution, compute_family_attribution, fetch_all_discount_codes_in_range
│   └── reports.py       # DashboardRow, assemble_dashboard_rows, weekly/monthly/producer generators, history upsert
│
├── output/                           # Regenerated each run (gitignored)
│   ├── campaign_detail.csv
│   ├── campaign_history.csv
│   ├── weekly_campaign_report.csv + weekly_insights.txt
│   ├── monthly_discount_report.csv
│   ├── producer_performance_report.csv
│   ├── qa_summary.txt, qa_excluded_campaigns.csv, qa_unmatched_shopify_codes.csv
│   └── Historical Email Offer Performance(2026.3.9~).csv
│
└── campaign*.json, emails_response.json, v1camp*.json   # Phase-1 API capture samples
```

## 4. Business Rules (from logic_spec.md — treat as authoritative)

- **Campaign name format:** `YYYY-MM-DD - Producer/Topic - Type - OfferValue - Code` (5 segments split by ` - `).
- **Date formats accepted:** `2026-03-20`, `2026-0323`, `2026_0317` — normalised via `^(\d{4})[-_]?(\d{2})-?(\d{2})`.
- **Campaign types:** `PROD`, `EDU`, `CONTENT`. Only PROD with a non-None code is eligible for Shopify attribution.
- **Attribution window:** 7 days standard, 3 days if name/producer matches `bin sale`, `holiday sale`, `flash sale`, `clearance`, or their reminders.
- **Sent gate:** at least one v1 campaign ID from `allEmailCampaignIds` must return `counters.delivered > 0`.
- **Date floor:** parsed send date must be ≥ 2026-03-18 for the main table (now relaxed to 2026-03-01 per commit `225ecb5`).
- **Attribution algorithm (spec-locked):** line-item level — for each matched order, find the `discount_applications` index whose code == campaign code, then sum `line_item.price * quantity` for line items whose `discount_allocations` reference that index. Sum `discount_allocations[i].amount` for Discount Value. Use `order.total_price` for Total Order Value.
- **Null vs zero:** `null` = attribution not attempted (EDU / CONTENT / Code=None). `0` = attempted, no matching orders. This distinction must be preserved in every output.
- **History upsert:** key = `(parsed_send_date, discount_code, campaign_name)`. Rows with `is_final_snapshot=true` are frozen forever. Open-window rows update in place.
- **Duplicate codes:** attribute to the most recent send_date preceding the order; flag `DUPLICATE_CODE_WARNING`.
- **Discount families:** a single campaign code that's actually a family key in `discount_family_mapping.csv` fans out to multiple Shopify identifiers (e.g., `BINSALE_GROUP` → `BinSale10` + `BinSale12`). Matched via `families.py`.
- **Known non-campaign codes** (excluded from unmatched-code review): `GrandCru`, `GCLA-*`, `THANKYOU*`.
- **Overrides:** `campaign_overrides.csv` keyed on exact HubSpot v3 `name`; applied BEFORE parsing; `force_exclude > force_include > field overrides`.
- **Minimum delivered threshold:** 50 (per `reports.MIN_DELIVERED_THRESHOLD`) for efficiency rankings in insights.

## 5. Known Issues

1. **Spec vs code metric drift** — `logic_spec.md` says Attributed Revenue is gross pre-discount line-item value, but commit `6fe3c1b` switched it to net and `export_historical_csv.py` uses the order-level `current_subtotal_price`. These three must be reconciled explicitly.
2. **Design docs are not in git** — `plan.md`, `logic_spec.md`, `prompts/phase1_research.txt` are untracked.
3. **`campaign_overrides.csv` is gitignored** — business-sensitive per `.gitignore` comment, but this also means the production overrides only live on one laptop.
4. **Parser fragility** — `1983 Palmer- PROD` failed to parse due to one missing space. A friendlier split or a diagnostic "near-miss" QA bucket would help.
5. **`STATS_UNAVAILABLE` false negatives risk** — 3 campaigns in last run. Need a retry-on-next-run verification and a clear owner for investigating.
6. **`dashboard.py` monolith** (1,494 lines) — duplicated imports of `src/*` between `dashboard.py` and `run_dashboard.py`. Could be refactored into shared data-loader module.
7. **`.env.txt`-style custom loader** — uses a bespoke key map (`load_env` in `config.py`) rather than standard `python-dotenv`; risk of silent missing keys.
8. **`SHOPIFY_API_VERSION` hardcoded to 2025-01** per plan.md §10.1.

## 6. Open Questions

1. Is Attributed Revenue **officially** gross or net now? Which of (a) dashboard display, (b) historical CSV, (c) `logic_spec.md` is correct?
2. Should the historical CSV's `Revenue` column and the dashboard's "Attributed Revenue" be the **same number**, or intentionally different?
3. Is the data floor locked at 2026-03-18 (per spec) or 2026-03-01 (per latest commit)?
4. What's the desired resolution for the Kapcsandy 6 unmatched orders and the `lastchance` code?
5. Should `LEGACY_FORMAT` campaigns with a trivial typo auto-repair, or always go through the overrides file?
6. Is the dashboard currently deployed to Streamlit Cloud and being used weekly, or only run locally?
7. Should `plan.md` / `logic_spec.md` be committed to the repo now?
8. Is there a target for future unit tests, or is manual spot-checking fine?

## 7. Recommended Next Actions (ordered)

1. **Commit the design docs** (`plan.md`, `logic_spec.md`, `prompts/`, this `HANDOFF.md`) so future sessions have ground truth in git.
2. **Reconcile the revenue definition.** Pick one: gross line-item, net line-item, or order-level net. Update `logic_spec.md`, `src/reports.py`, and `export_historical_csv.py` so all three agree. Add a one-line comment at the top of `reports.py` citing the spec section.
3. **Add a lightweight test harness** (`tests/test_parser.py` + `tests/test_attribution.py`) seeded by the existing `campaign*.json` and `emails_response.json` fixtures to lock in parser + attribution behavior before any more refactors.
4. **Harden the parser** against the `1983 Palmer- PROD` class of typos: either accept `- ?` as a delimiter or emit a `PARSE_NEAR_MISS` QA bucket that suggests an override.
5. **Investigate the 3 `STATS_UNAVAILABLE` campaigns** (Rauzan Segla, Trotanoy, Spottswoode) to confirm they really are unsent vs. a v1 lookup miss.
6. **Extract shared data loading** from `dashboard.py` + `run_dashboard.py` into `src/pipeline.py` to remove duplication.
7. **Document the overrides workflow** in README so the `campaign_overrides.csv` rules are discoverable without reading `plan.md` §7.4.
8. **Add `SHOPIFY_API_VERSION`** to `.env.txt` per plan.md §10.1 blocker #1.
