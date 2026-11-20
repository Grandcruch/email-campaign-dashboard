"""
offer_slots.py — Offer A vs Offer B under the 2-1-1 cadence (logic_spec 6.8,
added 2026-11-20).

On a day with two (or more) offers, the main offer is A and the other(s) B.
Days with one offer are "Single" (e.g. Fri/Sat/Sun anchors, 1-a-day periods).

Which offer is A comes from Everett's Offer Log (sheet "Offer Log" of
"Offer Evaluation Standard - Agent4.xlsx": column D = send date, E = offer,
I = Rank, "A" = main offer). A log row is matched to a campaign by send date
(same day preferred, ±1 day allowed) and producer-name word overlap. When the
log has no single "A" for a two-offer day, the offer with the bigger audience
(Delivered) is A and the day is marked "Estimated". HubSpot names win when the
log disagrees (log entries are sometimes moved after the fact).

Offers = campaigns with a discount code and attribution, excluding events
(BIN Sale, Large Format, Holiday) — the same set as the cadence evaluation.
"""

import io
import re
from datetime import date, datetime, timedelta

import pandas as pd

from .reports import event_mask

SLOT_A, SLOT_B, SLOT_SINGLE = "A", "B", "Single"
SOURCE_LOG, SOURCE_EST = "Offer Log", "Estimated (bigger audience)"

_STOP = {"the", "de", "du", "la", "le", "les", "chateau", "château", "domaine", "estate",
         "vineyard", "vineyards", "family", "reserve", "edu", "owc", "case", "cases",
         "napa", "valley", "and", "&", "of", "wine", "wines", "st", "saint", "offer"}


def _tokens(s: str) -> set[str]:
    s = re.sub(r"[’'`]", "", str(s or "").lower())
    s = re.sub(r"\b(19|20)\d{2}\b|\b\d{2,4}\b", " ", s)     # drop vintages / numbers
    return {t for t in re.findall(r"[a-zà-ü]+", s) if t not in _STOP and len(t) > 2}


def _cell_date(v, today: date) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
        m = re.match(r"^(\d{1,2})-([A-Za-z]{3})$", s)        # "19-Nov" (no year)
        if m:
            try:
                d = datetime.strptime(f"{s}-{today.year}", "%d-%b-%Y").date()
            except ValueError:
                return None
            return d if d <= today + timedelta(days=60) else d.replace(year=today.year - 1)
    return None


def parse_offer_log(xlsx_bytes: bytes, today: date) -> pd.DataFrame:
    """Rows of the 'Offer Log' sheet: log_send_date, log_offer, log_rank."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    ws = wb["Offer Log"]
    out = []
    for r in list(ws.iter_rows(values_only=True))[1:]:
        if len(r) < 9:
            continue
        sd = _cell_date(r[3], today)
        if sd is None:
            continue
        out.append({
            "log_send_date": sd,
            "log_offer": str(r[4]).strip() if r[4] else "",
            "log_rank": str(r[8]).strip().upper() if r[8] else "",
        })
    return pd.DataFrame(out, columns=["log_send_date", "log_offer", "log_rank"])


def _match_log(offers: pd.DataFrame, log_df: pd.DataFrame) -> pd.Series:
    """Log rank for each offer row (index-aligned), '' when unmatched."""
    ranks = pd.Series("", index=offers.index, dtype=object)
    if log_df is None or log_df.empty:
        return ranks
    used: set = set()
    by_date: dict[date, list] = {}
    for j, l in log_df.iterrows():
        by_date.setdefault(l["log_send_date"], []).append((j, l))
    for i, c in offers.iterrows():
        sd = c["_send"]
        ct = _tokens(c.get("Producer / Topic") or c.get("Campaign Name", ""))
        if not ct:
            continue
        best_s, best_j, best_rank = 0.0, None, ""
        for dd in (0, -1, 1):
            for j, l in by_date.get(sd + timedelta(days=dd), []):
                if j in used:
                    continue
                lt = _tokens(l["log_offer"])
                inter = len(ct & lt)
                if not lt or not inter:
                    continue
                s = inter / min(len(ct), len(lt)) - 0.15 * abs(dd)
                if s > best_s:
                    best_s, best_j, best_rank = s, j, l["log_rank"]
        if best_j is not None and best_s >= 0.5:
            used.add(best_j)
            ranks.at[i] = best_rank
    return ranks


def assign_offer_slots(df: pd.DataFrame, log_df: pd.DataFrame | None,
                       offer_type_map: dict) -> pd.DataFrame:
    """Return a frame indexed like df's offers with Offer Slot / Slot Source."""
    cols = ["Offer Slot", "Slot Source"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    m = (
        (df["Discount Code"] != "None")
        & df["Attributed Revenue"].notna()
        & df["Parsed Send Date"].notna()
        & ~event_mask(df, offer_type_map)
    )
    offers = df[m].copy()
    if offers.empty:
        return pd.DataFrame(columns=cols)
    offers["_send"] = pd.to_datetime(offers["Parsed Send Date"]).dt.date
    offers["_rank"] = _match_log(offers, log_df)

    out = pd.DataFrame(index=offers.index, columns=cols, dtype=object)
    for _, grp in offers.groupby("_send"):
        if len(grp) == 1:
            out.loc[grp.index, cols] = [SLOT_SINGLE, ""]
            continue
        a_idx = [i for i in grp.index if grp.at[i, "_rank"] == "A"]
        if len(a_idx) == 1:
            a, source = a_idx[0], SOURCE_LOG
        else:
            a, source = grp["Delivered"].idxmax(), SOURCE_EST
        for i in grp.index:
            out.loc[i, cols] = [SLOT_A if i == a else SLOT_B, source]
    return out
