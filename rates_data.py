"""Daily interest-rate LEVELS (in percent), stored separately from the
returns-based spdrfactors pickle because rates are a different data model:
we keep the yield *level*, not a % return. A basis-point move is then just
`100 * level.diff()`.

Output: `<data_hub>/rates_levels.pickle` — a wide DataFrame, DatetimeIndex,
columns named `<COUNTRY>_<TENOR>` (US_10Y, EA_10Y, CA_10Y, ...), values = yield
in percent. Different markets have different holiday calendars, so the index is
the union of trading days and any given column may be NaN on another market's
holiday (consumers should tolerate per-series NaN, like the Day Diagnostic page).

Sources (all free, no API key):
  - US:   US Treasury daily par yield curve CSV (full curve, current)
  - Euro: ECB Data Portal, euro-area AAA govt spot curve (YC dataset)
  - UK:   Bank of England GLC nominal gilt spot curve (Excel in zips; fitted
          spot rates, ~1 business-day lag on the current-month file)
  - CA:   Bank of Canada Valet API (benchmark bond yields)

Standalone:  python rates_data.py --db <data-hub>  [--rebuild]
"""
import os
import io
import re
import pickle
import zipfile
import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

HDRS = {"User-Agent": "Mozilla/5.0 (fin-data rates_update)"}
TIMEOUT = 30

# --- canonical tenor maps ---------------------------------------------------
US_TENOR_MAP = {
    "1 Mo": "US_1M", "2 Mo": "US_2M", "3 Mo": "US_3M", "6 Mo": "US_6M",
    "1 Yr": "US_1Y", "2 Yr": "US_2Y", "3 Yr": "US_3Y", "5 Yr": "US_5Y",
    "7 Yr": "US_7Y", "10 Yr": "US_10Y", "20 Yr": "US_20Y", "30 Yr": "US_30Y",
}
ECB_TENOR_MAP = {
    "SR_3M": "EA_3M", "SR_6M": "EA_6M", "SR_1Y": "EA_1Y", "SR_2Y": "EA_2Y",
    "SR_3Y": "EA_3Y", "SR_5Y": "EA_5Y", "SR_7Y": "EA_7Y", "SR_10Y": "EA_10Y",
    "SR_20Y": "EA_20Y", "SR_30Y": "EA_30Y",
}
BOC_SERIES_MAP = {
    "BD.CDN.2YR.DQ.YLD": "CA_2Y",
    "BD.CDN.5YR.DQ.YLD": "CA_5Y",
    "BD.CDN.10YR.DQ.YLD": "CA_10Y",
    "BD.CDN.LONG.DQ.YLD": "CA_LONG",
}
# Bank of England GLC nominal (gilt) spot curve — fitted spot rates, Excel in zips.
BOE_BASE = "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/"
BOE_LATEST_ZIP = "latest-yield-curve-data.zip"
BOE_LATEST_FILE = "GLC Nominal daily data current month.xlsx"
BOE_HISTORY_ZIP = "glcnominalddata.zip"   # ~39 MB, period files 1979..present
BOE_SPOT_SHEET = "4. spot curve"
BOE_TENORS = [2, 5, 10, 20, 30]           # years -> UK_2Y, UK_5Y, ...

PREFERRED_ORDER = (
    ["US_1M", "US_2M", "US_3M", "US_6M", "US_1Y", "US_2Y", "US_3Y",
     "US_5Y", "US_7Y", "US_10Y", "US_20Y", "US_30Y"]
    + ["EA_3M", "EA_6M", "EA_1Y", "EA_2Y", "EA_3Y", "EA_5Y", "EA_7Y",
       "EA_10Y", "EA_20Y", "EA_30Y"]
    + ["UK_2Y", "UK_5Y", "UK_10Y", "UK_20Y", "UK_30Y"]
    + ["CA_2Y", "CA_5Y", "CA_10Y", "CA_LONG"]
)


def _get(url, timeout=TIMEOUT):
    r = requests.get(url, headers=HDRS, timeout=timeout)
    r.raise_for_status()
    return r


# --- per-source fetchers ----------------------------------------------------
def fetch_us_treasury(years) -> pd.DataFrame:
    """US Treasury daily par yield curve, one CSV request per calendar year."""
    frames = []
    for yr in years:
        url = (f"https://home.treasury.gov/resource-center/data-chart-center/"
               f"interest-rates/daily-treasury-rates.csv/{yr}/all"
               f"?type=daily_treasury_yield_curve&field_tdr_date_value={yr}"
               f"&page&_format=csv")
        try:
            d = pd.read_csv(io.StringIO(_get(url).text))
        except Exception as e:
            print(f"  [US {yr}] skip: {e.__class__.__name__}")
            continue
        d["Date"] = pd.to_datetime(d["Date"])
        keep = {src: dst for src, dst in US_TENOR_MAP.items() if src in d.columns}
        d = (d[["Date"] + list(keep)]
             .rename(columns=keep)
             .set_index("Date")
             .apply(pd.to_numeric, errors="coerce")
             .sort_index())
        frames.append(d)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="last")]


def fetch_ecb(start_period: str) -> pd.DataFrame:
    """ECB euro-area AAA govt spot curve; one request per tenor."""
    cols = {}
    for tn, name in ECB_TENOR_MAP.items():
        url = (f"https://data-api.ecb.europa.eu/service/data/YC/"
               f"B.U2.EUR.4F.G_N_A.SV_C_YM.{tn}"
               f"?startPeriod={start_period}&format=csvdata")
        try:
            r = requests.get(url, headers=HDRS, timeout=TIMEOUT)
            if r.status_code != 200:
                print(f"  [ECB {tn}] HTTP {r.status_code}")
                continue
            d = pd.read_csv(io.StringIO(r.text))
            cols[name] = pd.Series(
                pd.to_numeric(d["OBS_VALUE"], errors="coerce").values,
                index=pd.to_datetime(d["TIME_PERIOD"]), name=name,
            )
        except Exception as e:
            print(f"  [ECB {tn}] skip: {e.__class__.__name__}")
    return pd.DataFrame(cols).sort_index() if cols else pd.DataFrame()


def fetch_boc(start_date: str) -> pd.DataFrame:
    """Bank of Canada Valet benchmark bond yields; one request per series."""
    cols = {}
    for sid, name in BOC_SERIES_MAP.items():
        url = (f"https://www.bankofcanada.ca/valet/observations/{sid}/json"
               f"?start_date={start_date}")
        try:
            j = _get(url).json()
            recs = [(o["d"], o[sid]["v"]) for o in j["observations"]
                    if o.get(sid, {}).get("v", "") not in ("", None)]
            if not recs:
                continue
            idx = pd.to_datetime([d for d, _ in recs])
            vals = pd.to_numeric([v for _, v in recs], errors="coerce")
            cols[name] = pd.Series(vals, index=idx, name=name)
        except Exception as e:
            print(f"  [BoC {sid}] skip: {e.__class__.__name__}")
    return pd.DataFrame(cols).sort_index() if cols else pd.DataFrame()


def _parse_boe_spot(xlsx_bytes, tenors=BOE_TENORS) -> pd.DataFrame:
    """Parse one BoE GLC 'spot curve' workbook -> UK_<tenor>Y columns.

    Layout: row 3 holds the maturity grid in years (0.5..40 by 0.5); data starts
    row 5 with the date in col 0 and fitted spot yields across the columns.
    """
    full = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=BOE_SPOT_SHEET, header=None)
    years = pd.to_numeric(full.iloc[3, 1:], errors="coerce").values
    idx = pd.to_datetime(full.iloc[5:, 0], errors="coerce")
    vals = full.iloc[5:, 1:].apply(pd.to_numeric, errors="coerce")
    vals.index = idx
    vals = vals[vals.index.notna()]
    cols = {}
    for t in tenors:
        j = int(np.nanargmin(np.abs(years - t)))
        cols[f"UK_{t}Y"] = vals.iloc[:, j]
    return pd.DataFrame(cols)


def fetch_boe_gilts(full_history=False, start_year=2000) -> pd.DataFrame:
    """UK gilt nominal spot yields from the Bank of England.

    Always pulls the small current-month file (fresh, ~1 business-day lag). When
    `full_history`, also downloads the ~39 MB archive zip and parses the period
    workbooks whose range reaches `start_year` or later.
    """
    frames = []
    try:
        z = zipfile.ZipFile(io.BytesIO(_get(BOE_BASE + BOE_LATEST_ZIP, timeout=60).content))
        frames.append(_parse_boe_spot(z.read(BOE_LATEST_FILE)))
    except Exception as e:
        print(f"  [BoE current-month] skip: {e.__class__.__name__}: {e}")

    if full_history:
        try:
            z = zipfile.ZipFile(io.BytesIO(_get(BOE_BASE + BOE_HISTORY_ZIP, timeout=180).content))
            for name in z.namelist():
                if not name.lower().endswith(".xlsx"):
                    continue
                m = re.search(r"_(\d{4})\s*to\s*(\d{4}|present)", name)
                end_year = 9999 if (m and m.group(2) == "present") else (int(m.group(2)) if m else 0)
                if end_year < start_year:
                    continue
                try:
                    frames.append(_parse_boe_spot(z.read(name)))
                except Exception as e:
                    print(f"  [BoE {name}] skip: {e.__class__.__name__}")
        except Exception as e:
            print(f"  [BoE history] skip: {e.__class__.__name__}: {e}")

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="last")]


# --- orchestration ----------------------------------------------------------
def rates_update(data_db_root, start_year=2000, full_rebuild=False, overwrite=True):
    """Fetch/refresh rates_levels.pickle. Incremental by default (pulls a
    trailing window and lets fresh values win on overlap); full_rebuild pulls
    from `start_year`."""
    os.makedirs(data_db_root, exist_ok=True)  # cold start: dir may not exist yet
    pkl = os.path.join(data_db_root, "rates_levels.pickle")
    try:
        with open(pkl, "rb") as f:
            existing = pickle.load(f)
    except FileNotFoundError:
        existing = None
        full_rebuild = True

    now = datetime.today()
    if full_rebuild:
        years = range(start_year, now.year + 1)
        start_period = f"{start_year}-01-01"
        start_date = f"{start_year}-01-01"
        print(f"rates_update: FULL rebuild {start_year}..{now.year}")
    else:
        last = existing.index.max()
        years = range(max(start_year, (last - timedelta(days=400)).year), now.year + 1)
        start_period = (last - timedelta(days=20)).strftime("%Y-%m-%d")
        start_date = start_period
        print(f"rates_update: incremental from {start_period} (last={last.date()})")

    us = fetch_us_treasury(years)
    ea = fetch_ecb(start_period)
    ca = fetch_boc(start_date)

    # UK: the archive rolls into monthly zips, so the cheap current-month file
    # alone can miss a prior month's tail. Pull the 39 MB history when rebuilding,
    # when UK is absent, or when the last UK point predates the current month.
    uk_last = None
    if existing is not None and "UK_10Y" in existing.columns:
        s = existing["UK_10Y"].dropna()
        uk_last = s.index.max() if len(s) else None
    need_uk_hist = (full_rebuild or uk_last is None
                    or uk_last < pd.Timestamp(now.year, now.month, 1))
    uk = fetch_boe_gilts(full_history=need_uk_hist, start_year=start_year)

    parts = [p for p in (us, ea, ca, uk) if not p.empty]
    if not parts:
        raise RuntimeError("rates_update: all sources returned empty")
    new = pd.concat(parts, axis=1).sort_index()

    if existing is not None:
        out = new.combine_first(existing)  # fresh values win on overlap
    else:
        out = new
    out = out[~out.index.duplicated(keep="last")].sort_index()
    ordered = [c for c in PREFERRED_ORDER if c in out.columns]
    out = out[ordered + [c for c in out.columns if c not in ordered]]

    if overwrite:
        with open(pkl, "wb") as f:
            pickle.dump(out, f)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="Update daily interest-rate levels")
    ap.add_argument("--db", "-d", default=r"C:\Users\gcubb\OneDrive\Python\data-hub")
    ap.add_argument("--rebuild", action="store_true", help="full history rebuild")
    ap.add_argument("--start-year", type=int, default=2000)
    args = ap.parse_args(argv)
    df = rates_update(args.db, start_year=args.start_year, full_rebuild=args.rebuild)
    print(f"rates_levels.pickle: {df.shape[0]} rows x {df.shape[1]} cols, "
          f"{df.index[0].date()} -> {df.index[-1].date()}")
    print("columns:", list(df.columns))


if __name__ == "__main__":
    main()
