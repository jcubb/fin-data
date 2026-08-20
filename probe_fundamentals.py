"""Standalone probe: pull Yahoo fundamental characteristics for the current
S&P 500 and report data quality BEFORE wiring anything into fin_data_update.py.

For each stock it reads a handful of raw `.info` fields (the same dict
fin_data_update's section-2 loop already fetches for sector/industry/mktcap) and
derives the three Barra-style axes:

  size          = ln(marketCap)
  value (B/M)   = 1 / priceToBook           (NaN when book equity <= 0)
  profitability = returnOnEquity / returnOnAssets / operatingMargins
                  (three candidates kept RAW; pick/derive at model-build time)

Nothing here centers, z-scores, or cap-weights — that stays at model time, per
the coherence rule in CLAUDE.md. Output is a raw per-stock table plus a coverage
and sanity report so you can judge the data before committing to a metric.

Usage:
  python probe_fundamentals.py --db <data-hub>            # full current S&P 500
  python probe_fundamentals.py --limit 20                 # quick sample
  python probe_fundamentals.py --tickers AAPL,JPM,XOM     # specific names
"""
import os
import pickle
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

# Raw .info keys to harvest. Grouped by the style axis they feed; all stored raw.
INFO_FIELDS = [
    # size
    "marketCap", "sharesOutstanding",
    # value
    "priceToBook", "bookValue", "priceToSalesTrailing12Months", "trailingPE",
    # profitability (several candidates — metric choice is sector-sensitive)
    "returnOnEquity", "returnOnAssets", "operatingMargins", "grossMargins",
    "profitMargins", "grossProfits", "totalRevenue",
]


def default_tickers(db):
    """Current S&P 500 ticker list + sector/mktcap from the latest
    sp500_history snapshot (offline; no scraping needed for a probe)."""
    with open(os.path.join(db, "sp500_history.pickle"), "rb") as f:
        hist = pickle.load(f)
    latest = hist[sorted(hist.keys())[-1]]
    return latest[["Sector", "Industry", "marketCap_yf"]].copy()


def fetch_one(tik):
    """One .info pull -> dict of the wanted fields (missing -> NaN)."""
    try:
        info = yf.Ticker(tik).info
    except Exception as e:
        return {"_error": f"{type(e).__name__}"}
    return {k: info.get(k, np.nan) for k in INFO_FIELDS}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", "-d", default=r"C:\Users\gcubb\OneDrive\Python\data-hub")
    ap.add_argument("--limit", type=int, default=None, help="probe only the first N tickers")
    ap.add_argument("--tickers", default=None, help="comma-separated tickers (overrides --db list)")
    ap.add_argument("--out", default=None, help="output CSV path (default: <db>/fundamentals_probe_<date>.csv)")
    args = ap.parse_args(argv)

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
        meta = pd.DataFrame(index=pd.Index(tickers, name="Ticker"))
    else:
        meta = default_tickers(args.db)
        tickers = list(meta.index)
    if args.limit:
        tickers = tickers[: args.limit]
        meta = meta.loc[tickers]

    print(f"Probing {len(tickers)} tickers via Yahoo .info ...")
    rows = {}
    errors = []
    for i, tik in enumerate(tickers, 1):
        rec = fetch_one(tik)
        if "_error" in rec:
            errors.append((tik, rec["_error"]))
            rec = {}
        rows[tik] = rec
        if i % 50 == 0:
            print(f"  {i}/{len(tickers)} ...")

    raw = pd.DataFrame.from_dict(rows, orient="index")
    raw.index.name = "Ticker"
    raw = raw.reindex(columns=INFO_FIELDS)  # stable column order even if some all-NaN
    df = meta.join(raw, how="left") if not meta.empty else raw

    # --- derived style axes (raw, un-standardized) --------------------------
    df["size_ln_mktcap"] = np.log(df["marketCap"].where(df["marketCap"] > 0))
    pb = pd.to_numeric(df["priceToBook"], errors="coerce")
    df["value_book_to_market"] = np.where(pb > 0, 1.0 / pb, np.nan)  # PB<=0 => neg book eq
    for src, name in [("returnOnEquity", "prof_roe"),
                      ("returnOnAssets", "prof_roa"),
                      ("operatingMargins", "prof_opmargin")]:
        df[name] = pd.to_numeric(df[src], errors="coerce")

    out = args.out or os.path.join(
        args.db, f"fundamentals_probe_{datetime.today():%Y%m%d}.csv")
    df.to_csv(out)

    # ================= DATA-QUALITY REPORT =================
    n = len(df)
    print("\n" + "=" * 64)
    print(f"DATA-QUALITY REPORT  ({n} tickers, {len(errors)} .info errors)")
    print("=" * 64)

    print("\nField coverage (non-null %):")
    styles = ["size_ln_mktcap", "value_book_to_market",
              "prof_roe", "prof_roa", "prof_opmargin"]
    for c in INFO_FIELDS + styles:
        if c in df.columns:
            pct = 100 * df[c].notna().mean()
            print(f"  {c:30s} {pct:5.1f}%")

    neg_bm = int((pb <= 0).sum())
    print(f"\nNegative/zero book equity (priceToBook<=0): {neg_bm}  "
          f"-> value_book_to_market NaN for these")

    print("\nDerived style distributions:")
    with pd.option_context("display.width", 160):
        print(df[styles].describe().round(4).T)

    if "Sector" in df.columns:
        print("\nProfitability sanity by sector (median) — watch financials on "
              "gross-based metrics:")
        gm = pd.to_numeric(df.get("grossMargins"), errors="coerce")
        san = df.assign(grossMargins=gm).groupby("Sector")[
            ["prof_roe", "prof_roa", "prof_opmargin"]].median().round(3)
        san["gm=0 count"] = df.assign(grossMargins=gm).groupby("Sector")[
            "grossMargins"].apply(lambda s: int((s == 0).sum()))
        san["n"] = df.groupby("Sector").size()
        print(san.sort_values("n", ascending=False))

    if errors:
        print(f"\nFirst .info errors: {errors[:10]}")
    print(f"\nWrote full table -> {out}")


if __name__ == "__main__":
    main()
