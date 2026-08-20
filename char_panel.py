"""Phase 2 of the factor-model data prep: build a RAW point-in-time monthly panel
of the fundamentals that feed the size / value / profitability exposures — the
"backfill arm" of route C. Reads existing data-hub pickles (never mutates them)
and pulls statement history + prices from Yahoo; writes a NEW pickle. Downstream,
char_exposures.py (Phase 3) standardizes these into factor_panel.pickle. See
factor_panel_spec.md for the hand-off contract.

WHAT COMES OUT  ->  <db>/char_panel_raw.pickle
  MultiIndex (month_end_date, ticker), columns:
    marketCap      point-in-time market cap  (restated shares x split-adj close)
    book_equity    Stockholders Equity, point-in-time (reporting lag applied)
    total_assets   Total Assets, point-in-time
    net_income     annual (FY) net income, point-in-time  -> profitability numerator
    operating_income, gross_profit   alt profitability numerators, point-in-time
                   (let downstream redefine the profitability style, not just
                   re-winsorize ROA; gross_profit is 0/NaN for many financials)
    raw_size       = marketCap                (ln taken later, at exposure step)
    raw_value      = book_equity / marketCap  (B/M; NaN if book_equity <= 0)
    raw_prof       = net_income / total_assets (ROA, annual granularity)
    sector, industry   labels (frozen, from spsect / latest sp500_history)

KEY CORRECTNESS POINTS
  * Split basis: Yahoo RESTATES balance-sheet share counts to CURRENT split basis
    (verified on NVDA — its FY2024-01-31 sheet already shows 24.6B shares, i.e.
    post the June-2024 10:1 split, not the ~2.5B as-reported), and yf.download
    'Close' (auto_adjust=False) is likewise split-adjusted to current basis and NOT
    dividend-adjusted. Both are already on the same basis, so market cap =
    shares x close needs NO split conversion. (The trap only appears if you pair
    get_shares_full, which IS as-reported, with the current-basis close.) Using the
    unadjusted-for-dividends close avoids distorting B/M.
  * Point-in-time: a statement dated at fiscal period-end D is treated as knowable
    only from D + LAG_DAYS (conservative ~1 quarter), then forward-filled until the
    next statement is knowable. No look-ahead into unfiled fundamentals.
  * Coverage: Yahoo free tier caps annual statements at ~4 years, so the panel is
    fundamentals-limited to roughly the last ~3-4 years; it reports its own actual
    span. Quarterly overlay for recency is a documented future enhancement.

Standalone:
  python char_panel.py --db <db>                 # full current universe
  python char_panel.py --tickers NVDA,AAPL,JPM   # spot check (NVDA tests splits)
  python char_panel.py --limit 25 --no-cache
"""
import os
import time
import pickle
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

LAG_DAYS = 90          # conservative reporting lag: fiscal period-end -> knowable
PRICE_START = "2018-01-01"
PRICE_CHUNK = 100      # tickers per batched price download

# Balance-sheet / income-statement row labels we need (Yahoo's canonical names).
BS_EQUITY = "Stockholders Equity"
BS_ASSETS = "Total Assets"
BS_SHARES = ["Ordinary Shares Number", "Share Issued"]  # restated to current basis
IS_NI = "Net Income"
IS_OPINC = "Operating Income"   # alt profitability numerator (operating profit)
IS_GP = "Gross Profit"          # alt profitability numerator (Novy-Marx style)

# Columns fetch_statement_facts is expected to produce. If a cached (non-empty)
# facts frame is missing any of these, it's stale and gets re-fetched — so adding a
# new field here auto-invalidates just what's needed on the next run.
EXPECTED_FACT_COLS = ["book_equity", "total_assets", "shares", "net_income",
                      "operating_income", "gross_profit"]


def load_universe(db):
    """Current S&P 500 tickers + frozen sector/industry labels, from the existing
    pickles (offline). Prefers the latest sp500_history snapshot; falls back to
    spsect for any label gaps."""
    with open(os.path.join(db, "sp500_history.pickle"), "rb") as f:
        hist = pickle.load(f)
    latest = hist[sorted(hist.keys())[-1]]
    labels = latest[["Sector", "Industry"]].copy()
    labels.columns = ["sector", "industry"]
    try:
        with open(os.path.join(db, "spsect.pickle"), "rb") as f:
            spsect = pickle.load(f)
        for tik in labels.index[labels["sector"].isna()]:
            if tik in spsect.index:
                labels.loc[tik, ["sector", "industry"]] = spsect.loc[
                    tik, ["Sector", "Industry"]].values
    except FileNotFoundError:
        pass
    return list(labels.index), labels


def _first_row(df, names):
    """Return the first of `names` present in df.index as a Series, else None."""
    for n in ([names] if isinstance(names, str) else names):
        if n in df.index:
            return df.loc[n]
    return None


def fetch_statement_facts(ticker):
    """One ticker -> facts_df indexed by fiscal period-end (Timestamp) with columns
    [book_equity, total_assets, shares, net_income]. Share counts come back already
    restated to CURRENT split basis (see module header), matching the split-adjusted
    close, so no split conversion is applied. Empty frame on any failure so the
    caller can skip cleanly."""
    tk = yf.Ticker(ticker)
    try:
        bs = tk.balance_sheet
        inc = tk.income_stmt
    except Exception:
        return pd.DataFrame()
    if bs is None or bs.empty:
        return pd.DataFrame()

    equity = _first_row(bs, BS_EQUITY)
    assets = _first_row(bs, BS_ASSETS)
    shares = _first_row(bs, BS_SHARES)
    has_inc = inc is not None and not inc.empty
    ni = _first_row(inc, IS_NI) if has_inc else None
    opinc = _first_row(inc, IS_OPINC) if has_inc else None
    gp = _first_row(inc, IS_GP) if has_inc else None

    facts = pd.DataFrame({
        "book_equity": equity,
        "total_assets": assets,
        "shares": shares,
        "net_income": ni,
        "operating_income": opinc,
        "gross_profit": gp,
    }).dropna(how="all")
    facts.index = pd.to_datetime(facts.index)
    return facts.sort_index()


def fetch_all_facts(tickers, cache_path, use_cache=True, pause=0.0):
    """Per-ticker statement facts, cached to a pickle so re-runs skip the network.
    Cache maps ticker -> facts DataFrame."""
    cache = {}
    if use_cache and cache_path and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            cache = pickle.load(f)

    def _stale(t):
        if t not in cache:
            return True
        f = cache[t]  # non-empty facts missing an expected column -> re-fetch
        return (f is not None and not f.empty
                and any(c not in f.columns for c in EXPECTED_FACT_COLS))

    missing = [t for t in tickers if _stale(t)]
    print(f"statement facts: {len(tickers)-len(missing)} cached, fetching {len(missing)} ...")
    for i, tik in enumerate(missing, 1):
        cache[tik] = fetch_statement_facts(tik)
        if i % 25 == 0:
            print(f"  {i}/{len(missing)} ...")
            if cache_path:
                with open(cache_path, "wb") as f:
                    pickle.dump(cache, f)
        if pause:
            time.sleep(pause)
    if cache_path:
        with open(cache_path, "wb") as f:
            pickle.dump(cache, f)
    return cache


def month_end_prices(tickers, start=PRICE_START):
    """Batched month-end split-adjusted (dividend-UNadjusted) close: wide DataFrame,
    month-end DatetimeIndex x ticker."""
    end = datetime.today().strftime("%Y-%m-%d")
    frames = []
    for i in range(0, len(tickers), PRICE_CHUNK):
        chunk = tickers[i:i + PRICE_CHUNK]
        raw = yf.download(chunk, start=start, end=end, auto_adjust=False,
                          progress=False, group_by="column")
        close = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw
        if isinstance(close, pd.Series):          # single-ticker chunk
            close = close.to_frame(chunk[0])
        frames.append(close)
        print(f"  prices {min(i+PRICE_CHUNK, len(tickers))}/{len(tickers)} ...")
    px = pd.concat(frames, axis=1)
    px.index = pd.to_datetime(px.index)
    return px.resample("ME").last()


def build_panel(db, tickers=None, use_cache=True):
    all_tickers, labels = load_universe(db)
    if tickers is None:
        tickers = all_tickers
    else:
        labels = labels.reindex(tickers)

    cache_path = os.path.join(db, "char_raw_cache.pickle")
    facts_cache = fetch_all_facts(tickers, cache_path, use_cache=use_cache)
    prices = month_end_prices(tickers)
    month_ends = prices.index

    rows = []
    for tik in tickers:
        facts = facts_cache.get(tik, pd.DataFrame())
        if facts is None or facts.empty or tik not in prices.columns:
            continue

        # Point-in-time: shift the fiscal-date index forward by the reporting lag,
        # then as-of forward-fill onto the monthly grid (last knowable statement).
        avail = facts.copy()
        avail.index = facts.index + pd.Timedelta(days=LAG_DAYS)
        avail = avail[~avail.index.duplicated(keep="last")].sort_index()
        monthly = (avail.reindex(avail.index.union(month_ends))
                        .sort_index().ffill().reindex(month_ends))

        px = prices[tik].reindex(month_ends)
        mcap = monthly["shares"] * px
        be = monthly["book_equity"]
        ta = monthly["total_assets"]
        ni = monthly["net_income"]

        sub = pd.DataFrame({
            "marketCap": mcap,
            "book_equity": be,
            "total_assets": ta,
            "net_income": ni,
            "operating_income": monthly["operating_income"],
            "gross_profit": monthly["gross_profit"],
            "raw_size": mcap,
            "raw_value": np.where(be > 0, be / mcap, np.nan),
            "raw_prof": ni / ta,
        }, index=month_ends)
        sub = sub.dropna(subset=["marketCap"], how="any")
        if sub.empty:
            continue
        sub["ticker"] = tik
        sub["sector"] = labels.loc[tik, "sector"] if tik in labels.index else np.nan
        sub["industry"] = labels.loc[tik, "industry"] if tik in labels.index else np.nan
        sub.index.name = "month_end_date"
        rows.append(sub.reset_index())

    if not rows:
        raise RuntimeError("build_panel: no rows assembled (check statement/price fetch)")
    panel = (pd.concat(rows, ignore_index=True)
             .set_index(["month_end_date", "ticker"]).sort_index())
    return panel


def report(panel):
    dates = panel.index.get_level_values(0)
    print("\n" + "=" * 64)
    print("CHAR PANEL (raw) — COVERAGE")
    print("=" * 64)
    print(f"rows: {len(panel)} | tickers: {panel.index.get_level_values(1).nunique()} "
          f"| months: {dates.nunique()} | span: {dates.min().date()} -> {dates.max().date()}")
    print("\nnon-null % by column:")
    for c in ["marketCap", "book_equity", "total_assets", "net_income",
              "raw_size", "raw_value", "raw_prof"]:
        print(f"  {c:14s} {100*panel[c].notna().mean():5.1f}%")
    last = dates.max()
    x = panel.xs(last, level=0)
    print(f"\nlatest cross-section {last.date()} ({len(x)} names) — B/M & ROA describe:")
    print(x[["raw_value", "raw_prof"]].describe().round(4).T.to_string())


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", "-d", default=r"C:\Users\gcubb\OneDrive\Python\data-hub")
    ap.add_argument("--tickers", default=None, help="comma-separated (spot check)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-cache", action="store_true", help="ignore statement cache")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)

    tickers = None
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    elif args.limit:
        tickers, _ = load_universe(args.db)
        tickers = tickers[: args.limit]

    panel = build_panel(args.db, tickers=tickers, use_cache=not args.no_cache)
    out = args.out or os.path.join(args.db, "char_panel_raw.pickle")
    with open(out, "wb") as f:
        pickle.dump(panel, f)
    report(panel)
    print(f"\nWrote raw char panel -> {out}")


if __name__ == "__main__":
    main()
