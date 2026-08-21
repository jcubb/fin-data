"""Phase 3 — the LAST fin-data step: turn the raw point-in-time panel
(char_panel_raw.pickle, from char_panel.py) into the hand-off deliverable
factor_panel.pickle: standardized size/value/profitability exposures plus monthly
returns and the raw components, ready for the SEPARATE downstream modeling project
(dummies / WLS / factor returns / covariance). See factor_panel_spec.md.

Design guarantee: each exp_*_eom is a PURE deterministic function of the stored raw
columns (raw_size/raw_value/raw_prof) and marketCap. So a downstream user can
reproduce them exactly, OR re-derive with different winsorization by calling
build_exposures(panel, lower=..., upper=...) on the same panel — nothing needed is
thrown away. The alternative profitability numerators (operating_income,
gross_profit) ride along too, so the profitability *definition* is re-derivable, not
just re-winsorizable.

DELIVERABLE  ->  <db>/factor_panel.pickle   MultiIndex (month_end_date, ticker)
  ret_m                         return OVER the month ending on month_end_date (%)
  exp_size_eom/value_eom/prof_eom   standardized exposures, AS-OF month_end_date
  marketCap, raw_size/value/prof, book_equity, total_assets, net_income,
  operating_income, gross_profit, sector, industry   (carried through raw)

TIMING (agnostic — lag NOT baked in; see factor_panel_spec.md and df.attrs):
  exposures are as-of the label date; ret_m is the trailing-month return. A
  same-date join is look-ahead. For predictive use, regress ret_m[t] on exposures
  at t-1 — use the shipped lag_exposures() helper (NOT applied to the stored panel).

Standalone:
  python char_exposures.py --db <db>
  python char_exposures.py --db <db> --lower 0.02 --upper 0.98
"""
import os
import pickle
import argparse

import numpy as np
import pandas as pd

WINSOR_LOWER = 0.01
WINSOR_UPPER = 0.99
EXP_COLS = ["exp_size_eom", "exp_value_eom", "exp_prof_eom"]


def _standardize(x, w, lower, upper):
    """One characteristic, one month cross-section -> z-scored exposure:
    winsorize at [lower,upper] -> cap-weighted demean (weights w) -> divide by
    equal-weighted cross-sectional SD -> NaN -> 0 (i.e. the cap-weighted mean).
    Pure function of (x, w, lower, upper)."""
    x = x.astype(float)
    valid = x.notna()
    if valid.sum() < 2:
        return pd.Series(0.0, index=x.index)
    lo, hi = x[valid].quantile([lower, upper])
    xw = x.clip(lo, hi)
    ww = w.where(valid).astype(float)
    mu = (ww * xw).sum() / ww.sum()          # cap-weighted mean -> market ~ 0 tilt
    xc = xw - mu
    sd = xc[valid].std(ddof=1)               # equal-weighted dispersion -> ~unit SD
    z = xc / sd if (sd and not np.isnan(sd)) else xc * 0.0
    return z.fillna(0.0)


def _exposures_one_month(df, lower, upper):
    w = df["marketCap"]
    return pd.DataFrame({
        "exp_size_eom": _standardize(np.log(df["raw_size"]), w, lower, upper),
        "exp_value_eom": _standardize(df["raw_value"], w, lower, upper),
        "exp_prof_eom": _standardize(df["raw_prof"], w, lower, upper),
    }, index=df.index)


def build_exposures(panel, lower=WINSOR_LOWER, upper=WINSOR_UPPER):
    """Add/replace exp_*_eom on a copy of `panel`, computed per-month cross-section
    from raw_size/raw_value/raw_prof + marketCap. Re-runnable with any winsorization
    bounds. size uses ln(marketCap)."""
    out = panel.drop(columns=[c for c in EXP_COLS if c in panel.columns])
    exp = (out.groupby(level="month_end_date", group_keys=False)
              .apply(lambda d: _exposures_one_month(d, lower, upper)))
    return out.join(exp)


def monthly_returns(db):
    """Monthly compounded % returns from daily sprtns -> Series indexed
    (month_end_date, ticker). Months with no observations for a ticker are NaN
    (dropped), not a spurious 0."""
    with open(os.path.join(db, "sprtns.pickle"), "rb") as f:
        r = pickle.load(f)
    growth = r.astype(float) / 100.0 + 1.0
    m = growth.resample("ME").prod(min_count=1)     # skip-NaN within month; all-NaN -> NaN
    ret = (m - 1.0) * 100.0
    ret.index.name = "month_end_date"
    s = ret.stack(future_stack=True).rename("ret_m")
    s.index = s.index.set_names(["month_end_date", "ticker"])
    return s.dropna()


def daily_returns(db):
    """Daily % returns from sprtns (wide date x ticker) -> long Series indexed
    (date, ticker). NaNs (non-trading / not-yet-in-universe / delisted) dropped."""
    with open(os.path.join(db, "sprtns.pickle"), "rb") as f:
        s = pickle.load(f)
    s = s.sort_index()
    s.index.name = "date"
    r = s.stack(future_stack=True).rename("ret_d")
    r.index = r.index.set_names(["date", "ticker"])
    return r.dropna()


def lag_exposures(panel, exp_cols=EXP_COLS):
    """SHIPPED, NOT APPLIED. Return a copy in which the exposures are shifted to the
    prior month-end within each ticker, so a row's ret_m[t] lines up with exposures
    known at t-1 — the regression-ready predictive alignment. Assumes contiguous
    monthly rows per ticker (true for this panel). Does not modify the stored panel."""
    out = panel.sort_index().copy()
    g = out.groupby(level="ticker", group_keys=False)
    for c in exp_cols:
        if c in out.columns:
            out[c] = g[c].shift(1)
    return out


def build_factor_panel(db, lower=WINSOR_LOWER, upper=WINSOR_UPPER):
    with open(os.path.join(db, "char_panel_raw.pickle"), "rb") as f:
        raw = pickle.load(f)
    panel = build_exposures(raw, lower=lower, upper=upper)
    panel = panel.join(monthly_returns(db))         # aligns on (month_end_date, ticker)

    front = ["ret_m"] + EXP_COLS + ["marketCap"]
    rest = [c for c in panel.columns if c not in front]
    panel = panel[[c for c in front if c in panel.columns] + rest]

    panel.attrs["timing"] = (
        "exposures (*_eom) are AS-OF month_end_date; ret_m is the return OVER the "
        "month ending on month_end_date. For predictive/Fama-MacBeth use, regress "
        "ret_m[t] on exposures at t-1 (use lag_exposures). A same-date join is "
        "look-ahead (the month-end price is in both ret_m and the B/M denominator).")
    panel.attrs["standardization"] = (
        f"per-month cross-section: winsorize raw at [{lower},{upper}] -> cap-weighted "
        f"demean (weights=marketCap) -> divide by equal-weighted cross-sectional SD "
        f"-> NaN->0. size=ln(marketCap). exp_*_eom = deterministic f(raw_*,marketCap); "
        f"re-derive via build_exposures(panel, lower=..., upper=...).")
    return panel


DAILY_KEEP = EXP_COLS + ["marketCap", "sector", "industry"]


def build_factor_panel_daily(db, lower=WINSOR_LOWER, upper=WINSOR_UPPER):
    """DAILY deliverable for the Barra-style model: one row per (date, ticker),
    with the daily return and the month-M-end exposure snapshot held FIXED across
    every trading day of month M+1 (the "beginning-of-month" specification).

    Timing is look-ahead-free BY CONSTRUCTION and needs NO further lag: the
    exposure as-of month-end M is known at M's close and explains only returns on
    days strictly after M (all of month M+1). The M-close price sits in both the
    exposure (marketCap / B-M denominator) and the first day's return denominator,
    but that price is a KNOWN, past quantity at estimation time — the returns being
    explained (the numerators, M+1 onward) never touch the exposure. Contrast the
    monthly panel, where ret_m[t]'s endpoint coincided with the exposure date and a
    t-1 shift was required.

    Built by broadcasting the existing monthly exposures — no exposures are
    recomputed. Each daily date d is mapped to snap(d) = the latest month-end
    strictly before d (merge_asof, backward, exact matches disallowed), then joined
    to that snapshot's exposures/cap/labels. `exp_asof_date` records snap(d).
    """
    with open(os.path.join(db, "char_panel_raw.pickle"), "rb") as f:
        raw = pickle.load(f)
    panel_m = build_exposures(raw, lower=lower, upper=upper)   # (month_end_date, ticker)

    ret = daily_returns(db)                                    # (date, ticker) ret_d
    daily_dates = ret.index.get_level_values("date").unique().sort_values()
    month_ends = panel_m.index.get_level_values("month_end_date").unique().sort_values()

    # snap(d) = last month-end strictly before d (holds M-end exposures over M+1)
    snap = pd.merge_asof(
        pd.DataFrame({"date": daily_dates}),
        pd.DataFrame({"month_end_date": month_ends, "snap": month_ends}),
        left_on="date", right_on="month_end_date",
        direction="backward", allow_exact_matches=False,
    ).set_index("date")["snap"]

    df = ret.reset_index()
    df["snap"] = df["date"].map(snap)
    df = df.dropna(subset=["snap"])                            # drop days before first snapshot

    expo = (panel_m[DAILY_KEEP].reset_index()
            .rename(columns={"month_end_date": "snap"}))
    out = (df.merge(expo, on=["snap", "ticker"], how="inner")  # inner: name must be in snapshot
             .rename(columns={"snap": "exp_asof_date"})
             .set_index(["date", "ticker"]).sort_index())

    front = ["ret_d"] + EXP_COLS + ["marketCap", "sector", "industry", "exp_asof_date"]
    out = out[[c for c in front if c in out.columns]]

    out.attrs["timing"] = (
        "BEGINNING-OF-MONTH / daily: exposures are the month-M-end snapshot held "
        "FIXED across every trading day of month M+1; ret_d is the daily return. "
        "Look-ahead-free by construction (exposure date < every return it explains) "
        "-- do NOT apply an additional lag. exp_asof_date = the snapshot month-end.")
    out.attrs["standardization"] = (
        f"per-month cross-section (at each snapshot month-end): winsorize raw at "
        f"[{lower},{upper}] -> cap-weighted demean (weights=marketCap) -> divide by "
        f"equal-weighted cross-sectional SD -> NaN->0. size=ln(marketCap). Exposures "
        f"are then held fixed across the following month's trading days.")
    return out


def report_daily(panel):
    dates = panel.index.get_level_values(0)
    print("\n" + "=" * 64)
    print("FACTOR PANEL (DAILY) — beginning-of-month deliverable")
    print("=" * 64)
    print(f"rows: {len(panel)} | tickers: {panel.index.get_level_values(1).nunique()} "
          f"| trading days: {dates.nunique()} | span {dates.min().date()} -> {dates.max().date()}")
    snaps = panel["exp_asof_date"].nunique()
    med = int(panel.groupby(level=0).size().median())
    print(f"exposure snapshots used: {snaps} | median names/day: {med}")
    print(f"ret_d non-null: {100*panel['ret_d'].notna().mean():.1f}%")
    print("\nattrs:")
    for k, v in panel.attrs.items():
        print(f"  [{k}] {v}")


def report(panel):
    dates = panel.index.get_level_values(0)
    print("\n" + "=" * 64)
    print("FACTOR PANEL — hand-off deliverable")
    print("=" * 64)
    print(f"rows: {len(panel)} | tickers: {panel.index.get_level_values(1).nunique()} "
          f"| months: {dates.nunique()} | span {dates.min().date()} -> {dates.max().date()}")
    print(f"ret_m non-null: {100*panel['ret_m'].notna().mean():.1f}%")
    last = dates.max()
    x = panel.xs(last, level=0)
    w = x["marketCap"] / x["marketCap"].sum()
    print(f"\nlatest cross-section {last.date()} ({len(x)} names) — exposures should be "
          f"~0 cap-weighted mean, ~1 SD:")
    for c in EXP_COLS:
        print(f"  {c:16s} cap-wtd mean={float((w*x[c]).sum()):+.4f}  sd={x[c].std():.3f}  "
              f"min={x[c].min():+.2f} max={x[c].max():+.2f}")
    print("\nattrs:")
    for k, v in panel.attrs.items():
        print(f"  [{k}] {v}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", "-d", default=r"C:\Users\gcubb\OneDrive\Python\data-hub")
    ap.add_argument("--lower", type=float, default=WINSOR_LOWER)
    ap.add_argument("--upper", type=float, default=WINSOR_UPPER)
    ap.add_argument("--out", default=None)
    ap.add_argument("--daily", action="store_true",
                    help="build the daily (beginning-of-month) panel -> factor_panel_daily.pickle")
    args = ap.parse_args(argv)

    if args.daily:
        panel = build_factor_panel_daily(args.db, lower=args.lower, upper=args.upper)
        out = args.out or os.path.join(args.db, "factor_panel_daily.pickle")
        with open(out, "wb") as f:
            pickle.dump(panel, f)
        report_daily(panel)
        print(f"\nWrote daily factor panel -> {out}")
    else:
        panel = build_factor_panel(args.db, lower=args.lower, upper=args.upper)
        out = args.out or os.path.join(args.db, "factor_panel.pickle")
        with open(out, "wb") as f:
            pickle.dump(panel, f)
        report(panel)
        print(f"\nWrote factor panel -> {out}")


if __name__ == "__main__":
    main()
