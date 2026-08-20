# Factor exposure panel — hand-off contract

fin-data's terminal deliverable for the Barra-style risk model. fin-data produces
a **regression-ready monthly panel of standardized style exposures plus the data
the WLS needs**. It does **not** build industry dummies, the constant/effects
scheme, the constraint, the WLS regression, the factor-return series, or the
covariance matrix — all of that lives in the separate downstream project.

Scope boundary: **data in shape ends here.** Exposure normalization / winsorizing /
z-scoring is fin-data's job; anything that touches the *design matrix* or the
*model* is downstream.

---

## Deliverable: `factor_panel.pickle`

A monthly panel, `MultiIndex (month_end_date, ticker)`:

| Column | Meaning | Downstream role |
|---|---|---|
| `ret_m` | stock return over the month **ending** on `month_end_date` (compounded from `sprtns`, %) | LHS |
| `exp_size_eom` | ln(marketCap), z-scored, **as-of** `month_end_date` | style column of X |
| `exp_value_eom` | B/M (1/PB), winsorized + z-scored, as-of date | style column of X |
| `exp_prof_eom` | ROA, winsorized + z-scored, as-of date | style column of X |
| `mktcap` | market cap as-of date | cap weights + √cap WLS weights |
| `sector`, `industry` | **labels only** | downstream builds dummies from these |
| `raw_size`, `raw_value`, `raw_prof` | pre-standardization values | audit |

No dummies. No constant column. No design matrix.

---

## Timing convention (NOT baked in — agnostic)

Each series is stored in its own natural, end-date frame:

- **Exposures** (`exp_*_eom`) are **as-of** `month_end_date` (end-of-month
  measurement — hence the `_eom` suffix).
- **`ret_m`** is the return **over** the month ending on `month_end_date`
  (backward-looking, matching `sprtns`'s existing end-date labeling).

So on a row dated `2026-06-30`: exposures = the 6/30 snapshot; `ret_m` = the
5/31→6/30 return. The two are contemporaneous *in label*, and the lag is left to
the consumer.

**A same-date join (`ret_m[t] ~ exp[t]`) is a look-ahead trap.** The 6/30 price
sits in both the June return and the 6/30 B/M denominator, manufacturing a
spurious value/return relation. For predictive / Fama–MacBeth use, regress
**`ret_m[t]` on exposures at `t-1`** (shift exposures forward one month). The
fundamental inputs are already point-in-time (reporting lag applied in Phase 2);
the one-period exposure lag removes the residual price/return-window overlap.

Guardrails shipped with the panel:
1. **`df.attrs`** documents the convention verbatim (exposures as-of label date;
   `ret_m` over trailing month; lag exposures one period for predictive use).
2. **`_eom` column suffix** so a naive same-date join looks suspicious.
3. **`lag_exposures()` helper** shipped alongside (not applied) — one call
   produces the regression-ready `ret_m[t] ~ exp[t-1]` alignment.

---

## Standardization

Per month, cross-sectionally: winsorize raw at 1/99% (esp. B/M, ROA) →
cap-weighted demean (so the market portfolio carries ~0 style tilt, coherent with
the downstream cap-weighted sum-to-zero constraint) → scale to unit SD → NaN → 0
(i.e. mean exposure). Style choices are settled: size = ln(mktcap);
value = B/M = 1/priceToBook (book-negative → NaN → mean); profitability = ROA.

## Data sources / history (route C)

- **Forward arm (Phase 1):** current-snapshot fundamentals harvested from the
  `.info` dict the section-2 loop already fetches → new columns on
  `sp500_history`. Zero extra API calls.
- **Backfill arm (Phase 2):** ~5 yr of point-in-time fundamentals from
  quarterly + annual balance sheet / income statement, forward-filled between
  reports with a conservative reporting lag; monthly market cap = point-in-time
  shares × month-end adjusted price (needs a month-end price pull; `sprtns` stores
  returns, not prices).
- **Survivorship:** backfill uses current constituents → historical cross-sections
  are current-survivors. Accepted for v1; the forward arm is clean going forward.

## Not in scope (downstream project)

Industry dummy / constant / effects coding · cap-weighted sum-to-zero constraint ·
bordered-KKT WLS (√cap weights) · Fama–MacBeth factor-return series · factor
covariance matrix · specific-risk model.
