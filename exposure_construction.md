# How the `exp_*_eom` exposures are constructed

Methodology + rationale behind the standardized style exposures in
`factor_panel.pickle`. Read alongside `factor_panel_spec.md` (the contract: schema,
timing, not-in-scope). This doc is the "why and exactly how," aimed at the downstream
Barra project that will regress on these exposures — including the parts you must
understand to keep the model *coherent* with how the exposures were built.

Implementation: `char_exposures.py` (`build_exposures` / `_standardize`).

---

## Inputs (all per stock, per month-end, point-in-time)

| Input | Source | Notes |
|---|---|---|
| `raw_size` | = `marketCap` | restated-share × split-adjusted close (see `char_panel.py`) |
| `raw_value` | B/M = book_equity / marketCap | NaN if book equity ≤ 0 (~6.5% of names) |
| `raw_prof` | ROA = net_income / total_assets | annual (FY) numerator |
| `marketCap` | point-in-time | used as the **cap weight** in centering |

Fundamentals are already lagged 90 days from fiscal period-end (knowable as-of the
label date) and forward-filled between reports. See the timing section below —
this lag is necessary but **not sufficient** for a look-ahead-free regression.

## The algorithm — per month, one cross-section at a time

For each characteristic *c* independently, within each month's cross-section:

1. **Transform.** `size = ln(marketCap)`; `value` and `prof` used as-is. (Log makes
   size roughly symmetric; market cap spans orders of magnitude.)

2. **Winsorize** at the [1%, 99%] cross-sectional quantiles (clip, don't drop).
   Tames fat tails. This is the one knob a downstream user is most likely to change —
   `build_exposures(panel, lower=…, upper=…)` re-derives from the stored raw columns.
   (Aside: this is also *why* profitability is ROA and not ROE — ROE's near-zero
   book-equity denominators blow up past any sane winsorization; see the probe notes.)

3. **Cap-weighted demean.** Subtract `μ = Σ wᵢxᵢ / Σ wᵢ`, weights `w = marketCap`.
   **This is the load-bearing choice.** It makes the *cap-weighted market portfolio*
   carry a style exposure of exactly zero. That is what keeps the exposures coherent
   with the downstream **cap-weighted sum-to-zero constraint** on the factor model:
   the market factor absorbs the average, and each style is a pure tilt *relative to
   the market*. If you re-center these exposures equal-weighted (or on any other
   weighting), you silently break that coherence and the style factors are no longer
   orthogonal to the market portfolio the way the constraint assumes.

4. **Scale** by the *equal-weighted* cross-sectional standard deviation → ≈ unit
   dispersion. Note the deliberate asymmetry: **cap-weighted mean, equal-weighted SD.**
   Center is "relative to the market" (economic); scale is "relative to the spread of
   names" (statistical) — one unit of exposure ≈ one cross-sectional SD, the usual
   Barra reading of a per-SD factor premium.

5. **`NaN → 0`.** After centering, 0 *is* the cap-weighted mean, so a missing exposure
   = "assign the market-average tilt," a neutral assumption. The names this hits are
   almost entirely `value` for negative/zero book equity (buyback-heavy firms: ABBV,
   MCD, MO, …). They still receive full market + industry exposure downstream; only
   their value tilt is neutralized.

Result: `exp_*_eom` with cap-weighted mean ≈ 0 and SD ≈ 1 each month (verify: the
`report()` output prints both).

## Determinism & re-derivation contract

`exp_*_eom = f(raw_size, raw_value, raw_prof, marketCap; lower, upper)` — a pure
function of stored columns. So you can:
- **Reproduce** exactly (same params → identical exposures).
- **Re-winsorize** — `build_exposures(panel, lower=0.02, upper=0.98)`.
- **Redefine the profitability *style*** — the panel also carries `operating_income`
  and `gross_profit`, so you can build operating- or gross-profitability instead of
  ROA and standardize it the same way. (Reminder: gross/operating profit is NaN for
  many financials — a definitional gap, not missing data.)

## Timing — the two-part story (get both halves)

The exposures are **not** regression-ready as-is. Two distinct lags are in play:

1. **Reporting lag (already applied):** fundamentals enter only 90 days after fiscal
   period-end, so `exp_*_eom` at month *t* uses no un-filed statements.
2. **Return-window lag (NOT applied — you must do it):** `exp_*_eom` is as-of
   month-end *t*, and `ret_m[t]` is the return *over* the month ending at *t*. A
   same-date join is look-ahead: the month-end price sits in **both** `ret_m[t]` and
   the B/M denominator of `exp_value_eom[t]`, manufacturing a spurious value/return
   link. **Regress `ret_m[t]` on exposures at `t-1`** — use the shipped
   `lag_exposures()` helper (it shifts exposures back one month within each ticker).

Having (1) without (2) still leaks. Both are required.

## Reconciliation checklist for the downstream model

- **Use the same cap weights** (`marketCap`) for the sum-to-zero constraint as were
  used to center these exposures. Coherence end-to-end.
- **Don't re-standardize** with a different centering weighting unless you also redo
  the constraint to match.
- **Apply the `t-1` exposure lag** before regressing (see above).
- **`NaN→0` names** contribute zero style tilt but full market/industry exposure —
  intended.
- **Sample depth ≈ 48 months.** Short for a ~15-factor covariance → plan on
  shrinkage / EWMA. The Phase-1 forward arm grows this past Yahoo's ~4-year wall.

## Known limitations (inherited from the data)

Survivorship (current constituents only, historical cross-sections are current
survivors) · annual-granularity fundamentals · ~4-year depth · single-country (US)
universe, so no country block — industry is the only categorical partition needing a
constraint.
