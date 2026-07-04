# Spec: Prior-informed RAPM (working name: IPM, "Informed Plus-Minus")

Roadmap item **B0** (`nbalineupviz/ROADMAP.md`). Upgrade our pure single-season RAPM into a
stable EPM-family flagship metric by re-fitting the existing ridge with a box-score prior —
the same structural move Dunks & Threes' EPM, ESPN's RPM, and BBall-Index's LEBRON all make
on top of RAPM. Everything below runs **offline from data already on disk**: no new
stats.nba.com fetches, no residential-IP constraint, no Mac mini dependency.

## Why

Pure ridge RAPM shrinks every coefficient toward **zero**, so in a single season role players
land near 0 and stars are underestimated; we honestly label it "descriptive/noisy" today. The
EPM family instead shrinks each player toward **what their box profile predicts** ("the prior"),
so low-sample players get a sane estimate and high-sample players are still driven by the
on/off evidence. We are unusually well positioned to do this *without borrowing anyone's
coefficients*, because we hold the training target ourselves:

| Input | File(s) | Status |
|---|---|---|
| RAPM targets | `data/rapm_{2017-18…2025-26}.csv` (O_RAPM / D_RAPM / POSS) | ✅ 9 seasons |
| Box features | `data/player_stats_<season>.csv` (totals + MIN) | ✅ 9 seasons |
| Pace / possessions | `data/team_stats_<season>.csv` (PACE, POSS) | ✅ 9 seasons |
| Stint data for refits + validation | `data/rapm_cache/` (~10.7k games of raw PBP/box JSON) | ✅ cached |
| Ridge machinery | `pipeline/fetch_rapm.py::_fit_ridge` | ✅ exists |

## The math (one paragraph)

Current model per stint: `y ≈ μ + Σ_off O_i − Σ_def D_j`, where `y` = points per 100
possessions by the offense five, design `X` is +1 on the offense's O-columns and −1 on the
defense's D-columns, `μ` = intercept ≈ league ORtg, and ridge shrinks all `O_i, D_j → 0`
(`fetch_rapm.py:452`). Prior injection is the standard xRAPM residualization: build a prior
vector `β₀` (each player's O/D prior), compute `y_adj = y − X·β₀`, fit the **same** RidgeCV on
`(X, y_adj)` to get deviations `Δ`, and publish `β = β₀ + Δ`. Shrinkage now pulls players
toward their prior instead of toward zero. Equivalent to a Bayesian normal prior centered on
`β₀`; a ~20-line change to the solver.

## Component 1 — `pipeline/compute_spm.py` (new): train our own SPM prior

Two possession-weighted ridge regressions over **player-seasons** (9 seasons pooled,
`POSS ≥ 200` rows only, ~2.5–3k rows): features → `O_RAPM` and features → `D_RAPM`.

**Features (v1, box-only, all per-100 possessions):**
- Volume/rates: PTS, FGA, FG3A, FTA, OREB, DREB, AST, TOV, STL, BLK, PF, PFD
- Efficiency: `TS_delta` = player TS% − league TS% (TS% = PTS / (2·(FGA + 0.44·FTA)))
- One interaction: "shot value added per 100" = `TS_delta · 2·(FGA100 + 0.44·FTA100)`
  (captures the volume×efficiency interplay linearly, BPM-style)

**Per-100 conversion:** player on-court offensive possessions ≈ `POSS/2` from the player's own
`rapm_<season>.csv` row (self-computed; off and def possessions alternate so the halves are
near-equal — closer to truth than any pace estimate). Fallback when a player has no RAPM row:
`TEAM_PACE × MIN / 48`.

**Weighting & centering:** `sample_weight = POSS` (reliability of the RAPM target). After
prediction, **center O_SPM and D_SPM to a possession-weighted league mean of 0** per season —
this keeps the downstream ridge intercept interpretable as league ORtg and keeps IPM on the
same "points per 100 vs average" scale as RAPM.

**Validation (in this module, printed + saved):** leave-one-season-out — train on 8 seasons,
predict the 9th, report possession-weighted R²/RMSE vs that season's actual RAPM, per side
(O/D). This is the honesty check that our SPM weights generalize across seasons.

**Outputs:**
- `data/spm_<season>.csv` — PLAYER_ID, PLAYER_NAME, O_SPM, D_SPM, SPM, POSS_100s used
- `data/spm_model.json` — coefficients, feature list, per-season LOSO metrics, seasons trained
  on (methodology-page fodder; also the frozen artifact the in-season pipeline loads)

**v2 features (later, not now):** `hustle_players_*` (deflections, contested shots),
`defense_tracking_*`, `estimated_metrics_*` — our stand-in for D&T's tracking inputs. Also
age curves + game-level exponential decay (D&T's "Estimated Skills" machinery) — explicitly
out of scope for v1.

## Component 2 — prior injection in `pipeline/fetch_rapm.py`

1. `_fit_ridge(rows, prior=None)` — optional `prior: Dict[pid, Tuple[float, float]]` (O, D).
   When present: build `β₀` aligned to the player index (O block then D block), compute
   `y_adj = y − X @ β₀`, fit on `y_adj`, and return `coef = β₀ + Δ` in the fit bundle
   (keep `Δ` too, for debugging). When absent: behavior is byte-identical to today.
2. **Extend the lambda grid upward** (add 4.0, 8.0): with a good prior, CV should want to
   shrink *deviations* harder than it shrinks raw coefficients today. Let RidgeCV decide.
3. **Low-sample prior regression:** a 50-possession rookie has a garbage SPM feature vector,
   so regress the prior itself toward a below-average floor:
   `prior_p = w·SPM_p + (1−w)·FLOOR`, `w = poss_p / (poss_p + K)`, with v1 constants
   `K = 1000` possessions, `FLOOR_O = −0.9`, `FLOOR_D = −0.4` (fringe players hurt offense
   more). Players absent from the SPM file get the floor outright. All three constants are
   tunable via Component 3.
4. New writer in `fetch_rapm(...)`: after the existing pure fit, if `spm_<season>.csv`
   exists, run the prior-informed fit **on the same rows** (zero extra reconstruction cost)
   and write `data/ipm_<season>.csv` with O_IPM / D_IPM / IPM / POSS, same shape as the RAPM
   table (`_rapm_table` reused with renamed columns).
5. **Unchanged on purpose:** the pure `rapm_<season>.csv` (still published — it's the honest
   raw signal), the chemistry table (stays on the pure fit so residuals keep their meaning),
   and `fetch_rapm_multi` (the 3-yr pooled variant becomes less important once IPM exists;
   don't touch it in v1).

## Component 3 — `pipeline/validate_ipm.py` (new): the retrodiction harness

The published proof that the prior helps — and the tuning loop for K/FLOOR/lambdas. Two tests,
both offline from `rapm_cache`:

- **Test A, within-season:** split each season's game ids 60/40 (NBA game ids sort roughly
  chronologically; exact dates aren't needed for a coarse split). Fit on train stints; predict held-out stint margins
  `ŷ = μ + X_test β`; report possession-weighted RMSE/R². Contestants: intercept-only,
  pure RAPM, SPM-only (`β = β₀`), IPM.
- **Test B, next-season (the D&T metric-comparison test):** freeze each metric's player values
  from season S, predict season S+1's stints (unseen players → floor prior). 8 season-pairs.
  This is the "which number actually predicts the future" table for the methodology page.

Needs one small refactor: `_season_rows(season, …, game_ids=None)` — accept an explicit
game-id subset instead of always aggregating the whole season (aggregation is already
per-matchup-dict; just filter the loop).

**Output:** `data/ipm_validation.csv` + a printed table. Publish the numbers whether or not we
win every row — same ethos as labeling "scoring WPA" honestly.

**Acceptance:** IPM beats pure RAPM on Test B in a clear majority of season-pairs (this is the
entire point of the metric); ties/losses on Test A are acceptable and worth publishing.

## Component 4 — export + frontend (separate PR, after 1–3 land)

- `export_web.py`: merge `ipm_<season>.csv` into `player_index` exactly like the 3-yr block
  (`export_web.py:296` pattern) → `O_IPM / D_IPM / IPM / IPM_POSS`.
- Frontend: /players column + sort, profile Impact group bar, /leaders board, methodology
  section (the SPM feature list, LOSO table, and Test A/B results), compare-page support.

## Ops & runtime

- SPM training: seconds. Per-season IPM refit: same cost as today's ridge (the stint rows are
  rebuilt from cached JSON — parsing only). Full 9-season validation sweep: roughly an hour of
  pure CPU, no network. Runs on the laptop today.
- In-season (Oct 2026+): `spm_model.json` is **frozen**; the weekly `run_rapm.sh` on the mini
  just gains the extra ridge fit + `ipm_<season>.csv` write. No new schedule, no new fetches.
- Prompted by: D&T mention on the Bill Simmons podcast (2026-07). We are *not* rebuilding
  their tracking-data or daily-update machinery in v1 — season-level SPM + prior-informed
  ridge captures most of the stability gain at ~5% of the complexity.

## Open decisions

1. **Public name.** Recommendation: **IPM — Informed Plus-Minus** (honest about the method,
   no collision with EPM/RPM/DPM/BPM/LEBRON). Columns/files above assume it; rename is a
   find-replace if a better brand emerges.
2. Floor/K constants — ship v1 defaults, revisit after the validation sweep.
3. Whether /players eventually demotes 3-yr RAPM once IPM is live (recommendation: yes, keep
   pure single-season RAPM + IPM as the headline pair).

## Build order

1. `compute_spm.py` + LOSO report (pure addition, no risk)
2. `_fit_ridge` prior param + `ipm_<season>.csv` for all 9 seasons (additive; pure RAPM
   untouched)
3. `validate_ipm.py` + tune constants + write the results table
4. Export + frontend + methodology page

## Status & results (2026-07-04)

Backend complete: components 1–3 plus the export merge are built and run; all nine
`spm_`/`ipm_<season>.csv` files and the re-exported `player_index_<season>.csv` (with
O_IPM/D_IPM/IPM/IPM_POSS) are in `data/`. Frontend + methodology page remain.

- **SPM LOSO** (weights trained without the season under test): offense r² 0.35–0.46,
  defense r² 0.10–0.25 — defense being far harder to predict from box stats is the
  expected public-analytics result. Full table in `data/spm_model.json`.
- **Retrodiction (`data/ipm_validation.csv`): IPM won 17/17** — lowest
  possession-weighted RMSE in all 9 within-season splits (Test A) and all 8
  next-season pairs (Test B), beating pure RAPM, SPM-only, and the intercept.
  Mean wRMSE, Test B: IPM 61.236 < RAPM 61.280 ≈ SPM 61.281 < intercept 61.375.
  The blend beats both of its ingredients — the point of the metric.
- Deltas are small in absolute terms because single stints are extremely noisy;
  the consistent ordering across 17 independent tests is the signal. Both facts go
  on the methodology page.
- The 2020-21→2021-22 pair shows elevated RMSE (~78) for **every** contestant
  including the intercept: early 2021-22 stints have intrinsic noise of 88.6 vs 60.6
  late-season (verified directly) — the omicron December of 10-day-contract lineups.
  Model-agnostic; footnote material.
- The prior-informed fit's CV consistently chooses ~2× the pure fit's shrinkage, as
  predicted. IPM↔RAPM correlation runs 0.906–0.936 by season. Sanity spot-checks:
  Jokić 2025-26 rises 4.91 → 5.65 (box profile pulls him up), Marcus Smart's
  box-unsupported +4.16 tempers to +1.93.
- Regression proof: the backfill rewrote every `rapm_` and `lineup_chemistry_` CSV
  byte-identically (nothing shows in git), so the `_fit_ridge` changes leave the
  pure fit untouched.
- v1 constants shipped as spec'd (K=1000, floors −0.9/−0.4); tuning them via the
  harness is open but not blocking.
