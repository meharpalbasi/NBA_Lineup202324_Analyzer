"""Retrodiction harness: does the box prior actually make RAPM more predictive?

Compares four contestants on held-out stint scoring margins (points per 100
possessions, possession-weighted):

  intercept  — league-average ORtg only (the r²≈0 reference)
  rapm       — the pure ridge (shrinks toward zero)
  spm        — the box prior alone, no on/off fit at all
  ipm        — prior-informed ridge (shrinks toward the SPM prior)

Two tests, both offline from the rapm_cache and both using LEAKAGE-FREE SPM
priors (the *_SPM_LOSO columns — weights trained without the season under test):

  A. Within-season: fit on the first ~60% of a season's games, predict the
     rest. Caveat, documented on purpose: the SPM *features* are full-season
     box rates (the stored spm_<season>.csv), so the prior peeks mildly into
     the holdout window's box lines. The weights don't. Rebuilding features
     from game logs per split would close this; not worth it for v1.
  B. Next-season (the metric-comparison test that matters): freeze each
     contestant's player values from season S, predict every stint of season
     S+1. Fully clean — and conservative, since production would retrain WITH
     season S. Unseen players get each metric's own unknown-player story:
     0 for pure RAPM (its shrink target), the below-average floor for spm/ipm.

Output: data/ipm_validation.csv + a printed pivot. Stint-level r² is tiny by
nature (single stints are wildly noisy); the *ordering* and RMSE deltas are
the signal. Publish the numbers either way — same ethos as "scoring WPA".

Run:  venv/bin/python -m pipeline.validate_ipm            (~30-60 min, pure CPU)
      venv/bin/python -m pipeline.validate_ipm --seasons 2023-24,2024-25
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .compute_spm import available_seasons
from .fetch_rapm import (
    IPM_FLOOR_D,
    IPM_FLOOR_O,
    MatchupRow,
    _fit_ridge,
    fetch_game_ids,
    load_ipm_prior,
    reconstruct_game,
)

logger = logging.getLogger("pipeline.validate_ipm")

TRAIN_FRACTION = 0.6  # Test A chronological split (game ids sort ~chronologically)


# ---------------------------------------------------------------------------
# Per-game stint records (parsed once per season, aggregated per split)
# ---------------------------------------------------------------------------
def _season_game_records(season: str) -> List[List[Dict[str, Any]]]:
    """Each game's stint records, in game-id (≈ chronological) order."""
    game_ids = fetch_game_ids(season, "Regular Season")
    out, failed = [], 0
    for n, gid in enumerate(game_ids, 1):
        try:
            records, _ = reconstruct_game(gid)
            out.append(records)
        except Exception as exc:
            failed += 1
            logger.warning("game %s failed: %s", gid, exc)
        if n % 300 == 0:
            logger.info("%s: %d/%d games parsed", season, n, len(game_ids))
    if failed:
        logger.warning("%s: %d/%d games failed", season, failed, len(game_ids))
    return out


def _aggregate(games: List[List[Dict[str, Any]]]) -> List[MatchupRow]:
    matchups: Dict[Tuple[Tuple[int, ...], Tuple[int, ...]], Dict[str, float]] = defaultdict(
        lambda: {"poss": 0.0, "pts": 0}
    )
    for records in games:
        for rec in records:
            agg = matchups[(rec["off"], rec["def"])]
            agg["poss"] += rec["poss"]
            agg["pts"] += rec["pts"]
    return [(off, dfn, agg["poss"], agg["pts"], 1.0) for (off, dfn), agg in matchups.items()]


# ---------------------------------------------------------------------------
# Contestants: (o_values, d_values, mu, unseen-player defaults)
# ---------------------------------------------------------------------------
Contestant = Tuple[Dict[int, float], Dict[int, float], float, float, float]


def _weighted_mu(rows: List[MatchupRow]) -> float:
    y = np.array([100.0 * pts / poss for _, _, poss, pts, _ in rows])
    w = np.array([poss for _, _, poss, _, _ in rows])
    return float(np.average(y, weights=w))


def _contestants(
    train_rows: List[MatchupRow],
    prior: Optional[Dict[int, Tuple[float, float]]],
) -> Dict[str, Contestant]:
    mu = _weighted_mu(train_rows)
    out: Dict[str, Contestant] = {"intercept": ({}, {}, mu, 0.0, 0.0)}

    fit = _fit_ridge(train_rows)
    n = len(fit["players"])
    out["rapm"] = (
        dict(zip(fit["players"], fit["coef"][:n])),
        dict(zip(fit["players"], fit["coef"][n:])),
        float(fit["model"].intercept_), 0.0, 0.0,
    )

    if prior is not None:
        out["spm"] = (
            {p: v[0] for p, v in prior.items()},
            {p: v[1] for p, v in prior.items()},
            mu, IPM_FLOOR_O, IPM_FLOOR_D,
        )
        fit_i = _fit_ridge(train_rows, prior=prior)
        n = len(fit_i["players"])
        out["ipm"] = (
            dict(zip(fit_i["players"], fit_i["coef"][:n])),
            dict(zip(fit_i["players"], fit_i["coef"][n:])),
            float(fit_i["model"].intercept_), IPM_FLOOR_O, IPM_FLOOR_D,
        )
    return out


def _score(rows: List[MatchupRow], c: Contestant) -> Dict[str, float]:
    o_val, d_val, mu, def_o, def_d = c
    y = np.array([100.0 * pts / poss for _, _, poss, pts, _ in rows])
    w = np.array([poss for _, _, poss, _, _ in rows])
    yhat = np.array([
        mu + sum(o_val.get(p, def_o) for p in off) - sum(d_val.get(p, def_d) for p in dfn)
        for off, dfn, _, _, _ in rows
    ])
    ybar = np.average(y, weights=w)
    ss_res = np.average((y - yhat) ** 2, weights=w)
    ss_tot = np.average((y - ybar) ** 2, weights=w)
    return {
        "wrmse": round(float(np.sqrt(ss_res)), 4),
        "wr2": round(1.0 - ss_res / ss_tot, 4) if ss_tot > 0 else 0.0,
        "n_stints": len(rows),
        "poss": round(float(w.sum())),
    }


# ---------------------------------------------------------------------------
# The two tests
# ---------------------------------------------------------------------------
def validate(seasons: Optional[List[str]] = None) -> pd.DataFrame:
    seasons = seasons or available_seasons()
    logger.info("validate_ipm: %d seasons (%s … %s)", len(seasons), seasons[0], seasons[-1])

    games: Dict[str, List[List[Dict[str, Any]]]] = {}
    for s in seasons:
        games[s] = _season_game_records(s)
        logger.info("%s: %d games parsed", s, len(games[s]))

    results: List[Dict[str, Any]] = []

    # Test A — within-season 60/40.
    for s in seasons:
        cut = int(TRAIN_FRACTION * len(games[s]))
        train_rows, test_rows = _aggregate(games[s][:cut]), _aggregate(games[s][cut:])
        prior = load_ipm_prior(s, loso=True)
        if prior is None:
            logger.warning("%s: no SPM prior — spm/ipm skipped in Test A", s)
        for model, cont in _contestants(train_rows, prior).items():
            results.append({"test": "A_within", "season": s, "model": model,
                            **_score(test_rows, cont)})
        logger.info("Test A %s done", s)

    # Test B — season S values → season S+1 stints.
    for s, s_next in zip(seasons, seasons[1:]):
        train_rows, test_rows = _aggregate(games[s]), _aggregate(games[s_next])
        prior = load_ipm_prior(s, loso=True)
        for model, cont in _contestants(train_rows, prior).items():
            results.append({"test": "B_next", "season": f"{s}→{s_next}", "model": model,
                            **_score(test_rows, cont)})
        logger.info("Test B %s→%s done", s, s_next)

    df = pd.DataFrame(results)
    out_path = config.DATA_DIR / "ipm_validation.csv"
    df.to_csv(out_path, index=False)
    logger.info("Saved %d result rows → %s", len(df), out_path)
    return df


def _print_summary(df: pd.DataFrame) -> None:
    for test in df["test"].unique():
        sub = df[df["test"] == test]
        print(f"\n=== {test}: possession-weighted RMSE (lower is better) ===")
        print(sub.pivot(index="season", columns="model", values="wrmse").to_string())
        print(f"\n=== {test}: mean wRMSE by model ===")
        print(sub.groupby("model")["wrmse"].mean().round(4).sort_values().to_string())


if __name__ == "__main__":  # pragma: no cover - manual runs
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seasons", help="comma-separated subset, e.g. 2023-24,2024-25")
    args = ap.parse_args()
    seasons = args.seasons.split(",") if args.seasons else None
    _print_summary(validate(seasons))
