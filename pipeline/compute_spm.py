"""Statistical Plus-Minus (SPM) — the box-score prior for prior-informed RAPM.

Two possession-weighted ridge regressions over player-seasons: per-100 box
rates → our own O_RAPM / D_RAPM (all nine self-computed seasons pooled). The
predictions become the Bayesian prior the IPM ridge shrinks toward instead of
zero (see docs/SPEC_PRIOR_INFORMED_RAPM.md). Runs OFFLINE from CSVs already in
data/ — no API calls.

Why train our own instead of transcribing published SPM weights (as
compute_impact does for BPM 2.0): the target is *our* RAPM, so the weights are
calibrated to the exact quantity the prior must predict, and the whole metric
stays self-computed end to end.

Notes:
  - player_stats_<season>.csv is PER-GAME (config.LEAGUE_STATS_PER_MODE), so
    totals are per_game * GP.
  - The per-100 denominator is the player's own on-court offensive possessions
    from rapm_<season>.csv (POSS/2 — off and def alternate, so the halves are
    near-equal). Pace * minutes is only a fallback for RAPM-less players.
  - O_SPM / D_SPM are centered to a possession-weighted league mean of 0 per
    season, so the downstream ridge intercept stays interpretable as league
    ORtg and SPM/IPM share RAPM's "per 100 vs average" scale.
  - Training rows need POSS >= MIN_POSSESSIONS (noisy RAPM targets teach the
    regression nothing); prediction covers everyone with box stats.

Standalone:
  venv/bin/python -m pipeline.compute_spm            # train + LOSO + write all seasons
  venv/bin/python -m pipeline.compute_spm --apply 2026-27   # frozen-weights apply (in-season)
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .fetch_rapm import MIN_POSSESSIONS

logger = logging.getLogger("pipeline.compute_spm")

_FT = 0.44  # free-throw possession weight (league-standard, same as fetch_rapm)

# Per-100 box rates + efficiency. SVA ("shot value added") is the one
# interaction term: TS margin times true-shot volume, i.e. points added over a
# league-average scorer taking the same shots — the linear stand-in for BPM's
# volume-x-efficiency machinery.
FEATURES: List[str] = [
    "PTS100", "FGA100", "FG3A100", "FTA100", "OREB100", "DREB100",
    "AST100", "TOV100", "STL100", "BLK100", "PF100", "PFD100",
    "TS_DELTA", "SVA100",
]

MODEL_PATH = config.DATA_DIR / "spm_model.json"


# ---------------------------------------------------------------------------
# Feature construction
# ---------------------------------------------------------------------------
def available_seasons() -> List[str]:
    """Seasons with every input on disk (rapm + player_stats + team_stats)."""
    seasons = []
    for p in config.DATA_DIR.glob("rapm_????-??.csv"):
        m = re.match(r"rapm_(\d{4}-\d{2})\.csv$", p.name)
        if not m:
            continue
        s = m.group(1)
        if (config.DATA_DIR / f"player_stats_{s}.csv").exists() and (
            config.DATA_DIR / f"team_stats_{s}.csv"
        ).exists():
            seasons.append(s)
    return sorted(seasons)


def build_features(season: str) -> pd.DataFrame:
    """One row per player: per-100 features + RAPM targets/possessions."""
    ps = pd.read_csv(config.DATA_DIR / f"player_stats_{season}.csv", low_memory=False)
    ps = ps[ps["SEASON_TYPE"] == "Regular Season"].copy()
    ps = ps[(ps["GP"] > 0) & (ps["MIN"] > 0)]

    # Season totals from the per-game lines.
    tot_cols = ["MIN", "PTS", "FGA", "FG3A", "FTA", "OREB", "DREB", "AST",
                "TOV", "STL", "BLK", "PF", "PFD"]
    tot = {c: ps[c].to_numpy(dtype=float) * ps["GP"].to_numpy(dtype=float) for c in tot_cols}

    # On-court offensive possessions: RAPM POSS/2 where we have it (exact,
    # self-computed), else team pace * player minutes.
    rapm_path = config.DATA_DIR / f"rapm_{season}.csv"
    rapm = None
    if rapm_path.exists():
        rapm = pd.read_csv(rapm_path, low_memory=False)[
            ["PLAYER_ID", "O_RAPM", "D_RAPM", "POSS"]
        ].rename(columns={"POSS": "RAPM_POSS"})

    ts = pd.read_csv(config.DATA_DIR / f"team_stats_{season}.csv", low_memory=False)
    if "SEASON_TYPE" in ts.columns:
        ts = ts[ts["SEASON_TYPE"] == "Regular Season"]
    pace_by_team = ts.drop_duplicates("TEAM_ID").set_index("TEAM_ID")["PACE"].to_dict()
    pace = ps["TEAM_ID"].map(pace_by_team).fillna(100.0).to_numpy(dtype=float)

    out = pd.DataFrame({
        "PLAYER_ID": ps["PLAYER_ID"].to_numpy(),
        "PLAYER_NAME": ps["PLAYER_NAME"].to_numpy(),
        "TEAM_ABBREVIATION": ps["TEAM_ABBREVIATION"].to_numpy(),
        "SEASON": season,
    })
    if rapm is not None:
        out = out.merge(rapm, on="PLAYER_ID", how="left")
    else:
        out["O_RAPM"] = np.nan
        out["D_RAPM"] = np.nan
        out["RAPM_POSS"] = np.nan

    poss_fallback = pace * tot["MIN"] / 48.0
    poss = np.where(out["RAPM_POSS"].notna(), out["RAPM_POSS"].to_numpy(dtype=float) / 2.0,
                    poss_fallback)
    poss = np.maximum(poss, 1.0)
    out["POSS100_BASE"] = poss

    # League TS% from this season's totals (the efficiency baseline).
    lg_tsa = tot["FGA"].sum() + _FT * tot["FTA"].sum()
    lg_ts = tot["PTS"].sum() / (2.0 * lg_tsa) if lg_tsa > 0 else 0.55
    tsa = tot["FGA"] + _FT * tot["FTA"]
    ts_pct = np.divide(tot["PTS"], 2.0 * tsa, out=np.full_like(tsa, lg_ts), where=tsa > 0)

    for c in ["PTS", "FGA", "FG3A", "FTA", "OREB", "DREB", "AST", "TOV",
              "STL", "BLK", "PF", "PFD"]:
        out[f"{c}100"] = tot[c] / poss * 100.0
    out["TS_DELTA"] = ts_pct - lg_ts
    out["SVA100"] = out["TS_DELTA"] * 2.0 * (out["FGA100"] + _FT * out["FTA100"])
    return out


# ---------------------------------------------------------------------------
# Ridge fit / predict (standardized features; params serialized to plain JSON)
# ---------------------------------------------------------------------------
def _fit_side(df: pd.DataFrame, target: str) -> Dict[str, Any]:
    from sklearn.linear_model import RidgeCV

    X = df[FEATURES].to_numpy(dtype=float)
    y = df[target].to_numpy(dtype=float)
    w = df["RAPM_POSS"].to_numpy(dtype=float)

    mean = X.mean(axis=0)
    scale = X.std(axis=0)
    scale[scale == 0] = 1.0
    Xs = (X - mean) / scale

    model = RidgeCV(alphas=np.logspace(-3, 5, 17), fit_intercept=True, cv=5)
    model.fit(Xs, y, sample_weight=w)
    return {
        "mean": mean.tolist(), "scale": scale.tolist(),
        "coef": model.coef_.tolist(), "intercept": float(model.intercept_),
        "alpha": float(model.alpha_),
    }


def _predict_side(side: Dict[str, Any], df: pd.DataFrame) -> np.ndarray:
    X = df[FEATURES].to_numpy(dtype=float)
    Xs = (X - np.asarray(side["mean"])) / np.asarray(side["scale"])
    return Xs @ np.asarray(side["coef"]) + side["intercept"]


def _weighted_r2(y: np.ndarray, yhat: np.ndarray, w: np.ndarray) -> float:
    ybar = np.average(y, weights=w)
    ss_res = np.average((y - yhat) ** 2, weights=w)
    ss_tot = np.average((y - ybar) ** 2, weights=w)
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0


def _center_predictions(df: pd.DataFrame) -> pd.DataFrame:
    """Center O/D SPM (and LOSO variants) to possession-weighted 0 within the season."""
    w = df["POSS100_BASE"].to_numpy(dtype=float)
    for col in ("O_SPM", "D_SPM", "O_SPM_LOSO", "D_SPM_LOSO"):
        if col in df.columns:
            centered = df[col].to_numpy(dtype=float)
            df[col] = np.round(centered - np.average(centered, weights=w), 2)
    df["SPM"] = np.round(df["O_SPM"] + df["D_SPM"], 2)
    if "O_SPM_LOSO" in df.columns:
        df["SPM_LOSO"] = np.round(df["O_SPM_LOSO"] + df["D_SPM_LOSO"], 2)
    return df


def _write_season_csv(df: pd.DataFrame, season: str) -> None:
    cols = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ABBREVIATION",
            "O_SPM", "D_SPM", "SPM", "SEASON"]
    # LOSO columns (weights trained WITHOUT this season) ride along when present —
    # validate_ipm uses them so its retrodiction tests are leakage-free.
    cols += [c for c in ("O_SPM_LOSO", "D_SPM_LOSO", "SPM_LOSO") if c in df.columns]
    out = df[cols].copy()
    # The per-100 denominator (offensive possessions) — the sample size behind
    # the prediction, used downstream to shrink thin-sample priors to the floor.
    out["POSS_BASE"] = np.round(df["POSS100_BASE"], 0).astype(int)
    out["SEASON_TYPE"] = "Regular Season"
    out = out.sort_values("SPM", ascending=False).reset_index(drop=True)
    path = config.DATA_DIR / f"spm_{season}.csv"
    out.to_csv(path, index=False)
    logger.info("Saved %d players → %s", len(out), path)


# ---------------------------------------------------------------------------
# Train (with leave-one-season-out validation) / apply
# ---------------------------------------------------------------------------
def _training_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["RAPM_POSS"] >= MIN_POSSESSIONS)
              & df["O_RAPM"].notna() & df["D_RAPM"].notna()]


def train_spm(seasons: Optional[List[str]] = None) -> Dict[str, Any]:
    """Fit SPM on all seasons, LOSO-validate, write spm_<season>.csv + model JSON."""
    seasons = seasons or available_seasons()
    if len(seasons) < 3:
        raise RuntimeError(f"SPM needs several seasons of rapm/player_stats; found {seasons}")
    logger.info("SPM: training on %d seasons (%s … %s)", len(seasons), seasons[0], seasons[-1])

    feats = {s: build_features(s) for s in seasons}
    pooled = pd.concat(feats.values(), ignore_index=True)
    train_all = _training_rows(pooled)
    logger.info("SPM: %d training rows (POSS >= %d) of %d player-seasons",
                len(train_all), MIN_POSSESSIONS, len(pooled))

    # Leave-one-season-out: the honesty check that the weights generalize. The
    # held-out season's full prediction is kept as *_SPM_LOSO so downstream
    # validation can use leakage-free priors.
    loso: List[Dict[str, Any]] = []
    for s in seasons:
        tr = _training_rows(pd.concat([feats[x] for x in seasons if x != s], ignore_index=True))
        te = _training_rows(feats[s])
        if te.empty:
            continue
        row: Dict[str, Any] = {"season": s, "n_test": len(te)}
        for side, target in (("O", "O_RAPM"), ("D", "D_RAPM")):
            m = _fit_side(tr, target)
            feats[s][f"{side}_SPM_LOSO"] = _predict_side(m, feats[s])
            yhat = _predict_side(m, te)
            y = te[target].to_numpy(dtype=float)
            w = te["RAPM_POSS"].to_numpy(dtype=float)
            row[f"{side}_r2"] = round(_weighted_r2(y, yhat, w), 3)
            row[f"{side}_rmse"] = round(float(np.sqrt(np.average((y - yhat) ** 2, weights=w))), 3)
        loso.append(row)
        logger.info("LOSO %s: O r2=%.3f rmse=%.2f | D r2=%.3f rmse=%.2f (n=%d)",
                    s, row["O_r2"], row["O_rmse"], row["D_r2"], row["D_rmse"], row["n_test"])

    # Final model on everything; per-season predictions, centered, written out.
    sides = {"O": _fit_side(train_all, "O_RAPM"), "D": _fit_side(train_all, "D_RAPM")}
    for s in seasons:
        df = feats[s].copy()
        df["O_SPM"] = _predict_side(sides["O"], df)
        df["D_SPM"] = _predict_side(sides["D"], df)
        _write_season_csv(_center_predictions(df), s)

    model = {
        "features": FEATURES,
        "sides": sides,
        "seasons_trained": seasons,
        "min_training_poss": MIN_POSSESSIONS,
        "n_training_rows": int(len(train_all)),
        "loso": loso,
        "note": "Per-100 box rates -> self-computed RAPM. See docs/SPEC_PRIOR_INFORMED_RAPM.md.",
    }
    MODEL_PATH.write_text(json.dumps(model, indent=2))
    logger.info("Saved model (alpha O=%.3g / D=%.3g) → %s",
                sides["O"]["alpha"], sides["D"]["alpha"], MODEL_PATH)
    return model


def apply_spm(season: str) -> pd.DataFrame:
    """Score one season with the FROZEN weights in spm_model.json (in-season path)."""
    model = json.loads(MODEL_PATH.read_text())
    if model["features"] != FEATURES:
        raise RuntimeError("spm_model.json feature list doesn't match this code — retrain")
    df = build_features(season)
    df["O_SPM"] = _predict_side(model["sides"]["O"], df)
    df["D_SPM"] = _predict_side(model["sides"]["D"], df)
    df = _center_predictions(df)
    _write_season_csv(df, season)
    return df


if __name__ == "__main__":  # pragma: no cover - manual runs
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", metavar="SEASON",
                    help="score one season with frozen weights instead of retraining")
    args = ap.parse_args()

    if args.apply:
        df = apply_spm(args.apply)
    else:
        train_spm()
        df = pd.read_csv(config.DATA_DIR / f"spm_{config.SEASON}.csv")
    qualified = df[df["POSS_BASE"] >= 2000]
    print(f"\n=== TOP 25 SPM, POSS_BASE >= 2000 ({args.apply or config.SEASON}) ===")
    print(qualified.head(25).to_string(index=False))
