"""Team power ratings (roadmap B1) — computed OFFLINE from data already on disk.

Two independent reads of team strength, published side by side:

  TEAM_IPM  minutes-weighted sum of player IPM per team (× 5 players on
            court) — the Dunks & Threes Team-EPM recipe. The "what the roster
            is worth" rating and the headline sort: it predicts held-out game
            margins better than SRS does (see validation).
  SRS       opponent- and venue-adjusted point margin, solved as one least
            squares over every game: margin = r_home − r_away + HCA. The
            "what actually happened" rating, shown as context.

A fitted SRS/IPM blend predicts better still, but least squares gives SRS a
NEGATIVE weight (the two are collinear; subtracting results-noise amplifies
the roster signal) — predictive yet perverse as a published number, and the
IPM weight is inflated by season-level look-ahead besides. So the blend stays
in ratings_validation.csv / ratings_model.json as research for the win-prob
tier (B2, where point-in-time ratings fix the look-ahead) and is NOT a
published column.

Schedule strength comes from the same solve: SOS = mean opponent SRS faced;
oSOS = mean opponent *defensive* rating your offense faced; dSOS = mean
opponent *offensive* rating your defense faced (both positive = harder).
O/D ratings are a points-scored decomposition (pts = mu + o_i − d_j ± HCA/2),
per game not per possession, so fast teams run slightly hot on both ends —
labeled honestly, same ethos as "scoring WPA".

Validation (written to ratings_validation.csv): chronological three-way split
per season — SRS fit on the first 60% of games, blend weights fit on the next
20% (pooled across seasons), every model evaluated on the final 20%. The blend
window must be disjoint from the SRS window: fit both on the same games and
least squares hands SRS all the weight because it is in-sample-optimal there,
even when it generalizes worse. Caveat, on purpose: IPM_RATING is
season-level, so within-season tests carry mild look-ahead (its box inputs
include the holdout window).

Runs standalone:  venv/bin/python -m pipeline.compute_ratings
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .compute_spm import available_seasons
from .fetch_rapm import IPM_FLOOR_D, IPM_FLOOR_O

logger = logging.getLogger("pipeline.compute_ratings")

TRAIN_FRACTION = 0.6   # SRS fit window
BLEND_FRACTION = 0.8   # blend-weight fit window ends here; the rest is test
MODEL_PATH = config.DATA_DIR / "ratings_model.json"

# One game per row, home perspective.
GameRow = Dict[str, Any]


# ---------------------------------------------------------------------------
# Game rows from team_game_logs (home rows carry "vs."; away rows "@")
# ---------------------------------------------------------------------------
def _season_games(season: str) -> Tuple[List[GameRow], Dict[str, Dict[str, Any]]]:
    path = config.DATA_DIR / f"team_game_logs_{season}.csv"
    df = pd.read_csv(path, low_memory=False)
    if "SEASON_TYPE" in df.columns:
        df = df[df["SEASON_TYPE"] == "Regular Season"]
    teams: Dict[str, Dict[str, Any]] = {}
    for _, r in df.drop_duplicates("TEAM_ABBREVIATION").iterrows():
        teams[r["TEAM_ABBREVIATION"]] = {"id": int(r["TEAM_ID"]), "name": r["TEAM_NAME"]}

    games: List[GameRow] = []
    home = df[df["MATCHUP"].str.contains(" vs. ", na=False)]
    for _, r in home.iterrows():
        m = re.match(r"^(\w+) vs\. (\w+)$", str(r["MATCHUP"]))
        if not m:
            continue
        margin = float(r["PLUS_MINUS"])
        games.append({
            "date": str(r["GAME_DATE"]),
            "home": m.group(1), "away": m.group(2),
            "margin": margin,
            "pts_home": float(r["PTS"]), "pts_away": float(r["PTS"]) - margin,
        })
    games.sort(key=lambda g: g["date"])
    logger.info("%s: %d regular-season games, %d teams", season, len(games), len(teams))
    return games, teams


# ---------------------------------------------------------------------------
# SRS + O/D decomposition (least squares with venue term)
# ---------------------------------------------------------------------------
def _fit_srs(games: List[GameRow]) -> Tuple[Dict[str, float], float]:
    """margin = r_home − r_away + HCA → (centered ratings, HCA)."""
    teams = sorted({g["home"] for g in games} | {g["away"] for g in games})
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    X = np.zeros((len(games), n + 1))
    y = np.zeros(len(games))
    for r, g in enumerate(games):
        X[r, idx[g["home"]]] = 1.0
        X[r, idx[g["away"]]] = -1.0
        X[r, n] = 1.0  # HCA
        y[r] = g["margin"]
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    ratings = coef[:n] - coef[:n].mean()  # identified up to a constant → center
    return dict(zip(teams, ratings)), float(coef[n])


def _fit_od(games: List[GameRow]) -> Tuple[Dict[str, float], Dict[str, float], float]:
    """pts = mu + o_scorer − d_opponent ± HCA/2 → (O, D) ratings, positive D = good."""
    teams = sorted({g["home"] for g in games} | {g["away"] for g in games})
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    rows, y = [], []
    for g in games:
        for scorer, opp, pts, at_home in (
            (g["home"], g["away"], g["pts_home"], 1.0),
            (g["away"], g["home"], g["pts_away"], -1.0),
        ):
            row = np.zeros(2 * n + 2)
            row[idx[scorer]] = 1.0            # offense
            row[n + idx[opp]] = -1.0          # opponent defense
            row[2 * n] = at_home / 2.0        # half the venue edge per end
            row[2 * n + 1] = 1.0              # league mean
            rows.append(row)
            y.append(pts)
    X = np.vstack(rows)
    coef, *_ = np.linalg.lstsq(X, np.asarray(y), rcond=None)
    o = coef[:n] - coef[:n].mean()
    d = coef[n:2 * n] - coef[n:2 * n].mean()
    return dict(zip(sorted(idx, key=idx.get), o)), dict(zip(sorted(idx, key=idx.get), d)), float(coef[2 * n])


def _sos(games: List[GameRow], srs: Dict[str, float],
         o: Dict[str, float], d: Dict[str, float]) -> Dict[str, Dict[str, float]]:
    """Per team: mean opponent SRS / opponent-D (for oSOS) / opponent-O (for dSOS)."""
    faced: Dict[str, List[str]] = {}
    for g in games:
        faced.setdefault(g["home"], []).append(g["away"])
        faced.setdefault(g["away"], []).append(g["home"])
    return {
        t: {
            "SOS": float(np.mean([srs[x] for x in opps])),
            "OSOS": float(np.mean([d[x] for x in opps])),
            "DSOS": float(np.mean([o[x] for x in opps])),
        }
        for t, opps in faced.items()
    }


# ---------------------------------------------------------------------------
# IPM roster rating (minutes-weighted, × 5 on court)
# ---------------------------------------------------------------------------
def _ipm_ratings(season: str) -> Optional[Dict[str, float]]:
    ipm_path = config.DATA_DIR / f"ipm_{season}.csv"
    if not ipm_path.exists():
        return None
    ipm = pd.read_csv(ipm_path, low_memory=False)[["PLAYER_ID", "IPM"]]
    ps = pd.read_csv(config.DATA_DIR / f"player_stats_{season}.csv", low_memory=False)
    ps = ps[(ps["SEASON_TYPE"] == "Regular Season") & (ps["GP"] > 0) & (ps["MIN"] > 0)].copy()
    ps["MINUTES"] = ps["MIN"] * ps["GP"]
    ps = ps.merge(ipm, on="PLAYER_ID", how="left")
    # Players too marginal for the ridge get the below-average floor; their
    # minutes weight is tiny anyway.
    ps["IPM"] = ps["IPM"].fillna(IPM_FLOOR_O + IPM_FLOOR_D)
    out: Dict[str, float] = {}
    for abbr, grp in ps.groupby("TEAM_ABBREVIATION"):
        w = grp["MINUTES"].to_numpy(dtype=float)
        out[str(abbr)] = 5.0 * float(np.average(grp["IPM"].to_numpy(dtype=float), weights=w))
    vals = np.array(list(out.values()))
    return {t: v - vals.mean() for t, v in out.items()}  # center like SRS


# ---------------------------------------------------------------------------
# Blend + validation
# ---------------------------------------------------------------------------
def _predict_rmse(games: List[GameRow], value: Dict[str, float], hca: float,
                  scale: float = 1.0) -> float:
    err = [g["margin"] - (scale * (value.get(g["home"], 0.0) - value.get(g["away"], 0.0)) + hca)
           for g in games]
    return float(np.sqrt(np.mean(np.square(err))))


def validate_ratings(seasons: List[str]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Three-way chronological split per season (see module docstring)."""
    results: List[Dict[str, Any]] = []
    pooled_X: List[List[float]] = []
    pooled_y: List[float] = []

    per_season: Dict[str, Dict[str, Any]] = {}
    for s in seasons:
        games, _ = _season_games(s)
        c1, c2 = int(TRAIN_FRACTION * len(games)), int(BLEND_FRACTION * len(games))
        train, blend_window, test = games[:c1], games[c1:c2], games[c2:]
        srs, hca = _fit_srs(train)
        ipm = _ipm_ratings(s)
        per_season[s] = {"test": test, "srs": srs, "hca": hca, "ipm": ipm}
        # Blend weights learn on games the SRS fit has never seen — the only
        # way the regression can judge the two ratings on generalization.
        for g in blend_window:
            if ipm is None:
                continue
            pooled_X.append([srs.get(g["home"], 0) - srs.get(g["away"], 0),
                             ipm.get(g["home"], 0) - ipm.get(g["away"], 0), 1.0])
            pooled_y.append(g["margin"])

    coef, *_ = np.linalg.lstsq(np.asarray(pooled_X), np.asarray(pooled_y), rcond=None)
    weights = {"srs": float(coef[0]), "ipm": float(coef[1]), "hca": float(coef[2])}
    logger.info("Blend weights (pooled train fits): %.3f·SRS + %.3f·IPM, HCA=%.2f",
                weights["srs"], weights["ipm"], weights["hca"])

    for s in seasons:
        d = per_season[s]
        test, srs, hca, ipm = d["test"], d["srs"], d["hca"], d["ipm"]
        results.append({"season": s, "model": "hca_only", "rmse":
                        round(_predict_rmse(test, {}, hca), 3), "n_games": len(test)})
        results.append({"season": s, "model": "srs", "rmse":
                        round(_predict_rmse(test, srs, hca), 3), "n_games": len(test)})
        if ipm is not None:
            results.append({"season": s, "model": "ipm_roster", "rmse":
                            round(_predict_rmse(test, ipm, hca), 3), "n_games": len(test)})
            blend = {t: weights["srs"] * srs.get(t, 0.0) + weights["ipm"] * ipm.get(t, 0.0)
                     for t in set(srs) | set(ipm)}
            results.append({"season": s, "model": "blend", "rmse":
                            round(_predict_rmse(test, blend, weights["hca"]), 3),
                            "n_games": len(test)})

    df = pd.DataFrame(results)
    out = config.DATA_DIR / "ratings_validation.csv"
    df.to_csv(out, index=False)
    logger.info("Saved %d validation rows → %s", len(df), out)
    return df, weights


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def compute_ratings(seasons: Optional[List[str]] = None) -> None:
    """Validate → write team_ratings_<season>.csv for each season."""
    seasons = seasons or available_seasons()
    _, weights = validate_ratings(seasons)
    # The fitted blend is research for B2 (win prob), not a published column —
    # see the module docstring for why.
    MODEL_PATH.write_text(json.dumps(
        {"weights": weights, "train_fraction": TRAIN_FRACTION,
         "blend_fraction": BLEND_FRACTION, "seasons": seasons,
         "note": "blend weights are experimental (negative SRS weight, "
                 "IPM look-ahead) — published ratings use TEAM_IPM + SRS only"},
        indent=2))

    for s in seasons:
        games, teams = _season_games(s)
        srs, hca = _fit_srs(games)
        o, d, _mu = _fit_od(games)
        sos = _sos(games, srs, o, d)
        ipm = _ipm_ratings(s)

        wl: Dict[str, List[int]] = {t: [0, 0] for t in teams}
        for g in games:
            winner, loser = (g["home"], g["away"]) if g["margin"] > 0 else (g["away"], g["home"])
            wl[winner][0] += 1
            wl[loser][1] += 1

        rows = []
        for t, meta in teams.items():
            ipm_r = ipm.get(t) if ipm else None
            rows.append({
                "TEAM_ID": meta["id"], "TEAM_ABBREVIATION": t, "TEAM_NAME": meta["name"],
                "GP": wl[t][0] + wl[t][1], "W": wl[t][0], "L": wl[t][1],
                "TEAM_IPM": round(ipm_r, 2) if ipm_r is not None else None,
                "SRS": round(srs[t], 2),
                "O_SRS": round(o[t], 2), "D_SRS": round(d[t], 2),
                "SOS": round(sos[t]["SOS"], 2),
                "OSOS": round(sos[t]["OSOS"], 2), "DSOS": round(sos[t]["DSOS"], 2),
                "SEASON": s, "SEASON_TYPE": "Regular Season",
            })
        df = pd.DataFrame(rows)
        sort_col = "TEAM_IPM" if df["TEAM_IPM"].notna().any() else "SRS"
        df = df.sort_values(sort_col, ascending=False).reset_index(drop=True)
        out = config.DATA_DIR / f"team_ratings_{s}.csv"
        df.to_csv(out, index=False)
        logger.info("%s: saved %d teams → %s (HCA=%.2f)", s, len(df), out, hca)


if __name__ == "__main__":  # pragma: no cover - manual runs
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    compute_ratings()
    df = pd.read_csv(config.DATA_DIR / f"team_ratings_{config.SEASON}.csv")
    print(f"\n=== {config.SEASON} POWER RATINGS (by Team IPM) ===")
    print(df[["TEAM_ABBREVIATION", "W", "L", "TEAM_IPM", "SRS",
              "O_SRS", "D_SRS", "SOS"]].head(12).to_string(index=False))
    val = pd.read_csv(config.DATA_DIR / "ratings_validation.csv")
    print("\n=== VALIDATION: mean held-out RMSE by model ===")
    print(val.groupby("model")["rmse"].mean().round(3).sort_values().to_string())
