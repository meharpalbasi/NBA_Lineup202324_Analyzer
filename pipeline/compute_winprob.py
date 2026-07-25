"""Pregame win probability (roadmap B2) — computed OFFLINE, validated honestly.

The model predicts P(home win) before tip-off from two *as-of-date* team
ratings, so every retrodicted prediction uses only information available on
the morning of the game:

  SRS-to-date   venue-adjusted point margin fit ONLY on the season's games
                played so far (ridge-shrunk toward 0 so opening week isn't
                garbage).
  Roster-IPM    the Dunks & Threes Team-EPM recipe with a one-season lag:
                LAST season's player IPM values, weighted by minutes actually
                played SO FAR this season (so trades and rotation changes are
                picked up with a short lag). On opening day, before any
                minutes exist, it seeds from last season's published Team IPM.

This is the fix for why the ratings blend went unpublished in B1: season-level
IPM peeks at the future inside a season, prior-season IPM cannot.

P(home win) is a logistic fit on [srs_diff, ipm_diff]; a parallel least-squares
margin model on the same features feeds the playoff simulator later (B3).

Validation: leave-one-season-out across 2018-19 … 2025-26 (2017-18 only serves
as a prior source — there is no 2016-17 IPM). Brier score, log loss, and a
pooled calibration table are written for the methodology page; contestants are
home-court-only, SRS-only, roster-only, and the blend. Known quirk we do NOT
special-case: the 2020 bubble and the empty-arena 2020-21 season mute home
court; it shows up as noise, not bias.

Runs standalone:  venv/bin/python -m pipeline.compute_winprob
In-season, the weekly run also writes winprob_upcoming.csv for unplayed games
on the schedule (offseason: skipped).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .compute_ratings import _season_games, GameRow

logger = logging.getLogger("pipeline.compute_winprob")

MODEL_PATH = config.DATA_DIR / "winprob_model.json"

# Ridge strength for the to-date SRS (in "games" units): with ~4 games of
# shrinkage per team, opening-week ratings stay near 0 instead of exploding
# off two blowouts; by midseason it's negligible.
SRS_LAMBDA = 4.0

# Players missing from last season's IPM (rookies, fringe) get the same
# below-average floor the IPM prior itself uses (FLOOR_O + FLOOR_D).
PRIOR_FLOOR = -1.3


def _season_list() -> List[str]:
    return [f"20{y}-{y+1}" for y in range(17, 26)]


def _prior_season(season: str) -> str:
    start = int(season[:4])
    return f"{start - 1}-{str(start)[2:]}"


# ---------------------------------------------------------------------------
# As-of-date SRS (ridge, refit per game — cheap at this size)
# ---------------------------------------------------------------------------
def _srs_to_date(games: List[GameRow]) -> List[Dict[str, float]]:
    """ratings_before[i] = team->SRS using only games[0:i] (empty dict at i=0)."""
    teams = sorted({g["home"] for g in games} | {g["away"] for g in games})
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    out: List[Dict[str, float]] = []
    X_rows: List[np.ndarray] = []
    y: List[float] = []
    # Ridge rows: shrink team coefficients (not HCA) toward zero.
    ridge = [np.sqrt(SRS_LAMBDA) * r for r in np.eye(n + 1)[:n]]
    for i, g in enumerate(games):
        if i == 0:
            out.append({})
        else:
            A = np.vstack(X_rows + ridge)
            b = np.concatenate([np.asarray(y), np.zeros(n)])
            coef, *_ = np.linalg.lstsq(A, b, rcond=None)
            r = coef[:n] - coef[:n].mean()
            out.append(dict(zip(teams, r)))
        row = np.zeros(n + 1)
        row[idx[g["home"]]] = 1.0
        row[idx[g["away"]]] = -1.0
        row[n] = 1.0
        X_rows.append(row)
        y.append(g["margin"])
    return out


# ---------------------------------------------------------------------------
# As-of-date roster rating from PRIOR-season player IPM
# ---------------------------------------------------------------------------
def _prior_player_ipm(season: str) -> Optional[Dict[int, float]]:
    path = config.DATA_DIR / f"ipm_{_prior_season(season)}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, low_memory=False)
    return dict(zip(df["PLAYER_ID"].astype(int), df["IPM"].astype(float)))


def _prior_team_ipm(season: str) -> Dict[str, float]:
    """Opening-day seed: last season's published Team IPM by abbreviation."""
    path = config.DATA_DIR / f"team_ratings_{_prior_season(season)}.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path, low_memory=False)
    if "TEAM_IPM" not in df.columns:
        return {}
    return {
        str(r.TEAM_ABBREVIATION): float(r.TEAM_IPM)
        for r in df.itertuples()
        if pd.notna(r.TEAM_IPM)
    }


class _RosterRating:
    """Walk a season chronologically; rating(team, date) uses minutes < date."""

    def __init__(self, season: str):
        self.prior = _prior_player_ipm(season)
        self.seed = _prior_team_ipm(season)
        logs = pd.read_csv(
            config.DATA_DIR / f"player_game_logs_{season}.csv", low_memory=False
        )
        logs = logs[logs["SEASON_TYPE"] == "Regular Season"]
        logs = logs.sort_values("GAME_DATE")
        self.rows: Dict[str, List[Tuple[str, int, float]]] = {}
        for r in logs.itertuples():
            self.rows.setdefault(str(r.TEAM_ABBREVIATION), []).append(
                (str(r.GAME_DATE), int(r.PLAYER_ID), float(r.MIN or 0.0))
            )
        self.cursor: Dict[str, int] = {t: 0 for t in self.rows}
        self.cum: Dict[str, Dict[int, float]] = {t: {} for t in self.rows}

    def rating(self, team: str, date: str) -> float:
        rows = self.rows.get(team, [])
        cur = self.cursor.get(team, 0)
        cum = self.cum.setdefault(team, {})
        while cur < len(rows) and rows[cur][0] < date:
            _, pid, minutes = rows[cur]
            cum[pid] = cum.get(pid, 0.0) + minutes
            cur += 1
        self.cursor[team] = cur
        total = sum(cum.values())
        if total <= 0 or self.prior is None:
            return self.seed.get(team, 0.0)
        weighted = sum(
            self.prior.get(pid, PRIOR_FLOOR) * m for pid, m in cum.items()
        )
        return 5.0 * weighted / total


# ---------------------------------------------------------------------------
# Per-season feature rows
# ---------------------------------------------------------------------------
def _season_features(season: str) -> pd.DataFrame:
    games, _teams = _season_games(season)
    srs_before = _srs_to_date(games)
    roster = _RosterRating(season)
    has_prior = roster.prior is not None
    rows = []
    for i, g in enumerate(games):
        srs = srs_before[i]
        rows.append({
            "season": season,
            "date": g["date"],
            "home": g["home"],
            "away": g["away"],
            "margin": g["margin"],
            "home_win": 1.0 if g["margin"] > 0 else 0.0,
            "srs_diff": srs.get(g["home"], 0.0) - srs.get(g["away"], 0.0),
            "ipm_diff": (
                roster.rating(g["home"], g["date"]) - roster.rating(g["away"], g["date"])
                if has_prior
                else np.nan
            ),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Models: logistic (probability) + least squares (margin, for B3)
# ---------------------------------------------------------------------------
def _fit_logistic(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    from sklearn.linear_model import LogisticRegression

    m = LogisticRegression(C=1e6, max_iter=2000)
    m.fit(X, y)
    return np.concatenate([m.intercept_, m.coef_[0]])


def _predict_logistic(coef: np.ndarray, X: np.ndarray) -> np.ndarray:
    z = coef[0] + X @ coef[1:]
    return 1.0 / (1.0 + np.exp(-z))


def _brier(p: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _logloss(p: np.ndarray, y: np.ndarray) -> float:
    q = np.clip(p, 1e-12, 1 - 1e-12)
    return float(-np.mean(y * np.log(q) + (1 - y) * np.log(1 - q)))


CONTESTANTS = {
    "home_only": [],
    "srs": ["srs_diff"],
    "roster_prior_ipm": ["ipm_diff"],
    "blend": ["srs_diff", "ipm_diff"],
}


def validate(seasons: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    seasons = seasons or _season_list()
    feats = {s: _season_features(s) for s in seasons}
    for s in seasons:
        logger.info("%s: %d games featurized", s, len(feats[s]))

    # 2017-18 has no prior-season IPM → prior-source only, never train/test.
    usable = [s for s in seasons if not feats[s]["ipm_diff"].isna().any()]
    logger.info("LOSO seasons: %s", ", ".join(usable))

    results: List[Dict[str, Any]] = []
    blend_pred: List[np.ndarray] = []
    blend_true: List[np.ndarray] = []
    for test in usable:
        train = pd.concat([feats[s] for s in usable if s != test], ignore_index=True)
        te = feats[test]
        y_tr = train["home_win"].to_numpy()
        y_te = te["home_win"].to_numpy()
        # The roster prior matters most before SRS has a sample — report the
        # season's first quarter separately (that's the window we go live in).
        early = np.arange(len(te)) < len(te) // 4
        for name, cols in CONTESTANTS.items():
            if not cols:
                p = np.full(len(te), y_tr.mean())
            else:
                coef = _fit_logistic(train[cols].to_numpy(), y_tr)
                p = _predict_logistic(coef, te[cols].to_numpy())
            for window, mask in (("full", np.ones(len(te), bool)), ("first_quarter", early)):
                results.append({
                    "season": test, "model": name, "window": window,
                    "brier": round(_brier(p[mask], y_te[mask]), 4),
                    "logloss": round(_logloss(p[mask], y_te[mask]), 4),
                    "accuracy": round(float(np.mean((p[mask] > 0.5) == (y_te[mask] > 0.5))), 4),
                    "n_games": int(mask.sum()),
                })
            if name == "blend":
                blend_pred.append(p)
                blend_true.append(y_te)

    val = pd.DataFrame(results)

    # Pooled calibration for the blend: 10 equal-width probability bins.
    p = np.concatenate(blend_pred)
    y = np.concatenate(blend_true)
    bins = np.clip((p * 10).astype(int), 0, 9)
    cal_rows = []
    for b in range(10):
        mask = bins == b
        if mask.sum() == 0:
            continue
        cal_rows.append({
            "bin": f"{b/10:.0%}–{(b+1)/10:.0%}",
            "n_games": int(mask.sum()),
            "predicted": round(float(p[mask].mean()), 3),
            "actual": round(float(y[mask].mean()), 3),
        })
    cal = pd.DataFrame(cal_rows)

    # Production fit on every usable season: probability + margin models.
    pooled = pd.concat([feats[s] for s in usable], ignore_index=True)
    X = pooled[["srs_diff", "ipm_diff"]].to_numpy()
    prob_coef = _fit_logistic(X, pooled["home_win"].to_numpy())
    A = np.column_stack([X, np.ones(len(pooled))])
    margin_coef, *_ = np.linalg.lstsq(A, pooled["margin"].to_numpy(), rcond=None)
    margin_rmse = float(np.sqrt(np.mean((A @ margin_coef - pooled["margin"]) ** 2)))
    model = {
        "features": ["srs_diff", "ipm_diff"],
        "prob_logistic": [round(float(c), 6) for c in prob_coef],
        "margin_lstsq": [round(float(c), 6) for c in margin_coef],
        "margin_rmse_in_sample": round(margin_rmse, 3),
        "seasons": usable,
        "srs_lambda": SRS_LAMBDA,
        "prior_floor": PRIOR_FLOOR,
        "note": "P(home) = logistic(b0 + b1*srs_diff + b2*ipm_diff); margin model feeds B3 sims.",
    }
    return val, cal, model


# ---------------------------------------------------------------------------
# In-season: predictions for unplayed scheduled games (no-op in the offseason)
# ---------------------------------------------------------------------------
def predict_upcoming(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    sched_path = config.DATA_DIR / f"schedule_{season}.csv"
    if not sched_path.exists() or not MODEL_PATH.exists():
        return None
    sched = pd.read_csv(sched_path, low_memory=False)
    upcoming = sched[(sched["STAGE"] == "Regular Season") & (sched["STATUS"] == 1)]
    if upcoming.empty:
        logger.info("No unplayed regular-season games on the %s schedule.", season)
        return None
    model = json.loads(MODEL_PATH.read_text())
    games, _ = _season_games(season)
    srs = _srs_to_date(games + [games[-1]])[-1] if games else {}
    roster = _RosterRating(season)
    end = "9999-99-99"
    rows = []
    for g in upcoming.itertuples():
        home, away = str(g.HOME_ABBR), str(g.AWAY_ABBR)
        x = np.array([
            srs.get(home, 0.0) - srs.get(away, 0.0),
            roster.rating(home, end) - roster.rating(away, end),
        ])
        coef = np.asarray(model["prob_logistic"])
        mar = np.asarray(model["margin_lstsq"])
        rows.append({
            "GAME_ID": g.GAME_ID, "GAME_DATE": g.GAME_DATE,
            "HOME": home, "AWAY": away,
            "P_HOME_WIN": round(float(_predict_logistic(coef, x[None, :])[0]), 3),
            "PRED_MARGIN": round(float(x @ mar[:2] + mar[2]), 1),
        })
    out = pd.DataFrame(rows)
    out_path = config.DATA_DIR / f"winprob_upcoming_{season}.csv"
    out.to_csv(out_path, index=False)
    logger.info("Saved %d upcoming predictions → %s", len(out), out_path)
    return out


def compute_winprob() -> None:
    val, cal, model = validate()
    val.to_csv(config.DATA_DIR / "winprob_validation.csv", index=False)
    cal.to_csv(config.DATA_DIR / "winprob_calibration.csv", index=False)
    MODEL_PATH.write_text(json.dumps(model, indent=2))
    logger.info("Saved validation (%d rows), calibration (%d bins), model → %s",
                len(val), len(cal), MODEL_PATH.name)
    predict_upcoming()


if __name__ == "__main__":  # pragma: no cover - manual runs
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    compute_winprob()
    val = pd.read_csv(config.DATA_DIR / "winprob_validation.csv")
    for window in ("full", "first_quarter"):
        sub = val[val["window"] == window]
        print(f"\n=== Held-out Brier by model — {window} (LOSO, lower is better) ===")
        print(sub.groupby("model")[["brier", "logloss", "accuracy"]].mean().round(4)
              .sort_values("brier").to_string())
    print("\n=== Calibration (pooled blend) ===")
    print(pd.read_csv(config.DATA_DIR / "winprob_calibration.csv").to_string(index=False))
