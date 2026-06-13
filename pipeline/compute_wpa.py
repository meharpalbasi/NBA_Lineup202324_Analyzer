"""Win Probability Added (scoring) + the biggest plays of the season.

Built entirely from the play-by-play cache the RAPM subsystem already maintains
(``data/rapm_cache/``) — no new API calls.

Model: the classic Brownian-motion win-probability curve (Stern 1994),
``P(home win) = Φ((margin + μ·τ) / (σ·√τ))`` where τ is the fraction of the
game remaining, μ is the home-court drift and σ the score-diffusion scale.
Both parameters are FIT by maximum likelihood on every scored event of the
cached seasons (~2M states across ~3,700 games), not taken from a paper.

Attribution (deliberately simple, honest v1): every **made basket / made free
throw** moves the win probability at that (margin, time); the swing — from the
shooter's team's perspective — is credited to the shooter. Misses and turnovers
are not debited (that needs a possession-aware state model), so this is
"scoring WPA": who actually swung games with buckets, weighted by leverage.
A regular-season game-winner can be worth ~±0.9 wins; a garbage-time bucket ~0.

Outputs:
  wpa_{season}.csv           — per player: WPA, scoring plays counted
  biggest_plays_{season}.csv — top swings of the season with game/date/desc
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .fetch_rapm import (
    CACHE_DIR,
    _clock_seconds,
    _period_len,
    fetch_game_ids,
    _cached,
)

logger = logging.getLogger("pipeline.compute_wpa")

REGULATION_SECS = 4 * 720.0

# Floor on τ so the curve stays finite at the buzzer; below ~2s the sign of the
# margin decides the game anyway.
MIN_TAU = 2.0 / REGULATION_SECS

# The model is fit once on every cached season, newest first.
FIT_SEASONS = 3


def _norm_cdf(z):
    return 0.5 * (1.0 + np.vectorize(math.erf)(z / math.sqrt(2.0)))


def _time_remaining(period: int, clock: str) -> float:
    """Seconds remaining: rest of regulation, or rest of the current OT."""
    if period <= 4:
        return (4 - period) * 720.0 + _clock_seconds(clock)
    return _clock_seconds(clock)


def _game_states(gid: str) -> Optional[Tuple[List[tuple], int, int, list]]:
    """(states, home_id, away_id, sorted events) for one cached game.

    Each state: (tau, home_margin) sampled at every event that carries a score.
    """
    pbp_path = CACHE_DIR / f"pbp_{gid}.json"
    box_path = CACHE_DIR / f"box_{gid}.json"
    if not (pbp_path.exists() and box_path.exists()):
        return None
    actions = json.loads(pbp_path.read_text())["game"]["actions"]
    box = json.loads(box_path.read_text())["boxScoreTraditional"]
    home_id, away_id = box["homeTeamId"], box["awayTeamId"]

    events = sorted(
        (a for a in actions if a.get("actionType") != "period"),
        key=lambda a: (
            sum(_period_len(p) for p in range(1, a["period"])) + _period_len(a["period"]) - _clock_seconds(a["clock"]),
            a["actionNumber"],
        ),
    )
    states = []
    h = v = 0
    for a in events:
        sh, sv = a.get("scoreHome"), a.get("scoreAway")
        if sh not in (None, "") and sv not in (None, ""):
            try:
                h, v = int(sh), int(sv)
            except (TypeError, ValueError):
                pass
        tau = max(_time_remaining(a["period"], a["clock"]) / REGULATION_SECS, MIN_TAU)
        states.append((tau, h - v))
    return states, home_id, away_id, events


def fit_wp_model(seasons: List[str], season_type: str = "Regular Season") -> Tuple[float, float]:
    """MLE fit of (mu, sigma) on every cached game state of the given seasons."""
    taus, margins, outcomes = [], [], []
    n_games = 0
    for season in seasons:
        try:
            gids = fetch_game_ids(season, season_type)
        except Exception as exc:
            logger.warning("WP fit: cannot enumerate %s (%s) — skipping", season, exc)
            continue
        for gid in gids:
            parsed = _game_states(gid)
            if not parsed:
                continue
            states, *_ = parsed
            if not states:
                continue
            final_margin = states[-1][1]
            if final_margin == 0:
                continue
            home_won = 1.0 if final_margin > 0 else 0.0
            n_games += 1
            for tau, margin in states:
                taus.append(tau)
                margins.append(margin)
                outcomes.append(home_won)
    tau = np.asarray(taus)
    m = np.asarray(margins, dtype=float)
    y = np.asarray(outcomes)
    logger.info("WP fit: %d states from %d games", len(tau), n_games)

    def nll(params):
        mu, sigma = params
        if sigma <= 1.0:
            return 1e12
        p = _norm_cdf((m + mu * tau) / (sigma * np.sqrt(tau)))
        p = np.clip(p, 1e-6, 1 - 1e-6)
        return -float(np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))

    from scipy.optimize import minimize

    res = minimize(nll, x0=[2.5, 13.0], method="Nelder-Mead")
    mu, sigma = float(res.x[0]), float(res.x[1])
    logger.info("WP model: mu=%.2f (home drift, pts/game), sigma=%.2f (logloss %.4f)", mu, sigma, res.fun)
    return mu, sigma


def _wp(margin: float, tau: float, mu: float, sigma: float) -> float:
    tau = max(tau, MIN_TAU)
    z = (margin + mu * tau) / (sigma * math.sqrt(tau))
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


_SCORING_TYPES = {"Made Shot", "Free Throw"}


def compute_wpa(
    season: str = config.SEASON,
    season_type: str = "Regular Season",
    model: Optional[Tuple[float, float]] = None,
    top_plays: int = 25,
) -> pd.DataFrame:
    """Per-player scoring WPA + the season's biggest plays, from cache."""
    if model is None:
        seasons = [season]
        for _ in range(FIT_SEASONS - 1):
            start = int(seasons[-1][:4])
            seasons.append(f"{start - 1}-{str(start)[2:]}")
        model = fit_wp_model(seasons, season_type)
    mu, sigma = model

    gids = fetch_game_ids(season, season_type)
    # Game date + matchup for the biggest-plays table, from the schedule cache.
    sched = _cached(
        f"schedule_{season}_{season_type.replace(' ', '')}",
        "leaguegamelog",
        {"Counter": 0, "Direction": "ASC", "LeagueID": "00", "PlayerOrTeam": "T",
         "Season": season, "SeasonType": season_type, "Sorter": "DATE"},
    )["resultSets"][0]
    si = {h: i for i, h in enumerate(sched["headers"])}
    game_meta: Dict[str, Tuple[str, str]] = {}
    for row in sched["rowSet"]:
        gid = row[si["GAME_ID"]]
        if gid not in game_meta or "vs." in str(row[si["MATCHUP"]]):
            game_meta[gid] = (str(row[si["GAME_DATE"]]), str(row[si["MATCHUP"]]))

    wpa: Dict[int, float] = defaultdict(float)
    plays_counted: Dict[int, int] = defaultdict(int)
    names: Dict[int, str] = {}
    teams: Dict[int, str] = {}
    biggest: List[dict] = []
    skipped = 0

    for gid in gids:
        parsed = _game_states(gid)
        if not parsed:
            skipped += 1
            continue
        _, home_id, away_id, events = parsed
        h = v = 0
        for a in events:
            prev_margin = h - v
            sh, sv = a.get("scoreHome"), a.get("scoreAway")
            if sh not in (None, "") and sv not in (None, ""):
                try:
                    h, v = int(sh), int(sv)
                except (TypeError, ValueError):
                    pass
            margin = h - v
            if margin == prev_margin or a.get("actionType") not in _SCORING_TYPES:
                continue
            pid = a.get("personId")
            tid = a.get("teamId")
            if not pid or tid not in (home_id, away_id):
                continue
            tau = max(_time_remaining(a["period"], a["clock"]) / REGULATION_SECS, MIN_TAU)
            wp_before = _wp(prev_margin, tau, mu, sigma)
            wp_after = _wp(margin, tau, mu, sigma)
            delta_home = wp_after - wp_before
            delta = delta_home if tid == home_id else -delta_home
            wpa[pid] += delta
            plays_counted[pid] += 1
            if a.get("playerName"):
                names[pid] = a.get("playerNameI") or a.get("playerName")
            if tid:
                teams.setdefault(pid, str(a.get("teamTricode") or ""))
            if abs(delta) > 0.15:
                date, matchup = game_meta.get(gid, ("", ""))
                home_persp = tid == home_id
                biggest.append({
                    "GAME_ID": gid,
                    "GAME_DATE": date,
                    "MATCHUP": matchup,
                    "PLAYER_ID": pid,
                    "PLAYER_NAME": a.get("playerNameI") or a.get("playerName") or str(pid),
                    "TEAM_ABBREVIATION": str(a.get("teamTricode") or ""),
                    "PERIOD": a["period"],
                    "CLOCK": a["clock"].replace("PT", "").replace("M", ":").replace("S", ""),
                    "DESCRIPTION": str(a.get("description") or "")[:120],
                    "WP_BEFORE": round(wp_before if home_persp else 1 - wp_before, 3),
                    "WP_AFTER": round(wp_after if home_persp else 1 - wp_after, 3),
                    "WPA": round(delta, 3),
                    "SEASON_TYPE": season_type,
                })

    if skipped:
        logger.warning("WPA: %d/%d games missing from cache (run the RAPM fetch first)", skipped, len(gids))

    out = pd.DataFrame({
        "PLAYER_ID": list(wpa.keys()),
        "PLAYER_NAME": [names.get(p, str(p)) for p in wpa],
        "TEAM_ABBREVIATION": [teams.get(p, "") for p in wpa],
        "WPA": [round(v, 2) for v in wpa.values()],
        "WPA_PLAYS": [plays_counted[p] for p in wpa],
        "SEASON": season,
        "SEASON_TYPE": season_type,
    }).sort_values("WPA", ascending=False).reset_index(drop=True)
    out_path = config.DATA_DIR / f"wpa_{season}.csv"
    out.to_csv(out_path, index=False)
    logger.info("Saved %d players → %s", len(out), out_path)

    plays = pd.DataFrame(sorted(biggest, key=lambda r: -abs(r["WPA"]))[:top_plays])
    plays_path = config.DATA_DIR / f"biggest_plays_{season}.csv"
    plays.to_csv(plays_path, index=False)
    logger.info("Saved top %d plays → %s", len(plays), plays_path)
    return out


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    compute_wpa()
