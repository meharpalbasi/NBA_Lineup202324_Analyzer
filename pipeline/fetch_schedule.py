"""League schedule — the one fetch that unlocks the predictive tier.

``scheduleleaguev2`` returns the full season calendar (played AND unplayed
games, with scores/status inline), which is what playoff simulation (B3) and
daily projections (B4) need and what game logs can't provide: game logs only
exist for games already played. For completed seasons the two sources agree —
that's the validation.

One light call per season, so no disk cache (the schedule mutates as games get
played; a cache would only go stale). GameId prefix encodes the stage:
001 preseason · 002 regular season · 003 all-star · 004 playoffs · 005 play-in
· 006 NBA Cup final (excluded from regular-season records).

Standalone:  venv/bin/python -m pipeline.fetch_schedule            (config.SEASON)
             NBA_SEASON=2026-27 venv/bin/python -m pipeline.fetch_schedule
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

from . import config
from .fetch_rapm import _get_json

logger = logging.getLogger("pipeline.fetch_schedule")

STAGE_BY_PREFIX = {
    "001": "Preseason",
    "002": "Regular Season",
    "003": "All-Star",
    "004": "Playoffs",
    "005": "Play-In",
    "006": "Cup Final",
}


def fetch_schedule(season: str = config.SEASON) -> pd.DataFrame:
    """Fetch the season schedule → ``schedule_<season>.csv`` (all stages, flagged)."""
    data = _get_json("scheduleleaguev2", {"Season": season, "LeagueID": "00"})
    game_dates = data.get("leagueSchedule", {}).get("gameDates", [])

    rows: List[Dict[str, Any]] = []
    for gd in game_dates:
        for g in gd.get("games", []):
            gid = g.get("gameId", "")
            home, away = g.get("homeTeam", {}) or {}, g.get("awayTeam", {}) or {}
            status = int(g.get("gameStatus") or 1)  # 1 scheduled · 2 live · 3 final
            rows.append({
                "GAME_ID": gid,
                "GAME_DATE": (g.get("gameDateEst") or "")[:10],
                "STAGE": STAGE_BY_PREFIX.get(gid[:3], "Other"),
                "HOME_TEAM_ID": home.get("teamId"),
                "HOME_ABBR": home.get("teamTricode"),
                "AWAY_TEAM_ID": away.get("teamId"),
                "AWAY_ABBR": away.get("teamTricode"),
                "HOME_PTS": home.get("score") if status == 3 else None,
                "AWAY_PTS": away.get("score") if status == 3 else None,
                "STATUS": status,
            })

    df = pd.DataFrame(rows).sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
    df["SEASON"] = season
    out = config.DATA_DIR / f"schedule_{season}.csv"
    df.to_csv(out, index=False)
    n_reg = int((df["STAGE"] == "Regular Season").sum())
    n_final = int((df["STATUS"] == 3).sum())
    logger.info("Saved %d games (%d regular season, %d final) → %s",
                len(df), n_reg, n_final, out)
    return df


if __name__ == "__main__":  # pragma: no cover - manual runs
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = fetch_schedule()
    print(df.groupby("STAGE").size().to_string())
    upcoming = df[df["STATUS"] == 1]
    if not upcoming.empty:
        print(f"\n{len(upcoming)} unplayed games, first: "
              f"{upcoming.iloc[0]['GAME_DATE']} {upcoming.iloc[0]['AWAY_ABBR']} @ {upcoming.iloc[0]['HOME_ABBR']}")
