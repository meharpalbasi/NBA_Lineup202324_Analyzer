"""Raw x/y shot locations → hex-binned shot chart data.

``LeagueDashPlayerShotLocations`` (the existing ``shot_zones`` pull) only gives
zone totals; this subsystem pulls every individual shot via ``ShotChartDetail``
(one call per player, ~570 calls for a season — heavy, so it runs standalone or
from the weekly heavy-jobs script, NOT inside --supplementary-only) and bins
them into pointy-top hexagons the frontend can render as a true heatmap.

Output: ``shot_hex_{season}.csv`` with one row per (player, hex):
  PLAYER_ID, HX, HY (hex center in NBA court units: 0.1ft, basket at origin),
  FGA, FGM, FG3A, FG3M, SEASON_TYPE
plus league-aggregate rows under PLAYER_ID=0 so the frontend can color each
player hex against the league eFG% in that exact location.

Raw per-player JSON is cached under ``data/shotdetail_cache/`` (gitignored), so
re-runs only fetch players it hasn't seen for the season.
"""

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from . import config
from .fetch_rapm import _get_json

logger = logging.getLogger("pipeline.fetch_shot_detail")

CACHE_DIR: Path = config.DATA_DIR / "shotdetail_cache"

# Hex size in court units (0.1 ft) — 25 ≈ 2.5ft "radius", ~4.3ft-wide cells.
HEX_SIZE: float = 25.0

# Keep the chart on the half court (basket y=0; past ~y 320 is heave country).
MAX_Y: float = 330.0

# Slimming floors: a player hex needs a real sample; league hexes even more so.
MIN_PLAYER_HEX_FGA: int = 3
MIN_LEAGUE_HEX_FGA: int = 25

SHOT_CALL_DELAY: float = 1.2


def _axial_round(qf: float, rf: float) -> tuple[int, int]:
    """Round fractional axial hex coords via cube rounding."""
    xf, zf = qf, rf
    yf = -xf - zf
    rx, ry, rz = round(xf), round(yf), round(zf)
    dx, dy, dz = abs(rx - xf), abs(ry - yf), abs(rz - zf)
    if dx > dy and dx > dz:
        rx = -ry - rz
    elif dy > dz:
        ry = -rx - rz
    else:
        rz = -rx - ry
    return int(rx), int(rz)


def hex_center(x: float, y: float, size: float = HEX_SIZE) -> tuple[float, float]:
    """Snap a court (x, y) to its pointy-top hex center."""
    qf = (math.sqrt(3) / 3 * x - y / 3) / size
    rf = (2 / 3 * y) / size
    q, r = _axial_round(qf, rf)
    cx = size * math.sqrt(3) * (q + r / 2)
    cy = size * 1.5 * r
    return round(cx, 1), round(cy, 1)


def _player_ids(season: str) -> List[int]:
    """Players to pull: everyone in the season's player_stats with a game played."""
    path = config.DATA_DIR / f"player_stats_{season}.csv"
    if not path.exists():
        raise FileNotFoundError(f"player_stats_{season}.csv not found — run the supplementary fetch first")
    df = pd.read_csv(path, usecols=["PLAYER_ID", "GP", "SEASON_TYPE"], low_memory=False)
    df = df[df["SEASON_TYPE"] == "Regular Season"]
    return sorted(set(df.loc[df["GP"] > 0, "PLAYER_ID"].astype(int)))


def _fetch_player_shots(player_id: int, season: str, season_type: str) -> Optional[dict]:
    """One cached ShotChartDetail call for a player-season."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = CACHE_DIR / f"shots_{season}_{season_type.replace(' ', '')}_{player_id}.json"
    if cache.exists():
        try:
            return json.loads(cache.read_text())
        except Exception:
            logger.warning("Corrupt cache %s — refetching", cache)
    data = _get_json(
        "shotchartdetail",
        {
            "PlayerID": player_id, "Season": season, "SeasonType": season_type,
            "TeamID": 0, "LeagueID": "00", "ContextMeasure": "FGA",
            "PlayerPosition": "", "DateFrom": "", "DateTo": "", "GameID": "",
            "GameSegment": "", "LastNGames": 0, "Location": "", "Month": 0,
            "OpponentTeamID": 0, "Outcome": "", "Period": 0, "Position": "",
            "RookieYear": "", "SeasonSegment": "", "VsConference": "", "VsDivision": "",
        },
    )
    tmp = cache.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(cache)
    time.sleep(SHOT_CALL_DELAY)
    return data


def fetch_shot_detail(season: str = config.SEASON, season_type: str = "Regular Season") -> pd.DataFrame:
    """Pull every player's shots, hex-bin them, save ``shot_hex_{season}.csv``."""
    players = _player_ids(season)
    logger.info("Shot detail: %d players for %s %s", len(players), season, season_type)

    # (player_id, hx, hy) -> [fga, fgm, fg3a, fg3m]; player_id 0 = league.
    agg: Dict[tuple, List[int]] = {}

    def bump(pid: int, hx: float, hy: float, made: int, is3: bool) -> None:
        key = (pid, hx, hy)
        row = agg.setdefault(key, [0, 0, 0, 0])
        row[0] += 1
        row[1] += made
        if is3:
            row[2] += 1
            row[3] += made

    failed = 0
    for n, pid in enumerate(players, 1):
        try:
            data = _fetch_player_shots(pid, season, season_type)
            rs = data["resultSets"][0]
            idx = {h: i for i, h in enumerate(rs["headers"])}
            for row in rs["rowSet"]:
                x = float(row[idx["LOC_X"]])
                y = float(row[idx["LOC_Y"]])
                if y > MAX_Y:
                    continue
                made = int(row[idx["SHOT_MADE_FLAG"]])
                is3 = "3PT" in str(row[idx["SHOT_TYPE"]])
                hx, hy = hex_center(x, y)
                bump(pid, hx, hy, made, is3)
                bump(0, hx, hy, made, is3)
        except Exception as exc:
            failed += 1
            logger.warning("player %d failed: %s", pid, exc)
        if n % 50 == 0:
            logger.info("Shot detail: %d/%d players (%d hexes, %d failed)", n, len(players), len(agg), failed)

    rows = []
    for (pid, hx, hy), (fga, fgm, fg3a, fg3m) in agg.items():
        floor = MIN_LEAGUE_HEX_FGA if pid == 0 else MIN_PLAYER_HEX_FGA
        if fga < floor:
            continue
        rows.append({
            "PLAYER_ID": pid, "HX": hx, "HY": hy,
            "FGA": fga, "FGM": fgm, "FG3A": fg3a, "FG3M": fg3m,
            "SEASON_TYPE": season_type,
        })
    df = pd.DataFrame(rows).sort_values(["PLAYER_ID", "FGA"], ascending=[True, False])
    out = config.DATA_DIR / f"shot_hex_{season}.csv"
    df.to_csv(out, index=False)
    logger.info("Saved %d hex rows (%d players, %d failed) → %s",
                len(df), df["PLAYER_ID"].nunique() - 1, failed, out)
    return df


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    fetch_shot_detail()
