"""Slim, web-ready lineup exports.

The full ``lineups_{2,3,5}man_<season>.csv`` files are huge (200+ columns, both
per-modes, every micro-sample lineup) — far too heavy for a browser to fetch and
parse. This module produces trimmed CSVs the frontend can load directly:

  * keep only ``PER_MODE == "Totals"`` (one row per lineup per season type),
  * drop lineups below a minutes floor (low-minute combos are pure noise),
  * keep only the ~40 columns the UI actually reads,
  * reconstruct team + players_list (the ``TeamDashLineups`` lineup frame carries
    neither a team column nor a clean player list).

Team reconstruction: 2/3-man ``GROUP_ID`` values are player-id concatenations
(e.g. ``-1630194-1641732-``). We map player ids -> team using the on/off CSV
(``VS_PLAYER_ID`` -> ``TEAM_ABBREVIATION``) and take the team common to a
lineup's players.

Run standalone:  ``python -m pipeline.export_web --season 2025-26 --min-minutes 100``
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from . import config
from .utils import save_dataframe, setup_logging

logger = logging.getLogger("pipeline.export_web")

# Group sizes we publish slim files for (5-man already ships via the legacy CSV).
SLIM_GROUP_QUANTITIES: List[int] = [2, 3]

# Columns the frontend reads — kept in a legacy-compatible order. Any that are
# absent in a source file are simply skipped.
SLIM_COLUMNS: List[str] = [
    "GROUP_SET", "GROUP_ID", "GROUP_NAME",
    "GP", "W", "L", "W_PCT", "MIN", "PLUS_MINUS",
    "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT",
    "OREB", "DREB", "REB", "AST", "TOV", "STL", "BLK", "PTS",
    "OFF_RATING", "DEF_RATING", "NET_RATING", "PACE", "POSS",
    "EFG_PCT", "TS_PCT", "AST_RATIO", "AST_PCT", "AST_TO",
    "OREB_PCT", "DREB_PCT", "REB_PCT", "TM_TOV_PCT", "PIE",
    "SEASON_TYPE", "team", "team_id", "players_list",
]

TeamKey = Tuple[str, int]  # (abbreviation, team_id)

# Curated columns for the pre-joined player index (the /players table). Drawn
# from player_stats (Base+Advanced) + estimated_metrics + on/off swing. Any that
# are absent in the merged frame are simply skipped.
PLAYER_INDEX_COLUMNS: List[str] = [
    "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "TEAM_ABBREVIATION", "AGE", "SEASON_TYPE",
    "GP", "MIN", "W_PCT",
    "PTS", "REB", "OREB", "DREB", "AST", "STL", "BLK", "TOV", "PF",
    "FG_PCT", "FG3_PCT", "FT_PCT", "EFG_PCT", "TS_PCT",
    "USG_PCT", "AST_PCT", "AST_TO", "AST_RATIO", "TM_TOV_PCT",
    "OREB_PCT", "DREB_PCT", "REB_PCT",
    "OFF_RATING", "DEF_RATING", "NET_RATING", "PACE", "PIE", "PLUS_MINUS",
    "E_OFF_RATING", "E_DEF_RATING", "E_NET_RATING", "E_USG_PCT",
    "ON_NET_RATING", "OFF_NET_RATING", "NET_SWING",
    "CLUTCH_NET_RATING", "CLUTCH_MIN",
    "OBPM", "DBPM", "BPM", "VORP",
    "XEFG", "SHOTMAKING_OVER_XEFG", "XEFG_FGA",
]


def build_player_team_map(season: str) -> Dict[int, Set[TeamKey]]:
    """Map player id -> set of (team_abbrev, team_id) from the on/off CSV.

    A traded player maps to more than one team, hence a set.
    """
    path = config.DATA_DIR / f"on_off_{season}.csv"
    if not path.exists():
        logger.warning("on/off file not found (%s) — cannot reconstruct team.", path)
        return {}

    df = pd.read_csv(path)
    mapping: Dict[int, Set[TeamKey]] = {}
    for pid, abbr, tid in zip(df["VS_PLAYER_ID"], df["TEAM_ABBREVIATION"], df["TEAM_ID"]):
        try:
            mapping.setdefault(int(pid), set()).add((str(abbr), int(tid)))
        except (TypeError, ValueError):
            continue
    logger.info("Player→team map: %d players from %s", len(mapping), path.name)
    return mapping


def _player_ids(group_id: str) -> List[int]:
    """Parse player ids out of a GROUP_ID like ``-1630194-1641732-``."""
    out: List[int] = []
    for part in str(group_id).split("-"):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


def _derive_team(group_id: str, pmap: Dict[int, Set[TeamKey]]) -> TeamKey:
    """Best team for a lineup: the team common to its players, else the modal team."""
    sets = [pmap[pid] for pid in _player_ids(group_id) if pid in pmap]
    if not sets:
        return (None, None)  # type: ignore[return-value]
    common = set.intersection(*sets) if len(sets) > 1 else sets[0]
    if len(common) == 1:
        return next(iter(common))
    counts: Counter = Counter(team for s in sets for team in s)
    return counts.most_common(1)[0][0] if counts else (None, None)  # type: ignore[return-value]


def slim_one(season: str, group_quantity: int, min_minutes: float, pmap: Dict[int, Set[TeamKey]]) -> Optional[Path]:
    """Produce one ``lineups_slim_{gq}man_<season>.csv``. Returns the path or None."""
    src = config.DATA_DIR / f"lineups_{group_quantity}man_{season}.csv"
    if not src.exists():
        logger.warning("Source not found: %s — skipping %d-man.", src, group_quantity)
        return None

    df = pd.read_csv(src, low_memory=False)

    if "PER_MODE" in df.columns:
        df = df[df["PER_MODE"] == "Totals"]
    df = df[pd.to_numeric(df["MIN"], errors="coerce").fillna(0) >= min_minutes].copy()

    # Reconstruct team (abbrev) + team_id from the player ids in GROUP_ID.
    abbrevs, team_ids = [], []
    for gid in df["GROUP_ID"]:
        abbr, tid = _derive_team(gid, pmap)
        abbrevs.append(abbr)
        team_ids.append(tid)
    df["team"] = abbrevs
    df["team_id"] = team_ids

    before = len(df)
    df = df[df["team"].notna()]
    dropped = before - len(df)
    if dropped:
        logger.warning("  %d %d-man rows dropped (no team match).", dropped, group_quantity)

    # Clean player list from GROUP_NAME, stored as JSON for the frontend.
    df["players_list"] = (
        df["GROUP_NAME"].fillna("").apply(
            lambda s: json.dumps([p.strip() for p in str(s).split(" - ") if p.strip()])
        )
    )

    cols = [c for c in SLIM_COLUMNS if c in df.columns]
    out = df[cols].sort_values("MIN", ascending=False)

    dest = config.DATA_DIR / f"lineups_slim_{group_quantity}man_{season}.csv"
    save_dataframe(out, dest)
    size_kb = dest.stat().st_size / 1024
    logger.info(
        "✓ %d-man slim: %d rows × %d cols → %s (%.0f KB)",
        group_quantity, len(out), len(cols), dest.name, size_kb,
    )
    return dest


def export_slim(season: str = config.SEASON, min_minutes: float = 100.0) -> List[Path]:
    """Produce slim 2/3-man lineup files for the frontend. Safe no-op if sources are missing."""
    logger.info("Building slim lineup exports (season %s, MIN ≥ %g)…", season, min_minutes)
    pmap = build_player_team_map(season)
    written: List[Path] = []
    for gq in SLIM_GROUP_QUANTITIES:
        dest = slim_one(season, gq, min_minutes, pmap)
        if dest:
            written.append(dest)
    return written


def _onoff_swing(season: str) -> Optional[pd.DataFrame]:
    """Per-player on/off net-rating swing (ΔNET) from the on/off CSV.

    For traded players we use the team where they logged the most on-court MIN.
    Returns a frame keyed by ``(PLAYER_ID, SEASON_TYPE)`` with ON/OFF/SWING, or
    ``None`` if the on/off file is missing.
    """
    path = config.DATA_DIR / f"on_off_{season}.csv"
    if not path.exists():
        logger.warning("on/off file not found (%s) — NET_SWING omitted.", path)
        return None

    df = pd.read_csv(path, low_memory=False)
    for col in ("MIN", "NET_RATING"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    rows: List[dict] = []
    for (pid, stype), grp in df.groupby(["VS_PLAYER_ID", "SEASON_TYPE"]):
        on = grp[grp["COURT_STATUS"] == "On"]
        off = grp[grp["COURT_STATUS"] == "Off"]
        if on.empty or on["MIN"].isna().all():
            continue
        on_row = on.loc[on["MIN"].idxmax()]  # the player's primary team
        team_off = off[off["TEAM_ID"] == on_row["TEAM_ID"]]
        on_net = on_row["NET_RATING"]
        off_net = team_off["NET_RATING"].iloc[0] if not team_off.empty else None
        has_off = off_net is not None and pd.notna(off_net)
        swing = on_net - off_net if pd.notna(on_net) and has_off else None
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            continue
        rows.append({
            "PLAYER_ID": pid_int,
            "SEASON_TYPE": stype,
            "ON_NET_RATING": round(float(on_net), 1) if pd.notna(on_net) else None,
            "OFF_NET_RATING": round(float(off_net), 1) if has_off else None,
            "NET_SWING": round(float(swing), 1) if swing is not None else None,
        })
    return pd.DataFrame(rows)


def export_player_index(season: str = config.SEASON) -> Optional[Path]:
    """Pre-join per-player stats (which already include the estimated E_* metrics
    from the Advanced measure) with on/off net swing into one slim table the
    /players page loads directly. Safe no-op if player_stats is missing.
    """
    logger.info("Building player index (season %s)…", season)
    stats_path = config.DATA_DIR / f"player_stats_{season}.csv"
    if not stats_path.exists():
        logger.warning("player_stats not found (%s) — skipping player index.", stats_path)
        return None

    df = pd.read_csv(stats_path, low_memory=False)

    # Note: the estimated impact metrics (E_OFF/DEF/NET/USG…) already ship inside
    # player_stats via the Advanced measure, so we do NOT merge estimated_metrics
    # here — doing so only collides those columns into _x/_y suffixes and drops them.

    # On/off net swing.
    swing = _onoff_swing(season)
    if swing is not None and not swing.empty:
        df = df.merge(swing, on=["PLAYER_ID", "SEASON_TYPE"], how="left")

    # Clutch on-court net rating (+ clutch minutes) for the leverage split.
    clutch_path = config.DATA_DIR / f"player_clutch_{season}.csv"
    if clutch_path.exists():
        cl = pd.read_csv(clutch_path, low_memory=False)
        if {"PLAYER_ID", "SEASON_TYPE", "NET_RATING", "MIN"}.issubset(cl.columns):
            cl = cl[["PLAYER_ID", "SEASON_TYPE", "NET_RATING", "MIN"]].rename(
                columns={"NET_RATING": "CLUTCH_NET_RATING", "MIN": "CLUTCH_MIN"}
            )
            df = df.merge(cl, on=["PLAYER_ID", "SEASON_TYPE"], how="left")

    # Box Plus/Minus (BPM 2.0) + VORP — computed offline from the box stats above
    # plus team pace/rating context. No new API calls.
    team_path = config.DATA_DIR / f"team_stats_{season}.csv"
    if team_path.exists():
        from .compute_impact import compute_bpm_vorp

        bpm = compute_bpm_vorp(df, pd.read_csv(team_path, low_memory=False))
        if not bpm.empty:
            df = df.merge(bpm, on=["PLAYER_ID", "SEASON_TYPE"], how="left")

    # Shot-making over expected (xeFG by shot location) from shot_zones.
    sz_path = config.DATA_DIR / f"shot_zones_{season}.csv"
    if sz_path.exists():
        from .compute_impact import compute_shotmaking

        sm = compute_shotmaking(pd.read_csv(sz_path, low_memory=False))
        if not sm.empty:
            df = df.merge(sm, on=["PLAYER_ID", "SEASON_TYPE"], how="left")

    cols = [c for c in PLAYER_INDEX_COLUMNS if c in df.columns]
    out = df[cols].copy()
    if "MIN" in out.columns:
        out = out.sort_values("MIN", ascending=False)

    dest = config.DATA_DIR / f"player_index_{season}.csv"
    save_dataframe(out, dest)
    size_kb = dest.stat().st_size / 1024
    logger.info(
        "✓ Player index: %d rows × %d cols → %s (%.0f KB)",
        len(out), len(cols), dest.name, size_kb,
    )
    return dest


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build slim web-ready lineup CSVs.")
    parser.add_argument("--season", default=config.SEASON)
    parser.add_argument("--min-minutes", type=float, default=100.0)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    setup_logging()
    export_slim(season=args.season, min_minutes=args.min_minutes)
    export_player_index(season=args.season)
