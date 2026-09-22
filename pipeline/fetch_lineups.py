"""Core lineup data — fetch and merge multi‑measure TeamDashLineups.

Uses ``TeamDashLineups`` (per‑team, 30 calls per combination) instead of
``LeagueDashLineups`` which caps at 2000 rows per request and silently
truncates data for larger group quantities.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from nba_api.stats.endpoints import teamdashlineups

from . import config
from .utils import (
    api_call_with_retry,
    get_all_team_ids,
    get_team_name,
    merge_measure_types,
    pace,
    save_dataframe,
)

logger = logging.getLogger("pipeline.fetch_lineups")


# ---------------------------------------------------------------------------
# Single fetch (per-team)
# ---------------------------------------------------------------------------


def fetch_all_lineups(
    season: str,
    season_type: str,
    group_quantity: int,
    per_mode: str,
    measure_type: str,
) -> Optional[pd.DataFrame]:
    """Fetch lineups for one (season_type, group_quantity, per_mode, measure_type) combo.

    Loops through all 30 teams using ``TeamDashLineups`` and concatenates the
    results.  This avoids the 2000‑row cap imposed by ``LeagueDashLineups``.

    Args:
        season: E.g. ``"2025-26"``.
        season_type: ``"Regular Season"`` or ``"Playoffs"``.
        group_quantity: Number of players in the lineup group (5, 3, or 2).
        per_mode: ``"Totals"`` or ``"Per100Possessions"``.
        measure_type: One of :data:`config.MEASURE_TYPES`.

    Returns:
        A concatenated ``DataFrame`` with lineup rows from all teams,
        or ``None`` on failure.
    """
    logger.info(
        "Fetching lineups: %s | %s | %d-man | %s | %s",
        season,
        season_type,
        group_quantity,
        per_mode,
        measure_type,
    )

    team_ids = get_all_team_ids()
    team_frames: List[pd.DataFrame] = []

    for idx, team_id in enumerate(team_ids, 1):
        team_name = get_team_name(team_id)
        logger.info(
            "  [%d/%d] %s (ID %d) — %s / %d-man / %s / %s",
            idx,
            len(team_ids),
            team_name,
            team_id,
            season_type,
            group_quantity,
            per_mode,
            measure_type,
        )
        try:
            result = api_call_with_retry(
                teamdashlineups.TeamDashLineups,
                params=dict(
                    group_quantity=group_quantity,
                    measure_type_detailed_defense=measure_type,
                    per_mode_detailed=per_mode,
                    season=season,
                    season_type_all_star=season_type,
                    team_id=team_id,
                    last_n_games=0,
                    month=0,
                    opponent_team_id=0,
                    pace_adjust="N",
                    plus_minus="N",
                    period=0,
                    rank="N",
                ),
            )
            # TeamDashLineups returns lineup data in the SECOND DataFrame (index 1)
            df_list = result.get_data_frames()
            if len(df_list) > 1 and not df_list[1].empty:
                team_frames.append(df_list[1])
        except Exception as exc:
            logger.error(
                "Failed to fetch lineups for %s (%s/%s/%d-man/%s/%s): %s",
                team_name,
                season,
                season_type,
                group_quantity,
                per_mode,
                measure_type,
                exc,
            )
        pace()  # respect rate limits between calls

    if not team_frames:
        logger.warning(
            "  → 0 rows for %s / %s / %d-man / %s",
            measure_type,
            season_type,
            group_quantity,
            per_mode,
        )
        return None

    df = pd.concat(team_frames, ignore_index=True)
    logger.info(
        "  → %d rows for %s / %s / %d-man / %s",
        len(df),
        measure_type,
        season_type,
        group_quantity,
        per_mode,
    )
    return df


# ---------------------------------------------------------------------------
# Orchestrator — fetch all combos, merge, and save
# ---------------------------------------------------------------------------


def fetch_and_merge_lineups(season: str = config.SEASON) -> Dict[int, pd.DataFrame]:
    """Fetch, merge, and save lineup data for every configured combination.

    For each ``(season_type, group_quantity, per_mode)`` tuple the function
    fetches all 7 measure types and merges them on ``GROUP_ID``.  The merged
    frames are then concatenated per group quantity (adding metadata columns)
    and written to CSV.

    Args:
        season: Season string, e.g. ``"2025-26"``.

    Returns:
        ``{group_quantity: merged_DataFrame}`` — one entry per group size.
    """
    # Accumulator: group_quantity → list of DataFrames (one per season_type × per_mode)
    accumulators: Dict[int, List[pd.DataFrame]] = {gq: [] for gq in config.GROUP_QUANTITIES}

    total_combos = (
        len(config.SEASON_TYPES)
        * len(config.GROUP_QUANTITIES)
        * len(config.PER_MODES)
    )
    combo_idx = 0

    for season_type in config.SEASON_TYPES:
        for group_quantity in config.GROUP_QUANTITIES:
            for per_mode in config.PER_MODES:
                combo_idx += 1
                logger.info(
                    "— Combo %d/%d: %s | %d-man | %s",
                    combo_idx,
                    total_combos,
                    season_type,
                    group_quantity,
                    per_mode,
                )

                measure_frames: Dict[str, pd.DataFrame] = {}
                for mt in config.MEASURE_TYPES:
                    df = fetch_all_lineups(season, season_type, group_quantity, per_mode, mt)
                    if df is not None and not df.empty:
                        measure_frames[mt] = df
                    pace()  # respect rate limits between calls

                if not measure_frames:
                    logger.warning(
                        "No data for combo %s/%d-man/%s — skipping.",
                        season_type,
                        group_quantity,
                        per_mode,
                    )
                    continue

                merged = merge_measure_types(measure_frames, merge_key="GROUP_ID")
                if merged.empty:
                    continue

                # Add metadata columns
                merged["SEASON_TYPE"] = season_type
                merged["GROUP_QUANTITY"] = group_quantity
                merged["PER_MODE"] = per_mode

                # Add team full name from TEAM_ID
                if "TEAM_ID" in merged.columns:
                    merged["team"] = merged["TEAM_ID"].apply(get_team_name)

                # Build a clean player list from GROUP_NAME
                if "GROUP_NAME" in merged.columns:
                    merged["players_list"] = (
                        merged["GROUP_NAME"]
                        .fillna("")
                        .str.split(" - ")
                    )

                accumulators[group_quantity].append(merged)

                # Slightly longer pause between different combos
                time.sleep(config.API_ENDPOINT_DELAY)

    # ---- Concatenate and save per group quantity ----
    results: Dict[int, pd.DataFrame] = {}

    for gq, frames in accumulators.items():
        if not frames:
            logger.warning("No data collected for %d-man lineups.", gq)
            continue

        combined = pd.concat(frames, ignore_index=True)

        # Sort for readability
        sort_cols = [c for c in ["team", "SEASON_TYPE", "PER_MODE", "MIN"] if c in combined.columns]
        if sort_cols:
            ascending = [True] * (len(sort_cols) - 1) + [False] if "MIN" in sort_cols else [True] * len(sort_cols)
            combined = combined.sort_values(by=sort_cols, ascending=ascending)

        filepath = config.DATA_DIR / f"lineups_{gq}man_{season}.csv"
        save_dataframe(combined, filepath)
        results[gq] = combined
        logger.info(
            "✓ %d-man lineups: %d rows × %d cols → %s",
            gq,
            len(combined),
            len(combined.columns),
            filepath,
        )

    return results


# ---------------------------------------------------------------------------
# Legacy 5-man file — the /dashboard 5-man table
# ---------------------------------------------------------------------------
#
# ``data/NBALineup<YYYYYY>_RegSeason_Playoffs_BaseAdvanced.csv`` is the file the
# frontend reads for the current season's 5-man dashboard, and whose existence
# (with ``player_index``) tells it a new season has started. Until 2026-09 a
# Railway cron produced it with the standalone ``fetchlineups.py``; this is that
# script ported into the pipeline so the residential publisher makes it too.
#
# It is deliberately *light*: 5-man × Totals × Base+Advanced × (Regular Season,
# Playoffs) = 120 ``TeamDashLineups`` calls, a few minutes — versus the ~2,000
# calls of the full lineup fetch above, which is why the weekly job can afford
# to run it every time.
#
# The output contract is kept byte-for-byte compatible with the original
# script — same column set and order, ``team`` as the full name, ``players_list``
# as a Python-list repr, deterministic row order — so an unchanged season
# produces an identical file and the publisher commits nothing.

LEGACY_MEASURE_TYPES: List[str] = ["Base", "Advanced"]
LEGACY_GROUP_QUANTITY: int = 5
LEGACY_PER_MODE: str = "Totals"


def legacy_lineups_path(season: str = config.SEASON) -> Path:
    """``data/NBALineup202627_RegSeason_Playoffs_BaseAdvanced.csv`` for ``"2026-27"``."""
    return config.DATA_DIR / f"NBALineup{season.replace('-', '')}_RegSeason_Playoffs_BaseAdvanced.csv"


def _fetch_team_legacy(
    season: str,
    season_type: str,
    team_id: int,
    measure_type: str,
) -> Optional[pd.DataFrame]:
    """One ``TeamDashLineups`` call (5-man, Totals) for one team.

    Returns the lineup frame (result set index 1) with ``SEASON_TYPE`` appended,
    or ``None`` when the call failed after retries or returned no rows (e.g. a
    team that missed the playoffs).
    """
    try:
        result = api_call_with_retry(
            teamdashlineups.TeamDashLineups,
            params=dict(
                group_quantity=LEGACY_GROUP_QUANTITY,
                measure_type_detailed_defense=measure_type,
                per_mode_detailed=LEGACY_PER_MODE,
                season=season,
                season_type_all_star=season_type,
                team_id=team_id,
                last_n_games=0,
                month=0,
                opponent_team_id=0,
                pace_adjust="N",
                plus_minus="N",
                period=0,
                rank="N",
            ),
        )
    except Exception as exc:
        logger.error(
            "Legacy lineups: %s / %s / %s failed for team %d: %s",
            season, season_type, measure_type, team_id, exc,
        )
        return None

    df_list = result.get_data_frames()
    if len(df_list) < 2 or df_list[1].empty:
        return None
    df = df_list[1].copy()
    df["SEASON_TYPE"] = season_type
    return df


def merge_legacy_team(
    base: Optional[pd.DataFrame],
    advanced: Optional[pd.DataFrame],
    team_name: str,
    team_id: int,
) -> Optional[pd.DataFrame]:
    """Merge one team/season-type's Base and Advanced frames the legacy way.

    Pure (no I/O) so it can be tested offline. Advanced contributes only the
    columns Base lacks, **in the API's own column order** (a set difference here
    once made the order random per process and every run rewrote the whole
    file); the join is an inner join on ``GROUP_ID``. If Advanced is missing
    the Base frame is used alone, matching the original script. Returns ``None``
    when there is nothing usable.
    """
    if base is None or base.empty:
        return None

    if advanced is None or advanced.empty:
        logger.warning("  %s: Advanced stats missing — using Base stats only.", team_name)
        merged = base.copy()
    else:
        base_cols = set(base.columns) - {"SEASON_TYPE"}
        adv_unique = [c for c in advanced.columns if c not in base_cols and c != "SEASON_TYPE"]
        merged = pd.merge(
            base,
            advanced[["GROUP_ID"] + adv_unique],
            on="GROUP_ID",
            how="inner",
            suffixes=("", "_adv"),
        )
        if merged.empty:
            logger.warning("  %s: no common lineups between Base and Advanced — skipped.", team_name)
            return None

    merged["team"] = team_name
    merged["team_id"] = team_id
    return merged


def finalize_legacy_lineups(frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate per-team frames into the published legacy table.

    Adds ``players_list`` (split from ``GROUP_NAME``) and applies the
    deterministic sort: team, season type, minutes descending, ``GROUP_ID`` as
    the tie-break, stable mergesort — so identical data yields an identical file.
    """
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["players_list"] = df["GROUP_NAME"].fillna("").str.split(" - ")
    return df.sort_values(
        by=["team", "SEASON_TYPE", "MIN", "GROUP_ID"],
        ascending=[True, True, False, True],
        kind="mergesort",
    )


def fetch_legacy_lineups(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Refresh the legacy 5-man lineup CSV for *season* (~120 API calls).

    Writes :func:`legacy_lineups_path`. The file is left untouched when the
    fetch returns nothing (pre-season, or the API is unreachable) and when it
    would cover *fewer* teams than the file already on disk — a partial fetch
    must never replace a complete table on the dashboard.

    Returns:
        The written ``DataFrame``, or ``None`` if nothing was written.
    """
    team_ids = get_all_team_ids()
    n_calls = len(team_ids) * len(config.SEASON_TYPES) * len(LEGACY_MEASURE_TYPES)
    logger.info(
        "Legacy 5-man lineups for %s: %d teams × %s × %s = %d calls",
        season, len(team_ids), "/".join(config.SEASON_TYPES), "+".join(LEGACY_MEASURE_TYPES), n_calls,
    )

    frames: List[pd.DataFrame] = []
    for idx, team_id in enumerate(team_ids, 1):
        team_name = get_team_name(team_id)
        for season_type in config.SEASON_TYPES:
            parts: Dict[str, Optional[pd.DataFrame]] = {}
            for measure_type in LEGACY_MEASURE_TYPES:
                parts[measure_type] = _fetch_team_legacy(season, season_type, team_id, measure_type)
                pace()  # respect rate limits between calls
            merged = merge_legacy_team(parts["Base"], parts["Advanced"], team_name, team_id)
            if merged is None:
                logger.info("  [%d/%d] %s — %s: no rows", idx, len(team_ids), team_name, season_type)
            else:
                frames.append(merged)
                logger.info("  [%d/%d] %s — %s: %d lineups", idx, len(team_ids), team_name, season_type, len(merged))

    df = finalize_legacy_lineups(frames)
    if df.empty:
        logger.warning("Legacy lineups: no rows for %s — existing file left untouched.", season)
        return None

    path = legacy_lineups_path(season)
    teams_now = int(df["team"].nunique())
    if path.exists():
        try:
            teams_before = int(pd.read_csv(path, usecols=["team"])["team"].nunique())
        except Exception:  # unreadable/odd file — overwrite it
            teams_before = 0
        if teams_now < teams_before:
            logger.error(
                "Legacy lineups: fetched %d teams but %s already covers %d — "
                "refusing to overwrite a complete table with a partial one.",
                teams_now, path.name, teams_before,
            )
            return None

    logger.info("Legacy lineups: %d rows from %d/%d teams", len(df), teams_now, len(team_ids))
    save_dataframe(df, path)
    return df
