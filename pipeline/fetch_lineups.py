"""Core lineup data — fetch and merge multi‑measure LeagueDashLineups.

Uses ``LeagueDashLineups`` (league‑wide, one call per combination) instead of
the old team‑by‑team ``TeamDashLineups`` approach — dramatically reducing the
total number of API calls.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import pandas as pd
from nba_api.stats.endpoints import leaguedashlineups

from . import config
from .utils import (
    api_call_with_retry,
    get_team_name,
    merge_measure_types,
    pace,
    save_dataframe,
)

logger = logging.getLogger("pipeline.fetch_lineups")


# ---------------------------------------------------------------------------
# Single fetch
# ---------------------------------------------------------------------------


def fetch_all_lineups(
    season: str,
    season_type: str,
    group_quantity: int,
    per_mode: str,
    measure_type: str,
) -> Optional[pd.DataFrame]:
    """Fetch lineups for one (season_type, group_quantity, per_mode, measure_type) combo.

    A single ``LeagueDashLineups`` call returns data for **all** teams at once.

    Args:
        season: E.g. ``"2025-26"``.
        season_type: ``"Regular Season"`` or ``"Playoffs"``.
        group_quantity: Number of players in the lineup group (5, 3, or 2).
        per_mode: ``"Totals"``, ``"PerGame"``, or ``"Per100Possessions"``.
        measure_type: One of :data:`config.MEASURE_TYPES`.

    Returns:
        A ``DataFrame`` with the lineup rows, or ``None`` on failure.
    """
    logger.info(
        "Fetching lineups: %s | %s | %d-man | %s | %s",
        season,
        season_type,
        group_quantity,
        per_mode,
        measure_type,
    )
    try:
        result = api_call_with_retry(
            leaguedashlineups.LeagueDashLineups,
            params=dict(
                group_quantity=group_quantity,
                measure_type_detailed_defense=measure_type,
                per_mode_detailed=per_mode,
                season=season,
                season_type_all_star=season_type,
                last_n_games=0,
                month=0,
                opponent_team_id=0,
                pace_adjust="N",
                period=0,
                plus_minus="N",
                rank="N",
            ),
        )
        dfs = result.get_data_frames()
        if dfs and not dfs[0].empty:
            df = dfs[0]
            logger.info(
                "  → %d rows for %s / %s / %d-man / %s",
                len(df),
                measure_type,
                season_type,
                group_quantity,
                per_mode,
            )
            return df
        logger.warning(
            "  → 0 rows for %s / %s / %d-man / %s",
            measure_type,
            season_type,
            group_quantity,
            per_mode,
        )
        return None

    except Exception as exc:
        logger.error(
            "Failed to fetch lineups (%s/%s/%d-man/%s/%s): %s",
            season,
            season_type,
            group_quantity,
            per_mode,
            measure_type,
            exc,
        )
        return None


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
