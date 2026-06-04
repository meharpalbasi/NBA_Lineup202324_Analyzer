"""Supplementary data fetchers — on/off, clutch, play‑types, hustle, tracking.

Each public function in this module fetches one category of supplementary data,
writes it to CSV, and returns the resulting DataFrame (or ``None`` on failure).
All functions use :func:`utils.api_call_with_retry` for resilient HTTP calls.
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional

import pandas as pd

from . import config
from .utils import (
    api_call_with_retry,
    get_all_team_ids,
    get_team_name,
    merge_measure_types,
    pace,
    save_dataframe,
)

logger = logging.getLogger("pipeline.fetch_supplementary")


# =========================================================================
# 1. On / Off court summary
# =========================================================================


def fetch_on_off(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch on/off court player stats for all 30 teams.

    Uses ``TeamPlayerOnOffSummary`` which requires a ``team_id``, so we loop
    through every team.

    Args:
        season: NBA season string.

    Returns:
        Combined on/off DataFrame, or ``None`` on total failure.
    """
    from nba_api.stats.endpoints import teamplayeronoffsummary

    logger.info("Fetching on/off court data for season %s …", season)
    all_frames: List[pd.DataFrame] = []
    team_ids = get_all_team_ids()

    for idx, team_id in enumerate(team_ids, 1):
        team_name = get_team_name(team_id)
        logger.info("  [%d/%d] %s (ID %d)", idx, len(team_ids), team_name, team_id)

        for season_type in config.SEASON_TYPES:
            try:
                result = api_call_with_retry(
                    teamplayeronoffsummary.TeamPlayerOnOffSummary,
                    params=dict(
                        team_id=team_id,
                        season=season,
                        season_type_all_star=season_type,
                        measure_type_detailed_defense="Base",
                        per_mode_detailed="Totals",
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

                # dfs[1] = PlayersOnCourt, dfs[2] = PlayersOffCourt
                if len(dfs) >= 3:
                    on_court = dfs[1].copy()
                    off_court = dfs[2].copy()

                    on_court["COURT_STATUS"] = "On"
                    off_court["COURT_STATUS"] = "Off"

                    combined = pd.concat([on_court, off_court], ignore_index=True)
                    combined["team"] = team_name
                    combined["SEASON_TYPE"] = season_type
                    all_frames.append(combined)

            except Exception as exc:
                logger.error(
                    "Failed on/off for %s (%s): %s", team_name, season_type, exc
                )

            pace()

        # Longer pause between teams
        time.sleep(config.API_ENDPOINT_DELAY)

    # Filter out empty frames to avoid FutureWarning on concat
    all_frames = [f for f in all_frames if not f.empty]
    if not all_frames:
        logger.error("No on/off data collected.")
        return None

    df = pd.concat(all_frames, ignore_index=True)
    filepath = config.DATA_DIR / f"on_off_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ On/off data: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 2. Clutch stats
# =========================================================================


def fetch_clutch(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch league‑wide team clutch stats (last 5 min, within 5 pts).

    Args:
        season: NBA season string.

    Returns:
        Clutch DataFrame or ``None``.
    """
    from nba_api.stats.endpoints import leaguedashteamclutch

    logger.info("Fetching clutch data for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        try:
            result = api_call_with_retry(
                leaguedashteamclutch.LeagueDashTeamClutch,
                params=dict(
                    season=season,
                    season_type_all_star=season_type,
                    measure_type_detailed_defense="Base",
                    per_mode_detailed="Totals",
                    ahead_behind="Ahead or Behind",
                    clutch_time="Last 5 Minutes",
                    point_diff=5,
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
                df["SEASON_TYPE"] = season_type
                frames.append(df)
                logger.info("  %s: %d rows", season_type, len(df))
        except Exception as exc:
            logger.error("Failed clutch (%s): %s", season_type, exc)

        pace()

    # Filter out empty frames to avoid FutureWarning on concat
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No clutch data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"clutch_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Clutch data: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 3. Synergy play types
# =========================================================================


def fetch_play_types(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch Synergy play‑type data for all play types, offense & defense, teams & players.

    Args:
        season: NBA season string.

    Returns:
        Play‑type DataFrame or ``None``.
    """
    from nba_api.stats.endpoints import synergyplaytypes

    logger.info("Fetching play‑type data for season %s …", season)
    frames: List[pd.DataFrame] = []

    type_groupings = ["Offensive", "Defensive"]
    player_or_team_values = [("T", "Team"), ("P", "Player")]

    total = (
        len(config.SYNERGY_PLAY_TYPES)
        * len(type_groupings)
        * len(player_or_team_values)
        * len(config.SEASON_TYPES)
    )
    call_idx = 0

    for season_type in config.SEASON_TYPES:
        for play_type in config.SYNERGY_PLAY_TYPES:
            for tg in type_groupings:
                for pt_abbr, pt_label in player_or_team_values:
                    call_idx += 1
                    logger.info(
                        "  [%d/%d] %s | %s | %s | %s | %s",
                        call_idx,
                        total,
                        season_type,
                        play_type,
                        tg,
                        pt_label,
                        season,
                    )
                    try:
                        result = api_call_with_retry(
                            synergyplaytypes.SynergyPlayTypes,
                            params=dict(
                                season=season,
                                season_type_all_star=season_type,
                                play_type_nullable=play_type,
                                type_grouping_nullable=tg,
                                player_or_team_abbreviation=pt_abbr,
                                per_mode_simple="Totals",
                                league_id="00",
                            ),
                        )
                        dfs = result.get_data_frames()
                        if dfs and not dfs[0].empty:
                            df = dfs[0]
                            df["PLAY_TYPE"] = play_type
                            df["TYPE_GROUPING"] = tg
                            df["PLAYER_OR_TEAM"] = pt_label
                            df["SEASON_TYPE"] = season_type
                            frames.append(df)
                    except Exception as exc:
                        logger.error(
                            "Failed play‑type %s/%s/%s/%s: %s",
                            play_type,
                            tg,
                            pt_label,
                            season_type,
                            exc,
                        )
                    pace()

    # Filter out empty frames to avoid FutureWarning on concat
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No play‑type data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"play_types_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Play‑type data: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 4. Hustle stats
# =========================================================================


def fetch_hustle(
    season: str = config.SEASON,
) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Fetch league hustle stats for players and teams.

    Args:
        season: NBA season string.

    Returns:
        ``(player_df, team_df)`` — either may be ``None`` on failure.
    """
    from nba_api.stats.endpoints import leaguehustlestatsplayer, leaguehustlestatsteam

    logger.info("Fetching hustle stats for season %s …", season)
    player_frames: List[pd.DataFrame] = []
    team_frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        # Players
        try:
            result = api_call_with_retry(
                leaguehustlestatsplayer.LeagueHustleStatsPlayer,
                params=dict(
                    season=season,
                    season_type_all_star=season_type,
                    per_mode_time="Totals",
                ),
            )
            dfs = result.get_data_frames()
            if dfs and not dfs[0].empty:
                df = dfs[0]
                df["SEASON_TYPE"] = season_type
                player_frames.append(df)
                logger.info("  Hustle players (%s): %d rows", season_type, len(df))
        except Exception as exc:
            logger.error("Failed hustle players (%s): %s", season_type, exc)
        pace()

        # Teams
        try:
            result = api_call_with_retry(
                leaguehustlestatsteam.LeagueHustleStatsTeam,
                params=dict(
                    season=season,
                    season_type_all_star=season_type,
                    per_mode_time="Totals",
                ),
            )
            dfs = result.get_data_frames()
            if dfs and not dfs[0].empty:
                df = dfs[0]
                df["SEASON_TYPE"] = season_type
                team_frames.append(df)
                logger.info("  Hustle teams (%s): %d rows", season_type, len(df))
        except Exception as exc:
            logger.error("Failed hustle teams (%s): %s", season_type, exc)
        pace()

    player_df: Optional[pd.DataFrame] = None
    team_df: Optional[pd.DataFrame] = None

    # Filter out empty frames to avoid FutureWarning on concat
    player_frames = [f for f in player_frames if not f.empty]
    team_frames = [f for f in team_frames if not f.empty]

    if player_frames:
        player_df = pd.concat(player_frames, ignore_index=True)
        save_dataframe(player_df, config.DATA_DIR / f"hustle_players_{season}.csv")
        logger.info("✓ Hustle players: %d rows", len(player_df))

    if team_frames:
        team_df = pd.concat(team_frames, ignore_index=True)
        save_dataframe(team_df, config.DATA_DIR / f"hustle_teams_{season}.csv")
        logger.info("✓ Hustle teams: %d rows", len(team_df))

    if player_df is None and team_df is None:
        logger.error("No hustle data collected.")

    return player_df, team_df


# =========================================================================
# 5. Player‑tracking stats (LeagueDashPtStats)
# =========================================================================


def fetch_tracking(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch player‑tracking stats for all 12 measure types, Player & Team level.

    Args:
        season: NBA season string.

    Returns:
        Concatenated tracking DataFrame or ``None``.
    """
    from nba_api.stats.endpoints import leaguedashptstats

    logger.info("Fetching tracking stats for season %s …", season)
    frames: List[pd.DataFrame] = []

    player_or_team_values = [("Player", "Player"), ("Team", "Team")]
    total = (
        len(config.PT_MEASURE_TYPES)
        * len(player_or_team_values)
        * len(config.SEASON_TYPES)
    )
    call_idx = 0

    for season_type in config.SEASON_TYPES:
        for pt_measure in config.PT_MEASURE_TYPES:
            for pot_param, pot_label in player_or_team_values:
                call_idx += 1
                logger.info(
                    "  [%d/%d] %s | %s | %s", call_idx, total, pt_measure, pot_label, season_type
                )
                try:
                    result = api_call_with_retry(
                        leaguedashptstats.LeagueDashPtStats,
                        params=dict(
                            season=season,
                            season_type_all_star=season_type,
                            pt_measure_type=pt_measure,
                            player_or_team=pot_param,
                            per_mode_simple="Totals",
                            last_n_games=0,
                            month=0,
                            opponent_team_id=0,
                        ),
                    )
                    dfs = result.get_data_frames()
                    if dfs and not dfs[0].empty:
                        df = dfs[0]
                        df["PT_MEASURE_TYPE"] = pt_measure
                        df["PLAYER_OR_TEAM"] = pot_label
                        df["SEASON_TYPE"] = season_type
                        frames.append(df)
                except Exception as exc:
                    logger.error(
                        "Failed tracking %s/%s/%s: %s", pt_measure, pot_label, season_type, exc
                    )
                pace()

    # Filter out empty frames to avoid FutureWarning on concat
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No tracking data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"tracking_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Tracking data: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 6. Defense tracking (LeagueDashPtDefend)
# =========================================================================


def fetch_defense_tracking(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch defense tracking data for all 6 defense categories.

    Args:
        season: NBA season string.

    Returns:
        Concatenated defense‑tracking DataFrame or ``None``.
    """
    from nba_api.stats.endpoints import leaguedashptdefend

    logger.info("Fetching defense tracking for season %s …", season)
    frames: List[pd.DataFrame] = []

    total = len(config.DEFENSE_CATEGORIES) * len(config.SEASON_TYPES)
    call_idx = 0

    for season_type in config.SEASON_TYPES:
        for category in config.DEFENSE_CATEGORIES:
            call_idx += 1
            logger.info("  [%d/%d] %s | %s", call_idx, total, category, season_type)
            try:
                result = api_call_with_retry(
                    leaguedashptdefend.LeagueDashPtDefend,
                    params=dict(
                        season=season,
                        season_type_all_star=season_type,
                        defense_category=category,
                        per_mode_simple="Totals",
                        league_id="00",
                    ),
                )
                dfs = result.get_data_frames()
                if dfs and not dfs[0].empty:
                    df = dfs[0]
                    df["DEFENSE_CATEGORY"] = category
                    df["SEASON_TYPE"] = season_type
                    frames.append(df)
            except Exception as exc:
                logger.error("Failed defense tracking %s/%s: %s", category, season_type, exc)
            pace()

    # Filter out empty frames to avoid FutureWarning on concat
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No defense tracking data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"defense_tracking_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Defense tracking: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 7. Estimated metrics
# =========================================================================


def fetch_estimated_metrics(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch player estimated advanced metrics.

    Args:
        season: NBA season string.

    Returns:
        Estimated‑metrics DataFrame or ``None``.
    """
    from nba_api.stats.endpoints import playerestimatedmetrics

    logger.info("Fetching estimated metrics for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        try:
            result = api_call_with_retry(
                playerestimatedmetrics.PlayerEstimatedMetrics,
                params=dict(
                    season=season,
                    season_type=season_type,
                    league_id="00",
                ),
            )
            dfs = result.get_data_frames()
            if dfs and not dfs[0].empty:
                df = dfs[0]
                df["SEASON_TYPE"] = season_type
                frames.append(df)
                logger.info("  Estimated metrics (%s): %d rows", season_type, len(df))
        except Exception as exc:
            logger.error("Failed estimated metrics (%s): %s", season_type, exc)
        pace()

    # Filter out empty frames to avoid FutureWarning on concat
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No estimated metrics collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"estimated_metrics_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Estimated metrics: %d rows → %s", len(df), filepath)
    return df


# =========================================================================
# 8. League player stats (LeagueDashPlayerStats — Base + Advanced)
# =========================================================================


def fetch_player_stats(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch league-wide per-player stats, Base + Advanced merged into one wide row.

    This is the classic per-game stat line (PTS/REB/AST/…) plus advanced rates
    (TS%, USG%, OFF/DEF/NET rating, PIE) — one row per (PLAYER_ID, SEASON_TYPE).
    It is the spine of the /players table and of the pre-joined player_index.

    Args:
        season: NBA season string.

    Returns:
        Combined player-stats DataFrame, or ``None`` on total failure.
    """
    from nba_api.stats.endpoints import leaguedashplayerstats

    logger.info("Fetching player stats for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        by_measure = {}
        for measure in config.PLAYER_STATS_MEASURE_TYPES:
            try:
                result = api_call_with_retry(
                    leaguedashplayerstats.LeagueDashPlayerStats,
                    params=dict(
                        season=season,
                        season_type_all_star=season_type,
                        measure_type_detailed_defense=measure,
                        per_mode_detailed=config.LEAGUE_STATS_PER_MODE,
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
                    by_measure[measure] = dfs[0]
                    logger.info("  %s | %s: %d rows", season_type, measure, len(dfs[0]))
            except Exception as exc:
                logger.error("Failed player stats %s/%s: %s", measure, season_type, exc)
            pace()

        if not by_measure:
            continue
        merged = merge_measure_types(by_measure, merge_key="PLAYER_ID")
        if merged.empty:
            continue
        merged["SEASON_TYPE"] = season_type
        frames.append(merged)

    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No player stats collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"player_stats_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Player stats: %d rows × %d cols → %s", len(df), len(df.columns), filepath)
    return df


# =========================================================================
# 9. League team stats (LeagueDashTeamStats — Base + Advanced + Four Factors)
# =========================================================================


def fetch_team_stats(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch league-wide per-team stats, Base + Advanced + Four Factors merged wide.

    One row per (TEAM_ID, SEASON_TYPE), carrying offensive & defensive four
    factors, efficiency ratings, and pace — the inputs for the /teams dashboard.

    Args:
        season: NBA season string.

    Returns:
        Combined team-stats DataFrame, or ``None`` on total failure.
    """
    from nba_api.stats.endpoints import leaguedashteamstats

    logger.info("Fetching team stats for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        by_measure = {}
        for measure in config.TEAM_STATS_MEASURE_TYPES:
            try:
                result = api_call_with_retry(
                    leaguedashteamstats.LeagueDashTeamStats,
                    params=dict(
                        season=season,
                        season_type_all_star=season_type,
                        measure_type_detailed_defense=measure,
                        per_mode_detailed=config.LEAGUE_STATS_PER_MODE,
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
                    by_measure[measure] = dfs[0]
                    logger.info("  %s | %s: %d rows", season_type, measure, len(dfs[0]))
            except Exception as exc:
                logger.error("Failed team stats %s/%s: %s", measure, season_type, exc)
            pace()

        if not by_measure:
            continue
        merged = merge_measure_types(by_measure, merge_key="TEAM_ID")
        if merged.empty:
            continue
        merged["SEASON_TYPE"] = season_type
        frames.append(merged)

    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No team stats collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"team_stats_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Team stats: %d rows × %d cols → %s", len(df), len(df.columns), filepath)
    return df


# =========================================================================
# 10. League player clutch stats (LeagueDashPlayerClutch — Base + Advanced)
# =========================================================================


def fetch_player_clutch(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Fetch per-player stats in clutch time (last 5 min, within 5 pts), Base +
    Advanced merged wide. One row per (PLAYER_ID, SEASON_TYPE).

    The Advanced NET_RATING is the team's net rating per 100 while the player is
    on the court in the clutch; compared to their overall on-court net it's the
    leverage signal — does the player lift or sink the team late?

    Args:
        season: NBA season string.

    Returns:
        Combined player-clutch DataFrame, or ``None`` on total failure.
    """
    from nba_api.stats.endpoints import leaguedashplayerclutch

    logger.info("Fetching player clutch stats for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        by_measure = {}
        for measure in config.PLAYER_STATS_MEASURE_TYPES:
            try:
                result = api_call_with_retry(
                    leaguedashplayerclutch.LeagueDashPlayerClutch,
                    params=dict(
                        season=season,
                        season_type_all_star=season_type,
                        measure_type_detailed_defense=measure,
                        per_mode_detailed="Totals",
                        ahead_behind="Ahead or Behind",
                        clutch_time="Last 5 Minutes",
                        point_diff=5,
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
                    by_measure[measure] = dfs[0]
                    logger.info("  %s | %s: %d rows", season_type, measure, len(dfs[0]))
            except Exception as exc:
                logger.error("Failed player clutch %s/%s: %s", measure, season_type, exc)
            pace()

        if not by_measure:
            continue
        merged = merge_measure_types(by_measure, merge_key="PLAYER_ID")
        if merged.empty:
            continue
        merged["SEASON_TYPE"] = season_type
        frames.append(merged)

    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No player clutch data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"player_clutch_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Player clutch: %d rows × %d cols → %s", len(df), len(df.columns), filepath)
    return df


# =========================================================================
# 11. Player game-by-game logs
# =========================================================================
# Slim set of columns kept from LeagueGameLog — enough for season-trend line
# charts on the profile without bloating the published CSV.
GAME_LOG_COLUMNS = [
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ABBREVIATION",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "PTS",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "FG_PCT",
    "FG3_PCT",
    "FT_PCT",
    "PLUS_MINUS",
]


def fetch_player_game_logs(season: str = config.SEASON) -> Optional[pd.DataFrame]:
    """Per-player game-by-game logs — one ``LeagueGameLog`` call per season type.

    One row per (player, game). Powers the season-trend line charts on the
    player profile (rolling PTS / +/- across the year). Light: a single
    league-wide call returns every player's log, so there is no per-player loop.

    Args:
        season: NBA season string.

    Returns:
        Combined game-log DataFrame, or ``None`` on total failure.
    """
    from nba_api.stats.endpoints import leaguegamelog

    logger.info("Fetching player game logs for season %s …", season)
    frames: List[pd.DataFrame] = []

    for season_type in config.SEASON_TYPES:
        try:
            result = api_call_with_retry(
                leaguegamelog.LeagueGameLog,
                params=dict(
                    season=season,
                    season_type_all_star=season_type,
                    player_or_team_abbreviation="P",
                    sorter="DATE",
                    direction="ASC",
                ),
            )
            dfs = result.get_data_frames()
            if dfs and not dfs[0].empty:
                df = dfs[0]
                keep = [c for c in GAME_LOG_COLUMNS if c in df.columns]
                df = df[keep].copy()
                df["SEASON_TYPE"] = season_type
                frames.append(df)
                logger.info("  %s: %d player-games", season_type, len(df))
        except Exception as exc:
            logger.error("Failed player game logs %s: %s", season_type, exc)
        pace()

    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.error("No player game-log data collected.")
        return None

    df = pd.concat(frames, ignore_index=True)
    filepath = config.DATA_DIR / f"player_game_logs_{season}.csv"
    save_dataframe(df, filepath)
    logger.info("✓ Player game logs: %d rows × %d cols → %s", len(df), len(df.columns), filepath)
    return df
