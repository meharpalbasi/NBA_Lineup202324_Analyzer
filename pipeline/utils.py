"""Shared utilities — retry logic, merge helpers, health check, logging.

Every module in the pipeline imports from here instead of rolling its own
retry / save / logging boiler‑plate.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from functools import reduce
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import pandas as pd

from . import config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the root pipeline logger.

    Call once at startup; every subsequent ``logging.getLogger("pipeline.*")``
    call will inherit the handler.

    Args:
        level: Logging level (default ``INFO``).

    Returns:
        The ``pipeline`` logger instance.
    """
    logger = logging.getLogger("pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


logger = setup_logging()

# ---------------------------------------------------------------------------
# Global API‑call counter
# ---------------------------------------------------------------------------

_api_call_count: int = 0


def get_api_call_count() -> int:
    """Return the total number of API calls made in this process."""
    return _api_call_count


def _increment_api_call_count() -> None:
    global _api_call_count
    _api_call_count += 1


# ---------------------------------------------------------------------------
# Generic retry wrapper
# ---------------------------------------------------------------------------


def api_call_with_retry(
    endpoint_class: Type[Any],
    params: Dict[str, Any],
    retries: int = config.API_RETRIES,
    base_delay: float = config.API_BASE_DELAY,
) -> Any:
    """Call an ``nba_api`` endpoint with exponential‑backoff retry.

    Works with **any** ``nba_api.stats.endpoints`` class — just pass the
    class itself (not an instance) and a dict of keyword arguments.

    Args:
        endpoint_class: The endpoint class, e.g.
            ``nba_api.stats.endpoints.LeagueDashLineups``.
        params: Keyword arguments forwarded to ``endpoint_class(**params)``.
        retries: Maximum number of attempts.
        base_delay: Seconds to wait after the first failure (doubles each
            subsequent attempt).

    Returns:
        The instantiated endpoint object (call ``.get_data_frames()`` etc.
        on the result).

    Raises:
        RuntimeError: If all retry attempts are exhausted.
    """
    endpoint_name = endpoint_class.__name__

    for attempt in range(retries):
        try:
            _increment_api_call_count()
            result = endpoint_class(**params, timeout=config.API_TIMEOUT)
            logger.debug("API call succeeded: %s (attempt %d)", endpoint_name, attempt + 1)
            return result

        except Exception as exc:
            wait = base_delay * (config.API_BACKOFF_MULTIPLIER ** attempt)
            if attempt < retries - 1:
                logger.warning(
                    "%s attempt %d/%d failed: %s — retrying in %.1fs",
                    endpoint_name,
                    attempt + 1,
                    retries,
                    str(exc)[:200],
                    wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "%s failed after %d attempts: %s",
                    endpoint_name,
                    retries,
                    str(exc)[:300],
                )
                raise RuntimeError(
                    f"{endpoint_name} failed after {retries} attempts"
                ) from exc

    # Should never reach here, but satisfy type checkers.
    raise RuntimeError(f"{endpoint_name} exhausted retries")  # pragma: no cover


# ---------------------------------------------------------------------------
# Merge helper
# ---------------------------------------------------------------------------


def merge_measure_types(
    dataframes_dict: Dict[str, pd.DataFrame],
    merge_key: str = "GROUP_ID",
) -> pd.DataFrame:
    """Merge multiple DataFrames (one per measure‑type) on a shared key.

    Duplicate columns (other than *merge_key*) are kept only from the first
    DataFrame that introduced them.

    Args:
        dataframes_dict: ``{measure_type_name: DataFrame, ...}``.
        merge_key: Column(s) to join on.

    Returns:
        A single merged ``DataFrame``.
    """
    if not dataframes_dict:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    seen_cols: set = {merge_key}

    for measure_type, df in dataframes_dict.items():
        if df is None or df.empty:
            logger.warning("Skipping empty DataFrame for measure type '%s'", measure_type)
            continue
        # Keep only the merge key plus columns we haven't seen yet.
        new_cols = [c for c in df.columns if c not in seen_cols or c == merge_key]
        if not new_cols or merge_key not in new_cols:
            continue
        frames.append(df[new_cols])
        seen_cols.update(new_cols)

    if not frames:
        return pd.DataFrame()

    merged = reduce(lambda left, right: pd.merge(left, right, on=merge_key, how="outer"), frames)
    logger.info(
        "Merged %d measure‑type frames → %d rows × %d cols",
        len(frames),
        len(merged),
        len(merged.columns),
    )
    return merged


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def health_check(season: str = config.SEASON) -> bool:
    """Quick connectivity test against the NBA Stats API.

    Attempts a lightweight ``LeagueDashLineups`` call (Base, 5‑man, single
    page) and returns ``True`` on success.

    Args:
        season: Season string, e.g. ``"2025-26"``.

    Returns:
        ``True`` if the API responded with data, ``False`` otherwise.
    """
    from nba_api.stats.endpoints import leaguedashlineups

    logger.info("Running API health check for season %s …", season)
    try:
        result = api_call_with_retry(
            leaguedashlineups.LeagueDashLineups,
            params=dict(
                group_quantity=5,
                measure_type_detailed_defense="Base",
                per_mode_detailed="Totals",
                season=season,
                season_type_all_star="Regular Season",
            ),
            retries=3,
            base_delay=2.0,
        )
        dfs = result.get_data_frames()
        if dfs and len(dfs[0]) > 0:
            logger.info("✓ Health check passed — got %d rows.", len(dfs[0]))
            return True
        logger.warning("⚠ Health check: API responded but returned 0 rows.")
        return True  # API is reachable even if season has no data yet
    except Exception as exc:
        logger.error("✗ Health check failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------


def save_dataframe(df: pd.DataFrame, filepath: str | Path) -> None:
    """Save a DataFrame to CSV, creating parent directories as needed.

    Args:
        df: The DataFrame to persist.
        filepath: Destination path (absolute or relative to cwd).
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)
    logger.info("Saved %d rows × %d cols → %s", len(df), len(df.columns), filepath)


# ---------------------------------------------------------------------------
# Team‑name lookup
# ---------------------------------------------------------------------------

_TEAM_MAP: Optional[Dict[int, str]] = None


def get_team_name(team_id: int) -> str:
    """Return the full team name for a given ``team_id``.

    Uses ``nba_api.stats.static.teams`` and caches the result.

    Args:
        team_id: NBA team identifier.

    Returns:
        Full team name (e.g. ``"Los Angeles Lakers"``), or ``"Unknown"`` if
        the id is not found.
    """
    global _TEAM_MAP
    if _TEAM_MAP is None:
        from nba_api.stats.static import teams

        _TEAM_MAP = {t["id"]: t["full_name"] for t in teams.get_teams()}
    return _TEAM_MAP.get(team_id, "Unknown")


def get_all_team_ids() -> List[int]:
    """Return a list of all 30 NBA team IDs.

    Returns:
        Sorted list of integer team IDs.
    """
    from nba_api.stats.static import teams

    return sorted([t["id"] for t in teams.get_teams()])


# ---------------------------------------------------------------------------
# Rate‑limit pacer
# ---------------------------------------------------------------------------


def pace(delay: float = config.API_CALL_DELAY) -> None:
    """Sleep for *delay* seconds to respect API rate limits.

    Args:
        delay: Seconds to sleep.
    """
    time.sleep(delay)
