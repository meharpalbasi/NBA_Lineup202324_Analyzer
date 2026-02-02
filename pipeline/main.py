"""Pipeline orchestrator — runs lineup and supplementary fetches in order.

Handles command‑line arguments, tracks success / failure of each section, and
prints a summary report at the end.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Dict, List, Tuple

from . import config
from .utils import get_api_call_count, health_check, setup_logging

logger = logging.getLogger("pipeline.main")


# ---------------------------------------------------------------------------
# Section runner
# ---------------------------------------------------------------------------


def _run_section(
    name: str,
    fn,  # Callable — deliberately untyped to avoid generics noise
    *args,
    **kwargs,
) -> Tuple[bool, int]:
    """Execute a fetch section, returning ``(success, rows)``.

    If the section raises, the error is logged and ``(False, 0)`` is returned
    so the pipeline continues with the next section.

    Args:
        name: Human‑readable section label (for logging).
        fn: Callable that does the actual work.
        *args: Positional args forwarded to *fn*.
        **kwargs: Keyword args forwarded to *fn*.

    Returns:
        ``(True, row_count)`` on success, ``(False, 0)`` on failure.
    """
    logger.info("━" * 60)
    logger.info("SECTION: %s", name)
    logger.info("━" * 60)
    start = time.time()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.time() - start

        # Count rows in the result
        rows = 0
        if result is None:
            rows = 0
        elif isinstance(result, dict):
            # fetch_and_merge_lineups returns {gq: df}
            import pandas as pd
            rows = sum(len(v) for v in result.values() if isinstance(v, pd.DataFrame))
        elif isinstance(result, tuple):
            # fetch_hustle returns (player_df, team_df)
            import pandas as pd
            rows = sum(len(v) for v in result if isinstance(v, pd.DataFrame))
        else:
            rows = len(result) if hasattr(result, "__len__") else 0

        if rows > 0:
            logger.info("✓ %s completed in %.1fs — %d rows", name, elapsed, rows)
            return True, rows
        else:
            logger.warning("⚠ %s completed but produced 0 rows (%.1fs)", name, elapsed)
            return False, 0

    except Exception as exc:
        elapsed = time.time() - start
        logger.error("✗ %s FAILED after %.1fs: %s", name, elapsed, exc)
        return False, 0


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------


def _print_summary(
    results: Dict[str, Tuple[bool, int]],
    files_written: List[str],
    wall_seconds: float,
) -> None:
    """Print a nicely formatted summary of the pipeline run.

    Args:
        results: ``{section_name: (success, row_count)}``.
        files_written: List of file paths written during the run.
        wall_seconds: Total wall‑clock time.
    """
    api_calls = get_api_call_count()
    total_rows = sum(r for _, r in results.values())
    succeeded = [k for k, (s, _) in results.items() if s]
    failed = [k for k, (s, _) in results.items() if not s]

    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("Total wall time    : %.1f s (%.1f min)", wall_seconds, wall_seconds / 60)
    logger.info("Total API calls    : %d", api_calls)
    logger.info("Total rows fetched : %d", total_rows)
    logger.info("Files written      : %d", len(files_written))

    if succeeded:
        logger.info("")
        logger.info("✓ Succeeded (%d):", len(succeeded))
        for s in succeeded:
            _, rows = results[s]
            logger.info("    • %s (%d rows)", s, rows)

    if failed:
        logger.info("")
        logger.info("✗ Failed (%d):", len(failed))
        for f in failed:
            logger.info("    • %s", f)

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------


def _parse_args(argv: list | None = None) -> argparse.Namespace:
    """Parse command‑line arguments.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        Parsed ``Namespace``.
    """
    parser = argparse.ArgumentParser(
        description="NBA Stats data pipeline — fetch lineups & supplementary data.",
    )
    parser.add_argument(
        "--season",
        default=config.SEASON,
        help=f"NBA season string (default: {config.SEASON}).",
    )
    parser.add_argument(
        "--lineups-only",
        action="store_true",
        help="Fetch only lineup data, skip supplementary.",
    )
    parser.add_argument(
        "--supplementary-only",
        action="store_true",
        help="Fetch only supplementary data, skip lineups.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test API connectivity only (no data fetch).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG‑level logging.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run(argv: list | None = None) -> None:
    """Run the full NBA data pipeline.

    This is the single entry point called by ``run_pipeline.py`` (and by
    ``python -m pipeline.main``).

    Args:
        argv: Optional argument list (for testing); defaults to ``sys.argv``.
    """
    args = _parse_args(argv)
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    season: str = args.season

    logger.info("=" * 60)
    logger.info("NBA DATA PIPELINE — season %s", season)
    logger.info("=" * 60)

    # --- Health check ---
    if not health_check(season):
        if args.dry_run:
            logger.error("Dry‑run health check failed. Exiting.")
            sys.exit(1)
        logger.warning("Health check failed — continuing anyway …")

    if args.dry_run:
        logger.info("Dry‑run complete — API is reachable. Exiting.")
        return

    wall_start = time.time()
    results: Dict[str, Tuple[bool, int]] = {}
    files_written: List[str] = []

    # ------------------------------------------------------------------
    # Core lineups
    # ------------------------------------------------------------------
    if not args.supplementary_only:
        from .fetch_lineups import fetch_and_merge_lineups

        ok, rows = _run_section("Lineups (5/3/2-man)", fetch_and_merge_lineups, season)
        results["Lineups"] = (ok, rows)
        if ok:
            for gq in config.GROUP_QUANTITIES:
                files_written.append(str(config.DATA_DIR / f"lineups_{gq}man_{season}.csv"))

        # Pause before supplementary
        time.sleep(config.API_ENDPOINT_DELAY)

    # ------------------------------------------------------------------
    # Supplementary data
    # ------------------------------------------------------------------
    if not args.lineups_only:
        from .fetch_supplementary import (
            fetch_clutch,
            fetch_defense_tracking,
            fetch_estimated_metrics,
            fetch_hustle,
            fetch_on_off,
            fetch_play_types,
            fetch_tracking,
        )

        # 1. On/Off
        ok, rows = _run_section("On/Off Court", fetch_on_off, season)
        results["On/Off"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"on_off_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 2. Clutch
        ok, rows = _run_section("Clutch", fetch_clutch, season)
        results["Clutch"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"clutch_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 3. Play Types
        ok, rows = _run_section("Play Types", fetch_play_types, season)
        results["Play Types"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"play_types_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 4. Hustle
        ok, rows = _run_section("Hustle Stats", fetch_hustle, season)
        results["Hustle"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"hustle_players_{season}.csv"))
            files_written.append(str(config.DATA_DIR / f"hustle_teams_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 5. Tracking
        ok, rows = _run_section("Player Tracking", fetch_tracking, season)
        results["Tracking"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"tracking_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 6. Defense Tracking
        ok, rows = _run_section("Defense Tracking", fetch_defense_tracking, season)
        results["Defense Tracking"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"defense_tracking_{season}.csv"))
        time.sleep(config.API_ENDPOINT_DELAY)

        # 7. Estimated Metrics
        ok, rows = _run_section("Estimated Metrics", fetch_estimated_metrics, season)
        results["Estimated Metrics"] = (ok, rows)
        if ok:
            files_written.append(str(config.DATA_DIR / f"estimated_metrics_{season}.csv"))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    wall_seconds = time.time() - wall_start
    _print_summary(results, files_written, wall_seconds)


# Allow ``python -m pipeline.main``
if __name__ == "__main__":
    run()
