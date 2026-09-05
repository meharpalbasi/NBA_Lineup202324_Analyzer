"""Season arithmetic: which NBA season is "current" on a given date.

An NBA season is labelled by its two calendar years ("2026-27"). The pipeline
treats the season as rolling over on **1 October**: October–December belong to
the season that starts in the current calendar year, January–September to the
one that started the year before.

Regular-season games only begin in the third or fourth week of October, so for
the first weeks after the rollover the API returns no rows for the new season.
Every fetcher already skips writing empty files, so those runs are harmless
no-ops and the previous season's CSVs stay published until real data lands.

``NBA_SEASON`` overrides the computed value everywhere (config, scripts):

    NBA_SEASON=2024-25 python run_pipeline.py --supplementary-only
"""

from __future__ import annotations

import datetime as _dt
import os

ROLLOVER_MONTH = 10  # October


def season_label(start_year: int) -> str:
    """``2026`` → ``"2026-27"``."""
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def current_season(today: _dt.date | None = None) -> str:
    """Season label for ``today`` (default: the real date) under the Oct-1 rule."""
    today = today or _dt.date.today()
    start = today.year if today.month >= ROLLOVER_MONTH else today.year - 1
    return season_label(start)


def previous_season(season: str) -> str:
    """``"2026-27"`` → ``"2025-26"``."""
    return season_label(int(season[:4]) - 1)


def resolve_season() -> str:
    """``NBA_SEASON`` if set, else the date-derived current season."""
    return os.getenv("NBA_SEASON") or current_season()


if __name__ == "__main__":  # `python -m pipeline.season` prints the season
    print(resolve_season())
