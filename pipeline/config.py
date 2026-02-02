"""Centralised configuration for the NBA data pipeline.

All tunable constants live here so the rest of the code-base stays free of
magic strings and numbers.  The current season can be overridden via the
``NBA_SEASON`` environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

# ---------------------------------------------------------------------------
# Season
# ---------------------------------------------------------------------------
SEASON: str = os.getenv("NBA_SEASON", "2025-26")

# ---------------------------------------------------------------------------
# LeagueDashLineups — measure types, group quantities, per‑modes
# ---------------------------------------------------------------------------
MEASURE_TYPES: List[str] = [
    "Base",
    "Advanced",
    "Four Factors",
    "Misc",
    "Scoring",
    "Opponent",
    "Defense",
]

GROUP_QUANTITIES: List[int] = [5, 3, 2]

PER_MODES: List[str] = [
    "Totals",
    "PerGame",
    "Per100Possessions",
]

SEASON_TYPES: List[str] = [
    "Regular Season",
    "Playoffs",
]

# ---------------------------------------------------------------------------
# Player‑tracking (LeagueDashPtStats) measure types
# ---------------------------------------------------------------------------
PT_MEASURE_TYPES: List[str] = [
    "SpeedDistance",
    "CatchShoot",
    "Drives",
    "Passing",
    "Possessions",
    "Rebounding",
    "Defense",
    "Efficiency",
    "PullUpShot",
    "PostTouch",
    "PaintTouch",
    "ElbowTouch",
]

# ---------------------------------------------------------------------------
# Synergy play types
# ---------------------------------------------------------------------------
SYNERGY_PLAY_TYPES: List[str] = [
    "Transition",
    "Isolation",
    "PRBallHandler",
    "PRRollman",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
]

# ---------------------------------------------------------------------------
# Defense‑tracking categories (LeagueDashPtDefend)
# ---------------------------------------------------------------------------
DEFENSE_CATEGORIES: List[str] = [
    "Overall",
    "2 Pointers",
    "3 Pointers",
    "Less Than 6Ft",
    "Less Than 10Ft",
    "Greater Than 15Ft",
]

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
DATA_DIR: Path = Path(__file__).resolve().parent.parent / "data"

# ---------------------------------------------------------------------------
# API / rate‑limiting settings
# ---------------------------------------------------------------------------
API_TIMEOUT: int = 120  # seconds per request
API_RETRIES: int = 5
API_BASE_DELAY: float = 3.0  # seconds — first retry wait
API_BACKOFF_MULTIPLIER: float = 2.0  # exponential backoff factor
API_CALL_DELAY: float = 1.5  # seconds between consecutive calls
API_ENDPOINT_DELAY: float = 3.0  # seconds between different endpoint types
