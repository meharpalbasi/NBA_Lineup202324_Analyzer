"""NBA Data Pipeline — the modular successor to the original fetchlineups.py
(whose legacy 5-man output now lives on as ``fetch_lineups.fetch_legacy_lineups``).

This package fetches lineup, supplementary, and tracking data from the NBA
Stats API, merges multiple measure types, and writes tidy CSV outputs to the
``data/`` directory.
"""

__version__ = "1.0.0"
