#!/bin/bash
# backfill_lineups.sh — backfill FULL lineup data for the historical seasons.
#
#   bash scripts/backfill_lineups.sh                  # all 8 seasons (2017-18 … 2024-25)
#   bash scripts/backfill_lineups.sh 2019-20 2020-21  # explicit list
#   START_SEASON=2021-22 bash scripts/backfill_lineups.sh  # resume from a season
#
# Per season it runs ONE generation pass via the pipeline's `--lineups-only`
# entry point, which:
#   1. fetch_and_merge_lineups(season) — every (Regular Season|Playoffs) ×
#      (5|3|2-man) × (Totals|Per100) × measure-type combo from TeamDashLineups
#      (~30 calls/combo, the long pole, ~73 min/season), writing the raw
#      lineups_{5,3,2}man_<season>.csv files.
#   2. export_slim(season) — trims those into the browser-ready
#      lineups_slim_{3,2}man_<season>.csv (needs on_off_<season>.csv to
#      reconstruct each lineup's team; present for all 8 backfill seasons).
#
# Order does NOT matter here — unlike backfill_seasons.sh's RAPM pass, the lineup
# fetch has no cross-season dependency. The list is an explicit array so it's
# easy to edit / resume from.
#
# Resumable + idempotent: a season whose slim outputs already exist is skipped
# (override with FORCE=1), and START_SEASON / an explicit arg list let a killed
# run pick up where it left off. The fetch loop tolerates a nonzero exit from any
# single season (warns, continues) so one bad season can't abort the ~10h run.
#
# Publishing mirrors run_supplementary.sh / run_rapm.sh exactly: sync, generate,
# stage the lineup CSVs, then commit + push only if something changed. The raw
# lineups_{5,3,2}man_*.csv are .gitignored (too large to serve normally), so they
# are force-added (`git add -f`) — for these backfilled seasons the raw 5-man file
# is the ONLY 5-man source (no Railway bot produces a legacy NBALineup…csv for
# them). The slim files are not ignored and stage normally.
#
# Residential IP only (stats.nba.com / Akamai blocks datacenter IPs; nba_api is
# routed through curl_cffi Chrome-TLS in pipeline/nba_http_patch.py). Keep the
# machine awake for the multi-hour duration, e.g.:
#   caffeinate -dimsu bash scripts/backfill_lineups.sh
set -uo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
PY="$REPO_DIR/venv/bin/python"
DATA_DIR="$REPO_DIR/data"

# Eight historical seasons to backfill. Edit this array to add/remove seasons.
ALL_SEASONS=(2017-18 2018-19 2019-20 2020-21 2021-22 2022-23 2023-24 2024-25)

# Selection: explicit args win; else the full list, optionally trimmed to start
# at START_SEASON (for resuming a killed run).
SEASONS=("$@")
if [ ${#SEASONS[@]} -eq 0 ]; then
  SEASONS=("${ALL_SEASONS[@]}")
  if [ -n "${START_SEASON:-}" ]; then
    trimmed=()
    started=0
    for s in "${ALL_SEASONS[@]}"; do
      [ "$s" = "$START_SEASON" ] && started=1
      [ "$started" -eq 1 ] && trimmed+=("$s")
    done
    if [ ${#trimmed[@]} -eq 0 ]; then
      echo "ERROR: START_SEASON=$START_SEASON not in season list: ${ALL_SEASONS[*]}" >&2
      exit 1
    fi
    SEASONS=("${trimmed[@]}")
  fi
fi

ts() { date '+%Y-%m-%d %H:%M:%S'; }
run() { echo "[$(ts)] $*"; "$@" || echo "[$(ts)] WARN: nonzero exit from: $*"; }

if [ ! -x "$PY" ]; then
  echo "ERROR: venv not found at $PY. Run the one-time setup (see scripts/SETUP_MACMINI.md)." >&2
  exit 1
fi

echo "===== $(ts) lineup backfill starting: ${SEASONS[*]} ====="

# 1. Sync with remote first (Railway + the supplementary/RAPM jobs also push to
#    main). --autostash so a dirty tree doesn't abort the rebase.
echo "[$(ts)] Syncing with origin/main…"
git pull --rebase --autostash origin main

# 2. Generate lineups + slim exports per season (the heavy, ~73 min/season pass).
for S in "${SEASONS[@]}"; do
  echo ""
  echo "########## $(ts) SEASON $S ##########"

  slim3="$DATA_DIR/lineups_slim_3man_$S.csv"
  slim2="$DATA_DIR/lineups_slim_2man_$S.csv"
  if [ "${FORCE:-0}" != "1" ] && [ -f "$slim3" ] && [ -f "$slim2" ]; then
    echo "[$(ts)] season $S already has slim outputs — skipping (FORCE=1 to refetch)."
    continue
  fi

  season_start=$(date +%s)
  run env NBA_SEASON="$S" "$PY" -m pipeline.main --lineups-only --season "$S"
  season_elapsed=$(( $(date +%s) - season_start ))
  echo "[$(ts)] season $S finished in $((season_elapsed / 60))m $((season_elapsed % 60))s"

  echo "[$(ts)] season $S generated files:"
  ls -la "$DATA_DIR"/*"$S"*.csv 2>/dev/null | grep -E "lineups_(5|3|2)man|lineups_slim_(3|2)man" || true
done

# 3. Stage the lineup CSVs. The raw lineups_{5,3,2}man_*.csv are .gitignored, so
#    force-add them (-f); the slim files are not ignored and add normally.
echo ""
echo "[$(ts)] Staging lineup CSVs…"
git add -f data/lineups_5man_*.csv data/lineups_3man_*.csv data/lineups_2man_*.csv
git add data/lineups_slim_3man_*.csv data/lineups_slim_2man_*.csv

# 4. Commit + push only if something actually changed.
if git diff --staged --quiet; then
  echo "[$(ts)] No changes — lineup data already current."
else
  git commit -m "data: backfill historical lineups (5/3/2-man + slim) - $(date '+%Y-%m-%d')"
  git push origin main
  echo "[$(ts)] Pushed backfilled lineup data."
fi

echo ""
echo "===== $(ts) lineup backfill complete ====="
