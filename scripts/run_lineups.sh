#!/bin/bash
# run_lineups.sh
# -----------------------------------------------------------------------------
# OPTIONAL mid-week refresh of the legacy 5-man lineup CSV only
# (`NBALineup<season>_RegSeason_Playoffs_BaseAdvanced.csv`, the /dashboard
# 5-man table). ~120 API calls, a few minutes.
#
# The weekly Monday job (run_supplementary.sh) already refreshes this file.
# Load this one too (scripts/com.nbalineup.lineups.mini.plist: Wed + Fri 08:00)
# if you want lineups fresher than weekly during the season — that restores the
# every-2-days cadence the retired Railway cron had. It commits nothing when the
# data hasn't changed, so it's free in the offseason.
#
# Residential IP only, same reason as everything else (stats.nba.com throttles
# datacenter IPs).
# -----------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="${REPO_DIR}/venv/bin/python"
LOG_DIR="${REPO_DIR}/scripts/logs"
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
echo "===== $(ts) — legacy 5-man lineup refresh starting ====="

if [ ! -x "$PYTHON" ]; then
  echo "ERROR: venv not found at $PYTHON. Run the one-time setup (see scripts/SETUP_MACMINI.md)." >&2
  exit 1
fi

# 1. Sync with remote first (the supplementary + RAPM jobs push to main too).
echo "[$(ts)] Syncing with origin/main…"
git pull --rebase --autostash origin main

# 2. Refresh the legacy file. The fetcher leaves the existing file untouched
#    when it gets nothing back or would shrink the table.
"$PYTHON" -m pipeline.main --legacy-lineups-only

# 3. Stage just that file; commit + push only if it changed.
git add data/NBALineup*_RegSeason_Playoffs_BaseAdvanced.csv
if git diff --staged --quiet; then
  echo "[$(ts)] No changes — lineup data already current."
else
  git commit -m "data: refresh 5-man lineups - $(date '+%Y-%m-%d')"
  git push origin main
  echo "[$(ts)] Pushed updated lineup data."
fi

echo "===== $(ts) — done ====="
