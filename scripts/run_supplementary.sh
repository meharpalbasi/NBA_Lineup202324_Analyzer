#!/bin/bash
# run_supplementary.sh
# -----------------------------------------------------------------------------
# Fetch ONLY the supplementary NBA data and publish it to GitHub.
#
# Designed for an always-on residential machine (e.g. a Mac mini) driven by
# launchd. stats.nba.com blocks cloud/datacenter IPs, so this MUST run from a
# home IP — never from a cloud host.
#
# Division of labour:
#   • Railway bot  -> keeps the legacy lineup CSV fresh (per-team lineups).
#   • This script  -> publishes the databallr-style rich data the cloud bot
#                     never produces: on/off, clutch, play types, tracking,
#                     hustle, defense tracking, estimated metrics.
#
# It runs `--supplementary-only`, so it makes ~220 API calls instead of the
# ~3,000 a full run would (the ~2,500-call lineup fetch is skipped).
# -----------------------------------------------------------------------------
set -euo pipefail

# Resolve repo root from this script's location (scripts/ -> repo root).
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="${REPO_DIR}/venv/bin/python"
LOG_DIR="${REPO_DIR}/scripts/logs"
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
echo "===== $(ts) — supplementary fetch starting ====="

if [ ! -x "$PYTHON" ]; then
  echo "ERROR: venv not found at $PYTHON. Run the one-time setup (see scripts/SETUP_MACMINI.md)." >&2
  exit 1
fi

# 1. Sync with remote first — Railway pushes the legacy lineup CSV to main too.
# --autostash so a dirty tree (e.g. file-mode quirks, leftover data from a
# previous partial run) doesn't abort the rebase.
echo "[$(ts)] Syncing with origin/main…"
git pull --rebase --autostash origin main

# 2. Fetch supplementary data only (skips the heavy per-team lineup fetch).
echo "[$(ts)] Fetching supplementary data…"
"$PYTHON" -m pipeline.main --supplementary-only

# 3. Stage only the rich files. The large lineups_*man CSVs are .gitignored.
git add data/on_off_*.csv data/clutch_*.csv data/play_types_*.csv \
        data/hustle_*.csv data/tracking_*.csv data/defense_tracking_*.csv \
        data/estimated_metrics_*.csv data/lineups_slim_*.csv \
        data/player_stats_*.csv data/team_stats_*.csv data/player_index_*.csv

# 4. Commit + push only if something actually changed.
if git diff --staged --quiet; then
  echo "[$(ts)] No changes — data already current."
else
  git commit -m "data: refresh supplementary stats - $(date '+%Y-%m-%d')"
  git push origin main
  echo "[$(ts)] Pushed updated supplementary data."
fi

echo "===== $(ts) — done ====="
