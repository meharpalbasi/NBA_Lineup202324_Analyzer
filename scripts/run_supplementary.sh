#!/bin/bash
# run_supplementary.sh
# -----------------------------------------------------------------------------
# Fetch the supplementary NBA data + the legacy 5-man lineup CSV and publish
# them to GitHub.
#
# Designed for an always-on residential machine (e.g. a Mac mini) driven by
# launchd. stats.nba.com blocks cloud/datacenter IPs, so this MUST run from a
# home IP — never from a cloud host.
#
# What it publishes:
#   • the databallr-style rich data: on/off, clutch, play types, tracking,
#     hustle, defense tracking, estimated metrics, player/team stats, game
#     logs, … (`--supplementary-only`, ~220 API calls);
#   • the legacy 5-man lineup CSV the /dashboard reads
#     (`NBALineup<season>_RegSeason_Playoffs_BaseAdvanced.csv`,
#     `--legacy-lineups-only`, ~120 calls). Until 2026-09 a Railway cron
#     produced this file; it was retired because datacenter IPs are throttled.
#
# The heavy ~2,000-call full lineup fetch (5/3/2-man × all measure types) is
# NOT part of this job — see scripts/backfill_lineups.sh.
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

# 1. Sync with remote first — the RAPM job (and any manual publish) pushes to
# main too. --autostash so a dirty tree (e.g. file-mode quirks, leftover data
# from a previous partial run) doesn't abort the rebase.
echo "[$(ts)] Syncing with origin/main…"
git pull --rebase --autostash origin main

# 2. Fetch supplementary data only (skips the heavy per-team lineup fetch).
echo "[$(ts)] Fetching supplementary data…"
"$PYTHON" -m pipeline.main --supplementary-only

# 2b. Refresh the legacy 5-man lineup CSV. A separate invocation so a failure
#     here can't take the supplementary publish down (or vice versa); the
#     fetcher leaves the existing file untouched when it gets nothing back or
#     would shrink the table, so a bad run never publishes a partial dashboard.
echo "[$(ts)] Refreshing legacy 5-man lineup CSV…"
"$PYTHON" -m pipeline.main --legacy-lineups-only \
  || echo "[$(ts)] WARN: legacy lineup refresh failed — keeping the previous file."

# 3. Stage only the published files. The large lineups_*man CSVs are .gitignored.
git add data/on_off_*.csv data/clutch_*.csv data/play_types_*.csv \
        data/hustle_*.csv data/tracking_*.csv data/defense_tracking_*.csv \
        data/estimated_metrics_*.csv data/lineups_slim_*.csv \
        data/player_stats_*.csv data/team_stats_*.csv data/player_index_*.csv \
        data/player_clutch_*.csv data/shot_zones_*.csv \
        data/player_game_logs_*.csv \
        data/team_game_logs_*.csv \
        data/pt_shot_defender_*.csv \
        data/matchups_*.csv \
        data/standings_*.csv \
        data/NBALineup*_RegSeason_Playoffs_BaseAdvanced.csv

# 4. Commit + push only if something actually changed.
if git diff --staged --quiet; then
  echo "[$(ts)] No changes — data already current."
else
  git commit -m "data: refresh supplementary stats - $(date '+%Y-%m-%d')"
  git push origin main
  echo "[$(ts)] Pushed updated supplementary data."
fi

echo "===== $(ts) — done ====="
