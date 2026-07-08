#!/bin/bash
# run_rapm.sh
# -----------------------------------------------------------------------------
# Compute RAPM (regularized adjusted plus-minus) for the current season and
# publish it to GitHub. RAPM reconstructs every game's on-court lineups from
# play-by-play, so it's the heaviest job we run (~1h, ~2,500 light per-game
# requests) — keep it on its own, infrequent cadence (e.g. weekly), separate
# from the fast supplementary refresh.
#
# Like the supplementary fetch, this MUST run from a residential IP — never a
# cloud host — because stats.nba.com (Akamai) drops datacenter fingerprints.
#
# Raw play-by-play JSON is cached under data/rapm_cache/ (gitignored), so a
# re-run only fetches games it hasn't seen yet.
# -----------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="${REPO_DIR}/venv/bin/python"
LOG_DIR="${REPO_DIR}/scripts/logs"
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
echo "===== $(ts) — RAPM run starting ====="

if [ ! -x "$PYTHON" ]; then
  echo "ERROR: venv not found at $PYTHON. Run the one-time setup (see scripts/SETUP_MACMINI.md)." >&2
  exit 1
fi

# 1. Sync with remote first (Railway + the supplementary job also push to main).
echo "[$(ts)] Syncing with origin/main…"
git pull --rebase --autostash origin main

# 1b. Season-rollover guard: the IPM ridge needs the season's SPM prior. A
#     brand-new season gets scored with the FROZEN weights in spm_model.json
#     (no retraining); skipped once the file exists.
SEASON="${NBA_SEASON:-$("$PYTHON" -c 'from pipeline import config; print(config.SEASON)')}"
if [ ! -f "data/spm_${SEASON}.csv" ]; then
  echo "[$(ts)] No SPM prior for ${SEASON} — applying frozen weights…"
  "$PYTHON" -m pipeline.compute_spm --apply "$SEASON"
fi

# 2. Compute RAPM (and IPM, via the prior above) and re-export the player index.
echo "[$(ts)] Computing RAPM (this takes a while)…"
"$PYTHON" -m pipeline.main --rapm-only

# 2b. Refresh the hex-binned shot chart (~580 light per-player calls, ~25min;
#     raw responses cached under data/shotdetail_cache/, so re-runs are cheap).
"$PYTHON" -m pipeline.fetch_shot_detail

# 2c. Schedule (one light call) + team power ratings (offline compute over
#     files the weekly jobs already produce) ride the same cadence, so the
#     /teams ratings card and the schedule never go stale in-season.
"$PYTHON" -m pipeline.fetch_schedule
"$PYTHON" -m pipeline.compute_ratings

# 3. Stage the RAPM tables (single-season + 3-yr pooled), lineup chemistry,
#    WPA + biggest plays, and the refreshed player index.
git add data/rapm_*.csv data/ipm_*.csv data/spm_*.csv data/spm_model.json \
        data/lineup_chemistry_*.csv data/player_index_*.csv \
        data/wpa_*.csv data/biggest_plays_*.csv data/shot_hex_*.csv \
        data/team_ratings_*.csv data/ratings_validation.csv data/ratings_model.json \
        data/schedule_*.csv

# 4. Commit + push only if something actually changed.
if git diff --staged --quiet; then
  echo "[$(ts)] No changes — RAPM already current."
else
  git commit -m "data: refresh RAPM - $(date '+%Y-%m-%d')"
  git push origin main
  echo "[$(ts)] Pushed updated RAPM data."
fi

echo "===== $(ts) — done ====="
