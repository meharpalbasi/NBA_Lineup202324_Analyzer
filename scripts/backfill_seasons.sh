#!/bin/bash
# backfill_seasons.sh — generate the FULL data set for historical seasons.
#
#   bash scripts/backfill_seasons.sh                 # default: 2017-18 … 2022-23
#   bash scripts/backfill_seasons.sh 2019-20 2020-21 # explicit list
#
# Per season it runs three generation passes:
#   1. supplementary  — box stats, on/off, clutch, play types, tracking, hustle,
#                       defense, estimated, shot zones, game logs, matchups, +
#                       the BPM/VORP/shot-making player_index merge (~220 calls).
#   2. RAPM           — RAPM + lineup chemistry + WPA + biggest plays from the
#                       play-by-play cache, then re-export player_index (the
#                       long pole — reconstructs every game, ~1–1.5h/season).
#   3. shot hexbins   — raw x/y shots → shot_hex (~570 calls).
#
# Why oldest-first: a season's 3-yr pooled RAPM needs the two PRIOR seasons'
# play-by-play already cached (multi_cache_ready guard), so we build forward.
#
# IMPORTANT scope note: RAPM/WPA/chemistry only reconstruct cleanly back to
# 2017-18 (older play-by-play breaks the per-period starter logic). Don't extend
# this list before 2017-18 without fixing + re-validating the reconstruction.
#
# Resumable: the RAPM and shot caches skip games already fetched, so a killed
# run picks up where it left off. Generates LOCAL files + caches only — it does
# NOT touch git. Publishing is a separate, per-season-validated step.
#
# Residential IP only (Akamai), and keep the machine awake (caffeinate) for the
# multi-hour duration.
set -uo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
PY="$REPO_DIR/venv/bin/python"

SEASONS=("$@")
if [ ${#SEASONS[@]} -eq 0 ]; then
  SEASONS=(2017-18 2018-19 2019-20 2020-21 2021-22 2022-23)
fi

ts() { date '+%Y-%m-%d %H:%M:%S'; }
run() { echo "[$(ts)] $*"; "$@" || echo "[$(ts)] WARN: nonzero exit from: $*"; }

echo "===== $(ts) backfill starting: ${SEASONS[*]} ====="
for S in "${SEASONS[@]}"; do
  echo ""
  echo "########## $(ts) SEASON $S ##########"
  run env NBA_SEASON="$S" "$PY" -m pipeline.main --supplementary-only --season "$S"
  run env NBA_SEASON="$S" "$PY" -m pipeline.main --rapm-only --season "$S"
  run env NBA_SEASON="$S" "$PY" -m pipeline.fetch_shot_detail

  echo "[$(ts)] season $S generated files:"
  ls -la data/*"$S"*.csv 2>/dev/null | awk '{printf "    %7.2fMB  %s\n", $5/1048576, $9}'
done
echo ""
echo "===== $(ts) backfill complete ====="
