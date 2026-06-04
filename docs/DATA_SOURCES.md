# Data sources & publishing

How NBA data gets from `stats.nba.com` into the web app. There are **two repos**, **two
automated producers** (Railway in the cloud, the Mac mini at home), and the frontend reads
everything as static CSVs over GitHub's raw CDN — there is no API server or database.

## The two repos

| Repo | What it is |
|------|------------|
| [`meharpalbasi/nbalineup`](https://github.com/meharpalbasi/nbalineup) | The Next.js web app (frontend). |
| [`meharpalbasi/NBA_Lineup202324_Analyzer`](https://github.com/meharpalbasi/NBA_Lineup202324_Analyzer) | **This repo**: the Python pipeline **and** the published CSVs in `data/`. The frontend fetches its raw URLs. |

## Flow

```mermaid
flowchart LR
  R["Railway cron bot<br/>(cloud, every 2 days)"]
  M["Mac mini<br/>(launchd, weekly) — active"]
  L["Laptop<br/>(manual fallback)"]
  D[("data/ on main<br/>NBA_Lineup202324_Analyzer")]
  F["Next.js app<br/>(client-side fetch + parse)"]
  R -->|legacy lineup CSV| D
  M -->|supplementary CSVs| D
  L -. manual run only .-> D
  D -->|raw.githubusercontent.com| F
```

## Who produces what

| Data file (in `data/`) | Producer | nba_api endpoint | Published to git? |
|---|---|---|---|
| `NBALineup202526_RegSeason_Playoffs_BaseAdvanced.csv` | **Railway** | `TeamDashLineups` (5-man) | ✅ |
| `on_off_2025-26.csv` | **Mac mini** | `TeamPlayerOnOffSummary` | ✅ |
| `clutch_2025-26.csv` | **Mac mini** | `LeagueDashTeamClutch` | ✅ |
| `play_types_2025-26.csv` | **Mac mini** | `SynergyPlayTypes` | ✅ |
| `tracking_2025-26.csv` | **Mac mini** | `LeagueDashPtStats` | ✅ |
| `defense_tracking_2025-26.csv` | **Mac mini** | `LeagueDashPtDefend` | ✅ |
| `hustle_players_2025-26.csv`, `hustle_teams_2025-26.csv` | **Mac mini** | `LeagueHustleStats*` | ✅ |
| `estimated_metrics_2025-26.csv` | **Mac mini** | `PlayerEstimatedMetrics` | ✅ |
| `lineups_slim_2man_2025-26.csv`, `lineups_slim_3man_2025-26.csv` | **Mac mini** | `TeamDashLineups` (slim) | ✅ |
| `lineups_5man/3man/2man_2025-26.csv` (full) | Mac mini (full run) | `TeamDashLineups` | ❌ `.gitignore`d (too big) |
| `NBALineup202425_…`, `NBALineup202324_…` | one-off historical | `TeamDashLineups` | ✅ (static) |

## Producer 1 — Railway (cloud), every 2 days

- Config: [`railway.json`](../railway.json) → `cronSchedule: "0 0 */2 * *"` = **00:00 UTC every 2 days**, `startCommand: bash update_and_commit.sh`.
- [`update_and_commit.sh`](../update_and_commit.sh): sync `main` → `python fetchlineups.py` → `git add data/` → commit `chore: update NBA lineup data - <date>` → push.
- Output: the **legacy 5-man lineup CSV** for the current season only.
- Keep-alive: [`.github/workflows/railway-keepalive.yml`](../.github/workflows/railway-keepalive.yml) pings Railway to redeploy **every Sunday 00:00 UTC** so the free-tier service doesn't sleep.
- ⚠️ Caveat: since ~Feb 2026, `stats.nba.com` (Akamai) throttles/blocks datacenter IPs, so the cloud lineup fetch can be flaky. The residential machine below is the dependable publisher.

## Producer 2 — Mac mini (launchd), Mondays 08:00 local — **active publisher**

- Config: [`scripts/com.nbalineup.supplementary.mini.plist`](../scripts/com.nbalineup.supplementary.mini.plist), installed on the mini as `~/Library/LaunchAgents/com.nbalineup.supplementary.plist` → `StartCalendarInterval` Weekday 1 (Monday), 08:00. Runs on next wake if asleep.
- [`scripts/run_supplementary.sh`](../scripts/run_supplementary.sh): pull `main` → `python -m pipeline.main --supplementary-only` (~220 API calls) → stage the rich CSVs → commit `data: refresh supplementary stats - <date>` → push (only if something changed).
- Output: **everything databallr-style** — on/off, clutch, play types, tracking, defense tracking, hustle, estimated metrics, and the **slim 2/3-man lineups**.
- Why residential: it routes nba_api through `curl_cffi` (Chrome TLS impersonation) from a home IP, which `stats.nba.com` accepts. See [`pipeline/nba_http_patch.py`](../pipeline/nba_http_patch.py).
- Setup runbook: [`scripts/SETUP_MACMINI.md`](../scripts/SETUP_MACMINI.md). (The mini was briefly mis-diagnosed as "blocked" — that was a test-command false negative; see [`docs/MINI_NBA_BLOCK_DEBUG.md`](./MINI_NBA_BLOCK_DEBUG.md).)

## Producer 3 — Laptop (manual fallback)

- Same repo + venv as the mini; can publish on demand with `bash scripts/run_supplementary.sh`.
- Its **scheduled** LaunchAgent has been **retired** (`launchctl unload …`) so it doesn't race the mini. Re-enable with `launchctl load -w …` if the mini is ever offline for a while.

## How the frontend consumes it

- URLs live in the app at `lib/seasons-config.js`, all pointing at
  `https://raw.githubusercontent.com/meharpalbasi/NBA_Lineup202324_Analyzer/main/data/<file>.csv`.
- The app fetches the CSV client-side, parses it in the browser, and renders. No backend API, no database.

| Page | File(s) read |
|------|--------------|
| `/dashboard` (5-man) | `NBALineup{season}_…BaseAdvanced.csv` |
| `/dashboard` (3/2-man) | `lineups_slim_3man/2man_*.csv` |
| `/wowy` | `on_off_*.csv` |
| `/clutch` | `clutch_*.csv` |
| `/playtypes` | `play_types_*.csv` |
| (planned `/players`, `/teams`) | tracking / estimated / hustle / play types / new pulls |

## Seasons

- **2025-26** — current: legacy lineups (Railway) + full supplementary (Mac mini).
- **2024-25, 2023-24** — historical: legacy 5-man lineup CSVs only (no supplementary).

## Operating notes

- **Publish supplementary data now (manually):** on a residential machine, `bash scripts/run_supplementary.sh` (commits + pushes only if data changed).
- **Health check (NOT a bare curl one-liner):** `python -m pipeline.main --supplementary-only --dry-run`. A header-less request to `/stats/*` hangs ~20s even when everything is fine — see [`docs/MINI_NBA_BLOCK_DEBUG.md`](./MINI_NBA_BLOCK_DEBUG.md).
- **Change a schedule:** Railway → edit `cronSchedule` in `railway.json`; mini → edit `StartCalendarInterval` in the `.mini` plist, then `launchctl unload && launchctl load -w` it.
- **Confirm a run happened:** look for the commit messages above on `main`, or tail `scripts/logs/launchd.{out,err}.log`.
- **One scheduled publisher at a time:** the Mac mini. The laptop's scheduled agent is retired to avoid push races.

## See also
- [`RAILWAY_SETUP.md`](../RAILWAY_SETUP.md) — Railway deploy + env vars.
- [`scripts/SETUP_MACMINI.md`](../scripts/SETUP_MACMINI.md) — residential publisher setup.
- [`docs/MINI_NBA_BLOCK_DEBUG.md`](./MINI_NBA_BLOCK_DEBUG.md) — why the mini "block" was a false negative.
