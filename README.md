# NBA Lineup Data Pipeline

Python pipeline that fetches NBA lineup + player/team statistics from
[`stats.nba.com`](https://stats.nba.com) (via [nba_api](https://github.com/swar/nba_api)),
computes a few advanced metrics offline, and publishes everything as static CSVs
in `data/` on `main`. The [NBA Lineup Analytics](https://github.com/meharpalbasi/nbalineup)
frontend reads those CSVs directly over GitHub's raw CDN — there is no API server
or database.

> **Read [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) first** for the full
> publishing picture: two repos, who produces which file, the residential-IP /
> `curl_cffi` story, and how the frontend consumes it. This README documents the
> pipeline code itself.

## Architecture

```
run_pipeline.py              ← entry point (thin wrapper around pipeline.main)
└── pipeline/
    ├── config.py            ← all tunable constants (measure types, delays, season)
    ├── nba_http_patch.py    ← routes nba_api through curl_cffi (Chrome TLS) — see below
    ├── fetch_lineups.py     ← core lineup data (LeagueDashLineups, 5/3/2-man)
    ├── fetch_supplementary.py ← player/team pulls (on/off, clutch, tracking, stats, …)
    ├── fetch_rapm.py        ← RAPM: play-by-play → lineup reconstruction → ridge (opt-in)
    ├── compute_impact.py    ← BPM 2.0 + VORP and shot-making/xeFG, computed OFFLINE
    ├── export_web.py        ← slim lineup CSVs + the pre-joined player_index table
    ├── main.py              ← orchestrator: CLI args, section runner, summary report
    └── utils.py             ← retry/backoff, health check, API-call tracking, save_dataframe
```

Run it as `python run_pipeline.py …` or `python -m pipeline.main …` (identical).
`fetchlineups.py` is the original single-file pipeline (Base+Advanced only), kept
for the Railway legacy-lineup job; use `run_pipeline.py` for everything else.

### Why curl_cffi

Since ~Feb 2026, `stats.nba.com` (Akamai Bot Manager) silently drops requests
whose TLS fingerprint isn't a real browser's — plain `requests`/`urllib3` complete
the handshake then time out. `pipeline/nba_http_patch.py` swaps nba_api's HTTP for
a [`curl_cffi`](https://github.com/lexiforest/curl_cffi) shim with
`impersonate="chrome"`, and `fetch_rapm.py` calls curl_cffi directly. **Datacenter
IPs are also throttled, so the dependable publisher is a residential machine** (the
Mac mini) — see `docs/DATA_SOURCES.md`.

## What it produces

### Core lineups — `fetch_lineups.py`

`LeagueDashLineups`, all measure types × per-modes merged into one wide CSV per
group size.

| Dimension | Values |
|-----------|--------|
| Measure types | Base, Advanced, Four Factors, Misc, Scoring, Opponent |
| Group sizes | 5-man, 3-man, 2-man |
| Per modes | Totals, Per100Possessions |
| Season types | Regular Season, Playoffs |

→ `lineups_{5,3,2}man_{season}.csv` (full, **.gitignored** — too big) and the
published slim `lineups_slim_{2,3}man_{season}.csv` (Totals, MIN≥100, ~40 cols).

### Player & team pulls — `fetch_supplementary.py`

| Source | Endpoint | Output |
|--------|----------|--------|
| On/Off Court (WOWY) | `TeamPlayerOnOffSummary` | `on_off_{season}.csv` |
| Clutch (team) | `LeagueDashTeamClutch` | `clutch_{season}.csv` |
| Clutch (player) | `LeagueDashPlayerClutch` | `player_clutch_{season}.csv` |
| Play types (Synergy, 11) | `SynergyPlayTypes` | `play_types_{season}.csv` |
| Player tracking (12 cats) | `LeagueDashPtStats` | `tracking_{season}.csv` |
| Defense tracking | `LeagueDashPtDefend` | `defense_tracking_{season}.csv` |
| Hustle | `LeagueHustleStats*` | `hustle_players_{season}.csv`, `hustle_teams_{season}.csv` |
| Estimated metrics | `PlayerEstimatedMetrics` | `estimated_metrics_{season}.csv` |
| Player stats (Base+Adv) | `LeagueDashPlayerStats` | `player_stats_{season}.csv` |
| Team stats (Base+Adv+4F) | `LeagueDashTeamStats` | `team_stats_{season}.csv` |
| Shot zones (FG% by zone) | `LeagueDashPlayerShotLocations` | `shot_zones_{season}.csv` |
| Player game logs | `LeagueGameLog` (player) | `player_game_logs_{season}.csv` |
| Team game logs | `LeagueGameLog` | `team_game_logs_{season}.csv` |
| Closest-defender shot splits | `LeagueDashPlayerPtShot` | `pt_shot_defender_{season}.csv` |

### Computed offline (no new API calls) — `compute_impact.py` → `export_web.py`

These are derived from the pulls above and merged into the player index:

- **BPM 2.0 + VORP** (Daniel Myers' formulation) from `player_stats` + `team_stats`.
- **Shot-making over expected (xeFG)** — actual eFG% minus the league-average eFG%
  for a player's shot-location mix, from `shot_zones`.

`export_web.export_player_index` joins `player_stats` + on/off swing + clutch +
BPM/VORP + shot-making (+ RAPM, if present) into **`player_index_{season}.csv`** —
the single table the `/players` page and profile read.

### RAPM (flagship) — `fetch_rapm.py` (opt-in, heavy)

Self-computed **Regularized Adjusted Plus-Minus** — the ridge-regression impact
metric EPM/DARKO/LEBRON are built on. No pre-built lineup feed exists for the
current season (`playbyplayv2` is dead, `pbpstats`/`nba_on_court` are v2-bound), so
it reconstructs on-court fives itself from `playbyplayv3` + `boxscoretraditionalv3`
(validated: reconstructed minutes match the box score to <0.5s across all games),
accumulates per-(offense-5, defense-5) possessions + points, and solves a sparse
`RidgeCV`. → **`rapm_{season}.csv`** (O/D/total RAPM + possessions) and the same
columns merged into `player_index`.

Heavy: ~2,500 light per-game calls, ~1h, **residential IP only**. Raw play-by-play
JSON is cached under `data/rapm_cache/` (gitignored) so reruns are free. See
[`memory` / `docs`] and the module docstring for the full recipe + gotchas.

## Usage

```bash
pip install -r requirements-pipeline.txt   # nba_api, pandas, curl_cffi, scikit-learn, scipy

python run_pipeline.py                      # lineups + supplementary + exports
python run_pipeline.py --supplementary-only # skip the heavy lineup fetch (~220 calls)
python run_pipeline.py --rapm-only          # just RAPM + re-export player_index (~1h)
python run_pipeline.py --season 2024-25
```

### CLI options

```
--season SEASON       NBA season (default: 2025-26, or NBA_SEASON env var)
--lineups-only        Fetch only lineup data
--supplementary-only  Fetch only supplementary/player/team data + exports
--with-rapm           Also compute RAPM (heavy; reconstructs every game, ~1h)
--rapm-only           Compute RAPM and re-export player_index; skip everything else
--dry-run             Test API connectivity only (use this for a health check)
--verbose             DEBUG logging
```

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NBA_SEASON` | Season to fetch | `2025-26` |
| `GITHUB_TOKEN`, `GITHUB_REPO` | Railway auto-commit (legacy lineup job) | — |

## Publishing (who runs what)

The CSVs in `data/` are published to `main` by automated producers, **not** by a
plain `git push` from a dev box. See [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md).
In short:

- **Railway (cloud, every 2 days)** — the legacy 5-man lineup CSV (`fetchlineups.py`).
- **Mac mini (residential, weekly)** — `scripts/run_supplementary.sh`: everything
  supplementary + the computed metrics + `player_index`.
- **RAPM** — `scripts/run_rapm.sh` (`--rapm-only`), on its own slower cadence
  because it's a ~1h job; publishes `rapm_*.csv` + the RAPM-merged `player_index`.

> ⚠️ Code that changes `export_web`/`compute_impact`/`fetch_*` must merge **before**
> the next scheduled run, or the producers regenerate the CSVs with old code.

## Reliability

- Exponential-backoff retry: `API_RETRIES=7`, base `3s`, ×2 each attempt.
- `2s` between calls, `5s` between endpoint types (`config.py`).
- Health check (`--dry-run`) before a full fetch.
- Per-section error isolation — one source failing doesn't stop the rest.
- Summary report with per-section success/rows and total API-call count.
