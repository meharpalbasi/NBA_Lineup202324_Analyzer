# CLAUDE.md — backend (data pipeline)

Onboarding for an agent picking up this repo. For depth, read
[`README.md`](README.md) (the pipeline code) and
[`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) (the publishing model). This file
is the 60-second orientation + the conventions that aren't obvious from the code.

## What this is

A Python pipeline that pulls NBA stats from `stats.nba.com`, computes a few
advanced metrics offline, and **publishes CSVs to `data/` on `main`**. The
separate frontend repo [`meharpalbasi/nbalineup`](https://github.com/meharpalbasi/nbalineup)
(a Next.js app) fetches those CSVs over GitHub's raw CDN and renders them.
**No API server, no database** — the CSVs are the interface.

## Module map (`pipeline/`)

| File | Role |
|------|------|
| `config.py` | All constants: season, measure types, API delays/retries. |
| `nba_http_patch.py` | Routes nba_api through `curl_cffi` (Chrome TLS) — imported once in `main.py`. |
| `fetch_lineups.py` | Core lineup data (`LeagueDashLineups`, 5/3/2-man). |
| `fetch_supplementary.py` | All player/team pulls (on/off, clutch, tracking, stats, shot zones, game logs, pt-shot, …). |
| `compute_impact.py` | BPM 2.0 + VORP and shot-making/xeFG — **computed offline, no new API calls**. |
| `fetch_rapm.py` | RAPM: play-by-play → on-court-five reconstruction → ridge. Opt-in, heavy. |
| `export_web.py` | Slim lineup CSVs + `player_index` (the pre-joined `/players` table). |
| `main.py` | Orchestrator: CLI flags, section runner, summary. Entry: `python -m pipeline.main` or `run_pipeline.py`. |
| `utils.py` | Retry/backoff, health check, `save_dataframe`. |

## Hard constraints (read before touching networking or publishing)

- **`stats.nba.com` blocks non-browser TLS** (Akamai, since ~Feb 2026). We defeat
  it with `curl_cffi` (`impersonate="chrome"`). Plain `requests` will time out.
- **Datacenter IPs are throttled too.** The real publisher is a **residential
  machine** (a Mac mini on launchd). Don't expect fetches to work from CI/cloud.
- **`playbyplayv2` is dead** (returns `{}`); use **`playbyplayv3`**. `pbpstats` /
  `nba_on_court` are v2-bound and unusable for the current season — that's why
  `fetch_rapm.py` reconstructs lineups itself. Its module docstring has the full
  recipe and the name-resolution gotchas (diacritics, surname collisions).

## Working conventions

- **Data → `main` directly** (mirrors the publisher bots: `data: …` / `chore:
  update NBA lineup data - <date>` commit messages). **Code → a branch + PR.**
- **Merge code PRs before the next scheduled run**, or the producers regenerate
  the CSVs with old code and your change silently doesn't take effect.
- The maintainer merges PRs; an agent should open them, not self-merge.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Running

```bash
pip install -r requirements-pipeline.txt
python -m pipeline.main --dry-run             # health check (NOT a bare curl)
python -m pipeline.main --supplementary-only  # the weekly residential job (~220 calls)
python -m pipeline.main --rapm-only           # RAPM + re-export player_index (~1h)
```

Publishers: `scripts/run_supplementary.sh` (Mac mini, weekly) and
`scripts/run_rapm.sh` (RAPM, separate slower cadence). RAPM caches raw PBP JSON
under `data/rapm_cache/` (gitignored) so reruns are free.

## See also

- [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) — two repos, who produces each file, the frontend page→file map.
- [`scripts/SETUP_MACMINI.md`](scripts/SETUP_MACMINI.md) — residential publisher setup.
- [`docs/MINI_NBA_BLOCK_DEBUG.md`](docs/MINI_NBA_BLOCK_DEBUG.md) — why a bare `curl` health check misleads.
