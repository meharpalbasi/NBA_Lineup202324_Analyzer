# Mac mini setup — supplementary data publisher

The Mac mini is the **active publisher** of the modular pipeline's supplementary data:
it runs `scripts/run_supplementary.sh` weekly on a home/residential IP and pushes the
rich CSVs to GitHub — the data Railway does *not* produce (on/off, clutch, play types,
tracking, hustle, defense tracking, estimated metrics, and the slim 2/3-man lineups).

Why a residential machine and not the cloud: `stats.nba.com` (Akamai) blocks
datacenter/cloud IPs (AWS/GCP/Railway/GitHub Actions); a home IP reaches it fine.
nba_api is routed through `curl_cffi` Chrome-TLS impersonation
(`pipeline/nba_http_patch.py`) to pass Akamai's fingerprinting.

> **Note (resolved 2026-06-03):** the mini was briefly thought to be "blocked" from
> `stats.nba.com`. That was a **false negative in the test command**, not a real block —
> see [`docs/MINI_NBA_BLOCK_DEBUG.md`](../docs/MINI_NBA_BLOCK_DEBUG.md). Use the health
> check below, never a bare header-less request.

| Job | Where | Produces |
|-----|-------|----------|
| `update_and_commit.sh` (`fetchlineups.py`) | Railway (cloud) | legacy lineup CSV (`NBALineup…BaseAdvanced.csv`) |
| `scripts/run_supplementary.sh` | **Mac mini** | on/off, clutch, play types, tracking, hustle, defense, estimated, slim 2/3-man lineups |

---

## One-time setup

```bash
# 1. Clone to the path the mini's launchd plist expects (skip if already cloned)
cd ~/Documents
git clone git@github.com:meharpalbasi/NBA_Lineup202324_Analyzer.git
cd NBA_Lineup202324_Analyzer

# 2. Create the virtualenv the script expects (./venv) and install runtime deps
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements-pipeline.txt   # nba_api, pandas, curl_cffi

# 3. Let git PUSH non-interactively from the mini:
#    - SSH (the clone URL above uses SSH): ensure the mini's key is on GitHub (test: ssh -T git@github.com), OR
#    - HTTPS + keychain: git config --global credential.helper osxkeychain, then one manual push.
git config user.name  "Meharpal Basi"
git config user.email "meharpalbasi45@gmail.com"
```

## Health check — use THIS, not a bare curl one-liner

```bash
./venv/bin/python -m pipeline.main --supplementary-only --dry-run
# Expect: "✓ Health check passed — got NNNN rows."
```

> ⚠️ Do **not** test reachability with `curl_cffi … r.get('…/stats/scoreboardv2?…', impersonate='chrome')`.
> The `/stats/*` endpoints **hang ~20s with no response when the NBA headers are missing**,
> even on a perfectly healthy machine — so that one-liner is a false negative. The dry-run
> above exercises the real path (nba_api + the curl_cffi patch + the required headers).
> Background: [`docs/MINI_NBA_BLOCK_DEBUG.md`](../docs/MINI_NBA_BLOCK_DEBUG.md).

## Smoke-test the full publish

```bash
bash scripts/run_supplementary.sh   # pulls main, ~220 calls, commits + pushes CSVs if changed
```

## Install the weekly schedule (launchd)

Use the **`.mini`** plist — the non-`.mini` one has the laptop's paths — installed under
the label filename:

```bash
cp scripts/com.nbalineup.supplementary.mini.plist ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
launchctl load -w ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
launchctl start com.nbalineup.supplementary     # run once now to confirm launchd can drive it
launchctl list | grep nbalineup
tail -f scripts/logs/launchd.out.log scripts/logs/launchd.err.log
```

> If this mini's username / clone path isn't `/Users/meharpal/Documents/NBA_Lineup202324_Analyzer`,
> edit the three hard-coded paths in `scripts/com.nbalineup.supplementary.mini.plist` before copying it.

Default schedule: **Mondays 08:00 local**. If the mini is asleep/off then, launchd runs the
job on next wake. To change the time, edit `StartCalendarInterval` in the `.mini` plist, then
reload:

```bash
launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
cp scripts/com.nbalineup.supplementary.mini.plist ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
launchctl load -w ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
```

## Retire the laptop's job (do this once, on the laptop)

So the laptop and mini don't both push and race:

```bash
# ON THE LAPTOP (not the mini):
launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
```

The laptop can still publish on demand with `bash scripts/run_supplementary.sh`; only its
*scheduled* agent is retired.

## Useful commands

```bash
launchctl list | grep nbalineup                                            # is it loaded?
launchctl start com.nbalineup.supplementary                                # run now
launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist  # disable
```

## See also
- [`docs/MINI_NBA_BLOCK_DEBUG.md`](../docs/MINI_NBA_BLOCK_DEBUG.md) — why the "block" was a false negative.
- [`docs/DATA_SOURCES.md`](../docs/DATA_SOURCES.md) — the whole data-publishing picture.
