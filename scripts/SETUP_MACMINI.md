# Mac mini setup — supplementary data publisher

The Mac mini runs the **modular pipeline's supplementary fetch** on a home/residential
IP and pushes the rich data to GitHub. This is the data Railway does *not* produce
(on/off, clutch, play types, tracking, hustle, defense tracking, estimated metrics).

Why the Mac mini and not the cloud: `stats.nba.com` blocks cloud/datacenter IPs
(AWS/GCP/Railway/GitHub Actions). A residential machine reaches it fine.

| Job | Where | Produces |
|-----|-------|----------|
| `update_and_commit.sh` (`fetchlineups.py`) | Railway | legacy lineup CSV (`NBALineup…BaseAdvanced.csv`) |
| `scripts/run_supplementary.sh` | **Mac mini (this)** | on/off, clutch, play types, tracking, hustle, defense, estimated |

---

## One-time setup

```bash
# 1. Clone the repo (skip if already cloned)
cd ~/Documents
git clone git@github.com:meharpalbasi/NBA_Lineup202324_Analyzer.git nbalineup_backend
cd nbalineup_backend

# 2. Create the virtualenv the script expects (./venv)
python3 -m venv venv
./venv/bin/pip install --upgrade pip
# Minimal runtime deps (works on the system Python 3.9). The full
# requirements.txt is a dev freeze that needs Python 3.10+ and isn't required.
./venv/bin/pip install -r requirements-pipeline.txt

# 3. Make sure git can PUSH non-interactively from the mini:
#    - SSH remote (recommended): the clone URL above already uses SSH; ensure
#      your SSH key is added (ssh-add) and registered on GitHub, OR
#    - HTTPS + credential helper: `git config --global credential.helper osxkeychain`
#      and do one manual push to cache the token.
git config user.name  "Meharpal Basi"
git config user.email "meharpalbasi45@gmail.com"

# 4. Smoke-test the fetch end to end (will commit+push if data changed):
bash scripts/run_supplementary.sh
```

> If you cloned to a path other than `~/Documents/nbalineup_backend`, edit the three
> hard-coded paths in `scripts/com.nbalineup.supplementary.plist` before installing it.

## Install the schedule (launchd)

```bash
# Copy the agent into place and load it
cp scripts/com.nbalineup.supplementary.plist ~/Library/LaunchAgents/
launchctl load  ~/Library/LaunchAgents/com.nbalineup.supplementary.plist

# Run it once now to confirm launchd can drive it
launchctl start com.nbalineup.supplementary

# Watch the logs
tail -f scripts/logs/launchd.out.log scripts/logs/launchd.err.log
```

Default schedule: **Mondays 08:00 local**. If the mini is asleep/off at that time,
launchd runs the job as soon as it wakes. To change the time, edit
`StartCalendarInterval` in the plist, then:

```bash
launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
cp scripts/com.nbalineup.supplementary.plist ~/Library/LaunchAgents/
launchctl load   ~/Library/LaunchAgents/com.nbalineup.supplementary.plist
```

## Useful commands

```bash
launchctl list | grep nbalineup          # is it loaded?
launchctl start com.nbalineup.supplementary   # run now
launchctl unload ~/Library/LaunchAgents/com.nbalineup.supplementary.plist  # disable
```
