# Railway Deployment Guide

This guide will help you deploy the NBA Lineup data fetcher as a cron job on Railway.

## Prerequisites

1. A [Railway](https://railway.app) account
2. A GitHub Personal Access Token (PAT) with `repo` permissions

## Setup Steps

### 1. Create GitHub Personal Access Token

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a name like "Railway NBA Lineup Updater"
4. Select scope: **`repo`** (Full control of private repositories)
5. Generate and **copy the token** (you won't see it again!)

### 2. Deploy to Railway

1. Go to [Railway Dashboard](https://railway.app/dashboard)
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Choose this repository (`nbalineup`)
5. Railway will auto-detect the `railway.json` configuration

### 3. Configure Environment Variables

In your Railway project, go to **Variables** and add:

| Variable | Value | Description |
|----------|-------|-------------|
| `GIT_USER_NAME` | `Railway Bot` | Name for git commits (optional) |
| `GIT_USER_EMAIL` | `bot@railway.app` | Email for git commits (optional) |
| `NBA_SEASON` | `2025-26` | NBA season to fetch (optional, defaults to 2025-26) |

**Important:** Railway needs to authenticate with GitHub to push changes.

#### Option A: Using GitHub Token in Remote URL
Railway will need to push to GitHub. The script uses `git push`, which will work if:
- Your repository is public, OR
- You configure git credentials

To configure git credentials, you can modify the repository's remote URL to include the token:
```bash
git remote set-url origin https://${GITHUB_TOKEN}@github.com/yourusername/nbalineup.git
```

You'll need to add `GITHUB_TOKEN` as an environment variable in Railway with your PAT.

#### Option B: Using SSH (Recommended)
1. Generate an SSH key pair
2. Add the private key to Railway as an environment variable
3. Add the public key to your GitHub account's SSH keys
4. Update `update_and_commit.sh` to configure SSH

### 4. Cron Schedule

The cron is configured in `railway.json`:
```json
"cronSchedule": "0 0 */2 * *"
```

This runs at **midnight (UTC) every 2 days**.

To modify the schedule, edit `railway.json`:
- `0 0 */2 * *` - Every 2 days at midnight
- `0 0 * * *` - Daily at midnight
- `0 */12 * * *` - Every 12 hours
- `0 0 * * 0` - Weekly (every Sunday)

### 5. Monitor the Job

- View logs in Railway dashboard under your service
- Check your GitHub repository for automated commits with message: `chore: update NBA lineup data - YYYY-MM-DD`
- CSV files will be updated in the `data/` folder

## How It Works

1. Railway runs `update_and_commit.sh` on the cron schedule
2. The script runs `fetchlineups.py` to fetch NBA lineup data
3. Data is saved to `data/NBALineup{season}_RegSeason_Playoffs_BaseAdvanced.csv`
4. Git detects changes and commits them
5. Changes are pushed back to your GitHub repository

## Cost Estimate

- **Compute**: ~2-5 minutes per run
- **Frequency**: Every 2 days = ~15 runs/month
- **Total**: ~75 minutes/month
- **Cost**: $0-1/month (well within Railway's free tier)

## Troubleshooting

### No data being pushed
- Check Railway logs for errors
- Verify GitHub PAT has correct permissions
- Ensure git remote URL includes token or SSH is configured

### API rate limiting
- NBA API has rate limits; the script includes delays (0.6s between calls)
- If you encounter issues, increase delays in `fetchlineups.py`

### Season data not found
- Update `NBA_SEASON` environment variable to current season
- Format: `YYYY-YY` (e.g., `2025-26`)

## Manual Trigger

To run the job manually (outside the cron schedule):
1. Go to Railway dashboard
2. Select your service
3. Click "Deploy" → "Redeploy"
