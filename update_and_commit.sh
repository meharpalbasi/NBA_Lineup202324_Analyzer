#!/bin/bash
set -e

# Initialize git repository FIRST (before fetching data)
if [ ! -d ".git" ]; then
    echo "Initializing git repository..."

    # Validate required environment variables
    if [ -z "$GITHUB_TOKEN" ] || [ -z "$GITHUB_REPO" ]; then
        echo "Error: GITHUB_TOKEN and GITHUB_REPO environment variables are required"
        echo "Example: GITHUB_REPO=username/nbalineup"
        exit 1
    fi

    # Initialize git
    git init -b main
    git config user.name "${GIT_USER_NAME:-Railway Bot}"
    git config user.email "${GIT_USER_EMAIL:-bot@railway.app}"

    # Add remote with authentication
    git remote add origin "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"

    # Fetch from GitHub
    echo "Fetching from GitHub..."
    git fetch origin main

    # Create/reset main branch to match origin/main exactly
    echo "Syncing with remote repository..."
    git checkout -B main origin/main
    git branch --set-upstream-to=origin/main main

    echo "Git repository initialized and synced with remote"
else
    echo "Git repository already initialized"

    # Configure git user
    git config user.name "${GIT_USER_NAME:-Railway Bot}"
    git config user.email "${GIT_USER_EMAIL:-bot@railway.app}"

    # Update remote URL with token if GITHUB_TOKEN is set
    if [ -n "$GITHUB_TOKEN" ] && [ -n "$GITHUB_REPO" ]; then
        git remote set-url origin "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"
    fi

    # Make sure we're on main branch and pull latest changes
    git checkout main 2>/dev/null || git checkout -b main
    echo "Pulling latest changes from GitHub..."
    git pull origin main --rebase || true
fi

echo "Starting NBA Lineup data fetch..."

# Run the Python script to fetch data
python fetchlineups.py

echo "Fetch complete. Checking for changes..."

# Add the data directory
git add data/

# Check if there are changes to commit
if git diff --staged --quiet; then
    echo "No changes to commit. Data is up to date."
else
    echo "Changes detected. Committing..."
    git commit -m "chore: update NBA lineup data - $(date '+%Y-%m-%d')"

    echo "Pushing to GitHub..."
    git push origin main

    echo "Successfully pushed updates to GitHub!"
fi

echo "Update complete."
