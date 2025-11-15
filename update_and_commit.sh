#!/bin/bash
set -e

echo "Starting NBA Lineup data fetch..."

# Run the Python script to fetch data
python fetchlineups.py

echo "Fetch complete. Checking for changes..."

# Configure git
git config user.name "${GIT_USER_NAME:-github-actions[bot]}"
git config user.email "${GIT_USER_EMAIL:-github-actions[bot]@users.noreply.github.com}"

# Add the data directory
git add data/

# Check if there are changes to commit
if git diff --staged --quiet; then
    echo "No changes to commit. Data is up to date."
else
    echo "Changes detected. Committing..."
    git commit -m "chore: update NBA lineup data - $(date '+%Y-%m-%d')"

    echo "Pushing to GitHub..."
    git push

    echo "Successfully pushed updates to GitHub!"
fi

echo "Update complete."
