#!/usr/bin/env bash
# Setup git remotes for service-beds
#
#   origin  → git.ucsc.edu (GitLab) — primary, push/fetch
#   github  → github.com           — mirror, push/fetch separately
#
# Usage:
#   ./scripts/setup-remotes.sh          # fresh clone setup
#   ./scripts/setup-remotes.sh --force  # overwrite existing remotes
#
set -euo pipefail

GITLAB_URL="git@git.ucsc.edu:microfaults/atropos-go.git"
GITHUB_URL="git@github.com:microfaults/atropos-go.git"

force=false
[[ "${1:-}" == "--force" ]] && force=true

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  echo "Error: not inside a git repository." >&2
  exit 1
fi

setup_remote() {
  local name="$1" url="$2"
  if git remote get-url "$name" &>/dev/null; then
    if $force; then
      echo "  Replacing remote '$name' → $url"
      git remote remove "$name"
      git remote add "$name" "$url"
    else
      echo "  Remote '$name' already exists ($(git remote get-url "$name")). Use --force to overwrite."
    fi
  else
    echo "  Adding remote '$name' → $url"
    git remote add "$name" "$url"
  fi
}

echo "Setting up remotes..."
setup_remote origin "$GITLAB_URL"
setup_remote github "$GITHUB_URL"

echo ""
echo "Result:"
git remote -v
echo ""
echo "Usage:"
echo "  git push origin <branch>   # push to GitLab (git.ucsc.edu)"
echo "  git push github <branch>   # push to GitHub"
echo "  git fetch origin            # fetch from GitLab"
echo "  git fetch github            # fetch from GitHub"
