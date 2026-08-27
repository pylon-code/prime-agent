#!/usr/bin/env bash
set -euo pipefail

repo="${GITHUB_REPOSITORY:-pylon-code/prime-agent}"
expected_repo="pylon-code/prime-agent"
blocker_title="Prime upstream sync blocked"

emit_output() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"
  fi
}

if [[ "$repo" != "$expected_repo" ]]; then
  echo "Refusing to synchronize unexpected repository: $repo" >&2
  exit 1
fi

find_blocker() {
  gh issue list \
    --repo "$repo" \
    --state open \
    --search "$blocker_title in:title" \
    --json number,title \
    --jq "map(select(.title == \"$blocker_title\")) | .[0].number // empty"
}

report_blocker() {
  local details="$1"
  local body_file
  local issue_number
  body_file="$(mktemp)"
  printf '%s\n' "$details" > "$body_file"
  issue_number="$(find_blocker)"

  if [[ -n "$issue_number" ]]; then
    gh issue comment "$issue_number" --repo "$repo" --body-file "$body_file"
  else
    gh issue create --repo "$repo" --title "$blocker_title" --body-file "$body_file"
  fi
  rm -f "$body_file"
}

close_blocker() {
  local resolution="$1"
  local issue_number
  local body_file
  issue_number="$(find_blocker)"
  if [[ -z "$issue_number" ]]; then
    return
  fi

  body_file="$(mktemp)"
  printf '%s\n' "$resolution" > "$body_file"
  gh issue comment "$issue_number" --repo "$repo" --body-file "$body_file"
  gh issue close "$issue_number" --repo "$repo"
  rm -f "$body_file"
}

git config user.name "pylon-upstream-sync[bot]"
git config user.email "pylon-upstream-sync[bot]@users.noreply.github.com"
git remote add upstream https://github.com/PrimeIntellect-ai/prime-agent.git 2>/dev/null || \
  git remote set-url upstream https://github.com/PrimeIntellect-ai/prime-agent.git
git remote set-url --push upstream disabled://PrimeIntellect-ai/prime-agent
git fetch --no-tags origin \
  +refs/heads/main:refs/remotes/origin/main \
  +refs/heads/pylon:refs/remotes/origin/pylon
git fetch --no-tags upstream +refs/heads/main:refs/remotes/upstream/main

origin_main="$(git rev-parse refs/remotes/origin/main)"
origin_pylon="$(git rev-parse refs/remotes/origin/pylon)"
upstream_sha="$(git rev-parse refs/remotes/upstream/main)"

if ! git merge-base --is-ancestor "$origin_main" "$upstream_sha"; then
  report_blocker "The mirror branch has diverged and cannot be fast-forwarded safely.

- origin/main: \`$origin_main\`
- upstream/main: \`$upstream_sha\`

Restore \`main\` as an exact upstream mirror manually. The workflow will not reset or force-push it."
  exit 1
fi

git push origin "$upstream_sha":refs/heads/main

if git merge-base --is-ancestor "$upstream_sha" "$origin_pylon"; then
  close_blocker "The product branch now contains Prime upstream \`$upstream_sha\`; closing this blocker." ||
    echo "Could not close the previous sync blocker." >&2
  echo "pylon already contains upstream/main at $upstream_sha"
  exit 0
fi

open_pr="$(gh pr list \
  --repo "$repo" \
  --base pylon \
  --state open \
  --limit 100 \
  --json number,headRefName,headRefOid,headRepositoryOwner,url,labels \
  --jq 'map(select(.headRepositoryOwner.login == "pylon-code" and (.headRefName | startswith("automation/prime-upstream-")) and any(.labels[]?; .name == "pylon-upstream-sync"))) | if length > 0 then "\(.[0].number)\t\(.[0].url)\t\(.[0].headRefOid)" else "" end')"
if [[ -n "$open_pr" ]]; then
  IFS=$'\t' read -r open_number open_url open_sha <<< "$open_pr"
  emit_output candidate_sha "$open_sha"
  emit_output pr_url "$open_url"
  echo "Upstream sync pull request #$open_number is already open: $open_url"
  echo "The mirror branch was updated; the next run after that PR settles will collect later commits."
  exit 0
fi

short_sha="${upstream_sha:0:12}"
run_suffix="${GITHUB_RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
branch="automation/prime-upstream-$short_sha-$run_suffix"
base_sha="$(git merge-base "$origin_pylon" "$upstream_sha")"
git switch --detach "$origin_pylon"
git switch -c "$branch"

if ! git merge --no-ff --no-edit "$upstream_sha"; then
  conflicts="$(git diff --name-only --diff-filter=U | awk 'BEGIN { first = 1 } { if (!first) printf ", "; printf "%s", $0; first = 0 } END { print "" }')"
  git merge --abort
  report_blocker "Prime upstream \`$upstream_sha\` conflicts with Pylon \`$origin_pylon\`.

Conflicted files: ${conflicts:-unknown}

Resolve this on a task branch from \`pylon\`. Preserve Pylon behavior, search the overlapping Prime work, and record an adopt, hybridize, retain, or redesign decision in the fork ledger."
  exit 1
fi

git push origin "HEAD:refs/heads/$branch"

body_file="$(mktemp)"
server_url="${GITHUB_SERVER_URL:-https://github.com}"
verification_url="$server_url/$repo/actions/runs/${GITHUB_RUN_ID:-manual}"
cat > "$body_file" <<EOF
## Upstream range

- Base previously shared with Pylon: \`$base_sha\`
- Prime upstream head: \`$upstream_sha\`
- Compare: https://github.com/PrimeIntellect-ai/prime-agent/compare/$base_sha...$upstream_sha

## Review requirements

- Inspect upstream source, issues, pull requests, and release notes for overlaps with \`.pylon/features.yaml\`.
- Preserve Pylon behavior unless the ledger records an explicit adopt, hybridize, retain, or redesign decision.
- Resolve conflicts manually. This workflow never auto-resolves them.
- Review the source before marking this draft ready. Merge only with a merge commit.
- Trusted candidate CI runs without secrets or persisted Git credentials: $verification_url
- Run affected Pylon integration checks before merging when an upstream change touches a Pylon integration boundary.

Created by the daily Pylon upstream synchronization workflow.
EOF

pr_url="$(gh pr create \
  --repo "$repo" \
  --base pylon \
  --head "$branch" \
  --draft \
  --label pylon-upstream-sync \
  --title "chore: sync Prime upstream through $short_sha" \
  --body-file "$body_file")"
rm -f "$body_file"

candidate_sha="$(git rev-parse HEAD)"
emit_output candidate_sha "$candidate_sha"
emit_output pr_url "$pr_url"
close_blocker "A clean synchronization pull request is available: $pr_url" ||
  echo "Could not close the previous sync blocker." >&2
echo "Created draft pull request $pr_url"
