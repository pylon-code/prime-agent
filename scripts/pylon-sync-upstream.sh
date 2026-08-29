#!/usr/bin/env bash
set -euo pipefail

repo="${GITHUB_REPOSITORY:-pylon-code/prime-agent}"
expected_repo="pylon-code/prime-agent"
blocker_title="Prime upstream sync blocked"
candidate_title="Prime upstream sync candidate ready"

export CLICOLOR=0
export CLICOLOR_FORCE=0
export FORCE_COLOR=0
export NO_COLOR=1

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

find_candidate_issue() {
  gh issue list \
    --repo "$repo" \
    --state open \
    --search "$candidate_title in:title" \
    --json number,title \
    --jq "map(select(.title == \"$candidate_title\")) | .[0].number // empty"
}

candidate_verification_succeeded() {
  local candidate_sha="$1"
  local verification_url="$2"
  local run_id="${verification_url##*/}"
  local run_file
  local jobs_file
  run_file="$(mktemp)"
  jobs_file="$(mktemp)"

  if [[ ! "$run_id" =~ ^[0-9]+$ ]]; then
    rm -f "$run_file" "$jobs_file"
    return 1
  fi
  if ! gh api "repos/$repo/actions/runs/$run_id" > "$run_file" ||
    ! gh api "repos/$repo/actions/runs/$run_id/jobs?per_page=100" > "$jobs_file"; then
    rm -f "$run_file" "$jobs_file"
    return 1
  fi
  if ! jq -e --arg repo "$repo" '
    .status == "completed"
    and .conclusion == "success"
    and (.event == "workflow_dispatch" or .event == "schedule")
    and .head_branch == "pylon"
    and .name == "Pylon upstream sync"
    and .path == ".github/workflows/pylon-upstream-sync.yml"
    and .repository.full_name == $repo
  ' "$run_file" > /dev/null; then
    rm -f "$run_file" "$jobs_file"
    return 1
  fi
  if ! jq -e --arg candidate "$candidate_sha" '
    any(.jobs[];
      .status == "completed"
      and .conclusion == "success"
      and (.name | contains($candidate))
      and (.name | endswith("/ build-check-test"))
    )
  ' "$jobs_file" > /dev/null; then
    rm -f "$run_file" "$jobs_file"
    return 1
  fi

  rm -f "$run_file" "$jobs_file"
}

report_candidate() {
  local details="$1"
  local body_file
  local issue_number
  body_file="$(mktemp)"
  printf '%s\n' "$details" > "$body_file"
  issue_number="$(find_candidate_issue)"

  if [[ -n "$issue_number" ]]; then
    gh issue comment "$issue_number" --repo "$repo" --body-file "$body_file"
  else
    gh issue create --repo "$repo" --title "$candidate_title" --body-file "$body_file"
  fi
  rm -f "$body_file"
}

close_candidate_issue() {
  local resolution="$1"
  local issue_number
  local body_file
  issue_number="$(find_candidate_issue)"
  if [[ -z "$issue_number" ]]; then
    return
  fi

  body_file="$(mktemp)"
  printf '%s\n' "$resolution" > "$body_file"
  gh issue comment "$issue_number" --repo "$repo" --body-file "$body_file"
  gh issue close "$issue_number" --repo "$repo"
  rm -f "$body_file"
}

fetch_sync_refs() {
  git fetch --no-tags origin \
    +refs/heads/main:refs/remotes/origin/main \
    +refs/heads/pylon:refs/remotes/origin/pylon
  git fetch --no-tags upstream +refs/heads/main:refs/remotes/upstream/main
}

verify_mirror_rules() {
  local repository_file
  local rules_file
  local ruleset_file
  local error_file
  local ruleset_id
  repository_file="$(mktemp)"
  rules_file="$(mktemp)"
  ruleset_file="$(mktemp)"
  error_file="$(mktemp)"

  if ! gh api "repos/$repo" > "$repository_file" 2> "$error_file"; then
    cat "$error_file" >&2
    sync_error="The workflow could not verify the fork parent. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi
  if ! jq -e '
    .fork == true
    and .parent.full_name == "PrimeIntellect-ai/prime-agent"
    and .parent.default_branch == "main"
  ' "$repository_file" > /dev/null; then
    sync_error="The repository is no longer a fork of \`PrimeIntellect-ai/prime-agent\` with upstream default branch \`main\`. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi

  if ! gh api "repos/$repo/rules/branches/main" > "$rules_file" 2> "$error_file"; then
    cat "$error_file" >&2
    sync_error="The workflow could not read the effective \`main\` rules. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi
  if ! ruleset_id="$(jq -er --arg repo "$repo" '
    map(select(
      .ruleset_source_type == "Repository"
      and .ruleset_source == $repo
    ))
    | group_by(.ruleset_id)
    | map(select(
      any(.[]; .type == "update" and .parameters.update_allows_fetch_and_merge == true)
      and any(.[]; .type == "deletion")
      and any(.[]; .type == "required_linear_history")
      and any(.[]; .type == "non_fast_forward")
    ))
    | if length == 1 then .[0][0].ruleset_id else empty end
  ' "$rules_file")"; then
    sync_error="The effective \`main\` rules are not fail-closed. One repository ruleset with no bypass actors must block updates except fork sync, require linear history, and forbid force-pushes and deletion. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi

  if ! gh api "repos/$repo/rulesets/$ruleset_id" > "$ruleset_file" 2> "$error_file"; then
    cat "$error_file" >&2
    sync_error="The workflow could not inspect the effective \`main\` ruleset. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi
  if ! jq -e --arg repo "$repo" --argjson id "$ruleset_id" '
    .id == $id
    and .source_type == "Repository"
    and .source == $repo
    and .target == "branch"
    and .enforcement == "active"
    and .current_user_can_bypass == "never"
    and .conditions.ref_name.include == ["refs/heads/main"]
    and .conditions.ref_name.exclude == []
    and ((.bypass_actors // []) | length == 0)
  ' "$ruleset_file" > /dev/null; then
    sync_error="The effective \`main\` ruleset is not active, exact-branch-only, or non-bypassable by this workflow token. No mirror update was attempted."
    rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
    return 1
  fi

  rm -f "$repository_file" "$rules_file" "$ruleset_file" "$error_file"
  echo "Verified fail-closed main ruleset $ruleset_id"
}

sync_fork_main() {
  local response_file
  local error_file
  local api_base_branch
  local api_merge_type

  if ! verify_mirror_rules; then
    return 1
  fi

  response_file="$(mktemp)"
  error_file="$(mktemp)"
  if ! gh api --method POST "repos/$repo/merge-upstream" -f branch=main \
    > "$response_file" 2> "$error_file"; then
    cat "$error_file" >&2
    sync_error="GitHub's fork synchronization API could not update \`main\`. No direct push was attempted. Inspect the failed workflow run, restore \`main\` as an exact Prime mirror if needed, and rerun synchronization."
    rm -f "$response_file" "$error_file"
    return 1
  fi

  if ! api_merge_type="$(jq -er '.merge_type | select(type == "string")' "$response_file")" ||
    ! api_base_branch="$(jq -er '.base_branch | select(type == "string")' "$response_file")"; then
    sync_error="GitHub's fork synchronization API returned malformed output. No direct push fallback was attempted."
    rm -f "$response_file" "$error_file"
    return 1
  fi
  rm -f "$response_file" "$error_file"

  if [[ "$api_base_branch" != "PrimeIntellect-ai:main" ]]; then
    sync_error="GitHub's fork synchronization API reported unexpected source \`$api_base_branch\` instead of \`PrimeIntellect-ai:main\`. The mirror source invariant failed; inspect \`main\` manually."
    return 1
  fi
  if [[ "$api_merge_type" == "merge" ]]; then
    sync_error="GitHub's fork synchronization API reported a merge commit despite \`main\` requiring linear history. The mirror protection invariant failed; inspect and restore \`main\` manually. The workflow will not reset or force-push it."
    return 1
  fi
  if [[ "$api_merge_type" != "fast-forward" && "$api_merge_type" != "none" ]]; then
    sync_error="GitHub's fork synchronization API returned unexpected merge type \`$api_merge_type\`. No direct push fallback was attempted."
    return 1
  fi

  echo "GitHub fork synchronization result: $api_merge_type"
}

git config user.name "pylon-upstream-sync[bot]"
git config user.email "pylon-upstream-sync[bot]@users.noreply.github.com"
git remote add upstream https://github.com/PrimeIntellect-ai/prime-agent.git 2>/dev/null || \
  git remote set-url upstream https://github.com/PrimeIntellect-ai/prime-agent.git
git remote set-url --push upstream disabled://PrimeIntellect-ai/prime-agent
fetch_sync_refs

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

mirror_settled=false
for attempt in 1 2; do
  if ! sync_fork_main; then
    report_blocker "$sync_error"
    exit 1
  fi

  fetch_sync_refs
  origin_main="$(git rev-parse refs/remotes/origin/main)"
  origin_pylon="$(git rev-parse refs/remotes/origin/pylon)"
  upstream_sha="$(git rev-parse refs/remotes/upstream/main)"
  if [[ "$origin_main" == "$upstream_sha" ]]; then
    mirror_settled=true
    break
  fi
  if ! git merge-base --is-ancestor "$origin_main" "$upstream_sha"; then
    report_blocker "The mirror branch diverged while GitHub's fork synchronization API was running.

- origin/main: \`$origin_main\`
- upstream/main: \`$upstream_sha\`

Restore \`main\` as an exact upstream mirror manually. The workflow will not reset or force-push it."
    exit 1
  fi
  if [[ "$attempt" == 1 ]]; then
    echo "Prime advanced during fork synchronization; retrying once."
  fi
done

if [[ "$mirror_settled" != true ]]; then
  report_blocker "GitHub's fork synchronization API did not settle \`main\` at the current Prime head after two attempts.

- origin/main: \`$origin_main\`
- upstream/main: \`$upstream_sha\`

No direct push fallback was attempted. Rerun synchronization after Prime's branch settles."
  exit 1
fi

if git merge-base --is-ancestor "$upstream_sha" "$origin_pylon"; then
  close_blocker "The product branch now contains Prime upstream \`$upstream_sha\`; closing this blocker." ||
    echo "Could not close the previous sync blocker." >&2
  close_candidate_issue "The product branch now contains Prime upstream \`$upstream_sha\`; closing this settled candidate." ||
    echo "Could not close the previous candidate-ready issue." >&2
  echo "pylon already contains upstream/main at $upstream_sha"
  exit 0
fi

open_pr="$(gh pr list \
  --repo "$repo" \
  --base pylon \
  --state open \
  --limit 100 \
  --json number,headRefName,headRepositoryOwner,url \
  --jq 'map(select(.headRepositoryOwner.login == "pylon-code" and (.headRefName | startswith("automation/prime-upstream-")))) | if length > 0 then "\(.[0].number)\t\(.[0].url)" else "" end')"
if [[ -n "$open_pr" ]]; then
  IFS=$'\t' read -r open_number open_url <<< "$open_pr"
  emit_output pr_url "$open_url"
  close_candidate_issue "A pull request now owns this candidate: $open_url" ||
    echo "Could not close the candidate-ready issue." >&2
  echo "Upstream sync pull request #$open_number is already open: $open_url"
  echo "The mirror branch was updated; the next run after that PR settles will collect later commits."
  exit 0
fi

short_sha="${upstream_sha:0:12}"
pylon_short_sha="${origin_pylon:0:12}"
branch="automation/prime-upstream-$short_sha-from-$pylon_short_sha"
base_sha="$(git merge-base "$origin_pylon" "$upstream_sha")"
server_url="${GITHUB_SERVER_URL:-https://github.com}"
current_verification_url="$server_url/$repo/actions/runs/${GITHUB_RUN_ID:-manual}"
git switch --detach "$origin_pylon"
git switch -c "$branch"

if ! git merge --no-ff --no-commit "$upstream_sha"; then
  conflicts="$(git diff --name-only --diff-filter=U | awk 'BEGIN { first = 1 } { if (!first) printf ", "; printf "%s", $0; first = 0 } END { print "" }')"
  git merge --abort
  report_blocker "Prime upstream \`$upstream_sha\` conflicts with Pylon \`$origin_pylon\`.

Conflicted files: ${conflicts:-unknown}

Resolve this on a task branch from \`pylon\`. Preserve Pylon behavior, search the overlapping Prime work, and record an adopt, hybridize, retain, or redesign decision in the fork ledger."
  exit 1
fi

git -c commit.gpgsign=false commit \
  -m "chore: merge Prime upstream through $short_sha" \
  -m "Pylon-Sync-Verification: $current_verification_url"
local_candidate_sha="$(git rev-parse HEAD)"
local_candidate_tree="$(git rev-parse "HEAD^{tree}")"
expected_parents="$origin_pylon $upstream_sha"
if ! remote_candidate="$(git ls-remote --heads origin "refs/heads/$branch")"; then
  report_blocker "The workflow could not inspect the deterministic candidate branch \`$branch\`. No candidate push was attempted."
  exit 1
fi

candidate_reused=false
candidate_original_verification_url=""
if [[ -n "$remote_candidate" ]]; then
  git fetch --no-tags origin "+refs/heads/$branch:refs/remotes/origin/$branch"
  candidate_sha="$(git rev-parse "refs/remotes/origin/$branch")"
  candidate_parents="$(git show -s --format=%P "$candidate_sha")"
  candidate_tree="$(git rev-parse "$candidate_sha^{tree}")"
  candidate_original_verification_url="$(git show -s --format=%B "$candidate_sha" |
    sed -n 's/^Pylon-Sync-Verification: //p' | tail -n 1)"
  if [[ "$candidate_parents" != "$expected_parents" || "$candidate_tree" != "$local_candidate_tree" ]]; then
    report_blocker "The deterministic candidate branch \`$branch\` already exists but does not match the expected merge.

- Existing candidate: \`$candidate_sha\`
- Expected parents: \`$expected_parents\`
- Actual parents: \`$candidate_parents\`

Inspect the branch manually. The workflow will not overwrite or force-push it."
    exit 1
  fi
  if [[ "$candidate_original_verification_url" != "$server_url/$repo/actions/runs/"* ]]; then
    report_blocker "The deterministic candidate branch \`$branch\` has no valid Pylon verification-run trailer. Inspect the branch manually. The workflow will not overwrite or force-push it."
    exit 1
  fi
  candidate_reused=true
  echo "Reusing unchanged candidate branch $branch at $candidate_sha"
else
  git push origin "HEAD:refs/heads/$branch"
  candidate_sha="$local_candidate_sha"
fi

candidate_issue_matches=false
candidate_issue_number=""
candidate_needs_verification=true
verification_url="$current_verification_url"
if [[ "$candidate_reused" == true ]]; then
  candidate_issue_number="$(find_candidate_issue)"
  if [[ -n "$candidate_issue_number" ]]; then
    issue_state_file="$(mktemp)"
    if gh issue view "$candidate_issue_number" --repo "$repo" --json body,comments > "$issue_state_file" &&
      jq -e --arg candidate "$candidate_sha" --arg verification "$candidate_original_verification_url" '
        ([.body] + [.comments[].body])
        | any(.[]; contains($candidate) and contains($verification))
      ' "$issue_state_file" > /dev/null &&
      candidate_verification_succeeded "$candidate_sha" "$candidate_original_verification_url"; then
      candidate_issue_matches=true
      candidate_needs_verification=false
      verification_url="$candidate_original_verification_url"
    else
      echo "Candidate-ready issue #$candidate_issue_number does not prove successful verification for $candidate_sha; refreshing it."
    fi
    rm -f "$issue_state_file"
  fi
fi

body_file="$(mktemp)"
pr_title="chore: sync Prime upstream through $short_sha"
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

encoded_title="$(printf '%s' "$pr_title" | jq -sRr @uri)"
encoded_body="$(jq -sRr @uri < "$body_file")"
pr_url="$server_url/$repo/compare/pylon...$branch?expand=1&title=$encoded_title&body=$encoded_body"
candidate_details="$(cat <<EOF
A clean Prime upstream candidate is ready for human review.

- Candidate branch: \`$branch\`
- Candidate commit: \`$candidate_sha\`
- Trusted candidate CI: $verification_url
- Open the prefilled pull request: $pr_url

When the form opens:

1. Keep the prefilled title \`$pr_title\` and body below.
2. Use **Create draft pull request**, not **Create pull request**.
3. Apply the \`pylon-upstream-sync\` label.
4. Keep \`pylon\` as the base and \`$branch\` as the head.

<details>
<summary>Expected pull request body</summary>

$(cat "$body_file")

</details>

The built-in GitHub Actions token cannot create the pull request. Mirror automation intentionally has no separate PAT or app token.
EOF
)"
rm -f "$body_file"

if [[ "$candidate_issue_matches" == true ]]; then
  echo "Candidate-ready issue #$candidate_issue_number already tracks this unchanged branch."
else
  report_candidate "$candidate_details"
fi

echo "Candidate branch is ready; a maintainer must open the draft pull request: $pr_url"
if [[ "$candidate_needs_verification" == true ]]; then
  emit_output candidate_sha "$candidate_sha"
fi
emit_output pr_url "$pr_url"
close_blocker "A clean synchronization candidate is available: $pr_url" ||
  echo "Could not close the previous sync blocker." >&2
