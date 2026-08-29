# Pylon Prime Agent fork

This repository is Pylon's long-lived Prime Agent fork. It follows Prime upstream closely while allowing Pylon-owned integration behavior to remain when upstream has no suitable equivalent.

## Branches and remotes

- `main` is an exact fast-forward mirror of `PrimeIntellect-ai/prime-agent:main`. Never commit Pylon changes to it.
- `pylon` is the default product branch. Branch feature work from the latest `origin/pylon` and open pull requests back to `pylon`.
- The local `upstream` remote must be fetch-only. Never push to a Prime Intellect remote.
- Do not hard-reset, force-push, or wholesale rebase `pylon` onto upstream.

While this repository has only one write-capable human, `pylon` requires pull requests, strict hosted checks, conversation resolution, and recorded maintainer approval, but zero GitHub approvals. The protection applies to administrators so direct pushes cannot bypass it. Raise the required approval count when a second eligible reviewer exists.

The daily `Pylon upstream sync` workflow fast-forwards `main`, prepares a clean merge candidate, and runs it through the trusted `pylon` CI definition with no secrets or persisted Git credentials. With a narrowly scoped `PYLON_SYNC_PR_TOKEN`, it opens a draft merge pull request. Without that token, it opens or updates one candidate-ready issue with the exact PR link. A conflict opens or updates one blocker issue and requires a human resolution branch from `pylon`; the workflow never resolves conflicts automatically.

Sync and manual conflict-resolution pull requests must use merge commits so `pylon` retains ancestry from the exact upstream commit. The mirror and candidate branch pushes must use the built-in `GITHUB_TOKEN`: replacing it with a PAT or app token would allow an upstream-owned push workflow to execute when `main` advances. A separate PR-only app token may be used only by `gh pr create`; never expose it to Git credentials or candidate verification.

## Upstream overlap decisions

Before adding or changing a Pylon-owned integration:

1. Search current Prime Agent issues, pull requests, releases, and source.
2. Update `.pylon/features.yaml` with the current ownership and upstream evidence.
3. Choose one explicit decision:
   - `adopt`: use the upstream implementation and remove the superseded fork behavior;
   - `hybridize`: use upstream primitives behind a Pylon-owned contract;
   - `retain`: keep the fork behavior because upstream does not meet Pylon's needs;
   - `redesign`: replace both implementations with a smaller coherent model.
4. Capability-gate optional daemon or ACP behavior and keep stock Prime degradation local.
5. Add the decision and validation result to `.pylon/upstream-review.md`.

Pylon divergence has no expiry date. Remove it only when evidence shows that another implementation genuinely supersedes it.

## Workflow boundary

Pylon tracks maintainer work in GitHub issues, not Prime's Linear workspace. The `pylon` branch intentionally omits the inherited Linear ticket gate. Do not restore it during upstream merges.

Only workflows explicitly reviewed and approved for Pylon may run from the default `pylon` branch. The fork currently retains CI, changelog, contribution-trust, read-only process-stress, and upstream-sync workflows; inheritance alone is not approval.

## Release boundary

Mirroring upstream must not publish packages, binaries, or beta releases. The `pylon` branch intentionally omits the inherited release workflow, and the fork must never expose upstream-known release secrets at repository scope. Do not restore that workflow during upstream merges.

Fork artifact names, package ownership, provenance, signing, and release channels require a separate reviewed design before automated publishing is enabled.

## Product integration principles

- Keep orchestration and durable ownership in Pylon when the behavior spans providers.
- Put Prime-specific protocol translation and missing Prime lifecycle primitives at the fork boundary.
- Prefer negotiated capabilities to version guesses.
- Keep structured progress concise. The root plan describes meaningful user-visible outcomes, not every tool call or delegated worker.
- Preserve a stock-Prime fallback when Pylon can do so without weakening correctness or trust boundaries.
