# Pylon Prime Agent fork

This repository is Pylon's long-lived Prime Agent fork. It follows Prime upstream closely while allowing Pylon-owned integration behavior to remain when upstream has no suitable equivalent.

## Branches and remotes

- `main` is an exact fast-forward mirror of `PrimeIntellect-ai/prime-agent:main`. Never commit Pylon changes to it.
- `pylon` is the default product branch. Branch feature work from the latest `origin/pylon` and open pull requests back to `pylon`.
- The local `upstream` remote must be fetch-only. Never push to a Prime Intellect remote.
- Do not hard-reset, force-push, or wholesale rebase `pylon` onto upstream.

While this repository has only one write-capable human, `pylon` requires pull requests, strict hosted checks, conversation resolution, and recorded maintainer approval, but zero GitHub approvals. The protection applies to administrators so direct pushes cannot bypass it. Raise the required approval count when a second eligible reviewer exists.

The daily `Pylon upstream sync` workflow updates `main` through GitHub's fork `merge-upstream` API, verifies that it exactly matches Prime, prepares one deterministic merge candidate, and runs a new candidate through the trusted `pylon` CI definition with no secrets or persisted Git credentials. An unchanged candidate skips duplicate CI only after the Actions API confirms that its stored run and candidate-bound aggregate succeeded. The workflow opens or updates one candidate-ready issue with a prefilled link and the exact draft title, body, label, and branch requirements. A conflict opens or updates one blocker issue and requires a human resolution branch from `pylon`; the workflow never resolves conflicts automatically.

`main` must be governed by one active repository ruleset with no bypass actors. That ruleset blocks updates except GitHub fork fetch-and-merge, requires linear history, and forbids force-pushes and deletion. The workflow reads the effective branch rules and registered Prime fork parent before calling `merge-upstream`, then verifies the returned source and exact commit. GitHub may fast-forward the mirror but may not write a divergent merge commit. Maintainers must also audit that the ruleset keeps an empty bypass list because GitHub hides other actors from low-privilege workflow tokens.

GitHub requires this repository to permit squash or rebase merging before `main` can require linear history. Pylon permits squash merging only for that platform prerequisite; sync and manual conflict-resolution pull requests must still use merge commits on `pylon` so the product branch retains ancestry from the exact upstream commit. If Prime starts adding merge commits to its current linear history again, synchronization must stop for a protection-design review. Do not temporarily disable the mirror guard and rerun automation.

Mirror automation must use only the built-in `GITHUB_TOKEN`: it never pushes directly to `main`, has no PAT or app token, and never falls back to a credential that could bypass `main` protections or trigger inherited upstream publication. Candidate branches also use the built-in token and require a human-opened pull request.

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
