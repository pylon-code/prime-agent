import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createRequire } from "node:module";
import { test } from "node:test";

import {
	createReleaseManifest,
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_NPM_VERSION,
	PYLON_RELEASE_NODE_VERSION,
} from "./lib/pylon-release.mjs";
import {
	assertCanonicalInvocation,
	assertImmutableReleaseIdentity,
	canonicalJson,
	createPreviewManifest,
	createStableManifest,
	GITHUB_ACTIONS_APP_ID,
	nextStableSequence,
	parsePreviewTag,
	parseStableTag,
	parseSupportedReleaseRecipeRegistry,
	publicationReleaseBody,
	PYLON_REQUIRED_CHECKS,
	PYLON_PREVIEW_MANIFEST,
	PYLON_PREVIEW_WORKFLOW,
	PYLON_PUBLICATION_REF,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_MANIFEST,
	PYLON_STABLE_WORKFLOW,
	sha256Bytes,
	stableManifestBytesFromReleaseBody,
	stableReservationMessage,
	stableSequenceReservationTag,
	parseStableSequenceReservationTag,
	stableTag,
	validateAttestationEvidence,
	validateMergedChangelogProof,
	validatePreviewManifest,
	validatePublishedReleaseManifest,
	validateRequiredChecks,
	validateStableHistory,
	validateStableManifest,
	validateWorkflowArtifactProvenance,
} from "./lib/pylon-publication.mjs";
import {
	ATTEST_ACTION_CHAIN,
	validateApprovedAttestationWorkflow,
	validateApprovedWorkflowBytes,
} from "./lib/pylon-workflow-policy.mjs";
import { validatePreviewWorkflowRunEvidence, verifyGhAttestationResult } from "./verify-pylon-publication-attestations.mjs";
import { recordPreviewHighWater } from "./verify-pylon-preview-history.mjs";
import { verifyStableHistoryWithState } from "./verify-pylon-stable-history.mjs";
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";
import { ensureDurableConsumerStateDirectory, withConsumerStateLock } from "./lib/pylon-consumer-lock.mjs";
import { isExactWithdrawalReplay, selectStableHistoryReleases } from "./prepare-pylon-stable-manifest.mjs";
import { recoverStableDraft } from "./recover-pylon-stable-manifest.mjs";

const root = resolve(import.meta.dirname, "..");
const nodeRequire = createRequire(import.meta.url);
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const source = {
	repository: "https://github.com/pylon-code/prime-agent",
	commit: "0123456789abcdef0123456789abcdef01234567",
	tree: "89abcdef0123456789abcdef0123456789abcdef",
};
const version = "0.8.1";
const invocation = {
	sequenceEpoch: 1,
	sequence: 17,
	workflowRunId: "33428882721",
	publicationPolicyRevision: 1,
};

function fakeReleaseManifest() {
	return createReleaseManifest({
		source,
		version,
		toolchain: { node: PYLON_RELEASE_NODE_VERSION, npm: PYLON_RELEASE_NPM_VERSION },
		lockfileSha256: "a".repeat(64),
		artifacts: [
			["prime-agent", "pylon-prime-agent-0.8.1.tgz", "d"],
			["@earendil-works/pi-ai", "pylon-prime-agent-ai-0.8.1.tgz", "a"],
			["@earendil-works/pi-agent-core", "pylon-prime-agent-core-0.8.1.tgz", "b"],
			["@earendil-works/pi-tui", "pylon-prime-agent-tui-0.8.1.tgz", "c"],
		].map(([packageName, file, byte]) => {
			const bytes = Buffer.from(byte);
			return {
				package: packageName,
				file,
				size: bytes.byteLength,
				sha256: byte.repeat(64),
				sha512: byte.repeat(128),
			};
		}),
	});
}

function manifests() {
	const release = fakeReleaseManifest();
	const releaseBytes = Buffer.from(`${JSON.stringify(release, null, 2)}\n`);
	const preview = createPreviewManifest(release, releaseBytes, invocation);
	const previewBytes = Buffer.from(canonicalJson(preview));
	return { release, releaseBytes, preview, previewBytes };
}

function firstStable() {
	const { preview, previewBytes } = manifests();
	return createStableManifest({
		previewManifest: preview,
		previewManifestBytes: previewBytes,
		sequence: 1,
		previous: null,
		promotion: { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 },
	});
}

function secondStable(previous = firstStable(), options = {}) {
	const { preview, previewBytes } = manifests();
	const sequence = 2;
	const revocation = {
		stableTag: previous.tag,
		buildTag: previous.build.previewTag,
		reason: "security-withdrawal",
		revokedBySequence: sequence,
	};
	return createStableManifest({
		previewManifest: preview,
		previewManifestBytes: previewBytes,
		sequence,
		previous: { tag: previous.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(previous))) },
		revocations: options.withdraw ? [revocation] : [],
		promotion: options.withdraw ? { kind: "withdraw", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1, revocation } : { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 },
	});
}

function slsaAttestationOutput(subject, expected, { runId = expected.workflowRunId ?? "445566", attempt = "2" } = {}) {
	return JSON.stringify([{ verificationResult: {
		statement: {
			predicateType: "https://slsa.dev/provenance/v1",
			subject: [{ name: subject.name, digest: { sha256: subject.sha256 } }],
			predicate: {
				buildDefinition: {
					buildType: "https://actions.github.io/buildtypes/workflow/v1",
					externalParameters: { workflow: {
						repository: `https://github.com/${expected.repository}`, path: expected.workflow, ref: PYLON_PUBLICATION_REF,
					} },
					internalParameters: { github: {
						event_name: expected.event, repository_id: "1349002285", repository_owner_id: "11325514", runner_environment: "github-hosted",
					} },
					resolvedDependencies: [{
						uri: `git+https://github.com/${expected.repository}@${PYLON_PUBLICATION_REF}`,
						digest: { gitCommit: expected.sourceSha },
					}],
				},
				runDetails: {
					builder: { id: `https://github.com/${expected.repository}/${expected.workflow}@${PYLON_PUBLICATION_REF}` },
					metadata: { invocationId: `https://github.com/${expected.repository}/actions/runs/${runId}/attempts/${attempt}` },
				},
			},
		},
		verifiedTimestamps: [{ type: "Tlog", uri: "https://rekor.sigstore.dev" }],
	} }]);
}

function githubScriptForStep(workflowPath, stepName) {
	const lines = readFileSync(join(root, workflowPath), "utf8").split("\n");
	const step = lines.findIndex((line) => line === `      - name: ${stepName}`);
	if (step < 0) throw new Error(`Missing workflow step ${stepName}.`);
	const marker = lines.findIndex((line, index) => index > step && line === "          script: |");
	if (marker < 0) throw new Error(`Missing github-script body for ${stepName}.`);
	const body = [];
	for (let index = marker + 1; index < lines.length; index += 1) {
		if (lines[index] && !lines[index].startsWith("            ")) break;
		body.push(lines[index].slice(12));
	}
	return body.join("\n");
}

function exactPublicationTagRuleset() {
	return {
		id: 21_950_766,
		name: "Pylon immutable publication tags",
		target: "tag",
		source_type: "Repository",
		source: "pylon-code/prime-agent",
		enforcement: "active",
		conditions: {
			ref_name: {
				exclude: [],
				include: ["refs/tags/pylon-build-*", "refs/tags/pylon-stable-*"],
			},
		},
		rules: [
			{ type: "update", parameters: { update_allows_fetch_and_merge: false } },
			{ type: "deletion" },
		],
	};
}

async function inlinePublicationTagRulesetValidator(responses) {
	const script = githubScriptForStep(".github/workflows/pylon-preview-release.yml", "Require the canonical protected push");
	const start = script.indexOf("const requireExactPublicationTagRuleset = async () => {");
	const end = script.indexOf("\nawait requireExactPublicationTagRuleset();", start);
	assert.ok(start >= 0 && end > start, "preview admission lacks the frozen tag-ruleset validator");
	const create = new AsyncFunction(
		"github", "owner", "repo",
		`${script.slice(start, end)}\nreturn requireExactPublicationTagRuleset;`,
	);
	let request = 0;
	const github = {
		request: async (route, parameters) => {
			assert.equal(route, "GET /repos/{owner}/{repo}/rulesets/{ruleset_id}");
			assert.deepEqual(parameters, {
				owner: "pylon-code", repo: "prime-agent", ruleset_id: 21_950_766,
				headers: { accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
			});
			const response = responses[Math.min(request, responses.length - 1)];
			request += 1;
			if (response instanceof Error) throw response;
			return { status: 200, data: response };
		},
	};
	return { validate: await create(github, "pylon-code", "prime-agent"), requests: () => request };
}

test("canonical publication JSON sorts every object key and rejects unsupported values", () => {
	assert.equal(canonicalJson({ z: 1, a: { y: 2, b: 3 } }), '{\n  "a": {\n    "b": 3,\n    "y": 2\n  },\n  "z": 1\n}\n');
	assert.throws(() => canonicalJson({ bad: undefined }), /undefined/);
	assert.throws(() => canonicalJson({ bad: Number.NaN }), /unsupported/);
});

test("preview and stable tags use exact closed grammars", () => {
	assert.deepEqual(parsePreviewTag("pylon-build-g0123456789ab-r1"), { commit12: "0123456789ab", recipeRevision: 1 });
	assert.equal(
		stableTag({ sequence: 7, sourceCommit: source.commit, recipeRevision: 1 }),
		"pylon-stable-000007-g0123456789ab-r1",
	);
	assert.equal(parseStableTag("pylon-stable-000007-g0123456789ab-r1").sequence, 7);
	assert.equal(stableSequenceReservationTag(7), "pylon-stable-sequence-000007");
	assert.equal(stableSequenceReservationTag(7), stableSequenceReservationTag(parseStableTag("pylon-stable-000007-gffffffffffff-r9").sequence));
	assert.equal(parseStableSequenceReservationTag("pylon-stable-sequence-000007").sequence, 7);
	for (const invalid of ["v1", "main", "pylon-build-g012345-r1", "pylon-stable-7-g0123456789ab-r1"]) {
		assert.throws(() => (invalid.startsWith("pylon-stable") ? parseStableTag(invalid) : parsePreviewTag(invalid)));
	}
	assert.throws(() => parseStableSequenceReservationTag("pylon-stable-sequence-000000"));
	assert.throws(() => parseStableSequenceReservationTag("pylon-stable-sequence-000001-g0123456789ab"));
});

test("preview manifest binds the full source tree, build, recipe, and build-manifest digest", () => {
	const { release, releaseBytes, preview } = manifests();
	assert.equal(validatePreviewManifest(preview, release, releaseBytes), preview);
	assert.equal(canonicalJson(createPreviewManifest(release, releaseBytes, { ...invocation, runAttempt: "2" })), canonicalJson(preview), "rerun attempts keep identical bytes");
	for (const invalid of [
		{ ...invocation, sequenceEpoch: 2 },
		{ ...invocation, sequence: 0 },
		{ ...invocation, workflowRunId: "01" },
	]) assert.throws(() => createPreviewManifest(release, releaseBytes, invalid), /sequence identity/);
	for (const invalid of [
		{ ...invocation, publicationPolicyRevision: 0 },
		{ ...invocation, publicationPolicyRevision: 2 },
		{ sequenceEpoch: 1, sequence: 17, workflowRunId: "33428882721" },
	]) assert.throws(() => createPreviewManifest(release, releaseBytes, invalid), /policy revision/);
	for (const mutate of [
		(value) => (value.build.source.commit = "f".repeat(40)),
		(value) => (value.build.source.tree = "f".repeat(40)),
		(value) => (value.build.recipeRevision = 2),
		(value) => (value.build.releaseManifest.sha256 = "f".repeat(64)),
	]) {
		const changed = structuredClone(preview);
		mutate(changed);
		assert.throws(() => validatePreviewManifest(changed, release, releaseBytes), /does not match/);
	}
});

test("current policy validates an older supported closed recipe without executing old source", () => {
	const oldRecipe = {
		recipeRevision: 7,
		manifestSchemaVersion: 1,
		nodeVersion: "20.19.1",
		npmVersion: "10.8.2",
		minimumNodeVersion: "20.12.0",
	};
	const release = fakeReleaseManifest();
	release.build.id = `pylon-build-g${source.commit.slice(0, 12)}-r7`;
	release.build.recipeRevision = 7;
	release.build.node = oldRecipe.nodeVersion;
	release.build.npm = oldRecipe.npmVersion;
	release.build.assetBaseUrl = `https://github.com/pylon-code/prime-agent/releases/download/${release.build.id}`;
	release.package.minimumNode = oldRecipe.minimumNodeVersion;
	const releaseBytes = Buffer.from(canonicalJson(release));
	const preview = createPreviewManifest(fakeReleaseManifest(), manifests().releaseBytes, invocation);
	preview.build.tag = release.build.id;
	preview.build.id = release.build.id;
	preview.build.recipeRevision = 7;
	preview.build.releaseManifest.sha256 = sha256Bytes(releaseBytes);
	assert.equal(validatePublishedReleaseManifest(release, [oldRecipe]), release);
	assert.equal(
		validatePreviewManifest(preview, release, releaseBytes, { historical: true, supportedRecipes: [oldRecipe] }),
		preview,
	);
	assert.throws(() => validatePublishedReleaseManifest({ ...release, extra: true }, [oldRecipe]), /Unsupported/);
	assert.throws(() => validatePreviewManifest(preview, release, releaseBytes), /release manifest|recipe|build/i);
	const stable = createStableManifest({
		previewManifest: preview,
		previewManifestBytes: Buffer.from(canonicalJson(preview)),
		sequence: 1,
		promotion: { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 },
	});
	assert.equal(validateStableManifest(stable, [oldRecipe]), stable);
	assert.throws(() => validateStableManifest(stable), /closed|malformed/i);
});

test("consumer preview high-water allows gaps but rejects rollback and same-sequence equivocation", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-preview-state-")));
	try {
		const { preview, previewBytes } = manifests();
		const statePath = join(fixture, "consumer", "nested", "preview.json");
		await assert.rejects(() => recordPreviewHighWater(preview, previewBytes, { statePath }), /--initialize/);
		assert.equal((await recordPreviewHighWater(preview, previewBytes, { statePath, initialize: true })).advanced, true);
		assert.equal((await recordPreviewHighWater(preview, previewBytes, { statePath })).advanced, false);
		const later = structuredClone(preview);
		later.sequence += 3;
		later.workflowRunId = String(Number(later.workflowRunId) + 3);
		assert.equal((await recordPreviewHighWater(later, Buffer.from(canonicalJson(later)), { statePath })).state.highWater.sequence, later.sequence);
		await assert.rejects(() => recordPreviewHighWater(preview, previewBytes, { statePath }), /older/);
		const equivocation = structuredClone(later);
		equivocation.build.releaseManifest.sha256 = "f".repeat(64);
		await assert.rejects(
			() => recordPreviewHighWater(equivocation, Buffer.from(canonicalJson(equivocation)), { statePath }),
			/equivocates/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("stable history is contiguous, previous-digest chained, high-water marked, sorted, and append-only", () => {
	const first = firstStable();
	const second = secondStable(first, { withdraw: true });
	assert.deepEqual(first.build.previewSequence, {
		sequenceEpoch: invocation.sequenceEpoch,
		sequence: invocation.sequence,
		workflowRunId: invocation.workflowRunId,
	});
	assert.equal(first.build.publicationPolicyRevision, invocation.publicationPolicyRevision);
	assert.equal(first.history.highWater, 0);
	assert.equal(second.history.highWater, 1);
	assert.equal(nextStableSequence([second, first]), 3);
	assert.deepEqual(validateStableHistory([first, second]), [first, second]);
	const wrongPrevious = structuredClone(second);
	wrongPrevious.history.previous.sha256 = "f".repeat(64);
	assert.throws(() => validateStableHistory([first, wrongPrevious]), /previous manifest digest/);
	const gap = structuredClone(second);
	gap.sequence = 3;
	gap.tag = gap.tag.replace("000002", "000003");
	gap.history.highWater = 2;
	assert.throws(() => validateStableHistory([first, gap]));
	const third = structuredClone(secondStable(first, { withdraw: false }));
	third.sequence = 3;
	third.tag = third.tag.replace("000002", "000003");
	third.history = { highWater: 2, previous: { tag: second.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(second))) } };
	assert.throws(() => validateStableHistory([first, second, third]), /append-only/);
});

test("consumer stable high-water requires explicit initialization, is idempotent, and advances atomically", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-state-")));
	try {
		const first = firstStable();
		const second = secondStable(first);
		const firstPath = join(fixture, "first.json");
		const secondPath = join(fixture, "second.json");
		const statePath = join(fixture, "consumer", "nested", "stable.json");
		writeFileSync(firstPath, canonicalJson(first));
		writeFileSync(secondPath, canonicalJson(second));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /--initialize/);
		const initialized = await verifyStableHistoryWithState([firstPath], { statePath, initialize: true });
		assert.equal(initialized.advanced, true);
		assert.equal(initialized.state.highWater.sequence, 1);
		const witnessedBytes = readFileSync(statePath, "utf8");
		const repeated = await verifyStableHistoryWithState([firstPath], { statePath });
		assert.equal(repeated.advanced, false);
		assert.equal(readFileSync(statePath, "utf8"), witnessedBytes);
		const advanced = await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(advanced.advanced, true);
		assert.equal(advanced.state.highWater.sequence, 2);
		writeFileSync(statePath, witnessedBytes);
		const repairedRollback = await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(repairedRollback.state.highWater.sequence, 2, "the immutable transaction tip outranks a rolled-back projection");
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2);
		rmSync(statePath);
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2, "a deleted projection is repaired from the journal");
		writeFileSync(statePath, "");
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2, "an empty projection is repaired from the journal");
		const legacyPath = join(fixture, "legacy.json");
		writeFileSync(legacyPath, canonicalJson(initialized.state));
		const migrated = await verifyStableHistoryWithState([firstPath], { statePath: legacyPath });
		assert.equal(migrated.advanced, false);
		assert.equal(readdirSync(`${legacyPath}.transactions`).filter((name) => !name.startsWith(".")).length, 1);
		await assert.rejects(() => verifyStableHistoryWithState([firstPath, secondPath], { statePath, initialize: true }), /cannot reset/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("consumer stable high-water rejects rollback and a rewritten witnessed sequence", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-state-")));
	try {
		const first = firstStable();
		const second = secondStable(first);
		const firstPath = join(fixture, "first.json");
		const secondPath = join(fixture, "second.json");
		const statePath = join(fixture, "stable.json");
		writeFileSync(firstPath, canonicalJson(first));
		writeFileSync(secondPath, canonicalJson(second));
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath, initialize: true });
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /older than/);
		const rewrittenFirst = structuredClone(first);
		rewrittenFirst.promotion.policyTree = "f".repeat(40);
		writeFileSync(firstPath, canonicalJson(rewrittenFirst));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /older than|rewrites/);
		const rewrittenSecond = createStableManifest({
			previewManifest: manifests().preview,
			previewManifestBytes: manifests().previewBytes,
			sequence: 2,
			previous: { tag: rewrittenFirst.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(rewrittenFirst))) },
			promotion: { kind: "promote", policyCommit: source.commit, policyTree: "e".repeat(40), publicationPolicyRevision: 1 },
		});
		writeFileSync(secondPath, canonicalJson(rewrittenSecond));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath, secondPath], { statePath }), /rewrites/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("consumer stable high-water rejects malformed, noncanonical, symlinked, and locked local state", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-state-")));
	try {
		const manifestPath = join(fixture, "first.json");
		const statePath = join(fixture, "stable.json");
		writeFileSync(manifestPath, canonicalJson(firstStable()));
		writeFileSync(statePath, "{}\n");
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath }), /malformed/);
		writeFileSync(statePath, JSON.stringify({
			schemaVersion: 1,
			repository: "https://github.com/pylon-code/prime-agent",
			channel: "stable",
			highWater: { sequence: 1, tag: firstStable().tag, sha256: sha256Bytes(Buffer.from(canonicalJson(firstStable()))) },
		}));
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath }), /not canonical/);
		rmSync(statePath);
		symlinkSync(manifestPath, statePath);
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath }), /regular.*file/);
		rmSync(statePath);
		const realDirectory = join(fixture, "real-state-directory");
		const linkedDirectory = join(fixture, "linked-state-directory");
		mkdirSync(realDirectory);
		symlinkSync(realDirectory, linkedDirectory);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: join(linkedDirectory, "stable.json"), initialize: true }),
			/canonical real directory/,
		);
		const badLockState = join(fixture, "bad-lock.json");
		symlinkSync(realDirectory, `${badLockState}.lock`);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: badLockState, initialize: true }),
			/metadata path.*real directory/,
		);
		const badTransactionState = join(fixture, "bad-transactions.json");
		symlinkSync(realDirectory, `${badTransactionState}.transactions`);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: badTransactionState, initialize: true }),
			/metadata path.*real directory/,
		);
		const badLockEntryState = join(fixture, "bad-lock-entry.json");
		mkdirSync(`${badLockEntryState}.lock`);
		writeFileSync(join(`${badLockEntryState}.lock`, "unexpected"), "bad\n");
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: badLockEntryState, initialize: true }),
			/lock directory contains a malformed entry/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("consumer stable high-water requires canonical regular manifest files", async () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-stable-state-"));
	try {
		const target = join(fixture, "target.json");
		const manifestPath = join(fixture, "first.json");
		const statePath = join(fixture, "stable.json");
		writeFileSync(target, canonicalJson(firstStable()));
		symlinkSync(target, manifestPath);
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath, initialize: true }), /regular file/);
		rmSync(manifestPath);
		writeFileSync(manifestPath, JSON.stringify(firstStable()));
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath, initialize: true }), /not canonical/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("stable manifest nested schema rejects extras, malformed identities, unsafe assets, duplicates, and ordering changes", () => {
	const stable = firstStable();
	assert.equal(validateStableManifest(stable), stable);
	for (const mutate of [
		(value) => (value.build.extra = true),
		(value) => (value.build.source.extra = true),
		(value) => (value.build.source.repository = "https://github.com/fork/prime-agent"),
		(value) => (value.build.source.commit = "abc"),
		(value) => (value.build.source.tree = "abc"),
		(value) => (value.build.recipeRevision = 2),
		(value) => delete value.build.publicationPolicyRevision,
		(value) => (value.build.publicationPolicyRevision = 2),
		(value) => (value.build.releaseManifest.file = "other.json"),
		(value) => (value.build.previewManifest.file = "other.json"),
		(value) => (value.build.assets[0].file = "../escape.tgz"),
		(value) => (value.build.assets[0].file = "pylon-prime-agent-ai-9.9.9.tgz"),
		(value) => (value.build.assets[0].size = 0),
		(value) => value.build.assets.push(structuredClone(value.build.assets[0])),
		(value) => value.build.assets.reverse(),
		(value) => (value.promotion.policyTree = "abc"),
		(value) => delete value.promotion.publicationPolicyRevision,
		(value) => (value.promotion.publicationPolicyRevision = 2),
	]) {
		const changed = structuredClone(stable);
		mutate(changed);
		assert.throws(() => validateStableManifest(changed));
	}
});

test("stable history binds a revocation build tag to the exact prior stable sequence", () => {
	const first = firstStable();
	const second = secondStable(first, { withdraw: true });
	second.revocations[0].buildTag = "pylon-build-gffffffffffff-r1";
	second.promotion.revocation.buildTag = "pylon-build-gffffffffffff-r1";
	assert.throws(() => validateStableHistory([first, second]), /exact previously published build/);
});

test("exact-SHA required checks reject wrong app, source, context, and result", () => {
	const requiredChecks = [
		{ context: "build-check-test", appId: GITHUB_ACTIONS_APP_ID },
		{ context: "Check changelog fragment", appId: GITHUB_ACTIONS_APP_ID },
	];
	const checkRuns = requiredChecks.map(({ context }) => ({
		name: context,
		workflowPath: PYLON_REQUIRED_CHECKS.find((required) => required.context === context).workflowPath,
		head_sha: source.commit,
		app: { id: GITHUB_ACTIONS_APP_ID },
		status: "completed",
		conclusion: "success",
	}));
	assert.equal(validateRequiredChecks({ sourceSha: source.commit, requiredChecks, checkRuns }), true);
	for (const changedPolicy of [
		requiredChecks.slice(0, 1),
		[...requiredChecks, { context: "extra", appId: GITHUB_ACTIONS_APP_ID }],
		requiredChecks.map((required, index) => index === 0 ? { ...required, appId: null } : required),
	]) assert.throws(() => validateRequiredChecks({ sourceSha: source.commit, requiredChecks: changedPolicy, checkRuns }), /policy differs/);
	const wrongPath = structuredClone(checkRuns);
	wrongPath[0].workflowPath = ".github/workflows/other.yml";
	assert.throws(() => validateRequiredChecks({ sourceSha: source.commit, requiredChecks, checkRuns: wrongPath }), /not green/);
	for (const mutate of [
		(run) => (run.app.id = 1),
		(run) => (run.head_sha = "f".repeat(40)),
		(run) => (run.name = "other"),
		(run) => (run.conclusion = "failure"),
	]) {
		const changed = structuredClone(checkRuns);
		mutate(changed[0]);
		assert.throws(() => validateRequiredChecks({ sourceSha: source.commit, requiredChecks, checkRuns: changed }), /not green/);
	}
});

test("merged changelog proof never relabels a PR-head check as merge-SHA evidence", () => {
	const mergeSha = "e".repeat(40);
	const headSha = "d".repeat(40);
	const proof = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		ref: PYLON_PUBLICATION_REF,
		eventName: "push",
		mergeSha,
		pullRequests: [{
			number: 29,
			merged_at: "2026-01-01T00:00:00Z",
			merge_commit_sha: mergeSha,
			base: { ref: "pylon", repo: { full_name: PYLON_PUBLICATION_REPOSITORY } },
			head: { sha: headSha, repo: { full_name: PYLON_PUBLICATION_REPOSITORY } },
		}],
		headChecks: [{
			id: 7,
			name: "Check changelog fragment",
			head_sha: headSha,
			app: { id: GITHUB_ACTIONS_APP_ID },
			status: "completed",
			conclusion: "success",
		}],
		workflowRuns: [{
			checkRunId: 7,
			event: "pull_request",
			head_sha: headSha,
			path: ".github/workflows/changelog-fragment.yml",
			repository: PYLON_PUBLICATION_REPOSITORY,
			headRepository: PYLON_PUBLICATION_REPOSITORY,
			pullRequests: [29],
		}],
	};
	assert.equal(validateMergedChangelogProof(proof).number, 29);
	for (const mutate of [
		(value) => (value.repository = "fork/prime-agent"),
		(value) => (value.ref = "refs/heads/main"),
		(value) => (value.pullRequests[0].head.repo.full_name = "fork/prime-agent"),
		(value) => (value.headChecks[0].app.id = 1),
		(value) => (value.headChecks[0].conclusion = "failure"),
		(value) => (value.workflowRuns[0].event = "push"),
		(value) => (value.workflowRuns[0].path = ".github/workflows/other.yml"),
		(value) => (value.workflowRuns[0].head_sha = mergeSha),
	]) {
		const changed = structuredClone(proof);
		mutate(changed);
		assert.throws(() => validateMergedChangelogProof(changed));
	}
});

test("canonical invocation rejects main, tags, PR events, forks, and malformed source", () => {
	const good = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		ref: PYLON_PUBLICATION_REF,
		eventName: "push",
		sha: source.commit,
		expectedEvent: "push",
	};
	assert.doesNotThrow(() => assertCanonicalInvocation(good));
	for (const [key, value] of [["repository", "fork/prime-agent"], ["ref", "refs/heads/main"], ["eventName", "pull_request"], ["sha", "abc"]]) {
		assert.throws(() => assertCanonicalInvocation({ ...good, [key]: value }));
	}
});

test("attestation policy rejects wrong repository, workflow, ref, source, issuer, Rekor proof, and subject", () => {
	const expected = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		workflow: PYLON_PREVIEW_WORKFLOW,
		ref: PYLON_PUBLICATION_REF,
		sourceSha: source.commit,
		subjectName: "artifact.tgz",
		subjectSha256: "a".repeat(64),
	};
	const evidence = { ...expected, issuer: "https://token.actions.githubusercontent.com", rekorIncluded: true };
	assert.equal(validateAttestationEvidence(evidence, expected), true);
	for (const [key, value] of [
		["repository", "fork/prime-agent"], ["workflow", ".github/workflows/other.yml"], ["ref", "refs/heads/main"],
		["sourceSha", "f".repeat(40)], ["issuer", "https://issuer.invalid"], ["rekorIncluded", false],
		["subjectName", "other.tgz"], ["subjectSha256", "f".repeat(64)],
	]) assert.throws(() => validateAttestationEvidence({ ...evidence, [key]: value }, expected));
});

test("gh verification result requires full workflow/v1 invocation, exact subjects, and Rekor", () => {
	const subject = { name: "artifact.tgz", sha256: "a".repeat(64) };
	const expected = {
		repository: PYLON_PUBLICATION_REPOSITORY, workflow: PYLON_STABLE_WORKFLOW,
		event: "workflow_dispatch", sourceSha: source.commit,
	};
	const output = slsaAttestationOutput(subject, expected);
	assert.deepEqual(verifyGhAttestationResult(output, subject, expected), [{ runId: "445566", runAttempt: "2" }]);
	assert.throws(() => verifyGhAttestationResult(output, subject), /invocation/);
	const missingPredicate = JSON.parse(output);
	delete missingPredicate[0].verificationResult.statement.predicate;
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(missingPredicate), subject, expected), /workflow run/);
	assert.throws(() => verifyGhAttestationResult(output, { ...subject, sha256: "f".repeat(64) }, expected), /subject set/);
	const noRekor = output.replace("Tlog", "TimestampAuthority");
	assert.throws(() => verifyGhAttestationResult(noRekor, subject, expected), /Rekor/);
	const extra = JSON.parse(output);
	extra[0].verificationResult.statement.subject.push({ name: "extra", digest: { sha256: "b".repeat(64) } });
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(extra), subject, expected), /subject set/);
});

test("attestation invocation binds the signed run id and attempt while run number stays API-derived", () => {
	const expected = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		workflow: PYLON_PREVIEW_WORKFLOW,
		event: "push",
		sourceSha: source.commit,
		workflowRunId: invocation.workflowRunId,
	};
	const subject = { name: "artifact.tgz", sha256: "a".repeat(64) };
	const statement = {
		predicateType: "https://slsa.dev/provenance/v1",
		subject: [{ name: subject.name, digest: { sha256: subject.sha256 } }],
		predicate: {
			buildDefinition: {
				buildType: "https://actions.github.io/buildtypes/workflow/v1",
				externalParameters: { workflow: {
					repository: `https://github.com/${expected.repository}`,
					path: expected.workflow,
					ref: PYLON_PUBLICATION_REF,
				} },
				internalParameters: { github: {
					event_name: "push", repository_id: "1349002285", repository_owner_id: "11325514", runner_environment: "github-hosted",
				} },
				resolvedDependencies: [{
					uri: `git+https://github.com/${expected.repository}@${PYLON_PUBLICATION_REF}`,
					digest: { gitCommit: source.commit },
				}],
			},
			runDetails: {
				builder: { id: `https://github.com/${expected.repository}/${expected.workflow}@${PYLON_PUBLICATION_REF}` },
				metadata: { invocationId: `https://github.com/${expected.repository}/actions/runs/${expected.workflowRunId}/attempts/2` },
			},
		},
	};
	const output = JSON.stringify([{ verificationResult: {
		statement,
		verifiedTimestamps: [{ type: "Tlog", uri: "https://rekor.sigstore.dev" }],
	} }]);
	assert.deepEqual(verifyGhAttestationResult(output, subject, expected), [{ runId: invocation.workflowRunId, runAttempt: "2" }]);
	const wrongRun = structuredClone(statement);
	wrongRun.predicate.runDetails.metadata.invocationId = `https://github.com/${expected.repository}/actions/runs/999/attempts/2`;
	assert.throws(() => verifyGhAttestationResult(JSON.stringify([{ verificationResult: {
		statement: wrongRun,
		verifiedTimestamps: [{ type: "Tlog", uri: "https://rekor.sigstore.dev" }],
	} }]), subject, expected), /workflow run/);
});

test("preview sequence rejects a workflow API run-id or run-number mismatch", () => {
	const { preview } = manifests();
	const run = {
		id: Number(invocation.workflowRunId), run_number: invocation.sequence, event: "push", head_branch: "pylon", head_sha: source.commit,
		path: PYLON_PREVIEW_WORKFLOW, repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
		head_repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
		check_suite_id: 7, status: "in_progress", conclusion: null,
	};
	run.run_attempt = 2;
	const attempt = {
		run,
		suite: { id: 7, app: { id: GITHUB_ACTIONS_APP_ID }, head_sha: source.commit },
		jobs: [{ name: "Approve and attest six preview subjects", run_attempt: 2, status: "completed", conclusion: "success" }],
	};
	const signed = [{ runId: invocation.workflowRunId, runAttempt: "2" }];
	assert.equal(validatePreviewWorkflowRunEvidence({ attempts: [attempt] }, preview, signed).workflowRunId, invocation.workflowRunId);
	assert.throws(() => validatePreviewWorkflowRunEvidence({ attempts: [{ ...attempt, run: { ...run, id: 9 } }] }, preview, signed), /sequence/);
	assert.throws(() => validatePreviewWorkflowRunEvidence({ attempts: [{ ...attempt, run: { ...run, run_number: 18 } }] }, preview, signed), /sequence/);
});

test("immutable release replay is idempotent only for identical metadata and bytes", () => {
	const expected = {
		tag: "pylon-build-g0123456789ab-r1",
		name: "Pylon Prime preview pylon-build-g0123456789ab-r1",
		body: publicationReleaseBody({ channel: "preview", tag: "pylon-build-g0123456789ab-r1", source: source.commit, tree: source.tree, recipeRevision: 1 }),
		prerelease: true,
		sourceSha: source.commit,
		assets: [{ name: "artifact.tgz", size: 1, sha256: "a".repeat(64) }],
	};
	const actual = {
		immutable: true,
		draft: false,
		tag_name: expected.tag,
		name: expected.name,
		body: expected.body,
		prerelease: true,
		target_commitish: source.commit,
		assets: [{ name: "artifact.tgz", size: 1, digest: `sha256:${"a".repeat(64)}` }],
	};
	assert.equal(assertImmutableReleaseIdentity(actual, expected), true);
	for (const mutate of [
		(value) => (value.immutable = false),
		(value) => (value.body = "changed"),
		(value) => (value.assets[0].digest = `sha256:${"f".repeat(64)}`),
	]) {
		const changed = structuredClone(actual);
		mutate(changed);
		assert.throws(() => assertImmutableReleaseIdentity(changed, expected));
	}
});

test("workflow artifacts reject wrong run, repository, workflow, event, ref, source, result, ambiguity, expiry, and digest", () => {
	const expected = { runId: 7, workflowPath: ".github/workflows/pylon-preview-release.yml", event: "push", headSha: source.commit, artifactName: "pack" };
	const actual = {
		runId: 7, repositoryId: 1_349_002_285, workflowPath: expected.workflowPath, event: "push", ref: PYLON_PUBLICATION_REF,
		headSha: source.commit, conclusion: "success", checkSuiteHeadSha: source.commit, checkSuiteConclusion: "success",
		artifacts: [{ name: "pack", expired: false, digest: `sha256:${"a".repeat(64)}` }],
	};
	assert.equal(validateWorkflowArtifactProvenance(actual, expected).name, "pack");
	for (const mutate of [
		(value) => (value.runId = 8), (value) => (value.repositoryId = 1), (value) => (value.workflowPath = "other"),
		(value) => (value.event = "pull_request"), (value) => (value.ref = "refs/heads/main"),
		(value) => (value.headSha = "f".repeat(40)), (value) => (value.conclusion = "failure"),
		(value) => value.artifacts.push(structuredClone(value.artifacts[0])), (value) => (value.artifacts[0].expired = true),
		(value) => (value.artifacts[0].digest = "missing"),
	]) {
		const changed = structuredClone(actual);
		mutate(changed);
		assert.throws(() => validateWorkflowArtifactProvenance(changed, expected));
	}
});

test("standalone preview verification rejects tamper, extras, symlinks, and noncanonical channel bytes", () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-publication-"));
	try {
		const release = fakeReleaseManifest();
		for (const asset of release.assets) {
			const byte = asset.sha256[0];
			writeFileSync(join(fixture, asset.file), byte);
			asset.sha256 = sha256Bytes(Buffer.from(byte));
			asset.sha512 = createHash("sha512").update(byte).digest("hex");
		}
		release.attestationSubjects = release.assets.map((asset) => ({
			name: asset.file,
			digest: { sha256: asset.sha256, sha512: asset.sha512 },
		}));
		const releaseBytes = Buffer.from(`${JSON.stringify(release, null, 2)}\n`);
		writeFileSync(join(fixture, PYLON_RELEASE_MANIFEST), releaseBytes);
		const preview = createPreviewManifest(release, releaseBytes, invocation);
		writeFileSync(join(fixture, PYLON_PREVIEW_MANIFEST), canonicalJson(preview));
		assert.equal(verifyPreviewPublication(fixture).subjects.length, 6);
		writeFileSync(join(fixture, "extra"), "bad");
		assert.throws(() => verifyPreviewPublication(fixture), /Unexpected/);
		rmSync(join(fixture, "extra"));
		writeFileSync(join(fixture, release.assets[0].file), "tamper");
		assert.throws(() => verifyPreviewPublication(fixture), /digest mismatch/);
		writeFileSync(join(fixture, release.assets[0].file), release.assets[0].sha256[0]);
		const target = join(fixture, release.assets[0].file);
		rmSync(target);
		symlinkSync(join(fixture, release.assets[1].file), target);
		assert.throws(() => verifyPreviewPublication(fixture), /regular file/);
		rmSync(target);
		writeFileSync(target, release.assets[0].sha256[0]);
		writeFileSync(join(fixture, PYLON_PREVIEW_MANIFEST), JSON.stringify(preview));
		assert.throws(() => verifyPreviewPublication(fixture), /not canonical/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("recipe and publication policy registries close independent immutable identities", () => {
	const registryText = readFileSync(join(root, "scripts/pylon-prime-supported-release-recipes-v1.json"), "utf8");
	const registry = parseSupportedReleaseRecipeRegistry(registryText);
	const policy = registry.publicationPolicies[0];
	const preview = readFileSync(join(root, policy.previewWorkflowPath), "utf8");
	const stable = readFileSync(join(root, policy.stableWorkflowPath), "utf8");
	assert.deepEqual(validateApprovedWorkflowBytes(
		policy.previewWorkflowPath,
		preview,
		"preview",
		policy.publicationPolicyRevision,
	), { workflow: policy.previewWorkflowPath, environment: "pylon-preview" });
	assert.deepEqual(validateApprovedWorkflowBytes(
		policy.stableWorkflowPath,
		stable,
		"stable",
		policy.publicationPolicyRevision,
	), { workflow: policy.stableWorkflowPath, environment: "pylon-stable" });
	for (const changed of [
		`${preview}\n  rogue:\n    permissions: write-all\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo arbitrary\n`,
		`${stable}\n  rogue-oidc:\n    permissions:\n      id-token: write\n      attestations: write\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo sign\n`,
		preview.replace("jobs:\n", "jobs:\n  publish:\n    permissions: write-all\n"),
		preview.replace("permissions: {}", "permissions: {}\npermissions: write-all"),
	]) assert.throws(() => validateApprovedWorkflowBytes(
		policy.previewWorkflowPath,
		changed,
		"preview",
		policy.publicationPolicyRevision,
	), /bytes differ/);
	assert.throws(() => validateApprovedWorkflowBytes(policy.previewWorkflowPath, preview, "preview", 999), /Unsupported/);

	const stableR2 = stable
		.replace("--publication-policy-revision 1", "--publication-policy-revision 2")
		.replaceAll("promotion?.publicationPolicyRevision !== 1", "promotion?.publicationPolicyRevision !== 2");
	assert.notEqual(stableR2, stable);
	assert.match(stableR2, /--publication-policy-revision 2/);
	assert.match(stableR2, /promotion\?\.publicationPolicyRevision !== 2/);
	assert.match(stableR2, /!\[1\]\.includes\(manifest\.build\.publicationPolicyRevision\)/);
	const policyR2 = {
		...policy,
		publicationPolicyRevision: 2,
		stableWorkflowSha256: sha256Bytes(Buffer.from(stableR2)),
	};
	const policies = [policy, policyR2];
	assert.deepEqual(validateApprovedWorkflowBytes(
		policyR2.stableWorkflowPath,
		stableR2,
		"stable",
		2,
		policies,
	), { workflow: policyR2.stableWorkflowPath, environment: "pylon-stable" });
	assert.throws(() => validateApprovedWorkflowBytes(policy.stableWorkflowPath, stableR2, "stable", 1, policies), /bytes differ/);
	const promotedByR2 = firstStable();
	promotedByR2.promotion.publicationPolicyRevision = 2;
	assert.equal(validateStableManifest(promotedByR2, registry.recipes, policies), promotedByR2);
	assert.equal(promotedByR2.build.recipeRevision, 1);
	assert.equal(promotedByR2.build.publicationPolicyRevision, 1);
	assert.equal(promotedByR2.promotion.publicationPolicyRevision, 2);
	const stableVerifier = readFileSync(join(root, "scripts/verify-pylon-stable-attestation.mjs"), "utf8");
	assert.match(stableVerifier, /manifest\.promotion\.publicationPolicyRevision/);
	assert.doesNotMatch(stableVerifier, /"stable",\s*manifest\.build\.recipeRevision/);
	const previewVerifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	assert.match(previewVerifier, /verified\.previewManifest\.publicationPolicyRevision/);
	for (const mutate of [
		(value) => delete value.promotion.publicationPolicyRevision,
		(value) => (value.promotion.publicationPolicyRevision = 3),
		(value) => (value.promotion.publicationPolicyRevision = 0),
		(value) => delete value.build.publicationPolicyRevision,
	]) {
		const changed = structuredClone(promotedByR2);
		mutate(changed);
		assert.throws(() => validateStableManifest(changed, registry.recipes, policies), /policy revision/);
	}

	for (const mutate of [
		(value) => (value.extra = true),
		(value) => (value.recipes[0].extra = true),
		(value) => value.recipes.push(structuredClone(value.recipes[0])),
		(value) => (value.recipes[0].recipeRevision = 0),
		(value) => (value.publicationPolicies[0].extra = true),
		(value) => value.publicationPolicies.push(structuredClone(value.publicationPolicies[0])),
		(value) => (value.publicationPolicies[0].publicationPolicyRevision = 0),
	]) {
		const changed = structuredClone(registry);
		mutate(changed);
		assert.throws(() => parseSupportedReleaseRecipeRegistry(JSON.stringify(changed)), /malformed/);
	}
	assert.throws(
		() => parseSupportedReleaseRecipeRegistry('{"schemaVersion":1,"schemaVersion":1,"recipes":[],"publicationPolicies":[]}'),
		/duplicate keys/,
	);
});

test("consumer state locking, recovery, transaction fencing, durability, and path boundary are deterministic", async () => {
	const deferred = () => {
		let resolvePromise;
		let rejectPromise;
		const promise = new Promise((resolve, reject) => {
			resolvePromise = resolve;
			rejectPromise = reject;
		});
		return { promise, resolve: resolvePromise, reject: rejectPromise };
	};
	const manualRuntime = (clock, hooks = {}) => {
		const beats = [];
		return {
			stale: 20,
			update: 10,
			stateMaxBytes: 1024,
			now: () => clock.value,
			hooks,
			startHeartbeat: ({ beat }) => {
				beats.push(beat);
				return async () => {};
			},
			beats,
		};
	};
	const bytes = (value) => Buffer.from(`${JSON.stringify({ value })}\n`);

	const virtualRoot = resolve("/");
	const first = join(virtualRoot, "pylon-durable-state-test");
	const second = join(first, "nested");
	const existing = new Set([virtualRoot]);
	const durabilityEvents = [];
	const operations = {
		lstatEntry: async (path) => {
			if (!existing.has(path)) throw Object.assign(new Error("missing"), { code: "ENOENT" });
			return { isDirectory: () => true };
		},
		makeDirectory: async (path, options) => {
			assert.deepEqual(options, { mode: 0o700 });
			durabilityEvents.push(`mkdir:${path}`);
			existing.add(path);
		},
		syncDirectory: async (path) => durabilityEvents.push(`sync:${path}`),
	};
	await ensureDurableConsumerStateDirectory(second, operations);
	assert.deepEqual(durabilityEvents, [
		`mkdir:${first}`, `sync:${virtualRoot}`, `mkdir:${second}`, `sync:${first}`,
	]);
	durabilityEvents.length = 0;
	await ensureDurableConsumerStateDirectory(second, operations);
	assert.deepEqual(durabilityEvents, [`sync:${virtualRoot}`, `sync:${first}`]);

	const creatorReachedSync = deferred();
	const letCreatorSync = deferred();
	const concurrentExisting = new Set([virtualRoot]);
	const creatorOperations = {
		lstatEntry: async (path) => {
			if (!concurrentExisting.has(path)) throw Object.assign(new Error("missing"), { code: "ENOENT" });
			return { isDirectory: () => true };
		},
		makeDirectory: async (path) => concurrentExisting.add(path),
		syncDirectory: async (path) => {
			if (path === virtualRoot) {
				creatorReachedSync.resolve();
				await letCreatorSync.promise;
			}
		},
	};
	const creator = ensureDurableConsumerStateDirectory(first, creatorOperations);
	await creatorReachedSync.promise;
	let observerSynced = false;
	await ensureDurableConsumerStateDirectory(first, {
		...creatorOperations,
		syncDirectory: async (path) => {
			assert.equal(path, virtualRoot);
			observerSynced = true;
		},
	});
	assert.equal(observerSynced, true, "an observer flushes the concurrent creator's ancestor entry before returning");
	letCreatorSync.resolve();
	await creator;

	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-consumer-lock-")));
	try {
		const activePath = join(fixture, "active.json");
		const activeClock = { value: 1 };
		const activeOptions = manualRuntime(activeClock);
		const releaseOwner = deferred();
		const ownerEntered = deferred();
		const owner = withConsumerStateLock(activePath, async (_path, transaction) => {
			ownerEntered.resolve();
			await releaseOwner.promise;
			await transaction.commitState(bytes("owner"));
		}, activeOptions);
		await ownerEntered.promise;
		activeClock.value = 16;
		assert.equal(await activeOptions.beats[0](), true);
		activeClock.value = 31;
		assert.equal(await activeOptions.beats[0](), true);
		activeClock.value = 45;
		await assert.rejects(
			() => withConsumerStateLock(activePath, async () => {}, manualRuntime(activeClock)),
			/actively locked/,
		);
		releaseOwner.resolve();
		await owner;
		assert.deepEqual(JSON.parse(readFileSync(activePath, "utf8")), { value: "owner" });

		const racePath = join(fixture, "recoverers.json");
		const raceClock = { value: 1 };
		const oldRelease = deferred();
		const oldEntered = deferred();
		const oldOwner = withConsumerStateLock(racePath, async () => {
			oldEntered.resolve();
			await oldRelease.promise;
		}, manualRuntime(raceClock));
		const oldOwnerRejected = assert.rejects(oldOwner, /retired/);
		await oldEntered.promise;
		raceClock.value = 100;
		const observedBoth = deferred();
		const releaseObserved = deferred();
		const retiredBoth = deferred();
		const releaseRetired = deferred();
		let observed = 0;
		let retired = 0;
		const recoveryHooks = {
			afterObserveStale: async () => {
				observed += 1;
				if (observed === 2) observedBoth.resolve();
				await releaseObserved.promise;
			},
			afterRetire: async () => {
				retired += 1;
				if (retired === 2) retiredBoth.resolve();
				await releaseRetired.promise;
			},
		};
		let recoveryActions = 0;
		const winnerRelease = deferred();
		const winnerEntered = deferred();
		const recover = () => withConsumerStateLock(racePath, async () => {
			recoveryActions += 1;
			winnerEntered.resolve();
			await winnerRelease.promise;
		}, manualRuntime(raceClock, recoveryHooks));
		const recoveryRejected = deferred();
		const trackRecovery = (promise) => promise.catch((error) => {
			recoveryRejected.resolve(error);
			throw error;
		});
		const firstRecoverer = trackRecovery(recover());
		const secondRecoverer = trackRecovery(recover());
		const recoveriesPromise = Promise.allSettled([firstRecoverer, secondRecoverer]);
		await observedBoth.promise;
		releaseObserved.resolve();
		await retiredBoth.promise;
		releaseRetired.resolve();
		await winnerEntered.promise;
		assert.match((await recoveryRejected.promise).message, /actively locked/);
		winnerRelease.resolve();
		const recoveries = await recoveriesPromise;
		assert.equal(recoveryActions, 1, "only one simultaneous stale recoverer enters the next generation");
		assert.deepEqual(recoveries.map((result) => result.status).sort(), ["fulfilled", "rejected"]);
		assert.match(recoveries.find((result) => result.status === "rejected").reason.message, /actively locked/);
		oldRelease.resolve();
		await oldOwnerRejected;
		const raceClaims = readdirSync(`${racePath}.lock`).filter((name) => name.startsWith("claim-"));
		assert.equal(raceClaims.length, 2);

		const fencedPath = join(fixture, "fenced.json");
		const fencedClock = { value: 1 };
		const oldDecisionReached = deferred();
		const letOldDecide = deferred();
		const retiredWriter = withConsumerStateLock(fencedPath, async (_path, transaction) => {
			await transaction.commitState(bytes("retired"));
		}, manualRuntime(fencedClock, {
			beforeCommitDecision: async () => {
				oldDecisionReached.resolve();
				await letOldDecide.promise;
			},
		}));
		await oldDecisionReached.promise;
		fencedClock.value = 100;
		await withConsumerStateLock(fencedPath, async (_path, transaction) => {
			await transaction.commitState(bytes("winner"));
		}, manualRuntime(fencedClock));
		letOldDecide.resolve();
		await assert.rejects(retiredWriter, /lost ownership/);
		assert.deepEqual(JSON.parse(readFileSync(fencedPath, "utf8")), { value: "winner" });
		assert.equal(
			readdirSync(`${fencedPath}.transactions`).filter((name) => !name.startsWith(".")).length,
			1,
			"a retired writer cannot publish a sibling transition from GENESIS",
		);

		const projectionPath = join(fixture, "projection.json");
		const projectionClock = { value: 1 };
		const staleProjectionReady = deferred();
		const letStaleProjectionRename = deferred();
		let blockedProjection = false;
		const firstProjection = withConsumerStateLock(projectionPath, async (_path, transaction) => {
			await transaction.commitState(bytes("one"));
		}, manualRuntime(projectionClock, {
			afterProjectionFileSync: async () => {
				if (blockedProjection) return;
				blockedProjection = true;
				staleProjectionReady.resolve();
				await letStaleProjectionRename.promise;
			},
		}));
		await staleProjectionReady.promise;
		await withConsumerStateLock(projectionPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "one" });
			await transaction.commitState(bytes("two"));
		}, manualRuntime(projectionClock));
		await withConsumerStateLock(projectionPath, async (_path, transaction) => {
			await transaction.commitState(bytes("three"));
		}, manualRuntime(projectionClock));
		letStaleProjectionRename.resolve();
		await firstProjection;
		assert.deepEqual(JSON.parse(readFileSync(projectionPath, "utf8")), { value: "three" });
		assert.equal(readdirSync(`${projectionPath}.transactions`).filter((name) => !name.startsWith(".")).length, 3);
		writeFileSync(join(`${projectionPath}.transactions`, `${"f".repeat(64)}.json`), "{}\n");
		await assert.rejects(
			() => withConsumerStateLock(projectionPath, async () => {}, manualRuntime(projectionClock)),
			/unreachable transition/,
		);

		for (const crashPoint of [
			["afterFileSync", "claim"],
			["afterMetadataLink", "claim"],
			["afterMetadataDirectorySync", "claim"],
			["afterFileSync", "terminal-commit"],
			["afterMetadataLink", "terminal-commit"],
			["afterMetadataDirectorySync", "terminal-commit"],
			["afterCommitDecision", null],
			["afterFileSync", "transition"],
			["afterMetadataLink", "transition"],
			["afterMetadataDirectorySync", "transition"],
			["afterProjectionFileSync", null],
			["afterProjectionRename", null],
			["afterProjectionDirectorySync", null],
			["afterApplied", null],
		]) {
			const [hookName, wantedKind] = crashPoint;
			const crashPath = join(fixture, `crash-${hookName}-${wantedKind ?? "projection"}.json`);
			const crashClock = { value: 1 };
			const reached = deferred();
			const crash = deferred();
			let armed = true;
			const hooks = {
				[hookName]: async (event = {}) => {
					if (!armed || (wantedKind !== null && event.kind !== wantedKind)) return;
					armed = false;
					reached.resolve();
					await crash.promise;
				},
			};
			const interrupted = withConsumerStateLock(crashPath, async (_path, transaction) => {
				await transaction.commitState(bytes("interrupted"));
			}, manualRuntime(crashClock, hooks));
			await reached.promise;
			crashClock.value = 100;
			await withConsumerStateLock(crashPath, async (_path, transaction) => {
				await transaction.commitState(bytes("recovered"));
			}, manualRuntime(crashClock));
			crash.reject(new Error(`simulated crash at ${hookName}`));
			await assert.rejects(interrupted, /simulated crash|retired/);
			await withConsumerStateLock(crashPath, async () => {}, manualRuntime(crashClock));
			assert.deepEqual(JSON.parse(readFileSync(crashPath, "utf8")), { value: "recovered" });
		}

		const swapRoot = join(fixture, "swap-root");
		const swapDirectory = join(swapRoot, "state");
		const movedDirectory = join(swapRoot, "state-moved");
		mkdirSync(swapDirectory, { recursive: true });
		const swapPath = join(swapDirectory, "state.json");
		let swapped = false;
		await assert.rejects(
			() => withConsumerStateLock(swapPath, async () => {}, manualRuntime({ value: 1 }, {
				beforePathOperation: async ({ operation }) => {
					if (swapped || operation !== "scan-claims") return;
					swapped = true;
					renameSync(swapDirectory, movedDirectory);
					symlinkSync(movedDirectory, swapDirectory);
				},
			})),
			/canonical real directory/,
		);
		assert.equal(swapped, true, "the injected component swap reached the immediate path revalidation boundary");
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("every inline admission and final publisher closes the exact branch-check trust root", async () => {
	for (const [workflow, step] of [
		[".github/workflows/pylon-preview-release.yml", "Require the canonical protected push"],
		[".github/workflows/pylon-preview-release.yml", "Verify exact checks and publish once"],
		[".github/workflows/pylon-stable-release.yml", "Require protected pylon and an exact verified preview source"],
		[".github/workflows/pylon-stable-release.yml", "Re-download the exact draft, reserve N once, and publish only that draft"],
	]) {
		const script = githubScriptForStep(workflow, step);
		for (const value of [
			"Check changelog fragment", "build-check-test", "15368",
			".github/workflows/changelog-merged-proof.yml", ".github/workflows/ci.yml",
		]) assert.ok(script.includes(value), `${workflow}:${step} lacks ${value}`);
		assert.match(script, /JSON\.stringify\(actualPolicy\) !== JSON\.stringify/);
		assert.doesNotMatch(script, /appId === null|!expectedPath/);
	}

	const rulesetSteps = [
		[".github/workflows/pylon-preview-release.yml", "Require the canonical protected push", 1],
		[".github/workflows/pylon-preview-release.yml", "Create or finish the exact durable draft", 2],
		[".github/workflows/pylon-preview-release.yml", "Verify exact checks and publish once", 2],
		[".github/workflows/pylon-stable-release.yml", "Require protected pylon and an exact verified preview source", 1],
		[".github/workflows/pylon-stable-release.yml", "Re-download the exact draft, reserve N once, and publish only that draft", 4],
	];
	const frozenValidators = new Set();
	for (const [workflow, step, expectedCalls] of rulesetSteps) {
		const script = githubScriptForStep(workflow, step);
		const start = script.indexOf("const requireExactPublicationTagRuleset = async () => {");
		const end = script.indexOf("\nawait requireExactPublicationTagRuleset();", start);
		assert.ok(start >= 0 && end > start, `${workflow}:${step} lacks an inline tag-ruleset proof`);
		frozenValidators.add(script.slice(start, end));
		assert.equal((script.match(/await requireExactPublicationTagRuleset\(\);/g) ?? []).length, expectedCalls);
		assert.match(script, /refs\/tags\/pylon-stable-sequence-000001/);
	}
	assert.equal(frozenValidators.size, 1, "every writer must use the same frozen inline validator bytes");

	const valid = exactPublicationTagRuleset();
	const { validate } = await inlinePublicationTagRulesetValidator([valid]);
	await validate();
	const mutations = [
		(value) => (value.enforcement = "disabled"),
		(value) => (value.bypass_actors = [{ actor_type: "RepositoryRole", actor_id: 5 }]),
		(value) => value.conditions.ref_name.exclude.push("refs/tags/pylon-stable-sequence-*"),
		(value) => (value.conditions.ref_name.include[1] = "refs/tags/pylon-stable-[0-9]*"),
		(value) => value.conditions.ref_name.include.pop(),
		(value) => value.rules.pop(),
		(value) => (value.rules[0].parameters.update_allows_fetch_and_merge = true),
		(value) => (value.rules[0].parameters.extra = false),
		(value) => value.rules.push({ type: "creation" }),
		(value) => (value.id = 1),
		(value) => (value.name = "Other ruleset"),
		(value) => (value.source = "fork/prime-agent"),
		(value) => (value.target = "branch"),
		(value) => delete value.conditions.ref_name.exclude,
		(value) => (value.conditions.extra = {}),
	];
	for (const mutate of mutations) {
		const changed = structuredClone(valid);
		mutate(changed);
		const rejected = await inlinePublicationTagRulesetValidator([changed]);
		await assert.rejects(() => rejected.validate(), /exact active non-bypassable immutable tag ruleset/);
	}
	const unavailable = await inlinePublicationTagRulesetValidator([new Error("ruleset auth or endpoint unavailable")]);
	await assert.rejects(() => unavailable.validate(), /unavailable/);
	const stale = structuredClone(valid);
	stale.enforcement = "disabled";
	const pointInTime = await inlinePublicationTagRulesetValidator([valid, stale]);
	await pointInTime.validate();
	await assert.rejects(() => pointInTime.validate(), /exact active non-bypassable immutable tag ruleset/);
	assert.equal(pointInTime.requests(), 2, "a stale admission proof must not authorize a later write");

	for (const workflow of [
		readFileSync(join(root, ".github/workflows/pylon-preview-release.yml"), "utf8"),
		readFileSync(join(root, ".github/workflows/pylon-stable-release.yml"), "utf8"),
	]) {
		for (const mutation of workflow.matchAll(/github\.rest\.git\.createRef/g)) {
			const proof = workflow.lastIndexOf("await requireExactPublicationTagRuleset();", mutation.index);
			const between = workflow.slice(proof + "await requireExactPublicationTagRuleset();".length, mutation.index).replace(/\(?await\s*$/, "");
			assert.ok(proof >= 0 && !/\bawait\b/.test(between), "tag CAS lacks an immediately fresh ruleset proof");
		}
		for (const mutation of workflow.matchAll(/github\.rest\.repos\.updateRelease/g)) {
			const proof = workflow.lastIndexOf("await requireExactPublicationTagRuleset();", mutation.index);
			const between = workflow.slice(proof + "await requireExactPublicationTagRuleset();".length, mutation.index).replace(/\(?await\s*$/, "");
			assert.ok(proof >= 0 && !/\bawait\b/.test(between), "immutable publish lacks an immediately fresh ruleset proof");
		}
	}
});

test("stable recovery body durably carries bounded exact canonical manifest bytes", () => {
	const manifest = firstStable();
	const bytes = Buffer.from(canonicalJson(manifest));
	const body = publicationReleaseBody({
		channel: "stable", tag: manifest.tag, source: manifest.build.source.commit, tree: manifest.build.source.tree,
		recipeRevision: manifest.build.recipeRevision, policyCommit: manifest.promotion.policyCommit,
		policyTree: manifest.promotion.policyTree, stableManifestBytes: bytes,
	});
	const recovered = stableManifestBytesFromReleaseBody(body);
	assert.equal(recovered.bytes.equals(bytes), true);
	assert.equal(recovered.digest, sha256Bytes(bytes));
	assert.throws(() => stableManifestBytesFromReleaseBody(body.replace("Manifest bytes: ", "Manifest bytes: 9")), /truncated|metadata/);
	assert.throws(() => stableManifestBytesFromReleaseBody(body.slice(0, -1)), /metadata/);
	assert.throws(() => publicationReleaseBody({
		channel: "stable", tag: manifest.tag, source: manifest.build.source.commit, tree: manifest.build.source.tree,
		recipeRevision: 1, policyCommit: source.commit, policyTree: source.tree,
		stableManifestBytes: Buffer.alloc(48 * 1024 + 1),
	}), /49152/);
});

test("stable zero-asset recovery reuses the body-carried attested bytes and excludes only its draft", async () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-stable-recovery-"));
	try {
		const manifest = firstStable();
		const bytes = Buffer.from(canonicalJson(manifest));
		const digest = sha256Bytes(bytes);
		const release = {
			id: 51, draft: true, immutable: false, tag_name: manifest.tag,
			name: `Pylon Prime stable ${manifest.tag}`, prerelease: false,
			target_commitish: manifest.promotion.policyCommit, assets: [],
			body: publicationReleaseBody({
				channel: "stable", tag: manifest.tag, source: manifest.build.source.commit, tree: manifest.build.source.tree,
				recipeRevision: manifest.build.recipeRevision, policyCommit: manifest.promotion.policyCommit,
				policyTree: manifest.promotion.policyTree, stableManifestBytes: bytes,
			}),
		};
		let verified = false;
		const notFound = Object.assign(new Error("not found"), { status: 404 });
		const recovered = await recoverStableDraft({
			draftId: 51, reservationTag: "", previewTag: manifest.build.previewTag, operation: "promote",
			revokeTag: "", reason: "", outDir: fixture,
		}, {
			api: async (path) => path.endsWith("/releases/51") ? release : Promise.reject(notFound),
			readHistory: async (options) => {
				assert.deepEqual(options, { verifyAllAttestations: true, excludeDraftId: 51, excludeDraftTag: manifest.tag });
				return [];
			},
			verifyAttestation: (path, policyCommit, policyTree) => {
				assert.equal(readFileSync(path).equals(bytes), true);
				assert.equal(policyCommit, source.commit);
				assert.equal(policyTree, source.tree);
				verified = true;
			},
		});
		assert.equal(recovered.digest, digest);
		assert.equal(verified, true);
		assert.deepEqual(selectStableHistoryReleases([release], { excludeDraftId: 51 }), []);
		assert.throws(() => selectStableHistoryReleases([release], { excludeDraftId: 52 }), /unexpected draft/);
		const altered = structuredClone(release);
		altered.body = altered.body.replace(digest, "f".repeat(64));
		await assert.rejects(() => recoverStableDraft({
			draftId: 51, reservationTag: "", previewTag: manifest.build.previewTag, operation: "promote",
			revokeTag: "", reason: "", outDir: fixture,
		}, { api: async () => altered }), /altered|truncated/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("withdrawal replay is a no-op only for the exact latest promotion tuple and reason", () => {
	const first = firstStable();
	const latest = secondStable(first, { withdraw: true });
	const request = { previewTag: latest.build.previewTag, revokeTag: first.tag, reason: "security-withdrawal" };
	assert.equal(isExactWithdrawalReplay(latest, request), true);
	for (const changed of [
		{ ...request, reason: "other-reason" },
		{ ...request, revokeTag: latest.tag },
		{ ...request, previewTag: "pylon-build-gffffffffffff-r1" },
	]) assert.equal(isExactWithdrawalReplay(latest, changed), false);
	const laterPromotion = structuredClone(latest);
	laterPromotion.promotion = { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 };
	assert.equal(isExactWithdrawalReplay(laterPromotion, request), false);
	const message = stableReservationMessage(latest, sha256Bytes(Buffer.from(canonicalJson(latest))), 51);
	for (const field of ["Withdraw stable tag", "Withdraw build tag", "Withdraw reason"]) assert.match(message, new RegExp(`^${field}:`, "m"));
});

test("signed preview attempt evidence ignores a later aggregate rerun and response-loss conclusion", () => {
	const { preview } = manifests();
	const signed = [{ runId: invocation.workflowRunId, runAttempt: "1" }];
	const attempt = {
		run: {
			id: Number(invocation.workflowRunId), run_attempt: 1, run_number: invocation.sequence,
			event: "push", head_branch: "pylon", head_sha: source.commit, path: PYLON_PREVIEW_WORKFLOW,
			repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
			head_repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
			check_suite_id: 77, status: "completed", conclusion: "failure",
		},
		suite: { id: 77, app: { id: GITHUB_ACTIONS_APP_ID }, head_sha: source.commit, conclusion: "failure" },
		jobs: [{ name: "Approve and attest six preview subjects", run_attempt: 1, status: "completed", conclusion: "success" }],
	};
	assert.equal(validatePreviewWorkflowRunEvidence({ attempts: [attempt], aggregateRun: { run_attempt: 2, conclusion: "failure" } }, preview, signed).workflowRunId, invocation.workflowRunId);
	const verifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	assert.match(verifier, /actions\/runs\/\$\{runId\}\/attempts\/\$\{runAttempt\}/);
	assert.doesNotMatch(verifier, /actions\/runs\/\$\{previewManifest\.workflowRunId\}`/);
});

test("stable stage survives a crash after createRelease by recovering its exact zero-asset body", async () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-stage-crash-"));
	const oldManifest = process.env.STABLE_MANIFEST;
	try {
		const manifest = firstStable();
		const bytes = Buffer.from(canonicalJson(manifest));
		const manifestPath = join(fixture, PYLON_STABLE_MANIFEST);
		writeFileSync(manifestPath, bytes);
		process.env.STABLE_MANIFEST = manifestPath;
		let draft;
		let crash = true;
		const missing = () => Promise.reject(Object.assign(new Error("missing"), { status: 404 }));
		const listReleases = async () => {};
		const github = {
			paginate: async (method) => method === listReleases ? (draft ? [draft] : []) : [],
			rest: {
				git: { getRef: missing },
				repos: {
					listReleases,
					createRelease: async (request) => {
						draft = { id: 51, draft: true, immutable: false, tag_name: request.tag_name, name: request.name,
							body: request.body, prerelease: false, target_commitish: request.target_commitish, assets: [] };
						if (crash) throw new Error("simulated crash after createRelease");
						return { data: draft };
					},
					getRelease: async () => ({ data: draft }),
				},
			},
			request: async (route, request) => {
				if (route.startsWith("POST ")) {
					draft.assets = [{ id: 9, name: request.name, size: request.data.length, digest: `sha256:${sha256Bytes(request.data)}` }];
					return { data: draft.assets[0] };
				}
				return { data: bytes };
			},
		};
		const context = { repo: { owner: "pylon-code", repo: "prime-agent" }, eventName: "workflow_dispatch", ref: PYLON_PUBLICATION_REF, sha: source.commit };
		const script = githubScriptForStep(".github/workflows/pylon-stable-release.yml", "Create or finish the exact durable draft");
		const execute = new AsyncFunction("github", "context", "core", "require", script);
		await assert.rejects(() => execute(github, context, {}, nodeRequire), /simulated crash/);
		assert.equal(draft.assets.length, 0);
		assert.equal(stableManifestBytesFromReleaseBody(draft.body).bytes.equals(bytes), true);
		crash = false;
		assert.equal(await execute(github, context, {}, nodeRequire), 51);
		assert.equal(draft.assets.length, 1);
		assert.equal(draft.assets[0].digest, `sha256:${sha256Bytes(bytes)}`);
	} finally {
		if (oldManifest === undefined) delete process.env.STABLE_MANIFEST;
		else process.env.STABLE_MANIFEST = oldManifest;
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("final tag CAS models reject squats and preserve reservation-tag-publish order", async () => {
	const exactCas = async ({ read, create, expected }) => {
		try {
			const existing = await read();
			if (existing.type !== "commit" || existing.sha !== expected) throw new Error("unsafe tag");
			return existing;
		} catch (error) {
			if (error.status !== 404) throw error;
			try { await create(); } catch (race) { if (race.status !== 422) throw race; }
			const raced = await read();
			if (raced.type !== "commit" || raced.sha !== expected) throw new Error("unsafe tag");
			return raced;
		}
	};
	const missing = Object.assign(new Error("missing"), { status: 404 });
	const raced = Object.assign(new Error("race"), { status: 422 });
	assert.deepEqual(await exactCas({ read: async () => ({ type: "commit", sha: source.commit }), create: async () => {}, expected: source.commit }), { type: "commit", sha: source.commit });
	let reads = 0;
	assert.deepEqual(await exactCas({
		read: async () => { reads += 1; if (reads === 1) throw missing; return { type: "commit", sha: source.commit }; },
		create: async () => { throw raced; }, expected: source.commit,
	}), { type: "commit", sha: source.commit });
	for (const unsafe of [{ type: "tag", sha: source.commit }, { type: "commit", sha: "f".repeat(40) }]) {
		await assert.rejects(() => exactCas({ read: async () => unsafe, create: async () => {}, expected: source.commit }), /unsafe tag/);
	}
	const preview = readFileSync(join(root, ".github/workflows/pylon-preview-release.yml"), "utf8");
	assert.ok(preview.indexOf("refs/tags/${tag}") < preview.indexOf("repos.createRelease"));
	assert.ok(preview.lastIndexOf("await requireExactTag()") < preview.lastIndexOf("repos.updateRelease"));
	const stable = readFileSync(join(root, ".github/workflows/pylon-stable-release.yml"), "utf8");
	const publish = stable.slice(stable.indexOf("name: Reserve and publish immutable stable sequence"));
	assert.ok(publish.indexOf('POST /repos/{owner}/{repo}/releases/{release_id}/assets') < publish.indexOf("refs/tags/${reservationTag}"));
	assert.ok(publish.indexOf("downloadedBytes.equals(bytes)") < publish.indexOf("refs/tags/${reservationTag}"));
	assert.ok(publish.indexOf("refs/tags/${reservationTag}") < publish.indexOf("refs/tags/${manifest.tag}"));
	assert.ok(publish.indexOf("refs/tags/${manifest.tag}") < publish.indexOf("repos.updateRelease"));
});

test("workflow static policy proves direct approvals and every contents-write graph", () => {
	const workflowFiles = readdirSync(join(root, ".github/workflows"))
		.filter((file) => /\.ya?ml$/.test(file))
		.map((file) => `.github/workflows/${file}`);
	const workflows = new Map(workflowFiles.map((file) => [file, readFileSync(join(root, file), "utf8")]));
	for (const [file, workflow] of workflows) {
		assert.doesNotMatch(workflow, /^permissions:\n(?:  [^\n]+\n)*  contents: write$/m, `${file} grants top-level contents write`);
		for (const match of workflow.matchAll(/^\s*uses:\s*[^\s@]+@([^\s#]+)/gm)) {
			assert.match(match[1], /^[0-9a-f]{40}$/, `${file} contains an unpinned action`);
		}
	}
	const blocks = (workflow) => {
		const matches = [...workflow.matchAll(/^  ([A-Za-z0-9_-]+):\s*$/gm)];
		return new Map(matches.map((match, index) => [
			match[1],
			workflow.slice(match.index, matches[index + 1]?.index ?? workflow.length),
		]));
	};
	const needs = (block, job) => new RegExp(`^    needs:.*(?:\\[|, | )${job}(?:\\]|,|$)`, "m").test(block);
	assert.equal(ATTEST_ACTION_CHAIN, "actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d");
	const preview = workflows.get(".github/workflows/pylon-preview-release.yml");
	const stable = workflows.get(".github/workflows/pylon-stable-release.yml");
	assert.deepEqual(validateApprovedAttestationWorkflow(preview, "preview"), {
		workflow: ".github/workflows/pylon-preview-release.yml",
		environment: "pylon-preview",
	});
	assert.deepEqual(validateApprovedAttestationWorkflow(stable, "stable"), {
		workflow: ".github/workflows/pylon-stable-release.yml",
		environment: "pylon-stable",
	});
	for (const changed of [
		preview.replace("environment: pylon-preview", "environment: other"),
		preview.replace("id-token: write", "id-token: read"),
		preview.replace("4d101475d8b20a2381f78447822ac1eab6504dd8", "f".repeat(40)),
		preview.replace("subject-path: .npm/pylon-release/artifacts/*", "subject-path: publication/*"),
		preview.replace("needs: [pack, reproducibility, install]", "needs: pack"),
		preview.replace("      - name: Generate build provenance", "      - run: node scripts/untrusted.mjs\n      - name: Generate build provenance"),
	]) assert.throws(() => validateApprovedAttestationWorkflow(changed, "preview"));

	const approvedWriters = new Set([
		".github/workflows/pylon-preview-release.yml:stage-draft",
		".github/workflows/pylon-preview-release.yml:publish",
		".github/workflows/pylon-stable-release.yml:stage-draft",
		".github/workflows/pylon-stable-release.yml:publish",
		".github/workflows/pylon-upstream-sync.yml:sync",
	]);
	const foundWriters = new Set();
	for (const [file, workflow] of workflows) {
		for (const [name, block] of blocks(workflow)) {
			if (!/^      contents: write$/m.test(block)) continue;
			const identity = `${file}:${name}`;
			foundWriters.add(identity);
			assert.ok(approvedWriters.has(identity), `unapproved contents writer: ${identity}`);
			if (identity !== ".github/workflows/pylon-upstream-sync.yml:sync") {
				assert.doesNotMatch(block, /actions\/checkout|actions\/setup-node|npm (?:ci|run|install)|node scripts\/|\.tgz\b.*(?:exec|run)/);
			}
			assert.doesNotMatch(block, /id-token: write|attestations: write/);
		}
	}
	assert.deepEqual(foundWriters, approvedWriters);
	const previewJobs = blocks(preview);
	assert.ok(needs(previewJobs.get("stage-draft"), "verify-attestation"));
	assert.ok(needs(previewJobs.get("publish"), "stage-draft"));
	assert.ok(needs(previewJobs.get("publish"), "verify-attestation"));
	const stableJobs = blocks(stable);
	assert.ok(needs(stableJobs.get("stage-draft"), "verify-attestation"));
	assert.ok(needs(stableJobs.get("publish"), "stage-draft"));
	assert.ok(needs(stableJobs.get("publish"), "authorize-stable-resume"));
	assert.match(stableJobs.get("authorize-stable-resume"), /environment: pylon-stable/);
	assert.match(stableJobs.get("authorize-stable-resume"), /permissions: \{\}/);
	assert.match(stableJobs.get("attest"), /if: .*mode == 'normal'/);
	assert.match(stableJobs.get("authorize-stable-resume"), /if: .*mode == 'resume'/);
	const upstream = blocks(workflows.get(".github/workflows/pylon-upstream-sync.yml"));
	assert.match(upstream.get("sync"), /environment: pylon-upstream-sync/);
	assert.doesNotMatch(workflows.get(".github/workflows/pylon-upstream-sync.yml"), /authorize-upstream-sync/);
	assert.match(stable, /group: pylon-stable-publication[\s\S]*cancel-in-progress: false/);
	assert.match(stable, /final live read authorizes the current tip/i);
	assert.match(stable, /refetched state and stopped without N\+1, move, or delete/);
	assert.match(stable, /Draft release: \$\{draft\.id\}/);
	assert.match(stable, /Withdraw build tag:/);
	assert.match(stable, /separate lightweight tag CAS/);
	assert.doesNotMatch(stable, /manifest\.sequence\s*\+\+|updateRef|deleteRef|deleteRelease|deleteReleaseAsset/);
	const attestationVerifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	for (const flag of ["--cert-identity", "--signer-digest", "--source-ref", "--source-digest", "--cert-oidc-issuer", "--predicate-type", "--deny-self-hosted-runners"]) {
		assert.match(attestationVerifier, new RegExp(flag));
	}
	assert.match(attestationVerifier, /verifyApprovedWorkflowAtSignerDigest/);
});
