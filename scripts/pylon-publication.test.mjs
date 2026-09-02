import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import {
	chmodSync,
	closeSync,
	existsSync,
	fsyncSync,
	fstatSync,
	linkSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	openSync,
	readFileSync,
	readSync,
	readdirSync,
	realpathSync,
	renameSync,
	rmSync,
	statSync,
	symlinkSync,
	truncateSync,
	utimesSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { lstat as lstatFile, open as openFileHandle, readdir as readDirectoryEntries } from "node:fs/promises";
import { test } from "node:test";
import { parse as parseYaml } from "yaml";

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
	CREATE_GITHUB_APP_TOKEN_ACTION,
	validateApprovedAttestationWorkflow,
	validateApprovedWorkflowBytes,
} from "./lib/pylon-workflow-policy.mjs";
import {
	PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY,
	PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES,
} from "./lib/pylon-ruleset-auditor.mjs";
import { validatePreviewWorkflowRunEvidence, verifyGhAttestationResult } from "./verify-pylon-publication-attestations.mjs";
import {
	assertContainerRemoved,
	assertExactDockerRunArgs,
	buildDockerRunArgs,
	copyBoundedSubjects,
	exactArtifactDirectory,
	formatPublicFailure,
	installSignalCleanup,
	PYLON_SANDBOX_POLICY,
	PYLON_SANDBOX_PUBLIC_FAIL_STAGES,
	readFinalReceipt,
	releaseOnlyCopy,
	runDetachedProcessGroup,
	validateImageInspection,
} from "./run-pylon-release-sandbox.mjs";
import { recordPreviewHighWater } from "./verify-pylon-preview-history.mjs";
import { verifyStableHistoryWithState } from "./verify-pylon-stable-history.mjs";
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";
import {
	ensureDurableConsumerStateDirectory,
	migrateConsumerStateJournal,
	rotateConsumerStateJournal,
	withConsumerStateLock,
} from "./lib/pylon-consumer-lock.mjs";
import {
	BoundedFileLinkRetiredBeforeReadError,
	BoundedFileLinkRetiredDuringReadError,
	BoundedFileUnlinkedDuringReadError,
	PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
	PYLON_STABLE_HISTORY_MAX_MANIFESTS,
	readBoundedRegularFile,
	readBoundedRegularFileSync,
} from "./lib/pylon-bounded-file.mjs";
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

function consumerJournal(statePath) {
	const journal = `${statePath}.journal`;
	const checkpoint = readdirSync(journal).find((name) => name.startsWith("checkpoint-"));
	const epoch = readdirSync(journal).find((name) => name.startsWith("epoch-"));
	if (!checkpoint || !epoch) throw new Error("consumer journal fixture is incomplete");
	return { journal, checkpoint: join(journal, checkpoint), epoch: join(journal, epoch) };
}

function transitionNames(statePath) {
	return readdirSync(consumerJournal(statePath).epoch).filter((name) => name.startsWith("transition-"));
}

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
		node_id: "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4",
		name: "Pylon immutable publication tags",
		target: "tag",
		source_type: "Repository",
		source: "pylon-code/prime-agent",
		enforcement: "active",
		bypass_actors: [],
		current_user_can_bypass: "never",
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

function exactPublicationTagRulesetGraphql() {
	return {
		repository: {
			id: "R_kgDOUGgkLQ",
			databaseId: 1_349_002_285,
			nameWithOwner: "pylon-code/prime-agent",
			ruleset: {
				id: "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4",
				databaseId: 21_950_766,
				name: "Pylon immutable publication tags",
				enforcement: "ACTIVE",
				target: "TAG",
				source: {
					__typename: "Repository",
					id: "R_kgDOUGgkLQ",
					databaseId: 1_349_002_285,
					nameWithOwner: "pylon-code/prime-agent",
				},
				bypassActors: { totalCount: 0 },
				conditions: {
					refName: { include: ["refs/tags/pylon-build-*", "refs/tags/pylon-stable-*"], exclude: [] },
					organizationProperty: null,
					repositoryId: null,
					repositoryName: null,
					repositoryProperty: null,
				},
				rules: {
					totalCount: 2,
					nodes: [
						{ type: "UPDATE", parameters: { __typename: "UpdateParameters", updateAllowsFetchAndMerge: false } },
						{ type: "DELETION", parameters: null },
					],
				},
			},
		},
	};
}

async function inlinePublicationTagRulesetValidator(restResponses, graphqlResponses = [exactPublicationTagRulesetGraphql()]) {
	const script = githubScriptForStep(
		".github/workflows/pylon-preview-release.yml",
		"Require authoritative publication tag ruleset before preview tag CAS",
	);
	const validate = new AsyncFunction("github", script);
	let restRequest = 0;
	let graphqlRequest = 0;
	const order = [];
	const github = {
		request: async (route, parameters) => {
			order.push("REST");
			assert.equal(route, "GET /repos/{owner}/{repo}/rulesets/{ruleset_id}");
			assert.deepEqual(parameters, {
				owner: "pylon-code", repo: "prime-agent", ruleset_id: 21_950_766, includes_parents: false,
				headers: { accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
			});
			const response = restResponses[Math.min(restRequest, restResponses.length - 1)];
			restRequest += 1;
			if (response instanceof Error) throw response;
			if (response && Object.hasOwn(response, "status") && Object.hasOwn(response, "data")) return response;
			return { status: 200, data: response };
		},
		graphql: async (query, variables) => {
			order.push("GraphQL");
			assert.equal(query, PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY);
			assert.deepEqual(variables, PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES);
			const response = graphqlResponses[Math.min(graphqlRequest, graphqlResponses.length - 1)];
			graphqlRequest += 1;
			if (response instanceof Error) throw response;
			return response;
		},
	};
	return {
		validate: async () => {
			const result = await validate(github);
			assert.equal(order.at(-1), "GraphQL", "GraphQL must be the last authoritative read before return");
			return result;
		},
		requests: () => ({ rest: restRequest, graphql: graphqlRequest, order: [...order] }),
	};
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
		{ ...invocation, publicationPolicyRevision: 4 },
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
		writeFileSync(legacyPath, canonicalJson(initialized.state), { mode: 0o600 });
		const migrated = await verifyStableHistoryWithState([firstPath], { statePath: legacyPath });
		assert.equal(migrated.advanced, false);
		assert.equal(transitionNames(legacyPath).length, 1);
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
		writeFileSync(statePath, "{}\n", { mode: 0o600 });
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
			/legacy consumer lock guard.*regular non-symlink file/i,
		);
		const badJournalState = join(fixture, "bad-journal.json");
		symlinkSync(realDirectory, `${badJournalState}.journal`);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: badJournalState, initialize: true }),
			/journal directory.*real directory/,
		);
		const badJournalEntryState = join(fixture, "bad-journal-entry.json");
		mkdirSync(`${badJournalEntryState}.journal`, { mode: 0o700 });
		writeFileSync(join(`${badJournalEntryState}.journal`, ".unexpected"), "bad\n");
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: badJournalEntryState, initialize: true }),
			/unexpected hidden entry/,
		);

		const exactMetadataState = join(fixture, "exact-metadata.json");
		await verifyStableHistoryWithState([manifestPath], { statePath: exactMetadataState, initialize: true });
		const exactMetadata = consumerJournal(exactMetadataState);
		assert.equal(statSync(`${exactMetadataState}.lock`).mode & 0o777, 0o600);
		assert.equal(statSync(exactMetadataState).mode & 0o777, 0o600);
		assert.equal(statSync(exactMetadata.journal).mode & 0o777, 0o700);
		assert.equal(statSync(join(exactMetadata.journal, ".owned-temporaries-v2")).mode & 0o777, 0o700);
		assert.equal(statSync(exactMetadata.epoch).mode & 0o777, 0o700);
		chmodSync(exactMetadataState, 0o666);
		chmodSync(`${exactMetadataState}.lock`, 0o666);
		chmodSync(exactMetadata.journal, 0o777);
		chmodSync(exactMetadata.checkpoint, 0o666);
		chmodSync(exactMetadata.epoch, 0o777);
		for (const name of readdirSync(exactMetadata.epoch)) chmodSync(join(exactMetadata.epoch, name), 0o666);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: exactMetadataState }),
			/exact 700 permissions|exact 600 permissions/,
		);
		chmodSync(exactMetadataState, 0o600);
		chmodSync(`${exactMetadataState}.lock`, 0o600);
		chmodSync(exactMetadata.journal, 0o700);
		chmodSync(exactMetadata.checkpoint, 0o600);
		chmodSync(exactMetadata.epoch, 0o700);
		for (const name of readdirSync(exactMetadata.epoch)) chmodSync(join(exactMetadata.epoch, name), 0o600);
		if (typeof process.getuid === "function") {
			await assert.rejects(
				() => withConsumerStateLock(exactMetadataState, async () => {}, { currentUid: process.getuid() + 1 }),
				/owned by the current uid/,
			);
		}

		const heldWritableFdState = join(fixture, "held-writable-fd.json");
		const highBytes = Buffer.from('{"value":"HIGH"}\n');
		const lowBytes = Buffer.from('{"value":"LOW!"}\n');
		await withConsumerStateLock(heldWritableFdState, async (_path, transaction) => {
			await transaction.commitState(highBytes);
		});
		const heldWritableFd = openSync(heldWritableFdState, "r+");
		chmodSync(heldWritableFdState, 0o666);
		await assert.rejects(
			() => withConsumerStateLock(heldWritableFdState, async () => {}),
			/exact 600 permissions/,
		);
		writeSync(heldWritableFd, lowBytes, 0, lowBytes.length, 0);
		fsyncSync(heldWritableFd);
		closeSync(heldWritableFd);
		chmodSync(heldWritableFdState, 0o600);
		await withConsumerStateLock(heldWritableFdState, async (_path, transaction) => {
			assert.equal(transaction.readStateBytes().equals(highBytes), true, "a held writable fd never rolls back journal authority");
		});
		assert.equal(readFileSync(heldWritableFdState).equals(highBytes), true);

		const orphanState = join(fixture, "orphan-metadata.json");
		await verifyStableHistoryWithState([manifestPath], { statePath: orphanState, initialize: true });
		const orphanEpoch = consumerJournal(orphanState).epoch;
		const orphanToken = randomUUID();
		for (const [kind, value, message] of [
			["heartbeat", { schemaVersion: 2, generation: 1, token: orphanToken, refreshedAtMs: 1 }, /orphan heartbeat/],
			["terminal", { schemaVersion: 2, generation: 1, token: orphanToken, outcome: "released" }, /orphan terminal/],
			["applied", { schemaVersion: 2, generation: 1, token: orphanToken, terminalSha256: "0".repeat(64) }, /orphan applied/],
		]) {
			const path = join(orphanEpoch, `${kind}-0000000000000001-${orphanToken}.json`);
			writeFileSync(path, `${JSON.stringify(value)}\n`, { mode: 0o600 });
			await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath: orphanState }), message);
			rmSync(path);
		}
		writeFileSync(join(orphanEpoch, "unexpected-extra"), "bad\n", { mode: 0o600 });
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: orphanState }),
			/malformed or unexpected entry/,
		);
		rmSync(join(orphanEpoch, "unexpected-extra"));
		const checkpoint = JSON.parse(readFileSync(consumerJournal(orphanState).checkpoint));
		const symlinkTemporary = join(
			orphanEpoch,
			`.pylon-consumer-tmp-v1-p1-e${checkpoint.epochId}-g0000000000000001-w${randomUUID()}` +
				`-n${"a".repeat(12)}-kclaim-t${"0".repeat(64)}.tmp`,
		);
		symlinkSync(manifestPath, symlinkTemporary);
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath: orphanState }),
			/owned temporary.*regular non-symlink file/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("consumer stable high-water pins and bounds every manifest before parsing", async () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-stable-state-"));
	try {
		const target = join(fixture, "target.json");
		const manifestPath = join(fixture, "first.json");
		const statePath = join(fixture, "stable.json");
		writeFileSync(target, canonicalJson(firstStable()));
		symlinkSync(target, manifestPath);
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath, initialize: true }), /regular non-symlink file/);
		rmSync(manifestPath);
		writeFileSync(manifestPath, JSON.stringify(firstStable()));
		await assert.rejects(() => verifyStableHistoryWithState([manifestPath], { statePath, initialize: true }), /not canonical/);

		writeFileSync(manifestPath, Buffer.alloc(PYLON_PUBLICATION_MANIFEST_MAX_BYTES + 1));
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], { statePath, initialize: true }),
			/format byte limit/,
		);
		writeFileSync(manifestPath, canonicalJson(firstStable()));
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], {
				statePath,
				initialize: true,
				fileOptions: { hooks: { afterInitialStat: ({ path }) => writeFileSync(path, "x", { flag: "a" }) } },
			}),
			/changed while it was read/,
		);
		writeFileSync(manifestPath, canonicalJson(firstStable()));
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], {
				statePath,
				initialize: true,
				fileOptions: { hooks: { afterInitialStat: ({ path }) => truncateSync(path, 1) } },
			}),
			/changed while it was read/,
		);

		writeFileSync(manifestPath, canonicalJson(firstStable()));
		const moved = join(fixture, "pinned-original.json");
		let swapped = false;
		await assert.rejects(
			() => verifyStableHistoryWithState([manifestPath], {
				statePath: join(fixture, "pinned-state.json"),
				initialize: true,
				fileOptions: {
					hooks: {
						afterInitialStat: ({ path }) => {
							if (swapped) return;
							swapped = true;
							renameSync(path, moved);
							writeFileSync(path, "replacement must not be read");
						},
					},
				},
			}),
			(error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			},
		);
		assert.equal(swapped, true, "the descriptor re-stat detects a pathname swap without reading the replacement");

		const replacedBeforeOpen = join(fixture, "replaced-before-open.json");
		const replacedBeforeOpenMoved = join(fixture, "replaced-before-open-original.json");
		writeFileSync(replacedBeforeOpen, "original");
		let replacedOnOpen = false;
		await assert.rejects(
			() => readBoundedRegularFile(replacedBeforeOpen, {
				maxBytes: 1024,
				description: "Replaced bounded input",
				openFile: async (path, flags) => {
					if (!replacedOnOpen) {
						replacedOnOpen = true;
						renameSync(path, replacedBeforeOpenMoved);
						writeFileSync(path, "replacement");
					}
					return openFileHandle(path, flags);
				},
			}),
			(error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			},
		);
		assert.equal(replacedOnOpen, true, "an inode replaced between lstat and open is terminal, not convergence");

		const overwrittenAsync = join(fixture, "overwritten-unlinked-async.bin");
		const originalAsyncBytes = Buffer.from("original-async");
		const replacementAsyncBytes = Buffer.from("replaced-async");
		assert.equal(originalAsyncBytes.length, replacementAsyncBytes.length);
		writeFileSync(overwrittenAsync, originalAsyncBytes);
		const originalAsyncStat = statSync(overwrittenAsync);
		await assert.rejects(
			() => readBoundedRegularFile(overwrittenAsync, {
				maxBytes: 1024,
				description: "Overwritten async bounded input",
				expectedSha256: sha256Bytes(originalAsyncBytes),
				hooks: {
					afterInitialStat: () => {
						writeFileSync(overwrittenAsync, replacementAsyncBytes);
						utimesSync(overwrittenAsync, originalAsyncStat.atime, originalAsyncStat.mtime);
						rmSync(overwrittenAsync);
					},
				},
			}),
			(error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			},
		);

		const unanchoredRemovalPath = join(fixture, "unanchored-removal.bin");
		writeFileSync(unanchoredRemovalPath, "legacy");
		await assert.rejects(
			() => readBoundedRegularFile(unanchoredRemovalPath, {
				maxBytes: 1024,
				description: "Unanchored bounded input",
				hooks: { afterInitialStat: () => rmSync(unanchoredRemovalPath) },
			}),
			(error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			},
		);

		const overwrittenSync = join(fixture, "overwritten-unlinked-sync.bin");
		const originalSyncBytes = Buffer.from("original-sync");
		const replacementSyncBytes = Buffer.from("replaced-sync");
		assert.equal(originalSyncBytes.length, replacementSyncBytes.length);
		writeFileSync(overwrittenSync, originalSyncBytes);
		const originalSyncStat = statSync(overwrittenSync);
		assert.throws(
			() => readBoundedRegularFileSync(overwrittenSync, {
				maxBytes: 1024,
				description: "Overwritten sync bounded input",
				expectedSha256: sha256Bytes(originalSyncBytes),
				hooks: {
					afterInitialStat: () => {
						writeFileSync(overwrittenSync, replacementSyncBytes);
						utimesSync(overwrittenSync, originalSyncStat.atime, originalSyncStat.mtime);
						rmSync(overwrittenSync);
					},
				},
			}),
			(error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			},
		);

		const asyncFstatFailurePath = join(fixture, "async-fstat-failure.bin");
		writeFileSync(asyncFstatFailurePath, "fstat");
		const asyncFstatFailure = new Error("injected async final fstat failure");
		await assert.rejects(
			() => readBoundedRegularFile(asyncFstatFailurePath, {
				maxBytes: 1024,
				description: "Async fstat failure input",
				expectedSha256: sha256Bytes(Buffer.from("fstat")),
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					let statCalls = 0;
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => {
							statCalls += 1;
							if (statCalls === 2) throw asyncFstatFailure;
							return handle.stat();
						},
					};
				},
			}),
			(error) => error === asyncFstatFailure,
		);

		const asyncFinalLstatFailurePath = join(fixture, "async-final-lstat-failure.bin");
		writeFileSync(asyncFinalLstatFailurePath, "lstat");
		const asyncFinalLstatFailure = new Error("injected async final lstat failure");
		let asyncLstatCalls = 0;
		await assert.rejects(
			() => readBoundedRegularFile(asyncFinalLstatFailurePath, {
				maxBytes: 1024,
				description: "Async lstat failure input",
				expectedSha256: sha256Bytes(Buffer.from("lstat")),
				lstatEntry: async (path) => {
					asyncLstatCalls += 1;
					if (asyncLstatCalls === 2) throw asyncFinalLstatFailure;
					return lstatFile(path);
				},
			}),
			(error) => error === asyncFinalLstatFailure,
		);

		const syncFstatFailurePath = join(fixture, "sync-fstat-failure.bin");
		writeFileSync(syncFstatFailurePath, "fstat");
		const syncFstatFailure = new Error("injected sync final fstat failure");
		let syncFstatCalls = 0;
		assert.throws(
			() => readBoundedRegularFileSync(syncFstatFailurePath, {
				maxBytes: 1024,
				description: "Sync fstat failure input",
				expectedSha256: sha256Bytes(Buffer.from("fstat")),
				statFile: (descriptor) => {
					syncFstatCalls += 1;
					if (syncFstatCalls === 2) throw syncFstatFailure;
					return fstatSync(descriptor);
				},
			}),
			(error) => error === syncFstatFailure,
		);

		const syncFinalLstatFailurePath = join(fixture, "sync-final-lstat-failure.bin");
		writeFileSync(syncFinalLstatFailurePath, "lstat");
		const syncFinalLstatFailure = new Error("injected sync final lstat failure");
		let syncLstatCalls = 0;
		assert.throws(
			() => readBoundedRegularFileSync(syncFinalLstatFailurePath, {
				maxBytes: 1024,
				description: "Sync lstat failure input",
				expectedSha256: sha256Bytes(Buffer.from("lstat")),
				lstatEntry: (path) => {
					syncLstatCalls += 1;
					if (syncLstatCalls === 2) throw syncFinalLstatFailure;
					return lstatSync(path);
				},
			}),
			(error) => error === syncFinalLstatFailure,
		);

		const maximum = join(fixture, "maximum.bin");
		writeFileSync(maximum, Buffer.alloc(PYLON_PUBLICATION_MANIFEST_MAX_BYTES, 0x61));
		assert.equal(
			(await readBoundedRegularFile(maximum, {
				maxBytes: PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
				description: "Maximum valid bounded input",
			})).length,
			PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
		);
		await assert.rejects(
			() => verifyStableHistoryWithState(
				Array(PYLON_STABLE_HISTORY_MAX_MANIFESTS + 1).fill(maximum),
				{ statePath, initialize: true },
			),
			/manifest work bound/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("bounded reads authenticate only exact pre-read link retirement transitions", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-bounded-link-retirement-")));
	const exactBytes = Buffer.from("exact-retirement-bytes");
	const exactDigest = sha256Bytes(exactBytes);
	const rejectGenericChange = async (operation) => {
		await assert.rejects(operation, (error) => {
			assert.equal(error instanceof BoundedFileLinkRetiredBeforeReadError, false);
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read|regular non-symlink file/);
			return true;
		});
	};
	try {
		const asyncPrivate = join(fixture, "async-private");
		const asyncPublic = join(fixture, "async-public");
		writeFileSync(asyncPrivate, exactBytes);
		linkSync(asyncPrivate, asyncPublic);
		await assert.rejects(
			() => readBoundedRegularFile(asyncPublic, {
				maxBytes: 1024,
				description: "Async linked input",
				expectedSha256: exactDigest,
				openFile: async (path, flags) => {
					rmSync(asyncPrivate);
					return openFileHandle(path, flags);
				},
			}),
			(error) => {
				assert.equal(error.constructor, BoundedFileLinkRetiredBeforeReadError);
				assert.equal(error.name, "BoundedFileLinkRetiredBeforeReadError");
				assert.equal(error.path, asyncPublic);
				assert.equal(error.description, "Async linked input");
				assert.equal(error.bytes.equals(exactBytes), true);
				assert.equal(error.expectedSha256, exactDigest);
				assert.equal(error.sha256, exactDigest);
				assert.equal(Object.isFrozen(error.statTransition), true);
				assert.deepEqual(Object.keys(error.statTransition).sort(), ["openedHandle", "pathEntry"]);
				const { pathEntry, openedHandle } = error.statTransition;
				assert.equal(Object.isFrozen(pathEntry), true);
				assert.equal(Object.isFrozen(openedHandle), true);
				assert.deepEqual(
					[pathEntry, openedHandle].map((stat) => Object.keys(stat).sort()),
					Array(2).fill(["ctimeMs", "dev", "ino", "mtimeMs", "nlink", "size"]),
				);
				assert.deepEqual(
					[pathEntry.dev, pathEntry.ino, pathEntry.size, pathEntry.mtimeMs],
					[openedHandle.dev, openedHandle.ino, openedHandle.size, openedHandle.mtimeMs],
				);
				assert.equal(pathEntry.ctimeMs === openedHandle.ctimeMs, false);
				assert.deepEqual([pathEntry.nlink, openedHandle.nlink], [2, 1]);
				const finalPath = lstatSync(asyncPublic);
				assert.equal(finalPath.isFile(), true);
				assert.equal(finalPath.isSymbolicLink(), false);
				assert.deepEqual(
					[finalPath.dev, finalPath.ino, finalPath.size, finalPath.mtimeMs, finalPath.ctimeMs, finalPath.nlink],
					[openedHandle.dev, openedHandle.ino, openedHandle.size, openedHandle.mtimeMs, openedHandle.ctimeMs, 1],
				);
				return true;
			},
		);

		const syncPrivate = join(fixture, "sync-private");
		const syncPublic = join(fixture, "sync-public");
		writeFileSync(syncPrivate, exactBytes);
		linkSync(syncPrivate, syncPublic);
		assert.throws(
			() => readBoundedRegularFileSync(syncPublic, {
				maxBytes: 1024,
				description: "Sync linked input",
				expectedSha256: exactDigest,
				openFile: (path, flags) => {
					rmSync(syncPrivate);
					return openSync(path, flags);
				},
			}),
			(error) => {
				assert.equal(error.constructor, BoundedFileLinkRetiredBeforeReadError);
				assert.equal(error.name, "BoundedFileLinkRetiredBeforeReadError");
				assert.equal(error.path, syncPublic);
				assert.equal(error.description, "Sync linked input");
				assert.equal(error.bytes.equals(exactBytes), true);
				assert.equal(error.expectedSha256, exactDigest);
				assert.equal(error.sha256, exactDigest);
				assert.equal(Object.isFrozen(error.statTransition), true);
				const { pathEntry, openedHandle } = error.statTransition;
				assert.equal(Object.isFrozen(pathEntry), true);
				assert.equal(Object.isFrozen(openedHandle), true);
				assert.deepEqual(
					[pathEntry, openedHandle].map((stat) => Object.keys(stat).sort()),
					Array(2).fill(["ctimeMs", "dev", "ino", "mtimeMs", "nlink", "size"]),
				);
				assert.deepEqual([pathEntry.nlink, openedHandle.nlink], [2, 1]);
				assert.deepEqual(
					[pathEntry.dev, pathEntry.ino, pathEntry.size, pathEntry.mtimeMs],
					[openedHandle.dev, openedHandle.ino, openedHandle.size, openedHandle.mtimeMs],
				);
				assert.notEqual(pathEntry.ctimeMs, openedHandle.ctimeMs);
				const finalPath = lstatSync(syncPublic);
				assert.equal(finalPath.isFile(), true);
				assert.equal(finalPath.isSymbolicLink(), false);
				assert.deepEqual(
					[finalPath.dev, finalPath.ino, finalPath.size, finalPath.mtimeMs, finalPath.ctimeMs, finalPath.nlink],
					[openedHandle.dev, openedHandle.ino, openedHandle.size, openedHandle.mtimeMs, openedHandle.ctimeMs, 1],
				);
				return true;
			},
		);

		const asyncUnlinked = join(fixture, "async-unlinked");
		writeFileSync(asyncUnlinked, exactBytes);
		await assert.rejects(
			() => readBoundedRegularFile(asyncUnlinked, {
				maxBytes: 1024,
				description: "Async unlinked input",
				expectedSha256: exactDigest,
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					rmSync(path);
					return handle;
				},
			}),
			(error) => {
				assert.equal(error.constructor, BoundedFileUnlinkedDuringReadError);
				assert.equal(error.name, "BoundedFileUnlinkedDuringReadError");
				assert.equal(error instanceof BoundedFileLinkRetiredBeforeReadError, false);
				assert.equal(error.path, asyncUnlinked);
				assert.equal(error.description, "Async unlinked input");
				assert.equal(error.bytes.equals(exactBytes), true);
				assert.equal(error.expectedSha256, exactDigest);
				assert.equal(error.sha256, exactDigest);
				assert.equal(Object.isFrozen(error.statTransition), true);
				const { pathEntry, before, after, confirmedHandle } = error.statTransition;
				assert.equal(confirmedHandle, null);
				assert.equal([pathEntry, before, after].every((stat) => Object.isFrozen(stat)), true);
				assert.deepEqual(
					[pathEntry, before, after].map((stat) => Object.keys(stat).sort()),
					Array(3).fill(["ctimeMs", "dev", "ino", "mtimeMs", "nlink", "size"]),
				);
				assert.deepEqual([pathEntry.nlink, before.nlink, after.nlink], [1, 0, 0]);
				assert.equal(before.ctimeMs, after.ctimeMs);
				assert.notEqual(pathEntry.ctimeMs, before.ctimeMs);
				assert.equal(
					new Set([pathEntry, before, after].map((stat) => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`)).size,
					1,
				);
				assert.equal(existsSync(error.path), false);
				return true;
			},
		);

		const syncUnlinked = join(fixture, "sync-unlinked");
		writeFileSync(syncUnlinked, exactBytes);
		assert.throws(
			() => readBoundedRegularFileSync(syncUnlinked, {
				maxBytes: 1024,
				description: "Sync unlinked input",
				expectedSha256: exactDigest,
				openFile: (path, flags) => {
					const descriptor = openSync(path, flags);
					rmSync(path);
					return descriptor;
				},
			}),
			(error) => {
				assert.equal(error.constructor, BoundedFileUnlinkedDuringReadError);
				assert.equal(error.name, "BoundedFileUnlinkedDuringReadError");
				assert.equal(error.path, syncUnlinked);
				assert.equal(error.description, "Sync unlinked input");
				assert.equal(error.bytes.equals(exactBytes), true);
				assert.equal(error.expectedSha256, exactDigest);
				assert.equal(error.sha256, exactDigest);
				assert.equal(Object.isFrozen(error.statTransition), true);
				const { pathEntry, before, after, confirmedHandle } = error.statTransition;
				assert.equal(confirmedHandle, null);
				assert.equal([pathEntry, before, after].every((stat) => Object.isFrozen(stat)), true);
				assert.deepEqual(
					[pathEntry, before, after].map((stat) => Object.keys(stat).sort()),
					Array(3).fill(["ctimeMs", "dev", "ino", "mtimeMs", "nlink", "size"]),
				);
				assert.deepEqual([pathEntry.nlink, before.nlink, after.nlink], [1, 0, 0]);
				assert.equal(before.ctimeMs, after.ctimeMs);
				assert.notEqual(pathEntry.ctimeMs, before.ctimeMs);
				assert.equal(
					new Set([pathEntry, before, after].map((stat) => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`)).size,
					1,
				);
				assert.equal(existsSync(error.path), false);
				return true;
			},
		);

		const replaced = join(fixture, "same-bytes-new-inode");
		const replacedOriginal = join(fixture, "same-bytes-old-inode");
		writeFileSync(replaced, exactBytes);
		await rejectGenericChange(() => readBoundedRegularFile(replaced, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			openFile: async (path, flags) => {
				renameSync(path, replacedOriginal);
				writeFileSync(path, exactBytes);
				return openFileHandle(path, flags);
			},
		}));

		const differentPrivate = join(fixture, "different-private");
		const differentPublic = join(fixture, "different-public");
		const differentBytes = Buffer.from("other-retirement-bytes");
		assert.equal(differentBytes.length, exactBytes.length);
		writeFileSync(differentPrivate, exactBytes);
		linkSync(differentPrivate, differentPublic);
		const differentInitial = statSync(differentPublic);
		await rejectGenericChange(() => readBoundedRegularFile(differentPublic, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			openFile: async (path, flags) => {
				rmSync(differentPrivate);
				writeFileSync(path, differentBytes);
				utimesSync(path, differentInitial.atime, differentInitial.mtime);
				return openFileHandle(path, flags);
			},
		}));

		const unanchoredPrivate = join(fixture, "unanchored-private");
		const unanchoredPublic = join(fixture, "unanchored-public");
		writeFileSync(unanchoredPrivate, exactBytes);
		linkSync(unanchoredPrivate, unanchoredPublic);
		await rejectGenericChange(() => readBoundedRegularFile(unanchoredPublic, {
			maxBytes: 1024,
			openFile: async (path, flags) => {
				rmSync(unanchoredPrivate);
				return openFileHandle(path, flags);
			},
		}));

		const threeLinkPrivate = join(fixture, "three-link-private");
		const threeLinkPublic = join(fixture, "three-link-public");
		const threeLinkOther = join(fixture, "three-link-other");
		writeFileSync(threeLinkPrivate, exactBytes);
		linkSync(threeLinkPrivate, threeLinkPublic);
		linkSync(threeLinkPrivate, threeLinkOther);
		await rejectGenericChange(() => readBoundedRegularFile(threeLinkPublic, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			openFile: async (path, flags) => {
				rmSync(threeLinkPrivate);
				return openFileHandle(path, flags);
			},
		}));

		const nonunitPrivate = join(fixture, "nonunit-private");
		const nonunitPublic = join(fixture, "nonunit-public");
		writeFileSync(nonunitPrivate, exactBytes);
		linkSync(nonunitPrivate, nonunitPublic);
		await rejectGenericChange(() => readBoundedRegularFile(nonunitPublic, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			openFile: async (path, flags) => {
				const handle = await openFileHandle(path, flags);
				rmSync(nonunitPrivate);
				rmSync(nonunitPublic);
				return handle;
			},
		}));

		const symlinkSource = join(fixture, "symlink-source");
		const symlinkTarget = join(fixture, "symlink-target");
		const symlinkMoved = join(fixture, "symlink-moved");
		writeFileSync(symlinkSource, exactBytes);
		writeFileSync(symlinkTarget, exactBytes);
		await rejectGenericChange(() => readBoundedRegularFile(symlinkSource, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			openFile: async (path, flags) => {
				renameSync(path, symlinkMoved);
				symlinkSync(symlinkTarget, path);
				return openFileHandle(path, flags);
			},
		}));

		const ioCases = [
			["initial lstat", (_path, failure) => ({ lstatEntry: async () => { throw failure; } })],
			["open", (_path, failure) => ({ openFile: async () => { throw failure; } })],
			["initial fstat", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => { throw failure; },
					};
				},
			})],
			["read", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					return {
						close: handle.close.bind(handle),
						read: async () => { throw failure; },
						stat: handle.stat.bind(handle),
					};
				},
			})],
			["final fstat", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					let statCalls = 0;
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => {
							statCalls += 1;
							if (statCalls === 2) throw failure;
							return handle.stat();
						},
					};
				},
			})],
			["final lstat", (_path, failure) => {
				let lstatCalls = 0;
				return {
					lstatEntry: async (path) => {
						lstatCalls += 1;
						if (lstatCalls === 2) throw failure;
						return lstatFile(path);
					},
				};
			}],
		];
		for (const [label, options] of ioCases) {
			const path = join(fixture, `io-${label.replace(" ", "-")}`);
			const failure = new Error(`injected ${label} failure`);
			writeFileSync(path, exactBytes);
			await assert.rejects(
				() => readBoundedRegularFile(path, {
					maxBytes: 1024,
					expectedSha256: exactDigest,
					...options(path, failure),
				}),
				(error) => error === failure,
			);
			assert.equal(readFileSync(path).equals(exactBytes), true, `${label} failure cannot write the input`);
		}
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("bounded reads authenticate every exact monotone retirement cut and confirmed unlink", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-bounded-monotone-retirement-")));
	const exactBytes = Buffer.from("monotone-retirement-bytes");
	const exactDigest = sha256Bytes(exactBytes);
	let fixtureNumber = 0;
	const nextPath = (label) => join(fixture, `${String(++fixtureNumber).padStart(3, "0")}-${label}`);
	const linkedPaths = (label, links = 2) => {
		const publicPath = nextPath(`${label}-public`);
		const privatePaths = [];
		writeFileSync(publicPath, exactBytes);
		for (let index = 1; index < links; index += 1) {
			const privatePath = nextPath(`${label}-private-${index}`);
			linkSync(publicPath, privatePath);
			privatePaths.push(privatePath);
		}
		return { publicPath, privatePaths };
	};
	const assertExactStatTransition = (stats, expectedLinks) => {
		assert.deepEqual(stats.map((stat) => stat.nlink), expectedLinks);
		assert.equal(stats.every((stat) => Object.isFrozen(stat)), true);
		assert.deepEqual(
			stats.map((stat) => Object.keys(stat).sort()),
			Array(stats.length).fill(["ctimeMs", "dev", "ino", "mtimeMs", "nlink", "size"]),
		);
		assert.equal(stats.every((stat) => stat.size === exactBytes.length), true);
		assert.equal(new Set(stats.map((stat) => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`)).size, 1);
		let cuts = 0;
		for (let index = 1; index < stats.length; index += 1) {
			if (stats[index - 1].nlink === stats[index].nlink) {
				assert.equal(stats[index - 1].ctimeMs, stats[index].ctimeMs);
			} else {
				cuts += 1;
				assert.notEqual(stats[index - 1].ctimeMs, stats[index].ctimeMs);
			}
		}
		assert.equal(cuts, 1);
	};
	const assertLaterLinkEvidence = (error, expectedCut) => {
		assert.equal(error.constructor, BoundedFileLinkRetiredDuringReadError);
		assert.equal(error.name, "BoundedFileLinkRetiredDuringReadError");
		assert.equal(error.bytes.equals(exactBytes), true);
		assert.equal(error.expectedSha256, exactDigest);
		assert.equal(error.sha256, exactDigest);
		assert.equal(Object.isFrozen(error.statTransition), true);
		assert.deepEqual(Object.keys(error.statTransition).sort(), ["after", "before", "finalPathEntry", "pathEntry"]);
		const stats = [
			error.statTransition.pathEntry,
			error.statTransition.before,
			error.statTransition.after,
			error.statTransition.finalPathEntry,
		];
		assertExactStatTransition(stats, expectedCut === 2 ? [2, 2, 1, 1] : [2, 2, 2, 1]);
		const finalPath = lstatSync(error.path);
		assert.equal(finalPath.isFile(), true);
		assert.equal(finalPath.isSymbolicLink(), false);
		assert.deepEqual(
			[finalPath.dev, finalPath.ino, finalPath.size, finalPath.mtimeMs, finalPath.ctimeMs, finalPath.nlink],
			[
				stats.at(-1).dev,
				stats.at(-1).ino,
				stats.at(-1).size,
				stats.at(-1).mtimeMs,
				stats.at(-1).ctimeMs,
				stats.at(-1).nlink,
			],
		);
		return true;
	};
	const assertUnlinkedEvidence = (error, expectedLinks, confirmed) => {
		assert.equal(error.constructor, BoundedFileUnlinkedDuringReadError);
		assert.equal(error.name, "BoundedFileUnlinkedDuringReadError");
		assert.equal(error.bytes.equals(exactBytes), true);
		assert.equal(error.expectedSha256, exactDigest);
		assert.equal(error.sha256, exactDigest);
		assert.equal(Object.isFrozen(error.statTransition), true);
		assert.deepEqual(
			Object.keys(error.statTransition).sort(),
			["after", "before", "confirmedHandle", "pathEntry"],
		);
		const { pathEntry, before, after, confirmedHandle } = error.statTransition;
		assert.deepEqual([pathEntry.nlink, before.nlink, after.nlink], expectedLinks);
		assert.equal(confirmedHandle === null, !confirmed);
		if (confirmed) assert.equal(confirmedHandle.nlink, 0);
		const stats = [pathEntry, before, after, ...(confirmedHandle === null ? [] : [confirmedHandle])];
		assertExactStatTransition(stats, [...expectedLinks, ...(confirmed ? [0] : [])]);
		assert.equal(existsSync(error.path), false);
		return true;
	};
	const rejectGenericAsync = async (operation) => {
		await assert.rejects(operation, (error) => {
			assert.equal(error instanceof BoundedFileLinkRetiredBeforeReadError, false);
			assert.equal(error instanceof BoundedFileLinkRetiredDuringReadError, false);
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read|regular non-symlink file/);
			return true;
		});
	};
	const rejectGenericSync = (operation) => {
		assert.throws(operation, (error) => {
			assert.equal(error instanceof BoundedFileLinkRetiredBeforeReadError, false);
			assert.equal(error instanceof BoundedFileLinkRetiredDuringReadError, false);
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read|regular non-symlink file/);
			return true;
		});
	};
	try {
		for (const [phase, expectedCut] of [["afterInitialStat", 2], ["afterFinalStat", 3]]) {
			const asyncPaths = linkedPaths(`async-link-${phase}`);
			await assert.rejects(
				() => readBoundedRegularFile(asyncPaths.publicPath, {
					maxBytes: 1024,
					expectedSha256: exactDigest,
					hooks: { [phase]: () => rmSync(asyncPaths.privatePaths[0]) },
				}),
				(error) => assertLaterLinkEvidence(error, expectedCut),
			);
			const syncPaths = linkedPaths(`sync-link-${phase}`);
			assert.throws(
				() => readBoundedRegularFileSync(syncPaths.publicPath, {
					maxBytes: 1024,
					expectedSha256: exactDigest,
					hooks: { [phase]: () => rmSync(syncPaths.privatePaths[0]) },
				}),
				(error) => assertLaterLinkEvidence(error, expectedCut),
			);
		}

		for (const phase of ["before-open", "after-initial", "after-final"]) {
			const asyncPath = nextPath(`async-unlink-${phase}`);
			writeFileSync(asyncPath, exactBytes);
			let asyncStatCalls = 0;
			const asyncOptions = {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					if (phase === "before-open") rmSync(path);
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => {
							asyncStatCalls += 1;
							return handle.stat();
						},
					};
				},
				hooks: phase === "after-initial"
					? { afterInitialStat: () => rmSync(asyncPath) }
					: phase === "after-final" ? { afterFinalStat: () => rmSync(asyncPath) } : {},
			};
			await assert.rejects(
				() => readBoundedRegularFile(asyncPath, asyncOptions),
				(error) => assertUnlinkedEvidence(
					error,
					phase === "before-open" ? [1, 0, 0] : phase === "after-initial" ? [1, 1, 0] : [1, 1, 1],
					phase === "after-final",
				),
			);
			assert.equal(asyncStatCalls, phase === "after-final" ? 3 : 2);

			const syncPath = nextPath(`sync-unlink-${phase}`);
			writeFileSync(syncPath, exactBytes);
			let syncStatCalls = 0;
			assert.throws(
				() => readBoundedRegularFileSync(syncPath, {
					maxBytes: 1024,
					expectedSha256: exactDigest,
					openFile: (path, flags) => {
						const descriptor = openSync(path, flags);
						if (phase === "before-open") rmSync(path);
						return descriptor;
					},
					statFile: (descriptor) => {
						syncStatCalls += 1;
						return fstatSync(descriptor);
					},
					hooks: phase === "after-initial"
						? { afterInitialStat: () => rmSync(syncPath) }
						: phase === "after-final" ? { afterFinalStat: () => rmSync(syncPath) } : {},
				}),
				(error) => assertUnlinkedEvidence(
					error,
					phase === "before-open" ? [1, 0, 0] : phase === "after-initial" ? [1, 1, 0] : [1, 1, 1],
					phase === "after-final",
				),
			);
			assert.equal(syncStatCalls, phase === "after-final" ? 3 : 2);
		}

		const preciseStat = (base, nlink, ctimeMs) => ({
			dev: base.dev,
			ino: base.ino,
			size: base.size,
			mtimeMs: base.mtimeMs,
			ctimeMs,
			nlink,
			isFile: () => true,
			isSymbolicLink: () => false,
		});
		const multipleTransitionPath = nextPath("multiple-observed-transitions");
		writeFileSync(multipleTransitionPath, exactBytes);
		const multipleBase = lstatSync(multipleTransitionPath);
		const multipleStats = [
			preciseStat(multipleBase, 2, 10),
			preciseStat(multipleBase, 1, 20),
			preciseStat(multipleBase, 2, 30),
			preciseStat(multipleBase, 1, 40),
		];
		let multipleLstats = 0;
		await rejectGenericAsync(() => readBoundedRegularFile(multipleTransitionPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			lstatEntry: async () => multipleStats[multipleLstats++ === 0 ? 0 : 3],
			openFile: async (path, flags) => {
				const handle = await openFileHandle(path, flags);
				let stats = 0;
				return {
					close: handle.close.bind(handle),
					read: handle.read.bind(handle),
					stat: async () => multipleStats[++stats],
				};
			},
		}));
		assert.equal(multipleLstats, 2);

		const asyncExtraTransition = async (label, transition) => {
			const path = nextPath(`extra-${label}`);
			writeFileSync(path, exactBytes);
			const base = lstatSync(path);
			const pathEntry = preciseStat(base, transition === "link" ? 2 : 1, 10);
			const before = preciseStat(base, transition === "link" ? 2 : 1, 10);
			const after = preciseStat(base, transition === "link" ? 1 : 0, 20);
			const finalPathEntry = preciseStat(base, 1, 20);
			let lstats = 0;
			await rejectGenericAsync(() => readBoundedRegularFile(path, {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				lstatEntry: async () => {
					lstats += 1;
					if (lstats === 1) return pathEntry;
					if (transition === "unlink") {
						const missing = new Error("precise final path is absent");
						missing.code = "ENOENT";
						throw missing;
					}
					return finalPathEntry;
				},
				openFile: async (openedPath, flags) => {
					const handle = await openFileHandle(openedPath, flags);
					let stats = 0;
					return {
						close: handle.close.bind(handle),
						stat: async () => (++stats === 1 ? before : after),
						read: async (buffer, offset, length, position) => {
							if (position === exactBytes.length) return { bytesRead: 1, buffer };
							return handle.read(buffer, offset, length, position);
						},
					};
				},
			}));
			assert.equal(lstats, 2);
		};
		await asyncExtraTransition("eligible-link", "link");
		await asyncExtraTransition("eligible-unlink", "unlink");

		for (const phase of ["afterInitialStat", "afterFinalStat"]) {
			const { publicPath, privatePaths } = linkedPaths(`new-inode-${phase}`);
			const movedPath = nextPath(`new-inode-${phase}-moved`);
			await rejectGenericAsync(() => readBoundedRegularFile(publicPath, {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				hooks: { [phase]: () => {
					renameSync(publicPath, movedPath);
					writeFileSync(publicPath, exactBytes);
					rmSync(privatePaths[0]);
				} },
			}));
		}

		for (const phase of ["afterInitialStat", "afterFinalStat"]) {
			const path = nextPath(`same-size-mtime-${phase}`);
			const replacement = Buffer.from(exactBytes);
			replacement[0] ^= 1;
			writeFileSync(path, exactBytes);
			const initial = statSync(path);
			await rejectGenericAsync(() => readBoundedRegularFile(path, {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				hooks: { [phase]: () => {
					writeFileSync(path, replacement);
					utimesSync(path, initial.atime, initial.mtime);
				} },
			}));
		}

		const appendPath = nextPath("append");
		writeFileSync(appendPath, exactBytes);
		await rejectGenericAsync(() => readBoundedRegularFile(appendPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => writeFileSync(appendPath, "x", { flag: "a" }) },
		}));
		const truncatePath = nextPath("truncate");
		writeFileSync(truncatePath, exactBytes);
		await rejectGenericAsync(() => readBoundedRegularFile(truncatePath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => truncateSync(truncatePath, 1) },
		}));

		for (const expectedSha256 of [null, "f".repeat(64)]) {
			const { publicPath, privatePaths } = linkedPaths(`digest-${expectedSha256 ?? "absent"}`);
			await rejectGenericAsync(() => readBoundedRegularFile(publicPath, {
				maxBytes: 1024,
				expectedSha256,
				hooks: { afterInitialStat: () => rmSync(privatePaths[0]) },
			}));
		}

		const threeLinks = linkedPaths("three-to-two", 3);
		await rejectGenericAsync(() => readBoundedRegularFile(threeLinks.publicPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => rmSync(threeLinks.privatePaths[0]) },
		}));
		const twoToZero = linkedPaths("two-to-zero");
		await rejectGenericAsync(() => readBoundedRegularFile(twoToZero.publicPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => {
				rmSync(twoToZero.privatePaths[0]);
				rmSync(twoToZero.publicPath);
			} },
		}));

		for (const replacementKind of ["symlink", "directory"]) {
			const path = nextPath(`final-${replacementKind}`);
			const moved = nextPath(`final-${replacementKind}-moved`);
			writeFileSync(path, exactBytes);
			await rejectGenericAsync(() => readBoundedRegularFile(path, {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				hooks: { afterFinalStat: () => {
					renameSync(path, moved);
					if (replacementKind === "symlink") symlinkSync(moved, path);
					else mkdirSync(path);
				} },
			}));
		}

		const asyncIoCases = [
			["initial-lstat", (_path, failure) => ({ lstatEntry: async () => { throw failure; } })],
			["open", (_path, failure) => ({ openFile: async () => { throw failure; } })],
			["initial-fstat", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					return { close: handle.close.bind(handle), read: handle.read.bind(handle), stat: async () => { throw failure; } };
				},
			})],
			["read", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					return { close: handle.close.bind(handle), read: async () => { throw failure; }, stat: handle.stat.bind(handle) };
				},
			})],
			["final-fstat", (_path, failure) => ({
				openFile: async (path, flags) => {
					const handle = await openFileHandle(path, flags);
					let calls = 0;
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => { if (++calls === 2) throw failure; return handle.stat(); },
					};
				},
			})],
			["confirm-fstat", (path, failure) => ({
				openFile: async (openedPath, flags) => {
					const handle = await openFileHandle(openedPath, flags);
					let calls = 0;
					return {
						close: handle.close.bind(handle),
						read: handle.read.bind(handle),
						stat: async () => { if (++calls === 3) throw failure; return handle.stat(); },
					};
				},
				hooks: { afterFinalStat: () => rmSync(path) },
			})],
			["final-lstat", (_path, failure) => {
				let calls = 0;
				return { lstatEntry: async (path) => { if (++calls === 2) throw failure; return lstatFile(path); } };
			}],
		];
		for (const [label, options] of asyncIoCases) {
			const path = nextPath(`async-io-${label}`);
			const failure = new Error(`async ${label} identity`);
			writeFileSync(path, exactBytes);
			await assert.rejects(
				() => readBoundedRegularFile(path, { maxBytes: 1024, expectedSha256: exactDigest, ...options(path, failure) }),
				(error) => error === failure,
			);
		}

		const syncIoCases = [
			["initial-lstat", (_path, failure) => ({ lstatEntry: () => { throw failure; } })],
			["open", (_path, failure) => ({ openFile: () => { throw failure; } })],
			["initial-fstat", (_path, failure) => ({ statFile: () => { throw failure; } })],
			["read", (_path, failure) => ({ readFile: () => { throw failure; } })],
			["final-fstat", (_path, failure) => {
				let calls = 0;
				return { statFile: (descriptor) => { if (++calls === 2) throw failure; return fstatSync(descriptor); } };
			}],
			["confirm-fstat", (path, failure) => {
				let calls = 0;
				return {
					statFile: (descriptor) => { if (++calls === 3) throw failure; return fstatSync(descriptor); },
					hooks: { afterFinalStat: () => rmSync(path) },
				};
			}],
			["final-lstat", (_path, failure) => {
				let calls = 0;
				return { lstatEntry: (path) => { if (++calls === 2) throw failure; return lstatSync(path); } };
			}],
		];
		for (const [label, options] of syncIoCases) {
			const path = nextPath(`sync-io-${label}`);
			const failure = new Error(`sync ${label} identity`);
			writeFileSync(path, exactBytes);
			assert.throws(
				() => readBoundedRegularFileSync(path, { maxBytes: 1024, expectedSha256: exactDigest, ...options(path, failure) }),
				(error) => error === failure,
			);
		}

		const syncNewInode = nextPath("sync-new-inode");
		const syncNewInodeMoved = nextPath("sync-new-inode-moved");
		writeFileSync(syncNewInode, exactBytes);
		rejectGenericSync(() => readBoundedRegularFileSync(syncNewInode, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => {
				renameSync(syncNewInode, syncNewInodeMoved);
				writeFileSync(syncNewInode, exactBytes);
			} },
		}));

		const syncTwoToZero = linkedPaths("sync-two-to-zero");
		rejectGenericSync(() => readBoundedRegularFileSync(syncTwoToZero.publicPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => {
				rmSync(syncTwoToZero.privatePaths[0]);
				rmSync(syncTwoToZero.publicPath);
			} },
		}));

		for (const replacementKind of ["symlink", "directory"]) {
			const path = nextPath(`sync-final-${replacementKind}`);
			const moved = nextPath(`sync-final-${replacementKind}-moved`);
			writeFileSync(path, exactBytes);
			rejectGenericSync(() => readBoundedRegularFileSync(path, {
				maxBytes: 1024,
				expectedSha256: exactDigest,
				hooks: { afterFinalStat: () => {
					renameSync(path, moved);
					if (replacementKind === "symlink") symlinkSync(moved, path);
					else mkdirSync(path);
				} },
			}));
		}

		for (const expectedSha256 of [null, "f".repeat(64)]) {
			const syncDigest = linkedPaths(`sync-digest-${expectedSha256 ?? "absent"}`);
			rejectGenericSync(() => readBoundedRegularFileSync(syncDigest.publicPath, {
				maxBytes: 1024,
				expectedSha256,
				hooks: { afterInitialStat: () => rmSync(syncDigest.privatePaths[0]) },
			}));
		}

		const syncExtraPath = nextPath("sync-extra-eligible-link");
		writeFileSync(syncExtraPath, exactBytes);
		const syncExtraBase = lstatSync(syncExtraPath);
		const syncExtraPathEntry = preciseStat(syncExtraBase, 2, 10);
		const syncExtraBefore = preciseStat(syncExtraBase, 2, 10);
		const syncExtraAfter = preciseStat(syncExtraBase, 1, 20);
		const syncExtraFinal = preciseStat(syncExtraBase, 1, 20);
		let syncExtraLstats = 0;
		let syncExtraStats = 0;
		rejectGenericSync(() => readBoundedRegularFileSync(syncExtraPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			lstatEntry: () => (++syncExtraLstats === 1 ? syncExtraPathEntry : syncExtraFinal),
			statFile: () => (++syncExtraStats === 1 ? syncExtraBefore : syncExtraAfter),
			readFile: (descriptor, buffer, offset, length, position) => {
				if (position === exactBytes.length) return 1;
				return readSync(descriptor, buffer, offset, length, position);
			},
		}));
		assert.equal(syncExtraLstats, 2);
		assert.equal(syncExtraStats, 2);

		const syncGeneric = linkedPaths("sync-nonunit", 3);
		rejectGenericSync(() => readBoundedRegularFileSync(syncGeneric.publicPath, {
			maxBytes: 1024,
			expectedSha256: exactDigest,
			hooks: { afterInitialStat: () => rmSync(syncGeneric.privatePaths[0]) },
		}));
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("checkpoint readers converge across exact publication-link and retained-link retirement", async () => {
	const deferred = () => {
		let resolvePromise;
		const promise = new Promise((resolvePromiseValue) => { resolvePromise = resolvePromiseValue; });
		return { promise, resolve: resolvePromise };
	};
	const stateBytes = (value) => Buffer.from(`${JSON.stringify({ value })}\n`);
	const runtime = (hooks = {}, operations = {}) => ({
		stale: 20,
		update: 10,
		stateMaxBytes: 1024,
		now: () => 2,
		hooks,
		processKill: process.kill.bind(process),
		startHeartbeat: () => async () => {},
		...operations,
	});
	const assertFinalEpoch = (statePath, expectedTip) => {
		const journalDirectory = `${statePath}.journal`;
		const names = readdirSync(journalDirectory).sort();
		const checkpointName = names.find((name) => name.startsWith("checkpoint-"));
		assert.ok(checkpointName);
		const checkpointBytes = readFileSync(join(journalDirectory, checkpointName));
		const checkpoint = JSON.parse(checkpointBytes);
		const epochName = `epoch-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}`;
		assert.deepEqual(names, [".owned-temporaries-v2", checkpointName, epochName].sort());
		assert.equal(checkpointBytes.equals(Buffer.from(`${JSON.stringify(checkpoint)}\n`)), true);
		assert.equal(checkpoint.epoch, 2);
		assert.equal(checkpoint.anchorDigest, expectedTip);
		assert.deepEqual(readdirSync(join(journalDirectory, ".owned-temporaries-v2")), []);
	};
	const directoryBytes = (directory) => new Map(
		readdirSync(directory).sort().map((name) => [name, readFileSync(join(directory, name))]),
	);
	const assertDirectoryBytes = (directory, expected) => {
		const actual = directoryBytes(directory);
		assert.deepEqual([...actual.keys()], [...expected.keys()]);
		for (const [name, value] of expected) assert.equal(actual.get(name).equals(value), true);
	};
	const linkCheckpointTemporary = (journal, checkpointPath, attempt) => {
		const checkpoint = JSON.parse(readFileSync(checkpointPath));
		const path = join(
			journal,
			".owned-temporaries-v2",
			`.pylon-consumer-tmp-v1-p${process.pid}-e${checkpoint.epochId}-g0000000000000000` +
				`-w${checkpoint.epochId}-n${attempt}-kcheckpoint-t${"a".repeat(64)}.tmp`,
		);
		linkSync(checkpointPath, path);
		assert.equal(lstatSync(checkpointPath).nlink, 2);
		return path;
	};
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-checkpoint-link-retirement-")));
	try {
		const linkedStatePath = join(fixture, "linked-current.json");
		const linkedAnchor = stateBytes("linked-current-anchor");
		await withConsumerStateLock(linkedStatePath, async (_path, transaction) => {
			await transaction.commitState(linkedAnchor);
		}, runtime());
		const linkedJournalDirectory = `${linkedStatePath}.journal`;
		const publicLinkSynced = deferred();
		const releasePublisher = deferred();
		const privateLinkRemoved = deferred();
		let publishedCheckpointPath;
		let publicPathStat;
		const publisher = rotateConsumerStateJournal(linkedStatePath, runtime({
			afterMetadataDirectorySync: async ({ kind, path, linked }) => {
				if (kind !== "checkpoint" || !linked) return;
				publishedCheckpointPath = path;
				publicPathStat = lstatSync(path);
				assert.equal(publicPathStat.nlink, 2);
				publicLinkSynced.resolve();
				await releasePublisher.promise;
			},
		}, {
			removeFile: async (path, options) => {
				rmSync(path, options);
				if (
					publishedCheckpointPath && path !== publishedCheckpointPath &&
					basename(path).includes("-kcheckpoint-")
				) privateLinkRemoved.resolve();
			},
		}));
		await publicLinkSynced.promise;

		const readerBeforeOpen = deferred();
		const releaseReaderOpen = deferred();
		let checkpointOpens = 0;
		let readerPathStat;
		let latestPathStat;
		const linkedReader = rotateConsumerStateJournal(linkedStatePath, runtime({}, {
			lstatEntry: async (path) => {
				const entry = await lstatFile(path);
				if (path === publishedCheckpointPath) latestPathStat = entry;
				return entry;
			},
			openFile: async (path, flags, mode) => {
				if (path === publishedCheckpointPath) {
					checkpointOpens += 1;
					if (checkpointOpens === 2) {
						readerPathStat = latestPathStat;
						readerBeforeOpen.resolve();
						await releaseReaderOpen.promise;
					}
				}
				return openFileHandle(path, flags, mode);
			},
		}));
		await readerBeforeOpen.promise;
		assert.equal(readerPathStat.nlink, 2);
		releasePublisher.resolve();
		await privateLinkRemoved.promise;
		const retiredLinkStat = lstatSync(publishedCheckpointPath);
		assert.deepEqual(
			[retiredLinkStat.dev, retiredLinkStat.ino, retiredLinkStat.size, retiredLinkStat.mtimeMs],
			[publicPathStat.dev, publicPathStat.ino, publicPathStat.size, publicPathStat.mtimeMs],
		);
		assert.deepEqual([publicPathStat.nlink, retiredLinkStat.nlink], [2, 1]);
		assert.equal(publicPathStat.ctimeMs === retiredLinkStat.ctimeMs, false);
		releaseReaderOpen.resolve();
		const [linkedReaderReceipt, publisherReceipt] = await Promise.all([linkedReader, publisher]);
		assert.equal(
			Buffer.from(JSON.stringify(linkedReaderReceipt)).equals(Buffer.from(JSON.stringify(publisherReceipt))),
			true,
		);
		assert.deepEqual(publisherReceipt, { epoch: 2, tipSha256: sha256Bytes(linkedAnchor) });
		assertFinalEpoch(linkedStatePath, sha256Bytes(linkedAnchor));
		assert.equal(dirname(publishedCheckpointPath), linkedJournalDirectory);

		const unlinkedStatePath = join(fixture, "unlinked-retained.json");
		const unlinkedAnchor = stateBytes("unlinked-retained-anchor");
		await withConsumerStateLock(unlinkedStatePath, async (_path, transaction) => {
			await transaction.commitState(unlinkedAnchor);
		}, runtime());
		const retainedJournal = consumerJournal(unlinkedStatePath);
		const checkpointPublished = deferred();
		const releaseCheckpointPublisher = deferred();
		const checkpointPublisher = rotateConsumerStateJournal(unlinkedStatePath, runtime({
			afterRotationCheckpoint: async () => {
				checkpointPublished.resolve();
				await releaseCheckpointPublisher.promise;
			},
		}));
		await checkpointPublished.promise;
		assert.equal(lstatSync(retainedJournal.checkpoint).nlink, 1);

		const readerBeforeInitialFstat = deferred();
		const releaseInitialFstat = deferred();
		let retainedCheckpointOpens = 0;
		let pinnedHandle;
		const retainedReader = rotateConsumerStateJournal(unlinkedStatePath, runtime({}, {
			openFile: async (path, flags, mode) => {
				const handle = await openFileHandle(path, flags, mode);
				if (path !== retainedJournal.checkpoint) return handle;
				retainedCheckpointOpens += 1;
				if (retainedCheckpointOpens !== 2) return handle;
				pinnedHandle = handle;
				let statCalls = 0;
				return {
					close: handle.close.bind(handle),
					read: handle.read.bind(handle),
					stat: async (...arguments_) => {
						statCalls += 1;
						if (statCalls === 1) {
							readerBeforeInitialFstat.resolve();
							await releaseInitialFstat.promise;
						}
						return handle.stat(...arguments_);
					},
				};
			},
		}));
		await readerBeforeInitialFstat.promise;
		assert.equal((await pinnedHandle.stat()).nlink, 1);
		releaseCheckpointPublisher.resolve();
		const checkpointPublisherReceipt = await checkpointPublisher;

		const retainedPathRemoved = deferred();
		const cleanup = rotateConsumerStateJournal(unlinkedStatePath, runtime({}, {
			removeFile: async (path, options) => {
				rmSync(path, options);
				if (path === retainedJournal.checkpoint) retainedPathRemoved.resolve();
			},
		}));
		await retainedPathRemoved.promise;
		assert.equal(existsSync(retainedJournal.checkpoint), false);
		assert.equal((await pinnedHandle.stat()).nlink, 0);
		releaseInitialFstat.resolve();
		const [retainedReaderReceipt, cleanupReceipt] = await Promise.all([retainedReader, cleanup]);
		const publisherReceiptBytes = Buffer.from(JSON.stringify(checkpointPublisherReceipt));
		assert.equal(Buffer.from(JSON.stringify(retainedReaderReceipt)).equals(publisherReceiptBytes), true);
		assert.equal(Buffer.from(JSON.stringify(cleanupReceipt)).equals(publisherReceiptBytes), true);
		assert.deepEqual(checkpointPublisherReceipt, { epoch: 2, tipSha256: sha256Bytes(unlinkedAnchor) });
		assertFinalEpoch(unlinkedStatePath, sha256Bytes(unlinkedAnchor));

		let earlyExitFixture = 0;
		const mutateInProgressRoot = (mutation, statePath, checkpointPath, nextEpochPath) => {
			const journalDirectory = `${statePath}.journal`;
			if (mutation === "mutate") {
				const original = readFileSync(checkpointPath);
				writeFileSync(checkpointPath, Buffer.alloc(original.length, 0x61));
			} else if (mutation === "add") {
				writeFileSync(join(journalDirectory, "unexpected-root-entry"), "unexpected\n");
			} else if (mutation === "remove") {
				rmSync(nextEpochPath, { recursive: true });
			} else if (mutation === "replace") {
				const original = readFileSync(checkpointPath);
				renameSync(checkpointPath, join(fixture, `replaced-checkpoint-${earlyExitFixture}`));
				writeFileSync(checkpointPath, original, { mode: 0o600 });
			} else if (mutation !== null) {
				throw new Error(`unknown in-progress root mutation ${mutation}`);
			}
		};
		const runKnownInProgressProof = async (mutation) => {
			earlyExitFixture += 1;
			const label = `known-in-progress-${mutation ?? "stable"}-${earlyExitFixture}`;
			const statePath = join(fixture, `${label}.json`);
			const anchor = stateBytes(`${label}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			const journal = consumerJournal(statePath);
			const proofReached = deferred();
			const releaseProof = deferred();
			let linkedTemporary;
			let retireLink = false;
			let nextCheckpointPath;
			let nextEpochPath;
			const rotation = rotateConsumerStateJournal(statePath, runtime({
				afterRotationEpochSync: async ({ checkpoint, nextEpoch }) => {
					nextEpochPath = nextEpoch;
					nextCheckpointPath = join(
						journal.journal,
						`checkpoint-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}.json`,
					);
					linkedTemporary = linkCheckpointTemporary(
						journal.journal,
						journal.checkpoint,
						String(earlyExitFixture).padStart(12, "0"),
					);
					retireLink = true;
				},
				beforeInProgressStableRootProof: async ({ kind }) => {
					assert.equal(kind, "known");
					proofReached.resolve();
					await releaseProof.promise;
				},
			}, {
				openFile: async (path, flags, mode) => {
					if (retireLink && path === journal.checkpoint) {
						rmSync(linkedTemporary);
						retireLink = false;
						assert.equal(lstatSync(journal.checkpoint).nlink, 1);
					}
					return openFileHandle(path, flags, mode);
				},
			}));
			await proofReached.promise;
			assert.deepEqual(readdirSync(journal.journal).sort(), [
				".owned-temporaries-v2",
				basename(journal.checkpoint),
				basename(journal.epoch),
				basename(nextEpochPath),
			].sort());
			assert.deepEqual(readdirSync(nextEpochPath), []);
			assert.equal(existsSync(nextCheckpointPath), false, "the proof cannot require a later checkpoint");
			const epochBefore = directoryBytes(journal.epoch);
			mutateInProgressRoot(mutation, statePath, journal.checkpoint, nextEpochPath);
			releaseProof.resolve();
			if (mutation === null) {
				const receipt = await rotation;
				assert.deepEqual(receipt, { epoch: 2, tipSha256: sha256Bytes(anchor) });
				const cleanupReceipt = await rotateConsumerStateJournal(statePath, runtime());
				assert.deepEqual(cleanupReceipt, receipt);
				assertFinalEpoch(statePath, sha256Bytes(anchor));
			} else {
				await assert.rejects(
					rotation,
					/journal root changed without one exact current or immediate-successor authority/,
				);
				assertDirectoryBytes(journal.epoch, epochBefore);
				assert.equal(existsSync(nextCheckpointPath), false);
			}
		};
		const runDiscoveredInProgressProof = async (mutation) => {
			earlyExitFixture += 1;
			const label = `discovered-in-progress-${mutation ?? "stable"}-${earlyExitFixture}`;
			const statePath = join(fixture, `${label}.json`);
			const anchor = stateBytes(`${label}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			const journal = consumerJournal(statePath);
			const epochPublished = deferred();
			const releasePublisherEpoch = deferred();
			let nextCheckpointPath;
			let nextEpochPath;
			const publisher = rotateConsumerStateJournal(statePath, runtime({
				afterRotationEpochSync: async ({ checkpoint, nextEpoch }) => {
					nextEpochPath = nextEpoch;
					nextCheckpointPath = join(
						journal.journal,
						`checkpoint-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}.json`,
					);
					epochPublished.resolve();
					await releasePublisherEpoch.promise;
				},
			}));
			const publisherOutcome = publisher.then(
				(value) => ({ value, error: null }),
				(error) => ({ value: null, error }),
			);
			await epochPublished.promise;
			const proofReached = deferred();
			const releaseProof = deferred();
			let linkedTemporary;
			let retireLink = false;
			let claimScans = 0;
			const reader = rotateConsumerStateJournal(statePath, runtime({
				beforePathOperation: async ({ operation }) => {
					if (operation !== "scan-claims") return;
					claimScans += 1;
					if (claimScans !== 1) return;
					linkedTemporary = linkCheckpointTemporary(
						journal.journal,
						journal.checkpoint,
						String(earlyExitFixture).padStart(12, "0"),
					);
					retireLink = true;
				},
				beforeInProgressStableRootProof: async ({ kind }) => {
					assert.equal(kind, "discovered");
					proofReached.resolve();
					await releaseProof.promise;
				},
			}, {
				openFile: async (path, flags, mode) => {
					if (retireLink && path === journal.checkpoint) {
						rmSync(linkedTemporary);
						retireLink = false;
						assert.equal(lstatSync(journal.checkpoint).nlink, 1);
					}
					return openFileHandle(path, flags, mode);
				},
			}));
			await proofReached.promise;
			assert.deepEqual(readdirSync(journal.journal).sort(), [
				".owned-temporaries-v2",
				basename(journal.checkpoint),
				basename(journal.epoch),
				basename(nextEpochPath),
			].sort());
			assert.deepEqual(readdirSync(nextEpochPath), []);
			assert.equal(existsSync(nextCheckpointPath), false, "discovery authenticates before checkpoint publication");
			const epochBefore = directoryBytes(journal.epoch);
			mutateInProgressRoot(mutation, statePath, journal.checkpoint, nextEpochPath);
			releaseProof.resolve();
			if (mutation === null) {
				releasePublisherEpoch.resolve();
				const [readerReceipt, publisherResult] = await Promise.all([reader, publisherOutcome]);
				assert.equal(publisherResult.error, null);
				assert.deepEqual(readerReceipt, publisherResult.value);
				assert.deepEqual(readerReceipt, { epoch: 2, tipSha256: sha256Bytes(anchor) });
				const cleanupReceipt = await rotateConsumerStateJournal(statePath, runtime());
				assert.deepEqual(cleanupReceipt, readerReceipt);
				assertFinalEpoch(statePath, sha256Bytes(anchor));
			} else {
				await assert.rejects(
					reader,
					/journal root changed without one exact current or immediate-successor authority/,
				);
				assertDirectoryBytes(journal.epoch, epochBefore);
				assert.equal(existsSync(nextCheckpointPath), false);
				releasePublisherEpoch.resolve();
				const publisherResult = await publisherOutcome;
				if (publisherResult.error === null) {
					assert.deepEqual(publisherResult.value, { epoch: 2, tipSha256: sha256Bytes(anchor) });
				} else {
					assert.match(publisherResult.error.message, /journal root|checkpoint|epoch/);
				}
			}
		};
		for (const mutation of [null, "mutate", "add", "remove", "replace"]) {
			await runKnownInProgressProof(mutation);
			await runDiscoveredInProgressProof(mutation);
		}

		for (const intentCase of ["nonrotation", "incomplete", "wrong-intent", "competitor"]) {
			earlyExitFixture += 1;
			const statePath = join(fixture, `in-progress-intent-${intentCase}-${earlyExitFixture}.json`);
			const anchor = stateBytes(`in-progress-intent-${intentCase}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			let validClaim;
			const capturedClaim = new Error(`capture ${intentCase} rotation claim`);
			await assert.rejects(
				() => rotateConsumerStateJournal(statePath, runtime({
					beforeRotationDecision: async ({ claim }) => {
						validClaim = claim;
						throw capturedClaim;
					},
				})),
				(error) => error === capturedClaim,
			);
			const journal = consumerJournal(statePath);
			const validNextEpochPath = join(
				journal.journal,
				`epoch-${String(validClaim.intent.checkpoint.epoch).padStart(16, "0")}-${validClaim.intent.checkpoint.epochId}`,
			);
			mkdirSync(validNextEpochPath, { mode: 0o700 });
			let forgedClaim;
			if (intentCase === "nonrotation") {
				forgedClaim = {
					schemaVersion: 2,
					generation: validClaim.generation,
					token: randomUUID(),
					type: "normal",
					ownerPid: process.pid,
					createdAtMs: 2,
				};
			} else {
				forgedClaim = structuredClone(validClaim);
				if (intentCase === "incomplete") {
					delete forgedClaim.intent.checkpoint;
				} else if (intentCase === "wrong-intent") {
					forgedClaim.intent.checkpoint.historySha256 = "f".repeat(64);
				} else {
					const competitorId = randomUUID();
					forgedClaim.token = competitorId;
					forgedClaim.intent.checkpoint.epochId = competitorId;
				}
			}
			const claimBytes = Buffer.from(`${JSON.stringify(forgedClaim)}\n`);
			const claimSha256 = sha256Bytes(claimBytes);
			writeFileSync(
				join(
					journal.epoch,
					`claim-${String(forgedClaim.generation).padStart(16, "0")}-${claimSha256}.json`,
				),
				claimBytes,
				{ mode: 0o600 },
			);
			writeFileSync(
				join(journal.epoch, `claim-index-${String(forgedClaim.generation).padStart(16, "0")}.json`),
				`${JSON.stringify({ schemaVersion: 1, generation: forgedClaim.generation, claimSha256 })}\n`,
				{ mode: 0o600 },
			);
			const epochBefore = directoryBytes(journal.epoch);
			await assert.rejects(
				() => rotateConsumerStateJournal(statePath, runtime()),
				/rotation intent|in-progress next epoch|operation claim/,
			);
			assertDirectoryBytes(journal.epoch, epochBefore);
			assert.equal(
				readdirSync(journal.journal).some((name) => name.startsWith("checkpoint-0000000000000002-")),
				false,
			);
		}

		const wrongEvidenceNameStatePath = join(fixture, "wrong-link-retirement-evidence-name.json");
		const wrongEvidenceAnchor = stateBytes("wrong-link-retirement-evidence-name-anchor");
		await withConsumerStateLock(wrongEvidenceNameStatePath, async (_path, transaction) => {
			await transaction.commitState(wrongEvidenceAnchor);
		}, runtime());
		const wrongEvidenceJournal = consumerJournal(wrongEvidenceNameStatePath);
		let wrongEvidenceArmed = false;
		let wrongEvidence;
		await assert.rejects(
			() => rotateConsumerStateJournal(wrongEvidenceNameStatePath, runtime({
				afterRotationEpochSync: async () => { wrongEvidenceArmed = true; },
				metadataRead: {
					afterInitialStat: async ({ path, stat }) => {
						if (!wrongEvidenceArmed || path !== wrongEvidenceJournal.checkpoint) return;
						wrongEvidenceArmed = false;
						const checkpointBytes = readFileSync(path);
						const openedHandle = {
							dev: stat.dev,
							ino: stat.ino,
							size: stat.size,
							mtimeMs: stat.mtimeMs,
							ctimeMs: stat.ctimeMs,
							nlink: 1,
						};
						const pathEntry = { ...openedHandle, ctimeMs: openedHandle.ctimeMs - 1, nlink: 2 };
						wrongEvidence = new BoundedFileLinkRetiredBeforeReadError(
							path,
							"Consumer high-water journal checkpoint",
							checkpointBytes,
							sha256Bytes(checkpointBytes),
							pathEntry,
							openedHandle,
						);
						wrongEvidence.name = "Error";
						throw wrongEvidence;
					},
				},
			})),
			(error) => error === wrongEvidence,
		);

		const normalClaimStatePath = join(fixture, "normal-claim-has-no-in-progress-checkpoint.json");
		let normalClaimRevalidations = 0;
		await withConsumerStateLock(normalClaimStatePath, async () => {}, runtime({
			beforePathOperation: async ({ operation, inProgressCheckpoint }) => {
				if (!operation.startsWith("claim")) return;
				normalClaimRevalidations += 1;
				assert.equal(inProgressCheckpoint, null);
			},
		}));
		assert.equal(normalClaimRevalidations >= 2, true);

		const transientStatePath = join(fixture, "claim-index-in-progress-checkpoint.json");
		const transientAnchor = stateBytes("claim-index-in-progress-checkpoint-anchor");
		await withConsumerStateLock(transientStatePath, async (_path, transaction) => {
			await transaction.commitState(transientAnchor);
		}, runtime());
		const transientJournal = consumerJournal(transientStatePath);
		const claimPublicationBarriers = () => {
			const claimFileReady = deferred();
			const releaseClaimFile = deferred();
			const claimDirectoryReady = deferred();
			const releaseClaimDirectory = deferred();
			const indexFileReady = deferred();
			const releaseIndexFile = deferred();
			const indexDirectoryReady = deferred();
			const releaseIndexDirectory = deferred();
			return {
				claimFileReady,
				releaseClaimFile,
				claimDirectoryReady,
				releaseClaimDirectory,
				indexFileReady,
				releaseIndexFile,
				indexDirectoryReady,
				releaseIndexDirectory,
				hooks: {
					afterFileSync: async ({ kind }) => {
						if (kind === "claim") {
							claimFileReady.resolve();
							await releaseClaimFile.promise;
						} else if (kind === "claim-index") {
							indexFileReady.resolve();
							await releaseIndexFile.promise;
						}
					},
					afterMetadataDirectorySync: async ({ kind, path, linked }) => {
						if (kind === "claim") {
							claimDirectoryReady.resolve({ path, linked });
							await releaseClaimDirectory.promise;
						} else if (kind === "claim-index") {
							indexDirectoryReady.resolve({ path, linked });
							await releaseIndexDirectory.promise;
						}
					},
				},
			};
		};
		const firstClaimPublisher = claimPublicationBarriers();
		const secondClaimPublisher = claimPublicationBarriers();
		const firstEpochReady = deferred();
		const releaseFirstEpoch = deferred();
		const secondExistingReadReady = deferred();
		const releaseSecondExistingRead = deferred();
		let inProgressCheckpoint;
		let inProgressEpochPath;
		let inProgressCheckpointPath;
		firstClaimPublisher.hooks.afterRotationEpochSync = async ({ checkpoint, nextEpoch }) => {
			inProgressCheckpoint = checkpoint;
			inProgressEpochPath = nextEpoch;
			inProgressCheckpointPath = join(
				transientJournal.journal,
				`checkpoint-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}.json`,
			);
			firstEpochReady.resolve();
			await releaseFirstEpoch.promise;
		};
		secondClaimPublisher.hooks.beforePathOperation = async ({ operation, inProgressCheckpoint: candidate }) => {
			if (operation !== "claim-index-existing") return;
			secondExistingReadReady.resolve(candidate);
			await releaseSecondExistingRead.promise;
		};
		const firstRotation = rotateConsumerStateJournal(
			transientStatePath,
			runtime(firstClaimPublisher.hooks),
		);
		await firstClaimPublisher.claimFileReady.promise;
		const secondRotation = rotateConsumerStateJournal(
			transientStatePath,
			runtime(secondClaimPublisher.hooks),
		);
		await secondClaimPublisher.claimFileReady.promise;
		firstClaimPublisher.releaseClaimFile.resolve();
		const firstClaimLink = await firstClaimPublisher.claimDirectoryReady.promise;
		assert.equal(firstClaimLink.linked, true);
		secondClaimPublisher.releaseClaimFile.resolve();
		const secondClaimLink = await secondClaimPublisher.claimDirectoryReady.promise;
		assert.equal(secondClaimLink.linked, false);
		assert.equal(secondClaimLink.path, firstClaimLink.path);
		firstClaimPublisher.releaseClaimDirectory.resolve();
		await firstClaimPublisher.indexFileReady.promise;
		secondClaimPublisher.releaseClaimDirectory.resolve();
		await secondClaimPublisher.indexFileReady.promise;
		firstClaimPublisher.releaseIndexFile.resolve();
		const firstIndexLink = await firstClaimPublisher.indexDirectoryReady.promise;
		assert.equal(firstIndexLink.linked, true);
		secondClaimPublisher.releaseIndexFile.resolve();
		const secondIndexLink = await secondClaimPublisher.indexDirectoryReady.promise;
		assert.equal(secondIndexLink.linked, false);
		assert.equal(secondIndexLink.path, firstIndexLink.path);
		firstClaimPublisher.releaseIndexDirectory.resolve();
		await firstEpochReady.promise;
		assert.deepEqual(readdirSync(transientJournal.journal).sort(), [
			".owned-temporaries-v2",
			basename(transientJournal.checkpoint),
			basename(transientJournal.epoch),
			basename(inProgressEpochPath),
		].sort());
		assert.deepEqual(readdirSync(inProgressEpochPath), []);
		assert.equal(existsSync(inProgressCheckpointPath), false);
		secondClaimPublisher.releaseIndexDirectory.resolve();
		const secondAuthenticatedCheckpoint = await secondExistingReadReady.promise;
		assert.deepEqual(secondAuthenticatedCheckpoint, inProgressCheckpoint);
		assert.deepEqual(readdirSync(transientJournal.journal).sort(), [
			".owned-temporaries-v2",
			basename(transientJournal.checkpoint),
			basename(transientJournal.epoch),
			basename(inProgressEpochPath),
		].sort());
		assert.deepEqual(readdirSync(inProgressEpochPath), []);
		assert.equal(existsSync(inProgressCheckpointPath), false);
		releaseSecondExistingRead.resolve();
		releaseFirstEpoch.resolve();
		const [firstTransientReceipt, secondTransientReceipt] = await Promise.all([
			firstRotation,
			secondRotation,
		]);
		assert.equal(
			Buffer.from(JSON.stringify(firstTransientReceipt)).equals(
				Buffer.from(JSON.stringify(secondTransientReceipt)),
			),
			true,
		);
		assert.deepEqual(firstTransientReceipt, { epoch: 2, tipSha256: sha256Bytes(transientAnchor) });
		const transientCleanupReceipt = await rotateConsumerStateJournal(transientStatePath, runtime());
		assert.deepEqual(transientCleanupReceipt, firstTransientReceipt);
		assertFinalEpoch(transientStatePath, sha256Bytes(transientAnchor));

		const runDiscoveredSuccessor = async (mutation) => {
			earlyExitFixture += 1;
			const label = `discovered-successor-${mutation ?? "exact"}-${earlyExitFixture}`;
			const statePath = join(fixture, `${label}.json`);
			const anchor = stateBytes(`${label}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			const journal = consumerJournal(statePath);
			const epochReady = deferred();
			const releasePublisher = deferred();
			let checkpoint;
			let checkpointPath;
			const publisher = rotateConsumerStateJournal(statePath, runtime({
				afterRotationEpochSync: async ({ checkpoint: candidate }) => {
					checkpoint = candidate;
					checkpointPath = join(
						journal.journal,
						`checkpoint-${String(candidate.epoch).padStart(16, "0")}-${candidate.epochId}.json`,
					);
					epochReady.resolve();
					await releasePublisher.promise;
				},
			}));
			const publisherOutcome = publisher.then(
				(value) => ({ value, error: null }),
				(error) => ({ value: null, error }),
			);
			await epochReady.promise;
			const discoveryReady = deferred();
			const releaseDiscovery = deferred();
			let scanClaims = 0;
			const reader = rotateConsumerStateJournal(statePath, runtime({
				beforePathOperation: async ({ operation }) => {
					if (operation !== "scan-claims") return;
					scanClaims += 1;
					if (scanClaims !== 1) return;
					discoveryReady.resolve();
					await releaseDiscovery.promise;
				},
			}));
			await discoveryReady.promise;
			const epochBefore = directoryBytes(journal.epoch);
			let publishedCheckpoint = checkpoint;
			if (mutation === "checkpoint") {
				publishedCheckpoint = { ...checkpoint, epochId: randomUUID() };
			} else if (["historySha256", "sourceAuthoritySha256", "anchorDigest"].includes(mutation)) {
				publishedCheckpoint = { ...checkpoint, [mutation]: "f".repeat(64) };
			} else if (mutation !== null) {
				throw new Error(`unknown successor mutation ${mutation}`);
			}
			writeFileSync(checkpointPath, `${JSON.stringify(publishedCheckpoint)}\n`, { mode: 0o600 });
			releaseDiscovery.resolve();
			if (mutation === null) {
				const readerReceipt = await reader;
				releasePublisher.resolve();
				const publisherResult = await publisherOutcome;
				assert.equal(publisherResult.error, null);
				assert.deepEqual(publisherResult.value, readerReceipt);
				assert.deepEqual(readerReceipt, { epoch: 2, tipSha256: sha256Bytes(anchor) });
				const cleanupReceipt = await rotateConsumerStateJournal(statePath, runtime());
				assert.deepEqual(cleanupReceipt, readerReceipt);
				assertFinalEpoch(statePath, sha256Bytes(anchor));
			} else {
				await assert.rejects(reader, /checkpoint|journal root|immediate successor/);
				assertDirectoryBytes(journal.epoch, epochBefore);
				releasePublisher.resolve();
				const publisherResult = await publisherOutcome;
				assert.equal(publisherResult.value, null);
				assert.match(publisherResult.error.message, /checkpoint|journal root|competing/);
			}
		};
		for (const mutation of [null, "checkpoint", "historySha256", "sourceAuthoritySha256", "anchorDigest"]) {
			await runDiscoveredSuccessor(mutation);
		}
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("stable checkpoint proof consumes exact retirements and fences namespace changes", async () => {
	const deferred = () => {
		let resolvePromise;
		const promise = new Promise((resolvePromiseValue) => { resolvePromise = resolvePromiseValue; });
		return { promise, resolve: resolvePromise };
	};
	const runtime = (hooks = {}, operations = {}) => ({
		stale: 20,
		update: 10,
		stateMaxBytes: 1024,
		now: () => 2,
		hooks,
		processKill: process.kill.bind(process),
		startHeartbeat: () => async () => {},
		...operations,
	});
	const stateBytes = (value) => Buffer.from(`${JSON.stringify({ value })}\n`);
	const directoryBytes = (directory) => new Map(
		readdirSync(directory).sort().map((name) => [name, readFileSync(join(directory, name))]),
	);
	const assertDirectoryBytes = (directory, expected) => {
		const actual = directoryBytes(directory);
		assert.deepEqual([...actual.keys()], [...expected.keys()]);
		for (const [name, value] of expected) assert.equal(actual.get(name).equals(value), true);
	};
	const assertFinalEpoch = (statePath, expectedTip) => {
		const journalDirectory = `${statePath}.journal`;
		const names = readdirSync(journalDirectory).sort();
		const checkpointName = names.find((name) => name.startsWith("checkpoint-"));
		assert.ok(checkpointName);
		const checkpoint = JSON.parse(readFileSync(join(journalDirectory, checkpointName)));
		const epochName = `epoch-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}`;
		assert.deepEqual(names, [".owned-temporaries-v2", checkpointName, epochName].sort());
		assert.equal(checkpoint.epoch, 2);
		assert.equal(checkpoint.anchorDigest, expectedTip);
		assert.deepEqual(readdirSync(join(journalDirectory, ".owned-temporaries-v2")), []);
	};
	const outcome = (promise) => promise.then(
		(value) => ({ value, error: null }),
		(error) => ({ value: null, error }),
	);
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-checkpoint-proof-")));
	let scenarioNumber = 0;
	try {
		const runSuccessorProof = async ({ label, proofEntry, phase = null, scanRemoval = false, mutate, succeeds }) => {
			scenarioNumber += 1;
			const statePath = join(fixture, `${String(scenarioNumber).padStart(2, "0")}-${label}.json`);
			const anchor = stateBytes(`${label}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			const original = consumerJournal(statePath);
			const originalCheckpointBytes = readFileSync(original.checkpoint);
			const readerPrepared = deferred();
			const releaseReader = deferred();
			const proofReady = deferred();
			const releaseProof = deferred();
			let walkTransactions = 0;
			let phaseArmed = false;
			let removeBeforeScanOpen = false;
			let scanRootCaptured = false;
			let newCheckpointPath;
			let proofPath;
			let retirementTemporary = null;
			const proofHooks = {
				beforePathOperation: async ({ operation }) => {
					if (operation !== "walk-transactions") return;
					walkTransactions += 1;
					if (walkTransactions !== 1) return;
					readerPrepared.resolve();
					await releaseReader.promise;
				},
				beforeStableCheckpointProofRead: async ({ path, target }) => {
					const wanted = proofEntry === "target" ? target : path === original.checkpoint;
					if (!wanted || (proofEntry === "target" && path !== newCheckpointPath)) return;
					proofPath = path;
					proofReady.resolve();
					await releaseProof.promise;
				},
				metadataRead: {
					afterInitialStat: async ({ path }) => {
						if (!phaseArmed || phase !== "afterInitialStat" || path !== proofPath) return;
						phaseArmed = false;
						mutate({ path, retirementTemporary, original, originalCheckpointBytes, newCheckpointPath });
					},
					afterFinalStat: async ({ path }) => {
						if (!phaseArmed || phase !== "afterFinalStat" || path !== proofPath) return;
						phaseArmed = false;
						mutate({ path, retirementTemporary, original, originalCheckpointBytes, newCheckpointPath });
					},
				},
			};
			const reader = rotateConsumerStateJournal(statePath, runtime(proofHooks, {
				readDirectory: async (path) => {
					const names = await readDirectoryEntries(path);
					if (removeBeforeScanOpen && path === original.journal) {
						assert.equal(names.includes(basename(original.checkpoint)), true);
						assert.equal(names.includes(basename(newCheckpointPath)), true);
						scanRootCaptured = true;
					}
					return names;
				},
				openFile: async (path, flags, mode) => {
					if (removeBeforeScanOpen && path === original.checkpoint) {
						assert.equal(scanRootCaptured, true);
						removeBeforeScanOpen = false;
						rmSync(path);
					}
					return openFileHandle(path, flags, mode);
				},
			}));
			const readerOutcome = outcome(reader);
			await readerPrepared.promise;
			const checkpointPublished = deferred();
			const releasePublisher = deferred();
			const publisher = rotateConsumerStateJournal(statePath, runtime({
				afterRotationCheckpoint: async ({ nextPath }) => {
					newCheckpointPath = nextPath;
					checkpointPublished.resolve();
					await releasePublisher.promise;
				},
			}));
			const publisherOutcome = outcome(publisher);
			await checkpointPublished.promise;
			const newCheckpoint = JSON.parse(readFileSync(newCheckpointPath));
			const newEpochPath = join(
				original.journal,
				`epoch-${String(newCheckpoint.epoch).padStart(16, "0")}-${newCheckpoint.epochId}`,
			);
			if (label.startsWith("link-")) {
				retirementTemporary = join(
					original.journal,
					".owned-temporaries-v2",
					`.pylon-consumer-tmp-v1-p${process.pid}-e${newCheckpoint.epochId}-g0000000000000000` +
						`-w${newCheckpoint.epochId}-n${String(scenarioNumber).padStart(12, "0")}-kcheckpoint-t${"a".repeat(64)}.tmp`,
				);
				linkSync(newCheckpointPath, retirementTemporary);
				assert.equal(lstatSync(newCheckpointPath).nlink, 2);
			}
			const oldEpochBefore = directoryBytes(original.epoch);
			const newEpochBefore = directoryBytes(newEpochPath);
			removeBeforeScanOpen = scanRemoval;
			releaseReader.resolve();
			await proofReady.promise;
			if (phase === null) {
				mutate({ path: proofPath, retirementTemporary, original, originalCheckpointBytes, newCheckpointPath });
			}
			else phaseArmed = true;
			releaseProof.resolve();
			const readerResult = await readerOutcome;
			if (succeeds) {
				assert.equal(readerResult.error, null);
				releasePublisher.resolve();
				const publisherResult = await publisherOutcome;
				assert.equal(publisherResult.error, null);
				assert.deepEqual(readerResult.value, publisherResult.value);
				assert.deepEqual(readerResult.value, { epoch: 2, tipSha256: sha256Bytes(anchor) });
				const cleanupReceipt = await rotateConsumerStateJournal(statePath, runtime());
				assert.deepEqual(cleanupReceipt, readerResult.value);
				assertFinalEpoch(statePath, sha256Bytes(anchor));
			} else {
				assert.notEqual(readerResult.error, null);
				assertDirectoryBytes(original.epoch, oldEpochBefore);
				assertDirectoryBytes(newEpochPath, newEpochBefore);
				releasePublisher.resolve();
				await publisherOutcome;
			}
			return { readerResult, original, newCheckpointPath, newEpochPath };
		};

		for (const phase of ["afterInitialStat", "afterFinalStat"]) {
			await runSuccessorProof({
				label: `link-${phase}`,
				proofEntry: "target",
				phase,
				mutate: ({ retirementTemporary }) => rmSync(retirementTemporary),
				succeeds: true,
			});
		}
		await runSuccessorProof({
			label: "optional-unlink-after-final",
			proofEntry: "optional",
			phase: "afterFinalStat",
			mutate: ({ path }) => rmSync(path),
			succeeds: true,
		});
		await runSuccessorProof({
			label: "optional-disappears-before-open",
			proofEntry: "optional",
			mutate: ({ path }) => rmSync(path),
			succeeds: true,
		});

		await runSuccessorProof({
			label: "current-disappears-before-scan-open",
			proofEntry: "target",
			scanRemoval: true,
			mutate: () => {},
			succeeds: true,
		});

		const reappearedCurrent = await runSuccessorProof({
			label: "removed-current-reappears-before-final",
			proofEntry: "target",
			scanRemoval: true,
			mutate: ({ original, originalCheckpointBytes }) => {
				writeFileSync(original.checkpoint, originalCheckpointBytes, { mode: 0o600 });
			},
			succeeds: false,
		});
		assert.match(reappearedCurrent.readerResult.error.message, /journal root changed/);

		const identityStatePath = join(fixture, "post-read-identity-removal.json");
		const identityAnchor = stateBytes("post-read-identity-removal-anchor");
		await withConsumerStateLock(identityStatePath, async (_path, transaction) => {
			await transaction.commitState(identityAnchor);
		}, runtime());
		const identityJournal = consumerJournal(identityStatePath);
		const identityReaderPrepared = deferred();
		const releaseIdentityReader = deferred();
		const identityStatReady = deferred();
		const releaseIdentityStat = deferred();
		let identityWalks = 0;
		let identityStatArmed = false;
		const identityReader = rotateConsumerStateJournal(identityStatePath, runtime({
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				identityWalks += 1;
				if (identityWalks !== 1) return;
				identityReaderPrepared.resolve();
				await releaseIdentityReader.promise;
			},
			beforeCheckpointIdentityStat: async ({ path }) => {
				if (!identityStatArmed || path !== identityJournal.checkpoint) return;
				identityStatArmed = false;
				identityStatReady.resolve();
				await releaseIdentityStat.promise;
			},
		}));
		const identityReaderOutcome = outcome(identityReader);
		await identityReaderPrepared.promise;
		const identityCheckpointReady = deferred();
		const releaseIdentityPublisher = deferred();
		const identityPublisher = rotateConsumerStateJournal(identityStatePath, runtime({
			afterRotationCheckpoint: async () => {
				identityCheckpointReady.resolve();
				await releaseIdentityPublisher.promise;
			},
		}));
		await identityCheckpointReady.promise;
		identityStatArmed = true;
		releaseIdentityReader.resolve();
		await identityStatReady.promise;
		releaseIdentityPublisher.resolve();
		const identityPublisherReceipt = await identityPublisher;
		const identityCurrentRemoved = deferred();
		const identityCleaner = rotateConsumerStateJournal(identityStatePath, runtime({}, {
			removeFile: async (path, options) => {
				rmSync(path, options);
				if (path === identityJournal.checkpoint) identityCurrentRemoved.resolve();
			},
		}));
		await identityCurrentRemoved.promise;
		const identityCleanerReceipt = await identityCleaner;
		assert.equal(existsSync(identityJournal.checkpoint), false);
		releaseIdentityStat.resolve();
		const identityReaderResult = await identityReaderOutcome;
		assert.equal(identityReaderResult.error, null);
		assert.deepEqual(identityReaderResult.value, identityPublisherReceipt);
		assert.deepEqual(identityCleanerReceipt, identityPublisherReceipt);
		assert.deepEqual(identityReaderResult.value, { epoch: 2, tipSha256: sha256Bytes(identityAnchor) });
		assertFinalEpoch(identityStatePath, sha256Bytes(identityAnchor));

		const recreated = await runSuccessorProof({
			label: "optional-remove-recreate",
			proofEntry: "optional",
			mutate: ({ path }) => {
				const bytes = readFileSync(path);
				renameSync(path, join(fixture, `removed-${scenarioNumber}`));
				writeFileSync(path, bytes, { mode: 0o600 });
			},
			succeeds: false,
		});
		assert.match(recreated.readerResult.error.message, /journal root changed/);

		const requiredUnlink = await runSuccessorProof({
			label: "required-target-unlink",
			proofEntry: "target",
			phase: "afterFinalStat",
			mutate: ({ path }) => rmSync(path),
			succeeds: false,
		});
		assert.equal(requiredUnlink.readerResult.error.constructor, BoundedFileUnlinkedDuringReadError);

		for (const phase of ["afterInitialStat", "afterFinalStat"]) {
			const replaced = await runSuccessorProof({
				label: `target-new-inode-${phase}`,
				proofEntry: "target",
				phase,
				mutate: ({ path }) => {
					const bytes = readFileSync(path);
					renameSync(path, join(fixture, `same-bytes-old-${scenarioNumber}`));
					writeFileSync(path, bytes, { mode: 0o600 });
				},
				succeeds: false,
			});
			assert.match(replaced.readerResult.error.message, /changed while it was read/);
		}

		for (const phase of ["afterInitialStat", "afterFinalStat"]) {
			const mutated = await runSuccessorProof({
				label: `target-mutated-${phase}`,
				proofEntry: "target",
				phase,
				mutate: ({ path }) => {
					const initial = statSync(path);
					const bytes = readFileSync(path);
					bytes[0] ^= 1;
					writeFileSync(path, bytes);
					utimesSync(path, initial.atime, initial.mtime);
				},
				succeeds: false,
			});
			assert.match(mutated.readerResult.error.message, /changed while it was read/);
		}

		for (const mutation of ["append", "truncate"]) {
			const changedSize = await runSuccessorProof({
				label: `target-${mutation}`,
				proofEntry: "target",
				phase: "afterInitialStat",
				mutate: ({ path }) => {
					if (mutation === "append") writeFileSync(path, "x", { flag: "a" });
					else truncateSync(path, 1);
				},
				succeeds: false,
			});
			assert.match(changedSize.readerResult.error.message, /changed while it was read/);
		}

		for (const rootMutation of ["added", "competing"]) {
			const rootChanged = await runSuccessorProof({
				label: `root-${rootMutation}`,
				proofEntry: "target",
				mutate: ({ original, newCheckpointPath }) => {
					if (rootMutation === "added") {
						writeFileSync(join(original.journal, "unexpected-proof-entry"), "unexpected\n");
					} else {
						const checkpoint = JSON.parse(readFileSync(newCheckpointPath));
						checkpoint.epochId = randomUUID();
						writeFileSync(
							join(
								original.journal,
								`checkpoint-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}.json`,
							),
							`${JSON.stringify(checkpoint)}\n`,
							{ mode: 0o600 },
						);
					}
				},
				succeeds: false,
			});
			assert.match(rootChanged.readerResult.error.message, /journal root changed/);
		}

		const runRemovedCurrentInvalid = async (label, mutateRoot, { recreateCurrent = false } = {}) => {
			scenarioNumber += 1;
			const statePath = join(fixture, `${String(scenarioNumber).padStart(2, "0")}-removed-current-${label}.json`);
			const anchor = stateBytes(`removed-current-${label}-anchor`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(anchor);
			}, runtime());
			const journal = consumerJournal(statePath);
			const readerPrepared = deferred();
			const releaseReader = deferred();
			let walks = 0;
			let removeOnOpen = false;
			const reader = rotateConsumerStateJournal(statePath, runtime({
				beforePathOperation: async ({ operation }) => {
					if (operation !== "walk-transactions") return;
					walks += 1;
					if (walks !== 1) return;
					readerPrepared.resolve();
					await releaseReader.promise;
				},
			}, {
				readDirectory: async (path) => {
					const names = await readDirectoryEntries(path);
					if (path !== journal.journal) return names;
					return names.sort((left, right) => {
						if (left === basename(journal.checkpoint)) return -1;
						if (right === basename(journal.checkpoint)) return 1;
						return left.localeCompare(right);
					});
				},
				openFile: async (path, flags, mode) => {
					if (removeOnOpen && path === journal.checkpoint) {
						removeOnOpen = false;
						const checkpointBytes = recreateCurrent ? readFileSync(path) : null;
						rmSync(path);
						if (checkpointBytes !== null) writeFileSync(path, checkpointBytes, { mode: 0o600 });
					}
					return openFileHandle(path, flags, mode);
				},
			}));
			const readerOutcome = outcome(reader);
			await readerPrepared.promise;
			const checkpointReady = deferred();
			const releasePublisher = deferred();
			let nextCheckpointPath;
			const publisher = rotateConsumerStateJournal(statePath, runtime({
				afterRotationCheckpoint: async ({ nextPath }) => {
					nextCheckpointPath = nextPath;
					checkpointReady.resolve();
					await releasePublisher.promise;
				},
			}));
			const publisherOutcome = outcome(publisher);
			await checkpointReady.promise;
			const checkpoint = JSON.parse(readFileSync(nextCheckpointPath));
			const nextEpochPath = join(
				journal.journal,
				`epoch-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}`,
			);
			mutateRoot({ checkpoint, journal, nextCheckpointPath });
			const oldEpochBefore = directoryBytes(journal.epoch);
			const nextEpochBefore = directoryBytes(nextEpochPath);
			removeOnOpen = true;
			releaseReader.resolve();
			const readerResult = await readerOutcome;
			assert.equal(readerResult.value, null);
			assert.notEqual(readerResult.error, null);
			assert.equal(existsSync(journal.checkpoint), recreateCurrent);
			assertDirectoryBytes(journal.epoch, oldEpochBefore);
			assertDirectoryBytes(nextEpochPath, nextEpochBefore);
			releasePublisher.resolve();
			await publisherOutcome;
			return readerResult.error;
		};
		for (const invalidSuccessor of [
			"fake",
			"malformed",
			"unanchored",
			"wrong-digest",
			"historical",
			"source",
			"competing",
		]) {
			const error = await runRemovedCurrentInvalid(invalidSuccessor, ({ checkpoint, journal, nextCheckpointPath }) => {
				if (invalidSuccessor === "fake") {
					writeFileSync(nextCheckpointPath, readFileSync(journal.checkpoint));
					return;
				}
				if (invalidSuccessor === "malformed") {
					writeFileSync(nextCheckpointPath, "{}\n");
					return;
				}
				if (invalidSuccessor === "competing") {
					const competitor = { ...checkpoint, epochId: randomUUID() };
					writeFileSync(
						join(
							journal.journal,
							`checkpoint-${String(competitor.epoch).padStart(16, "0")}-${competitor.epochId}.json`,
						),
						`${JSON.stringify(competitor)}\n`,
						{ mode: 0o600 },
					);
					return;
				}
				const field = invalidSuccessor === "unanchored"
					? "previousCheckpointSha256"
					: invalidSuccessor === "historical" ? "historySha256"
						: invalidSuccessor === "source" ? "sourceAuthoritySha256" : "anchorDigest";
				checkpoint[field] = "f".repeat(64);
				writeFileSync(nextCheckpointPath, `${JSON.stringify(checkpoint)}\n`);
			});
			assert.match(error.message, /checkpoint|journal root|unbounded/);
		}

		const recreatedCurrentError = await runRemovedCurrentInvalid(
			"recreated-current",
			() => {},
			{ recreateCurrent: true },
		);
		assert.match(recreatedCurrentError.message, /changed while it was read/);

		const noHigherPath = join(fixture, "current-null-without-higher.json");
		await withConsumerStateLock(noHigherPath, async (_path, transaction) => {
			await transaction.commitState(stateBytes("current-null-without-higher-anchor"));
		}, runtime());
		const noHigherJournal = consumerJournal(noHigherPath);
		const noHigherEpochBefore = directoryBytes(noHigherJournal.epoch);
		let removeNoHigher = true;
		await assert.rejects(
			() => rotateConsumerStateJournal(noHigherPath, runtime({}, {
				openFile: async (path, flags, mode) => {
					if (removeNoHigher && path === noHigherJournal.checkpoint) {
						removeNoHigher = false;
						rmSync(path);
					}
					return openFileHandle(path, flags, mode);
				},
			})),
			/lost its current checkpoint/,
		);
		assertDirectoryBytes(noHigherJournal.epoch, noHigherEpochBefore);

		for (const code of ["ENOENT", "EIO"]) {
			const statePath = join(fixture, `identity-stat-${code.toLowerCase()}.json`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(stateBytes(`identity-stat-${code.toLowerCase()}-anchor`));
			}, runtime());
			const journal = consumerJournal(statePath);
			const epochBefore = directoryBytes(journal.epoch);
			const identityFailure = new Error(`injected checkpoint identity ${code}`);
			identityFailure.code = code;
			let failIdentityStat = false;
			await assert.rejects(
				() => rotateConsumerStateJournal(statePath, runtime({
					beforeCheckpointIdentityStat: async ({ path }) => {
						if (path === journal.checkpoint) failIdentityStat = true;
					},
				}, {
					lstatEntry: async (path) => {
						if (failIdentityStat && path === journal.checkpoint) {
							failIdentityStat = false;
							throw identityFailure;
						}
						return lstatFile(path);
					},
				})),
				(error) => error === identityFailure,
			);
			assertDirectoryBytes(journal.epoch, epochBefore);
		}

		const currentStatePath = join(fixture, "required-current-unlink.json");
		const currentAnchor = stateBytes("required-current-unlink-anchor");
		await withConsumerStateLock(currentStatePath, async (_path, transaction) => {
			await transaction.commitState(currentAnchor);
		}, runtime());
		const currentJournal = consumerJournal(currentStatePath);
		const currentProofReady = deferred();
		const releaseCurrentProof = deferred();
		let retirementTemporary;
		let retireBeforeOpen = false;
		let unlinkAfterFinal = false;
		let nextEpochPath;
		const currentRotation = rotateConsumerStateJournal(currentStatePath, runtime({
			afterRotationEpochSync: async ({ checkpoint, nextEpoch }) => {
				nextEpochPath = nextEpoch;
				retirementTemporary = join(
					currentJournal.journal,
					".owned-temporaries-v2",
					`.pylon-consumer-tmp-v1-p${process.pid}-e${checkpoint.epochId}-g0000000000000000` +
						`-w${checkpoint.epochId}-n${"f".repeat(12)}-kcheckpoint-t${"a".repeat(64)}.tmp`,
				);
				linkSync(currentJournal.checkpoint, retirementTemporary);
				retireBeforeOpen = true;
			},
			beforeStableCheckpointProofRead: async ({ path, target }) => {
				if (nextEpochPath === undefined || !target || path !== currentJournal.checkpoint) return;
				currentProofReady.resolve();
				await releaseCurrentProof.promise;
				unlinkAfterFinal = true;
			},
			metadataRead: {
				afterFinalStat: async ({ path }) => {
					if (!unlinkAfterFinal || path !== currentJournal.checkpoint) return;
					unlinkAfterFinal = false;
					rmSync(path);
				},
			},
		}, {
			openFile: async (path, flags, mode) => {
				if (retireBeforeOpen && path === currentJournal.checkpoint) {
					retireBeforeOpen = false;
					rmSync(retirementTemporary);
				}
				return openFileHandle(path, flags, mode);
			},
		}));
		const currentOutcome = outcome(currentRotation);
		await currentProofReady.promise;
		const currentEpochBefore = directoryBytes(currentJournal.epoch);
		const nextEpochBefore = directoryBytes(nextEpochPath);
		releaseCurrentProof.resolve();
		const currentResult = await currentOutcome;
		assert.equal(currentResult.value, null);
		assert.equal(currentResult.error.constructor, BoundedFileUnlinkedDuringReadError);
		assertDirectoryBytes(currentJournal.epoch, currentEpochBefore);
		assertDirectoryBytes(nextEpochPath, nextEpochBefore);
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
		(value) => (value.build.publicationPolicyRevision = 4),
		(value) => (value.build.releaseManifest.file = "other.json"),
		(value) => (value.build.previewManifest.file = "other.json"),
		(value) => (value.build.assets[0].file = "../escape.tgz"),
		(value) => (value.build.assets[0].file = "pylon-prime-agent-ai-9.9.9.tgz"),
		(value) => (value.build.assets[0].size = 0),
		(value) => value.build.assets.push(structuredClone(value.build.assets[0])),
		(value) => value.build.assets.reverse(),
		(value) => (value.promotion.policyTree = "abc"),
		(value) => delete value.promotion.publicationPolicyRevision,
		(value) => (value.promotion.publicationPolicyRevision = 4),
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
			pullRequests: [],
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

test("merged changelog workflow accepts the actual deleted PR-head run response with an empty pull_requests array", async () => {
	const mergeSha = "c34a2dd3b393700d512b0cbb30eb0bf66bd6dcc8";
	const headSha = "f4d9ef03b529faf2e07031c8b7cd703363316ae5";
	const pullNumber = 42;
	const headRef = "feat/pylon-protected-publication";
	const checkSuiteId = 90_698_064_557;
	const checkRunId = 99_743_581_069;
	const workflowRunId = 33_472_036_328;
	const workflowId = 344_179_420;
	const listAssociated = async () => {};
	const listChecks = async () => {};
	const script = githubScriptForStep(
		".github/workflows/changelog-merged-proof.yml",
		"Prove the merged pull request head check",
	);
	for (const binding of [
		"pull.merge_commit_sha === mergeSha",
		'pull.base.ref === "pylon"',
		"pull.head.repo?.full_name !== repository",
		"check.head_sha === pull.head.sha",
		"check.app?.id === 15368",
		"run.check_suite_id === suite.id",
		'run.event === "pull_request"',
		"run.head_sha === pull.head.sha",
		"run.head_branch === pull.head.ref",
		'workflow.path === ".github/workflows/changelog-fragment.yml"',
	]) assert.ok(script.includes(binding), `merged changelog workflow lost direct binding: ${binding}`);
	assert.doesNotMatch(script, /run\.pull_requests/, "Actions run pull_requests is not stable after head deletion");

	let deletedHeadLookups = 0;
	const github = {
		paginate: async (method, parameters) => {
			if (method === listAssociated) {
				assert.deepEqual(parameters, {
					owner: "pylon-code", repo: "prime-agent", commit_sha: mergeSha, per_page: 100,
				});
				return [{ number: pullNumber }];
			}
			if (method === listChecks) {
				assert.deepEqual(parameters, {
					owner: "pylon-code", repo: "prime-agent", ref: headSha, filter: "latest", per_page: 100,
				});
				return [{
					id: checkRunId,
					name: "Check changelog fragment",
					head_sha: headSha,
					app: { id: GITHUB_ACTIONS_APP_ID },
					status: "completed",
					conclusion: "success",
					check_suite: { id: checkSuiteId },
					details_url: `https://github.com/pylon-code/prime-agent/actions/runs/${workflowRunId}/job/${checkRunId}`,
				}];
			}
			throw new Error("Unexpected pagination endpoint.");
		},
		rest: {
			repos: {
				listPullRequestsAssociatedWithCommit: listAssociated,
			},
			pulls: {
				get: async (parameters) => {
					assert.deepEqual(parameters, { owner: "pylon-code", repo: "prime-agent", pull_number: pullNumber });
					return { data: {
						number: pullNumber,
						merged_at: "2026-09-01T17:39:57Z",
						merge_commit_sha: mergeSha,
						base: { ref: "pylon", repo: { full_name: PYLON_PUBLICATION_REPOSITORY } },
						head: { ref: headRef, sha: headSha, repo: { full_name: PYLON_PUBLICATION_REPOSITORY } },
					} };
				},
			},
			checks: {
				listForRef: listChecks,
				getSuite: async (parameters) => {
					assert.deepEqual(parameters, {
						owner: "pylon-code", repo: "prime-agent", check_suite_id: checkSuiteId,
					});
					return { data: {
						id: checkSuiteId,
						app: { id: GITHUB_ACTIONS_APP_ID },
						head_sha: headSha,
						status: "completed",
						conclusion: "success",
					} };
				},
			},
			actions: {
				getWorkflowRun: async (parameters) => {
					assert.deepEqual(parameters, {
						owner: "pylon-code", repo: "prime-agent", run_id: workflowRunId,
					});
					return { data: {
						id: workflowRunId,
						workflow_id: workflowId,
						check_suite_id: checkSuiteId,
						event: "pull_request",
						status: "completed",
						conclusion: "success",
						head_sha: headSha,
						head_branch: headRef,
						head_repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
						repository: { id: 1_349_002_285, full_name: PYLON_PUBLICATION_REPOSITORY },
						pull_requests: [],
					} };
				},
				getWorkflow: async (parameters) => {
					assert.deepEqual(parameters, {
						owner: "pylon-code", repo: "prime-agent", workflow_id: workflowId,
					});
					return { data: { path: ".github/workflows/changelog-fragment.yml" } };
				},
			},
			git: {
				getRef: async () => {
					deletedHeadLookups += 1;
					throw Object.assign(new Error("Reference does not exist"), { status: 404 });
				},
			},
		},
	};
	const execute = new AsyncFunction("github", "context", "core", script);
	await execute(github, {
		repo: { owner: "pylon-code", repo: "prime-agent" },
		eventName: "push",
		ref: PYLON_PUBLICATION_REF,
		sha: mergeSha,
	}, {});
	assert.equal(deletedHeadLookups, 0, "proof must stay SHA-bound when the merged head branch no longer resolves");
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
		writeFileSync(join(fixture, PYLON_PREVIEW_MANIFEST), Buffer.alloc(PYLON_PUBLICATION_MANIFEST_MAX_BYTES + 1));
		assert.throws(() => verifyPreviewPublication(fixture), /format byte limit/);
		rmSync(join(fixture, PYLON_PREVIEW_MANIFEST));
		symlinkSync(join(fixture, PYLON_RELEASE_MANIFEST), join(fixture, PYLON_PREVIEW_MANIFEST));
		assert.throws(() => verifyPreviewPublication(fixture), /regular non-symlink file/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("recipe and publication policy registries close independent immutable identities", () => {
	const registryText = readFileSync(join(root, "scripts/pylon-prime-supported-release-recipes-v1.json"), "utf8");
	const registry = parseSupportedReleaseRecipeRegistry(registryText);
	const policies = registry.publicationPolicies;
	const historicalPolicy = policies[0];
	const previousPolicy = policies[1];
	const policy = policies.at(-1);
	assert.deepEqual(policies.map((candidate) => candidate.publicationPolicyRevision), [1, 2, 3]);
	assert.equal(historicalPolicy.previewWorkflowSha256, "e790a5da7063bd40fbd886e84945c3200291194fdbd5b002079349e45356a41d");
	assert.equal(historicalPolicy.stableWorkflowSha256, "dfcecdf6b58f143f9b7a543eadd124c190350ae29ac9eadccb907f1398b0958a");
	assert.equal(previousPolicy.previewWorkflowSha256, "9f4e3f38fb0bdb9c11662310c5369fb792765a3090e0f74b0ec0b34127b43ed8");
	assert.equal(previousPolicy.stableWorkflowSha256, "0f04d1f55f54312d933087d88de6883e8408bb0cd9f060d3b5851d710698b1af");
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
	const retiredAdministrationRead = "branchProtectionRule requiredStatusChecks";
	const legacyPreviewShape = preview.replace(
		"// Exact-SHA workflow proof is checked by the final publisher after the push checks can complete.",
		`// ${retiredAdministrationRead}`,
	);
	const legacyStableShape = stable.replace(
		'            const requestedPreview = process.env.PREVIEW_TAG;\n',
		`            // ${retiredAdministrationRead}\n            const requestedPreview = process.env.PREVIEW_TAG;\n`,
	);
	const legacyShapePolicy = {
		...historicalPolicy,
		previewWorkflowSha256: createHash("sha256").update(legacyPreviewShape).digest("hex"),
		stableWorkflowSha256: createHash("sha256").update(legacyStableShape).digest("hex"),
	};
	for (const [workflowPath, workflow, channel, environment] of [
		[legacyShapePolicy.previewWorkflowPath, legacyPreviewShape, "preview", "pylon-preview"],
		[legacyShapePolicy.stableWorkflowPath, legacyStableShape, "stable", "pylon-stable"],
	]) {
		assert.match(workflow, /branchProtectionRule requiredStatusChecks/);
		assert.deepEqual(
			validateApprovedWorkflowBytes(workflowPath, workflow, channel, 1, [legacyShapePolicy]),
			{ workflow: workflowPath, environment },
		);
		assert.throws(
			() => validateApprovedWorkflowBytes(workflowPath, workflow, channel, 1, policies),
			/bytes differ/,
			"p1 revision dispatch must remain behind the exact registered workflow hash",
		);
		const revision2ShapePolicy = { ...legacyShapePolicy, publicationPolicyRevision: 2 };
		assert.throws(
			() => validateApprovedWorkflowBytes(workflowPath, workflow, channel, 2, [revision2ShapePolicy]),
			/Administration-gated branch-protection read/,
		);
	}
	assert.throws(
		() => validateApprovedWorkflowBytes(
			legacyShapePolicy.previewWorkflowPath,
			`${legacyPreviewShape}# arbitrary p1 bytes\n`,
			"preview",
			1,
			[legacyShapePolicy],
		),
		/bytes differ/,
	);
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
	assert.throws(
		() => validateApprovedAttestationWorkflow(preview.replace(
			"// Exact-SHA workflow proof is checked by the final publisher after the push checks can complete.",
			"// branchProtectionRule requiredStatusChecks",
		), "preview"),
		/Administration-gated branch-protection read/,
	);

	assert.match(preview, /--publication-policy-revision 3/);
	assert.match(preview, /preview(?:Manifest)?\.publicationPolicyRevision !== 3/);
	assert.match(stable, /--publication-policy-revision 3/);
	assert.match(stable, /promotion\?\.publicationPolicyRevision !== 3/);
	assert.match(stable, /!\[1, 2, 3\]\.includes\(manifest\.build\.publicationPolicyRevision\)/);
	assert.throws(
		() => validateApprovedWorkflowBytes(historicalPolicy.previewWorkflowPath, preview, "preview", 1, policies),
		/bytes differ/,
	);
	assert.throws(
		() => validateApprovedWorkflowBytes(historicalPolicy.stableWorkflowPath, stable, "stable", 1, policies),
		/bytes differ/,
	);
	const promotedByR3 = firstStable();
	promotedByR3.promotion.publicationPolicyRevision = 3;
	assert.equal(validateStableManifest(promotedByR3, registry.recipes, policies), promotedByR3);
	assert.equal(promotedByR3.build.recipeRevision, 1);
	assert.equal(promotedByR3.build.publicationPolicyRevision, 1);
	assert.equal(promotedByR3.promotion.publicationPolicyRevision, 3);
	const stableVerifier = readFileSync(join(root, "scripts/verify-pylon-stable-attestation.mjs"), "utf8");
	assert.match(stableVerifier, /manifest\.promotion\.publicationPolicyRevision/);
	assert.doesNotMatch(stableVerifier, /"stable",\s*manifest\.build\.recipeRevision/);
	const previewVerifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	assert.match(previewVerifier, /verified\.previewManifest\.publicationPolicyRevision/);
	for (const mutate of [
		(value) => delete value.promotion.publicationPolicyRevision,
		(value) => (value.promotion.publicationPolicyRevision = 4),
		(value) => (value.promotion.publicationPolicyRevision = 0),
		(value) => delete value.build.publicationPolicyRevision,
	]) {
		const changed = structuredClone(promotedByR3);
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
			processKill: (pid, signal) => {
				if (pid === 999_999) throw Object.assign(new Error("dead fixture owner"), { code: "ESRCH" });
				return process.kill(pid, signal);
			},
			startHeartbeat: ({ beat }) => {
				beats.push(beat);
				return async () => {};
			},
			beats,
		};
	};
	const bytes = (value) => Buffer.from(`${JSON.stringify({ value })}\n`);
	const metadata = (value) => Buffer.from(`${JSON.stringify(value)}\n`);
	const writePrivate = (path, value) => {
		writeFileSync(path, value, { mode: 0o600 });
		chmodSync(path, 0o600);
	};
	const capturedChildren = new Set();
	const captureChild = (args, options) => {
		const child = spawn(process.execPath, args, options);
		let spawnError = null;
		child.once("error", (error) => { spawnError = error; });
		const closed = new Promise((resolveChild) => {
			child.once("close", (code, signal) => resolveChild({ code, signal, spawnError }));
		});
		const captured = { child, closed };
		capturedChildren.add(captured);
		void closed.then(() => capturedChildren.delete(captured));
		return captured;
	};
	const terminateCapturedChildren = async () => {
		const children = [...capturedChildren];
		for (const { child } of children) {
			if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
		}
		await Promise.all(children.map(({ closed }) => closed));
	};
	const closedChildError = (description, status, stderr) => {
		if (status.spawnError !== null) return new Error(`${description} failed to spawn: ${status.spawnError.message}`);
		return new Error(`${description} failed with ${status.code ?? status.signal}: ${stderr}`);
	};
	const runConsumerChild = (statePath, candidate) => {
		const source = `
			import { withConsumerStateLock } from ${JSON.stringify(pathToFileURL(resolve("scripts/lib/pylon-consumer-lock.mjs")).href)};
			const statePath = process.argv[1];
			const candidate = process.argv[2];
			try {
				await withConsumerStateLock(statePath, async (_path, transaction) => {
					await transaction.commitState(Buffer.from(JSON.stringify({ value: candidate }) + "\\n"));
				});
				process.exitCode = 0;
			} catch (error) {
				if (/actively locked/.test(error.message)) process.exitCode = 2;
				else { console.error(error.stack); process.exitCode = 1; }
			}
		`;
		const captured = captureChild(
			["--input-type=module", "--eval", source, statePath, candidate],
			{ cwd: resolve("."), stdio: ["ignore", "ignore", "pipe"] },
		);
		let stderr = "";
		captured.child.stderr.setEncoding("utf8");
		captured.child.stderr.on("data", (chunk) => { stderr += chunk; });
		return captured.closed.then((status) => {
			if ([0, 2].includes(status.code) && status.spawnError === null) return status.code;
			throw closedChildError("consumer child", status, stderr);
		});
	};
	const runRotationChild = (statePath) => {
		const source = `
			import { rotateConsumerStateJournal } from ${JSON.stringify(pathToFileURL(resolve("scripts/lib/pylon-consumer-lock.mjs")).href)};
			try {
				const result = await rotateConsumerStateJournal(process.argv[1]);
				process.stdout.write(JSON.stringify(result));
			} catch (error) {
				console.error(error.stack);
				process.exitCode = 1;
			}
		`;
		const captured = captureChild(
			["--input-type=module", "--eval", source, statePath],
			{ cwd: resolve("."), stdio: ["ignore", "pipe", "pipe"] },
		);
		let stdout = "";
		let stderr = "";
		captured.child.stdout.setEncoding("utf8");
		captured.child.stderr.setEncoding("utf8");
		captured.child.stdout.on("data", (chunk) => { stdout += chunk; });
		captured.child.stderr.on("data", (chunk) => { stderr += chunk; });
		return captured.closed.then((status) => {
			if (status.code === 0 && status.spawnError === null) return JSON.parse(stdout);
			throw closedChildError("rotation child", status, stderr);
		});
	};
	const startLegacyLockChild = async (statePath, afterReleasePath = "") => {
		const source = `
			import { open } from "node:fs/promises";
			import { createRequire } from "node:module";
			const require = createRequire(import.meta.url);
			const properLockfile = require(${JSON.stringify(nodeRequire.resolve("proper-lockfile"))});
			const release = await properLockfile.lock(process.argv[1], { realpath: false, retries: 0 });
			process.stdout.write("locked\\n");
			process.stdin.resume();
			await new Promise((resolveInput) => process.stdin.once("end", resolveInput));
			await release();
			if (process.argv[2]) {
				const handle = await open(process.argv[2], "w", 0o600);
				try {
					await handle.writeFile("closed\\n");
					await handle.sync();
				} finally {
					await handle.close();
				}
			}
		`;
		const captured = captureChild(
			["--input-type=module", "--eval", source, statePath, afterReleasePath],
			{ cwd: resolve("."), stdio: ["pipe", "pipe", "pipe"] },
		);
		const ready = deferred();
		let stdout = "";
		let stderr = "";
		captured.child.stdout.setEncoding("utf8");
		captured.child.stderr.setEncoding("utf8");
		captured.child.stdout.on("data", (chunk) => {
			stdout += chunk;
			if (stdout.includes("locked\n")) ready.resolve();
		});
		captured.child.stderr.on("data", (chunk) => { stderr += chunk; });
		await Promise.race([
			ready.promise,
			captured.closed.then((status) => { throw closedChildError("legacy lock child", status, stderr); }),
		]);
		return async () => {
			captured.child.stdin.end();
			const status = await captured.closed;
			if (status.code !== 0 || status.spawnError !== null) throw closedChildError("legacy lock child", status, stderr);
		};
	};
	const v1Fixture = (
		statePath,
		{ projection = "one", secondTransition = false, terminal = true, applied = false, ownerPid = 999_999 } = {},
	) => {
		const lockDirectory = `${statePath}.lock`;
		const transactionDirectory = `${statePath}.transactions`;
		mkdirSync(lockDirectory, { mode: 0o700 });
		mkdirSync(transactionDirectory, { mode: 0o700 });
		const token = "12345678-1234-4123-8123-123456789abc";
		const firstBytes = bytes("one");
		const secondBytes = bytes("two");
		const firstDigest = sha256Bytes(firstBytes);
		const secondDigest = sha256Bytes(secondBytes);
		const firstTransaction = {
			schemaVersion: 1,
			baseDigest: "0".repeat(64),
			candidateDigest: firstDigest,
			candidateBase64: firstBytes.toString("base64"),
		};
		const secondTransaction = {
			schemaVersion: 1,
			baseDigest: firstDigest,
			candidateDigest: secondDigest,
			candidateBase64: secondBytes.toString("base64"),
		};
		const claim = { schemaVersion: 1, generation: 1, token, ownerPid, createdAtMs: 1 };
		const heartbeat = { schemaVersion: 1, generation: 1, token, refreshedAtMs: 1 };
		const commit = { schemaVersion: 1, generation: 1, token, outcome: "commit", transactions: [firstTransaction, secondTransaction] };
		writePrivate(join(lockDirectory, "claim-0000000000000001.json"), metadata(claim));
		writePrivate(join(lockDirectory, `heartbeat-0000000000000001-${token}.json`), metadata(heartbeat));
		if (terminal) writePrivate(join(lockDirectory, `terminal-0000000000000001-${token}.json`), metadata(commit));
		if (applied) writePrivate(join(lockDirectory, `applied-0000000000000001-${token}.json`), metadata({
			schemaVersion: 1,
			generation: 1,
			token,
			terminalSha256: sha256Bytes(metadata(commit)),
		}));
		writePrivate(join(transactionDirectory, `${"0".repeat(64)}.json`), metadata(firstTransaction));
		if (secondTransition) writePrivate(join(transactionDirectory, `${firstDigest}.json`), metadata(secondTransaction));
		if (projection !== null) writePrivate(statePath, projection === "one" ? firstBytes : secondBytes);
		return { lockDirectory, transactionDirectory, firstBytes, secondBytes, firstDigest, secondDigest, token };
	};

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
		const properLockfile = nodeRequire("proper-lockfile");
		const legacyLivePath = join(fixture, "legacy-live.json");
		const legacyHelperCompletionPath = join(fixture, "legacy-helper-complete");
		const releaseLegacy = await startLegacyLockChild(legacyLivePath, legacyHelperCompletionPath);
		await assert.rejects(
			() => withConsumerStateLock(legacyLivePath, async () => {}, manualRuntime({ value: 1 })),
			/Legacy consumer lock directory exists.*confirm that no owner remains.*manually/i,
		);
		assert.equal(statSync(`${legacyLivePath}.lock`).isDirectory(), true);
		await releaseLegacy();
		assert.equal(
			readFileSync(legacyHelperCompletionPath, "utf8"),
			"closed\n",
			"captured helper completion includes its final async write and file-handle close",
		);
		rmSync(legacyHelperCompletionPath);
		await new Promise((resolveImmediate) => setImmediate(resolveImmediate));
		assert.equal(existsSync(legacyHelperCompletionPath), false, "a captured helper cannot write after completion");
		await withConsumerStateLock(legacyLivePath, async () => {}, manualRuntime({ value: 1 }));
		assert.equal(statSync(`${legacyLivePath}.lock`).isFile(), true, "the handoff guard is a permanent regular file");
		await assert.rejects(
			() => properLockfile.lock(legacyLivePath, { realpath: false, retries: 0 }),
			/lock|directory|ENOTDIR|EEXIST/i,
		);

		const legacyRacePath = join(fixture, "legacy-race.json");
		const guardReady = deferred();
		const letGuardLink = deferred();
		const currentRacer = withConsumerStateLock(legacyRacePath, async () => {}, manualRuntime({ value: 1 }, {
			afterFileSync: async ({ kind }) => {
				if (kind !== "legacy-guard") return;
				guardReady.resolve();
				await letGuardLink.promise;
			},
		}));
		await guardReady.promise;
		const releaseRaceLegacy = await startLegacyLockChild(legacyRacePath);
		letGuardLink.resolve();
		await assert.rejects(currentRacer, /Legacy consumer lock directory exists/);
		await releaseRaceLegacy();
		await withConsumerStateLock(legacyRacePath, async () => {}, manualRuntime({ value: 100 }));
		await assert.rejects(
			() => properLockfile.lock(legacyRacePath, { realpath: false, retries: 0 }),
			/lock|directory|ENOTDIR|EEXIST/i,
		);

		const ambiguousLegacyPath = join(fixture, "legacy-ambiguous.json");
		mkdirSync(`${ambiguousLegacyPath}.lock`, { mode: 0o700 });
		await assert.rejects(
			() => withConsumerStateLock(ambiguousLegacyPath, async () => {}, manualRuntime({ value: 100 })),
			/remove that directory manually/,
		);
		assert.equal(statSync(`${ambiguousLegacyPath}.lock`).isDirectory(), true, "ambiguous legacy leases are never stolen");

		const v1MigrationPath = join(fixture, "v1-migration.json");
		const migratedV1 = v1Fixture(v1MigrationPath);
		await assert.rejects(
			() => withConsumerStateLock(v1MigrationPath, async () => {}, manualRuntime({ value: 1 })),
			/explicit quiescent consumer journal migration command/,
		);
		assert.equal(existsSync(`${v1MigrationPath}.journal`), false, "v1 authority is detected before v2 is seeded");
		const migrationResult = await migrateConsumerStateJournal(v1MigrationPath, manualRuntime({ value: 2 }));
		assert.equal(migrationResult.tipSha256, migratedV1.secondDigest);
		assert.notEqual(migrationResult.sourceAuthoritySha256, "0".repeat(64));
		assert.equal(readFileSync(v1MigrationPath).equals(migratedV1.secondBytes), true, "the immutable v1 decision repairs a one-behind projection");
		assert.equal(statSync(`${v1MigrationPath}.lock`).isDirectory(), true);
		assert.equal(existsSync(join(`${v1MigrationPath}.lock`, ".pylon-consumer-v1-retired.json")), true);
		assert.equal(existsSync(`${v1MigrationPath}.lock.v1-retired`), false);
		await assert.rejects(
			() => properLockfile.lock(v1MigrationPath, { realpath: false, retries: 0, stale: 1 }),
			/lock|directory|ENOTEMPTY|EEXIST/i,
		);
		assert.equal(existsSync(join(migratedV1.transactionDirectory, `${migratedV1.firstDigest}.json`)), true);
		assert.equal(
			readdirSync(`${v1MigrationPath}.lock`).some((name) => name.startsWith("applied-")),
			true,
			"a definitively dead incomplete v1 commit is completed only behind the durable guard",
		);
		await withConsumerStateLock(v1MigrationPath, async (_path, transaction) => {
			assert.equal(transaction.readStateBytes().equals(migratedV1.secondBytes), true);
		}, manualRuntime({ value: 3 }));

		for (const [hookName, wantedKind] of [
			["afterMigrationAuthorityRead", null],
			["afterFileSync", "legacy-retirement"],
			["afterMetadataLink", "legacy-retirement"],
			["afterMetadataDirectorySync", "legacy-retirement"],
			["afterMigrationRetirementMarker", null],
			["afterMigrationGuard", null],
			["afterFileSync", "checkpoint"],
			["afterMetadataLink", "checkpoint"],
			["afterMetadataDirectorySync", "checkpoint"],
			["afterMigrationComplete", null],
		]) {
			const migrationCrashPath = join(fixture, `v1-migration-crash-${hookName}-${wantedKind ?? "migration"}.json`);
			const expected = v1Fixture(migrationCrashPath);
			let armed = true;
			await assert.rejects(
				() => migrateConsumerStateJournal(migrationCrashPath, manualRuntime({ value: 10 }, {
					[hookName]: async (event = {}) => {
						if (!armed || (wantedKind !== null && event.kind !== wantedKind)) return;
						armed = false;
						throw new Error(`simulated v1 migration crash at ${hookName}`);
					},
				})),
				/simulated v1 migration crash/,
			);
			const recovered = await migrateConsumerStateJournal(migrationCrashPath, manualRuntime({ value: 20 }));
			assert.equal(recovered.tipSha256, expected.secondDigest);
			assert.equal(readFileSync(migrationCrashPath).equals(expected.secondBytes), true);
		}

		const concurrentMigrationPath = join(fixture, "v1-concurrent-migration.json");
		const concurrentExpected = v1Fixture(concurrentMigrationPath);
		const bothRead = deferred();
		const releaseMigration = deferred();
		let migrationReaders = 0;
		const concurrentMigrationOptions = manualRuntime({ value: 1 }, {
			afterMigrationAuthorityRead: async () => {
				migrationReaders += 1;
				if (migrationReaders === 2) bothRead.resolve();
				await releaseMigration.promise;
			},
		});
		const firstMigrator = migrateConsumerStateJournal(concurrentMigrationPath, concurrentMigrationOptions);
		const secondMigrator = migrateConsumerStateJournal(concurrentMigrationPath, concurrentMigrationOptions);
		await bothRead.promise;
		releaseMigration.resolve();
		const concurrentMigrations = await Promise.all([firstMigrator, secondMigrator]);
		assert.deepEqual(concurrentMigrations.map((result) => result.tipSha256), [concurrentExpected.secondDigest, concurrentExpected.secondDigest]);

		const markerRacePath = join(fixture, "v1-marker-race.json");
		const markerRaceExpected = v1Fixture(markerRacePath);
		const bothMarkersSynced = deferred();
		const releaseMarkers = deferred();
		let markerWriters = 0;
		const markerRaceOptions = manualRuntime({ value: 1 }, {
			afterFileSync: async ({ kind }) => {
				if (kind !== "legacy-retirement") return;
				markerWriters += 1;
				if (markerWriters === 2) bothMarkersSynced.resolve();
				await releaseMarkers.promise;
			},
		});
		const markerMigrators = [
			migrateConsumerStateJournal(markerRacePath, markerRaceOptions),
			migrateConsumerStateJournal(markerRacePath, markerRaceOptions),
		];
		await bothMarkersSynced.promise;
		releaseMarkers.resolve();
		const markerRaceResults = await Promise.all(markerMigrators);
		assert.deepEqual(markerRaceResults, [markerRaceResults[0], markerRaceResults[0]]);
		assert.equal(markerRaceResults[0].tipSha256, markerRaceExpected.secondDigest);

		const priorLayoutPath = join(fixture, "v1-prior-retired-layout.json");
		const priorLayout = v1Fixture(priorLayoutPath, { secondTransition: true, applied: true });
		renameSync(priorLayout.lockDirectory, `${priorLayoutPath}.lock.v1-retired`);
		const priorLayoutResult = await migrateConsumerStateJournal(priorLayoutPath, manualRuntime({ value: 1 }));
		assert.equal(priorLayoutResult.tipSha256, priorLayout.secondDigest);
		assert.equal(statSync(`${priorLayoutPath}.lock`).isFile(), true);
		assert.equal(statSync(`${priorLayoutPath}.lock.v1-retired`).isDirectory(), true);
		assert.equal(existsSync(join(`${priorLayoutPath}.lock.v1-retired`, ".pylon-consumer-v1-retired.json")), false);

		const retiredOnlyPath = join(fixture, "v1-retired-only.json");
		mkdirSync(`${retiredOnlyPath}.lock.v1-retired`, { mode: 0o700 });
		await assert.rejects(
			() => withConsumerStateLock(retiredOnlyPath, async () => {}, manualRuntime({ value: 1 })),
			/transaction namespace is missing/,
		);
		assert.equal(existsSync(`${retiredOnlyPath}.journal`), false, "a retired-only legacy signal is detected before v2 initialization");

		const deletedTransactionsPath = join(fixture, "v1-deleted-transactions.json");
		v1Fixture(deletedTransactionsPath, { secondTransition: true, applied: true });
		await migrateConsumerStateJournal(deletedTransactionsPath, manualRuntime({ value: 1 }));
		rmSync(`${deletedTransactionsPath}.transactions`, { recursive: true });
		await assert.rejects(
			() => withConsumerStateLock(deletedTransactionsPath, async () => {}, manualRuntime({ value: 2 })),
			/transaction namespace is missing/,
		);
		await assert.rejects(
			() => rotateConsumerStateJournal(deletedTransactionsPath, manualRuntime({ value: 2 })),
			/transaction namespace is missing/,
		);

		const malformedMarkerOnlyPath = join(fixture, "v1-malformed-marker-only.json");
		mkdirSync(`${malformedMarkerOnlyPath}.lock`, { mode: 0o700 });
		writePrivate(join(`${malformedMarkerOnlyPath}.lock`, ".pylon-consumer-v1-retired.json"), "{}\n");
		await assert.rejects(
			() => withConsumerStateLock(malformedMarkerOnlyPath, async () => {}, manualRuntime({ value: 1 })),
			/retirement marker.*malformed/i,
		);
		assert.equal(existsSync(`${malformedMarkerOnlyPath}.journal`), false);

		const corruptMigrationPath = join(fixture, "v1-corrupt-migration.json");
		const corruptV1 = v1Fixture(corruptMigrationPath);
		writePrivate(join(corruptV1.transactionDirectory, "extra.json"), "{}\n");
		await assert.rejects(
			() => migrateConsumerStateJournal(corruptMigrationPath, manualRuntime({ value: 1 })),
			/malformed or extra entry/,
		);
		const activeMigrationPath = join(fixture, "v1-active-migration.json");
		const activeV1 = v1Fixture(activeMigrationPath, { terminal: false, ownerPid: process.pid });
		rmSync(join(activeV1.transactionDirectory, `${"0".repeat(64)}.json`));
		await assert.rejects(
			() => migrateConsumerStateJournal(activeMigrationPath, manualRuntime({ value: 100 })),
			/live or uncertain incomplete v1 commit owner/,
		);
		const falseAppliedPath = join(fixture, "v1-false-applied.json");
		v1Fixture(falseAppliedPath, { applied: true });
		await assert.rejects(
			() => migrateConsumerStateJournal(falseAppliedPath, manualRuntime({ value: 1 })),
			/applied marker is missing its completed transition/,
		);

		const liveCommitPath = join(fixture, "v1-live-incomplete-commit.json");
		const liveCommit = v1Fixture(liveCommitPath, { ownerPid: process.pid });
		await assert.rejects(
			() => migrateConsumerStateJournal(liveCommitPath, manualRuntime({ value: 1 }, {
				afterMigrationAuthorityRead: async ({ legacy }) => {
					const [missing] = legacy.recoveries[0].missingTransactions;
					writePrivate(join(liveCommit.transactionDirectory, `${missing.baseDigest}.json`), metadata(missing));
				},
			})),
			/live or uncertain incomplete v1 commit owner/,
		);
		assert.equal(statSync(liveCommit.lockDirectory).isDirectory(), true);
		assert.equal(existsSync(`${liveCommitPath}.lock.v1-retired`), false);
		assert.equal(existsSync(join(liveCommit.transactionDirectory, `${liveCommit.firstDigest}.json`)), true);

		for (const [label, processKill] of [
			["pid-reuse", () => {}],
			["eperm", () => { throw Object.assign(new Error("uncertain"), { code: "EPERM" }); }],
		]) {
			const uncertainPath = join(fixture, `v1-${label}.json`);
			v1Fixture(uncertainPath);
			await assert.rejects(
				() => migrateConsumerStateJournal(uncertainPath, { ...manualRuntime({ value: 1 }), processKill }),
				/live or uncertain incomplete v1 commit owner/,
			);
			assert.equal(statSync(`${uncertainPath}.lock`).isDirectory(), true);
			assert.equal(existsSync(join(`${uncertainPath}.lock`, ".pylon-consumer-v1-retired.json")), false);
			assert.equal(existsSync(`${uncertainPath}.lock.v1-retired`), false);
		}

		const completeAppliedPath = join(fixture, "v1-complete-applied.json");
		const completeApplied = v1Fixture(completeAppliedPath, {
			secondTransition: true,
			applied: true,
			ownerPid: process.pid,
		});
		const completeAppliedResult = await migrateConsumerStateJournal(completeAppliedPath, {
			...manualRuntime({ value: 1 }),
			processKill: () => { throw Object.assign(new Error("must not inspect a complete owner"), { code: "EPERM" }); },
		});
		assert.equal(completeAppliedResult.tipSha256, completeApplied.secondDigest);

		const markerReauthPath = join(fixture, "v1-marker-final-reauth.json");
		v1Fixture(markerReauthPath, { secondTransition: true, applied: true });
		let markerSourceMutated = false;
		await assert.rejects(
			() => migrateConsumerStateJournal(markerReauthPath, manualRuntime({ value: 1 }, {
				afterFileSync: async ({ kind }) => {
					if (kind !== "legacy-retirement" || markerSourceMutated) return;
					markerSourceMutated = true;
					const token = "abcdefab-cdef-4abc-8def-abcdefabcdef";
					const claim = { schemaVersion: 1, generation: 2, token, ownerPid: process.pid, createdAtMs: 2 };
					const heartbeat = { schemaVersion: 1, generation: 2, token, refreshedAtMs: 2 };
					writePrivate(join(`${markerReauthPath}.lock`, "claim-0000000000000002.json"), metadata(claim));
					writePrivate(join(`${markerReauthPath}.lock`, `heartbeat-0000000000000002-${token}.json`), metadata(heartbeat));
				},
			})),
			/authority changed before retirement marker publication/,
		);
		assert.equal(markerSourceMutated, true);
		assert.equal(existsSync(join(`${markerReauthPath}.lock`, ".pylon-consumer-v1-retired.json")), false);

		const mutableBeforeSuccessPath = join(fixture, "v1-mutable-before-success.json");
		v1Fixture(mutableBeforeSuccessPath, { secondTransition: true, applied: true });
		let mutatedBeforeSuccess = false;
		await assert.rejects(
			() => migrateConsumerStateJournal(mutableBeforeSuccessPath, manualRuntime({ value: 1 }, {
				beforeProjectionWrite: async () => {
					if (mutatedBeforeSuccess) return;
					mutatedBeforeSuccess = true;
					const token = "87654321-4321-4321-8321-cba987654321";
					const claim = { schemaVersion: 1, generation: 2, token, ownerPid: 999_998, createdAtMs: 2 };
					const heartbeat = { schemaVersion: 1, generation: 2, token, refreshedAtMs: 2 };
					const terminal = { schemaVersion: 1, generation: 2, token, outcome: "released" };
					const retired = `${mutableBeforeSuccessPath}.lock`;
					writePrivate(join(retired, "claim-0000000000000002.json"), metadata(claim));
					writePrivate(join(retired, `heartbeat-0000000000000002-${token}.json`), metadata(heartbeat));
					writePrivate(join(retired, `terminal-0000000000000002-${token}.json`), metadata(terminal));
				},
			})),
			/does not authenticate the complete prior v1 authority and tip|retirement marker conflicts/,
		);
		assert.equal(mutatedBeforeSuccess, true);

		const dualAuthorityPath = join(fixture, "v1-dual-authority.json");
		v1Fixture(dualAuthorityPath);
		await assert.rejects(
			() => migrateConsumerStateJournal(dualAuthorityPath, manualRuntime({ value: 1 }, {
				afterMigrationRetirementMarker: async () => mkdirSync(`${dualAuthorityPath}.lock.v1-retired`, { mode: 0o700 }),
			})),
			/Live and retired v1 consumer lock authority both exist/,
		);
		assert.equal(statSync(`${dualAuthorityPath}.lock`).isDirectory(), true);
		assert.equal(statSync(`${dualAuthorityPath}.lock.v1-retired`).isDirectory(), true);
		await assert.rejects(
			() => migrateConsumerStateJournal(dualAuthorityPath, manualRuntime({ value: 2 })),
			/Live and retired v1 consumer lock authority both exist/,
		);
		assert.equal(statSync(`${dualAuthorityPath}.lock.v1-retired`).isDirectory(), true);

		for (const hookName of ["afterFileSync", "afterMetadataLink", "afterMetadataDirectorySync"]) {
			const bootstrapCrashPath = join(fixture, `bootstrap-crash-${hookName}.json`);
			const reached = deferred();
			const crash = deferred();
			const interrupted = withConsumerStateLock(bootstrapCrashPath, async () => {}, manualRuntime({ value: 1 }, {
				[hookName]: async ({ kind }) => {
					if (kind !== "checkpoint") return;
					reached.resolve();
					await crash.promise;
				},
			}));
			await reached.promise;
			await withConsumerStateLock(bootstrapCrashPath, async (_path, transaction) => {
				await transaction.commitState(bytes("recovered-bootstrap"));
			}, manualRuntime({ value: 100 }));
			crash.reject(new Error(`simulated bootstrap crash at ${hookName}`));
			await assert.rejects(interrupted, /simulated bootstrap crash/);
			assert.deepEqual(JSON.parse(readFileSync(bootstrapCrashPath, "utf8")), { value: "recovered-bootstrap" });
		}

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

		const stagedPath = join(fixture, "staged-callback-exclusivity.json");
		const stagedReady = deferred();
		const returnStagedAction = deferred();
		const stagedOwner = withConsumerStateLock(stagedPath, async (_path, transaction) => {
			await transaction.commitState(bytes("staged-owner"));
			stagedReady.resolve();
			await returnStagedAction.promise;
		}, manualRuntime({ value: 1 }));
		await stagedReady.promise;
		const stagedJournal = consumerJournal(stagedPath);
		assert.equal(
			readdirSync(stagedJournal.epoch)
				.filter((name) => name.startsWith("terminal-"))
				.map((name) => JSON.parse(readFileSync(join(stagedJournal.epoch, name))))
				.some((decision) => decision.outcome === "commit"),
			false,
			"commitState stages bytes but publishes no resolvable commit while the callback is active",
		);
		await assert.rejects(
			() => withConsumerStateLock(stagedPath, async () => {}, manualRuntime({ value: 1 })),
			/actively locked/,
		);
		await assert.rejects(
			() => rotateConsumerStateJournal(stagedPath, manualRuntime({ value: 1 })),
			/actively locked/,
		);
		returnStagedAction.resolve();
		await stagedOwner;
		assert.deepEqual(JSON.parse(readFileSync(stagedPath, "utf8")), { value: "staged-owner" });
		await withConsumerStateLock(stagedPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "staged-owner" });
		}, manualRuntime({ value: 2 }));

		const stagedThrowPath = join(fixture, "staged-then-throw.json");
		await assert.rejects(
			() => withConsumerStateLock(stagedThrowPath, async (_path, transaction) => {
				await transaction.commitState(bytes("must-not-commit"));
				throw new Error("action failed after staging");
			}, manualRuntime({ value: 1 })),
			/action failed after staging/,
		);
		await withConsumerStateLock(stagedThrowPath, async (_path, transaction) => {
			assert.equal(transaction.readStateBytes(), null);
		}, manualRuntime({ value: 2 }));
		assert.equal(transitionNames(stagedThrowPath).length, 0);

		const stagedCrashPath = join(fixture, "staged-then-crashed.json");
		const crashingSource = `
			import { withConsumerStateLock } from ${JSON.stringify(pathToFileURL(resolve("scripts/lib/pylon-consumer-lock.mjs")).href)};
			const hold = setInterval(() => {}, 1000);
			await withConsumerStateLock(process.argv[1], async (_path, transaction) => {
				await transaction.commitState(Buffer.from(JSON.stringify({ value: "crashed-stage" }) + "\\n"));
				process.stdout.write("staged\\n");
				await new Promise(() => {});
			}, { stale: 20, update: 10, now: () => 1, startHeartbeat: () => async () => {} });
			clearInterval(hold);
		`;
		const crashingCapture = captureChild(
			["--input-type=module", "--eval", crashingSource, stagedCrashPath],
			{ cwd: resolve("."), stdio: ["ignore", "pipe", "pipe"] },
		);
		const crashingChild = crashingCapture.child;
		const crashingChildStaged = deferred();
		let crashingChildOutput = "";
		let crashingChildError = "";
		crashingChild.stdout.setEncoding("utf8");
		crashingChild.stderr.setEncoding("utf8");
		crashingChild.stdout.on("data", (chunk) => {
			crashingChildOutput += chunk;
			if (crashingChildOutput.includes("staged\n")) crashingChildStaged.resolve();
		});
		crashingChild.stderr.on("data", (chunk) => { crashingChildError += chunk; });
		const crashingChildClosed = crashingCapture.closed;
		await Promise.race([
			crashingChildStaged.promise,
			crashingChildClosed.then((status) => {
				throw closedChildError("staged crash child exited before staging", status, crashingChildError);
			}),
		]);
		assert.equal(crashingChild.kill("SIGKILL"), true);
		const crashed = await crashingChildClosed;
		assert.equal(crashed.signal, "SIGKILL", crashingChildError);
		await withConsumerStateLock(stagedCrashPath, async (_path, transaction) => {
			assert.equal(transaction.readStateBytes(), null);
		}, manualRuntime({ value: 100 }));
		assert.equal(transitionNames(stagedCrashPath).length, 0);

		const stagedRetirePath = join(fixture, "staged-then-retired.json");
		const stagedRetireClock = { value: 1 };
		const stagedBeforeRetire = deferred();
		const returnRetiredAction = deferred();
		const retiredAfterStage = withConsumerStateLock(stagedRetirePath, async (_path, transaction) => {
			await transaction.commitState(bytes("retired-stage"));
			stagedBeforeRetire.resolve();
			await returnRetiredAction.promise;
		}, manualRuntime(stagedRetireClock));
		await stagedBeforeRetire.promise;
		stagedRetireClock.value = 100;
		await withConsumerStateLock(stagedRetirePath, async (_path, transaction) => {
			await transaction.commitState(bytes("retirement-winner"));
		}, manualRuntime(stagedRetireClock));
		returnRetiredAction.resolve();
		await assert.rejects(retiredAfterStage, /lost ownership|retired/);
		assert.deepEqual(JSON.parse(readFileSync(stagedRetirePath, "utf8")), { value: "retirement-winner" });
		assert.equal(transitionNames(stagedRetirePath).length, 1);

		for (const [label, candidates] of [
			["identical", ["same", "same"]],
			["distinct", ["left", "right"]],
		]) {
			const multiprocessPath = join(fixture, `multiprocess-${label}.json`);
			const childResults = await Promise.all(candidates.map((candidate) => runConsumerChild(multiprocessPath, candidate)));
			assert.equal(childResults.every((code) => [0, 2].includes(code)), true);
			await withConsumerStateLock(multiprocessPath, async (_path, transaction) => {
				await transaction.commitState(bytes(`joined-${label}`));
			}, manualRuntime({ value: 100 }));
			const journal = consumerJournal(multiprocessPath);
			const commitTerminals = readdirSync(journal.epoch)
				.filter((name) => name.startsWith("terminal-"))
				.map((name) => JSON.parse(readFileSync(join(journal.epoch, name))))
				.filter((terminal) => terminal.outcome === "commit");
			const commitBases = commitTerminals.flatMap((terminal) => terminal.transactions.map((transaction) => transaction.baseDigest));
			assert.equal(new Set(commitBases).size, commitBases.length, "multiprocess candidates never commit twice from one base");
			if (label === "identical") {
				assert.equal(commitTerminals.filter((terminal) => terminal.transactions.some(
					(transaction) => transaction.candidateDigest === sha256Bytes(bytes("same")),
				)).length, 1);
			}
		}

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
		const raceClaims = readdirSync(consumerJournal(racePath).epoch).filter((name) => name.startsWith("claim-index-"));
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
			transitionNames(fencedPath).length,
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
		assert.equal(transitionNames(projectionPath).length, 3);

		const ctimeProjectionPath = join(fixture, "projection-ctime-retry.json");
		await withConsumerStateLock(ctimeProjectionPath, async (_path, transaction) => {
			await transaction.commitState(bytes("ctime-base"));
		}, manualRuntime({ value: 1 }));
		let changedProjectionCtime = false;
		const authenticatedProjectionOperations = [];
		await withConsumerStateLock(ctimeProjectionPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "ctime-base" });
		}, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => authenticatedProjectionOperations.push(operation),
			projectionRead: {
				beforeFinalStat: async () => {
					if (changedProjectionCtime) return;
					changedProjectionCtime = true;
					writePrivate(ctimeProjectionPath, bytes("ctime-base"));
				},
			},
		}));
		assert.equal(changedProjectionCtime, true);
		assert.equal(authenticatedProjectionOperations.includes("projection-retry-authentication"), true);
		assert.deepEqual(JSON.parse(readFileSync(ctimeProjectionPath, "utf8")), { value: "ctime-base" });

		const projectionStressPath = join(fixture, "projection-helper-stress.json");
		const firstStressProjectionReady = deferred();
		const releaseFirstStressProjection = deferred();
		let heldFirstStressProjection = false;
		const firstStressWriter = withConsumerStateLock(projectionStressPath, async (_path, transaction) => {
			await transaction.commitState(Buffer.from(`${JSON.stringify({ count: 1 })}\n`));
		}, manualRuntime({ value: 1 }, {
			afterProjectionFileSync: async () => {
				if (heldFirstStressProjection) return;
				heldFirstStressProjection = true;
				firstStressProjectionReady.resolve();
				await releaseFirstStressProjection.promise;
			},
		}));
		await firstStressProjectionReady.promise;
		for (let count = 2; count <= 8; count += 1) {
			await withConsumerStateLock(projectionStressPath, async (_path, transaction) => {
				assert.equal(JSON.parse(transaction.readStateBytes()).count, count - 1);
				await transaction.commitState(Buffer.from(`${JSON.stringify({ count })}\n`));
			}, manualRuntime({ value: count }));
		}
		releaseFirstStressProjection.resolve();
		await firstStressWriter;
		const projectionStressRotations = await Promise.all(Array.from(
			{ length: 3 },
			() => rotateConsumerStateJournal(projectionStressPath, manualRuntime({ value: 20 })),
		));
		assert.deepEqual(projectionStressRotations, Array.from({ length: 3 }, () => projectionStressRotations[0]));
		assert.equal(projectionStressRotations[0].epoch, 2);
		assert.deepEqual(JSON.parse(readFileSync(projectionStressPath, "utf8")), { count: 8 });

		const symlinkProjectionPath = join(fixture, "projection-symlink-corruption.json");
		await withConsumerStateLock(symlinkProjectionPath, async (_path, transaction) => {
			await transaction.commitState(bytes("symlink-base"));
		}, manualRuntime({ value: 1 }));
		const symlinkProjectionTarget = join(fixture, "projection-symlink-target.json");
		writePrivate(symlinkProjectionTarget, bytes("symlink-base"));
		rmSync(symlinkProjectionPath);
		symlinkSync(symlinkProjectionTarget, symlinkProjectionPath);
		await assert.rejects(
			() => withConsumerStateLock(symlinkProjectionPath, async () => {}, manualRuntime({ value: 2 })),
			/not one regular non-symlink file/,
		);

		writeFileSync(join(consumerJournal(projectionPath).epoch, `transition-${"f".repeat(64)}.json`), "{}\n");
		await assert.rejects(
			() => withConsumerStateLock(projectionPath, async () => {}, manualRuntime(projectionClock)),
			/unreachable transition/,
		);

		for (const crashPoint of [
			["afterFileSync", "legacy-guard"],
			["afterMetadataLink", "legacy-guard"],
			["afterMetadataDirectorySync", "legacy-guard"],
			["afterFileSync", "claim"],
			["afterMetadataLink", "claim"],
			["afterMetadataDirectorySync", "claim"],
			["afterFileSync", "claim-index"],
			["afterMetadataLink", "claim-index"],
			["afterMetadataDirectorySync", "claim-index"],
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
			assert.equal(
				readdirSync(consumerJournal(crashPath).epoch).some((name) => name.startsWith(".")),
				false,
				"owned crash temporaries converge after fencing",
			);
		}

		const appliedReceiptPath = join(fixture, "already-applied-rotation.json");
		await withConsumerStateLock(appliedReceiptPath, async (_path, transaction) => {
			await transaction.commitState(bytes("already-applied-anchor"));
		}, manualRuntime({ value: 1 }));
		const redundantAppliedWrites = [];
		const appliedReceiptRotation = await rotateConsumerStateJournal(appliedReceiptPath, manualRuntime({ value: 2 }, {
			afterFileSync: async ({ kind }) => {
				if (!["transition", "applied"].includes(kind)) return;
				redundantAppliedWrites.push(kind);
				throw new Error(`already-applied claim attempted a redundant ${kind} publication`);
			},
		}));
		assert.deepEqual(redundantAppliedWrites, []);
		assert.deepEqual(appliedReceiptRotation, {
			epoch: 2,
			tipSha256: sha256Bytes(bytes("already-applied-anchor")),
		});

		const legacyRotationPath = join(fixture, "legacy-rotation.json");
		writeFileSync(legacyRotationPath, bytes("legacy-anchor"), { mode: 0o600 });
		assert.equal((await rotateConsumerStateJournal(legacyRotationPath, manualRuntime({ value: 1 }))).epoch, 2);
		await withConsumerStateLock(legacyRotationPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "legacy-anchor" });
		}, manualRuntime({ value: 2 }));

		const rotationAuthorityPath = join(fixture, "rotation-authority.json");
		const rotationClock = { value: 1 };
		const pausedRotationOwner = deferred();
		const rotationOwnerEntered = deferred();
		const oldRotationOwner = withConsumerStateLock(rotationAuthorityPath, async (_path, transaction) => {
			rotationOwnerEntered.resolve();
			await pausedRotationOwner.promise;
			await transaction.commitState(bytes("stale-after-rotation"));
		}, manualRuntime(rotationClock));
		await rotationOwnerEntered.promise;
		await assert.rejects(
			() => rotateConsumerStateJournal(rotationAuthorityPath, manualRuntime(rotationClock)),
			/actively locked/,
		);
		rotationClock.value = 100;
		const rotatedAuthority = await rotateConsumerStateJournal(rotationAuthorityPath, manualRuntime(rotationClock));
		assert.equal(rotatedAuthority.epoch, 2);
		pausedRotationOwner.resolve();
		await assert.rejects(oldRotationOwner, /retired|fenced|lost ownership/);
		await withConsumerStateLock(rotationAuthorityPath, async (_path, transaction) => {
			assert.equal(transaction.readStateBytes(), null);
			await transaction.commitState(bytes("after-rotation"));
		}, manualRuntime(rotationClock));
		assert.deepEqual(JSON.parse(readFileSync(rotationAuthorityPath, "utf8")), { value: "after-rotation" });
		assert.deepEqual(
			readdirSync(`${rotationAuthorityPath}.journal`).filter((name) => !name.startsWith(".")),
			readdirSync(`${rotationAuthorityPath}.journal`).filter((name) => !name.startsWith(".")).filter(
				(name) => name.startsWith("checkpoint-") || name.startsWith("epoch-"),
			),
		);
		assert.equal(readdirSync(`${rotationAuthorityPath}.journal`).filter((name) => name.startsWith("checkpoint-")).length, 1);
		assert.equal(readdirSync(`${rotationAuthorityPath}.journal`).filter((name) => name.startsWith("epoch-")).length, 1);

		const depthRotationPath = join(fixture, "depth-rotation.json");
		const depthOptions = { ...manualRuntime({ value: 1 }), maxTransactionDepth: 1 };
		await withConsumerStateLock(depthRotationPath, async (_path, transaction) => {
			await transaction.commitState(bytes("one"));
		}, depthOptions);
		await assert.rejects(
			() => withConsumerStateLock(depthRotationPath, async (_path, transaction) => {
				await transaction.commitState(bytes("blocked"));
			}, { ...manualRuntime({ value: 2 }), maxTransactionDepth: 1 }),
			/run the consumer journal rotation command/,
		);
		await rotateConsumerStateJournal(depthRotationPath, { ...manualRuntime({ value: 3 }), maxTransactionDepth: 1 });
		await withConsumerStateLock(depthRotationPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "one" });
			await transaction.commitState(bytes("two"));
		}, { ...manualRuntime({ value: 4 }), maxTransactionDepth: 1 });
		assert.deepEqual(JSON.parse(readFileSync(depthRotationPath, "utf8")), { value: "two" });

		const normalWinsPath = join(fixture, "normal-wins-rotation-slot.json");
		await withConsumerStateLock(normalWinsPath, async (_path, transaction) => {
			await transaction.commitState(bytes("base"));
		}, manualRuntime({ value: 1 }));
		const normalClaimLinked = deferred();
		const releaseNormalClaim = deferred();
		const normalWinner = withConsumerStateLock(normalWinsPath, async (_path, transaction) => {
			await transaction.commitState(bytes("normal-winner"));
		}, manualRuntime({ value: 2 }, {
			afterMetadataLink: async ({ kind }) => {
				if (kind !== "claim-index") return;
				normalClaimLinked.resolve();
				await releaseNormalClaim.promise;
			},
		}));
		await normalClaimLinked.promise;
		await assert.rejects(
			() => rotateConsumerStateJournal(normalWinsPath, manualRuntime({ value: 2 })),
			/actively locked/,
		);
		releaseNormalClaim.resolve();
		await normalWinner;
		let rederivedRotationClaim;
		const normalWinsRotation = await rotateConsumerStateJournal(normalWinsPath, manualRuntime({ value: 3 }, {
			beforeRotationDecision: async ({ claim }) => { rederivedRotationClaim = claim; },
		}));
		assert.equal(rederivedRotationClaim.generation, 3);
		assert.equal(rederivedRotationClaim.intent.tipSha256, sha256Bytes(bytes("normal-winner")));
		assert.equal(normalWinsRotation.tipSha256, sha256Bytes(bytes("normal-winner")));

		const rotationWinsPath = join(fixture, "rotation-wins-normal-slot.json");
		await withConsumerStateLock(rotationWinsPath, async (_path, transaction) => {
			await transaction.commitState(bytes("rotation-base"));
		}, manualRuntime({ value: 1 }));
		const rotationClaimLinked = deferred();
		const releaseRotationClaim = deferred();
		const rotationWinner = rotateConsumerStateJournal(rotationWinsPath, manualRuntime({ value: 2 }, {
			afterMetadataLink: async ({ kind }) => {
				if (kind !== "claim-index") return;
				rotationClaimLinked.resolve();
				await releaseRotationClaim.promise;
			},
		}));
		await rotationClaimLinked.promise;
		const pendingRotationJournal = consumerJournal(rotationWinsPath);
		const pendingRotationClaim = readdirSync(pendingRotationJournal.epoch)
			.filter((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name))
			.map((name) => JSON.parse(readFileSync(join(pendingRotationJournal.epoch, name))))
			.at(-1);
		assert.equal(pendingRotationClaim.type, "rotation");
		assert.equal(pendingRotationClaim.generation, 2);
		let callbackAfterRotation = 0;
		await withConsumerStateLock(rotationWinsPath, async (_path, transaction) => {
			callbackAfterRotation += 1;
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "rotation-base" });
		}, manualRuntime({ value: 3 }));
		assert.equal(callbackAfterRotation, 1);
		releaseRotationClaim.resolve();
		assert.equal((await rotationWinner).epoch, 2);

		const claimRotationPath = join(fixture, "claim-rotation.json");
		for (let generation = 1; generation <= 3; generation += 1) {
			await withConsumerStateLock(
				claimRotationPath,
				async () => {},
				{ ...manualRuntime({ value: generation }), maxLockGenerations: 3 },
			);
		}
		await assert.rejects(
			() => withConsumerStateLock(
				claimRotationPath,
				async () => {},
				{ ...manualRuntime({ value: 3 }), maxLockGenerations: 3 },
			),
			/claim epoch is exhausted.*rotation command/,
		);
		assert.equal((await rotateConsumerStateJournal(
			claimRotationPath,
			{ ...manualRuntime({ value: 4 }), maxLockGenerations: 3 },
		)).epoch, 2);

		const finalClaimCrashPath = join(fixture, "final-claim-crash.json");
		await withConsumerStateLock(
			finalClaimCrashPath,
			async () => {},
			{ ...manualRuntime({ value: 1 }), maxLockGenerations: 2 },
		);
		await assert.rejects(
			() => withConsumerStateLock(finalClaimCrashPath, async () => {}, {
				...manualRuntime({ value: 2 }, { afterClaim: async () => { throw new Error("simulated final claim crash"); } }),
				maxLockGenerations: 2,
			}),
			/simulated final claim crash/,
		);
		await assert.rejects(
			() => withConsumerStateLock(
				finalClaimCrashPath,
				async () => {},
				{ ...manualRuntime({ value: 100 }), maxLockGenerations: 2 },
			),
			/claim epoch is exhausted/,
		);
		const finalCrashJournal = consumerJournal(finalClaimCrashPath);
		const finalCrashCheckpoint = JSON.parse(readFileSync(finalCrashJournal.checkpoint));
		const finalCrashClaim = JSON.parse(readFileSync(join(
			finalCrashJournal.epoch,
			readdirSync(finalCrashJournal.epoch).find((name) => /^claim-0000000000000002-[0-9a-f]{64}\.json$/.test(name)),
		)));
		const finalCrashTemporaryDirectory = join(finalCrashJournal.journal, ".owned-temporaries-v2");
		for (let index = 0; index < 17; index += 1) {
			const temporaryName = `.pylon-consumer-tmp-v1-p999999-e${finalCrashCheckpoint.epochId}` +
				`-g0000000000000002-w${finalCrashClaim.token}-n${index.toString(16).padStart(12, "0")}` +
				`-ktransition-t${"c".repeat(64)}.tmp`;
			writePrivate(join(finalCrashTemporaryDirectory, temporaryName), "dead final-claim temporary");
		}
		let rotationClaimHooks = 0;
		assert.equal((await rotateConsumerStateJournal(finalClaimCrashPath, {
			...manualRuntime({ value: 100 }, { afterClaim: async () => { rotationClaimHooks += 1; } }),
			maxLockGenerations: 2,
		})).epoch, 2);
		assert.equal(rotationClaimHooks, 0, "rotation never consumes a normal claim, including the finite final claim");
		assert.deepEqual(readdirSync(finalCrashTemporaryDirectory), []);
		await withConsumerStateLock(finalClaimCrashPath, async (_path, transaction) => {
			await transaction.commitState(bytes("normal-after-final-crash-rotation"));
		}, { ...manualRuntime({ value: 101 }), maxLockGenerations: 2 });

		const temporaryFloodPath = join(fixture, "temporary-flood.json");
		await withConsumerStateLock(temporaryFloodPath, async () => {}, {
			...manualRuntime({ value: 1 }),
			maxLockGenerations: 2,
		});
		const floodJournal = consumerJournal(temporaryFloodPath);
		const floodCheckpoint = JSON.parse(readFileSync(floodJournal.checkpoint));
		const floodClaimName = readdirSync(floodJournal.epoch).find((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name));
		const floodClaim = JSON.parse(readFileSync(join(floodJournal.epoch, floodClaimName)));
		const floodTemporaryDirectory = join(floodJournal.journal, ".owned-temporaries-v2");
		const temporaryFileName = ({ pid, generation, token, attempt, kind }) =>
			`.pylon-consumer-tmp-v1-p${pid}-e${floodCheckpoint.epochId}-g${String(generation).padStart(16, "0")}` +
			`-w${token}-n${attempt.toString(16).padStart(12, "0")}-k${kind}-t${"a".repeat(64)}.tmp`;
		for (let index = 0; index < 40; index += 1) {
			writePrivate(join(floodTemporaryDirectory, temporaryFileName({
				pid: 999_999,
				generation: index < 20 ? 0 : 1,
				token: index < 20 ? "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" : floodClaim.token,
				attempt: index,
				kind: index < 20 ? "checkpoint" : "transition",
			})), "crashed temporary");
		}
		await withConsumerStateLock(temporaryFloodPath, async () => {}, {
			...manualRuntime({ value: 2 }),
			maxLockGenerations: 2,
			processKill: () => { throw Object.assign(new Error("dead"), { code: "ESRCH" }); },
		});
		assert.deepEqual(readdirSync(floodTemporaryDirectory), [], "temporary cleanup runs before logical journal caps");
		const symlinkTemporary = join(floodTemporaryDirectory, temporaryFileName({
			pid: 999_999,
			generation: 0,
			token: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			attempt: 99,
			kind: "checkpoint",
		}));
		symlinkSync(temporaryFloodPath, symlinkTemporary);
		await assert.rejects(
			() => rotateConsumerStateJournal(temporaryFloodPath, {
				...manualRuntime({ value: 3 }),
				maxLockGenerations: 2,
			}),
			/owned temporary.*regular non-symlink file/,
		);
		rmSync(symlinkTemporary);

		const liveTemporaryPath = join(fixture, "live-rotation-temporary.json");
		await withConsumerStateLock(liveTemporaryPath, async () => {}, manualRuntime({ value: 1 }));
		const liveJournal = consumerJournal(liveTemporaryPath);
		const liveCheckpoint = JSON.parse(readFileSync(liveJournal.checkpoint));
		const liveTemporaryDirectory = join(liveJournal.journal, ".owned-temporaries-v2");
		const liveTemporaryName = `.pylon-consumer-tmp-v1-p${process.pid}-e${liveCheckpoint.epochId}` +
			`-g0000000000000000-waaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa-n000000000001-kcheckpoint-t${"b".repeat(64)}.tmp`;
		writePrivate(join(liveTemporaryDirectory, liveTemporaryName), "live temporary");
		await assert.rejects(
			() => rotateConsumerStateJournal(liveTemporaryPath, manualRuntime({ value: 2 })),
			/rotation operation is pending.*temporary writer quiesces/,
		);
		assert.equal(readdirSync(liveJournal.epoch).filter((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name)).some(
			(name) => JSON.parse(readFileSync(join(liveJournal.epoch, name))).type === "rotation",
		), true);
		await assert.rejects(
			() => withConsumerStateLock(liveTemporaryPath, async () => {}, manualRuntime({ value: 3 })),
			/rotation operation is pending.*temporary writer quiesces/,
		);
		rmSync(join(liveTemporaryDirectory, liveTemporaryName));
		assert.equal((await rotateConsumerStateJournal(liveTemporaryPath, manualRuntime({ value: 4 }))).epoch, 2);

		const frontierHandoffPath = join(fixture, "frontier-completion-handoff.json");
		await withConsumerStateLock(frontierHandoffPath, async (_path, transaction) => {
			await transaction.commitState(bytes("frontier-handoff-anchor"));
		}, manualRuntime({ value: 1 }));
		const firstSecondScanReached = deferred();
		const secondSecondScanReached = deferred();
		const releaseFirstSecondScan = deferred();
		const releaseSecondSecondScan = deferred();
		const rotationAtSecondScanBarrier = (reached, release) => {
			let claimScans = 0;
			return rotateConsumerStateJournal(frontierHandoffPath, manualRuntime({ value: 2 }, {
				beforePathOperation: async ({ operation }) => {
					if (operation !== "scan-claims") return;
					claimScans += 1;
					if (claimScans !== 2) return;
					reached.resolve();
					await release.promise;
				},
			}));
		};
		const frontierWinner = rotationAtSecondScanBarrier(firstSecondScanReached, releaseFirstSecondScan);
		const frontierResumed = rotationAtSecondScanBarrier(secondSecondScanReached, releaseSecondSecondScan);
		await Promise.all([firstSecondScanReached.promise, secondSecondScanReached.promise]);
		releaseFirstSecondScan.resolve();
		const frontierWinnerResult = await frontierWinner;
		releaseSecondSecondScan.resolve();
		const frontierResumedResult = await frontierResumed;
		assert.equal(Buffer.from(JSON.stringify(frontierResumedResult)).equals(
			Buffer.from(JSON.stringify(frontierWinnerResult)),
		), true, "the stale rotator returns the winner's byte-identical completion receipt");
		assert.deepEqual(frontierWinnerResult, {
			epoch: 2,
			tipSha256: sha256Bytes(bytes("frontier-handoff-anchor")),
		});

		const removedClaimHandoffPath = join(fixture, "removed-claim-completion-handoff.json");
		await withConsumerStateLock(removedClaimHandoffPath, async (_path, transaction) => {
			await transaction.commitState(bytes("removed-claim-handoff-anchor"));
		}, manualRuntime({ value: 1 }));
		const removedClaimJournal = consumerJournal(removedClaimHandoffPath);
		const removedClaimIndexName = readdirSync(removedClaimJournal.epoch).find((name) => name.startsWith("claim-index-"));
		const removedClaimIndex = JSON.parse(readFileSync(join(removedClaimJournal.epoch, removedClaimIndexName)));
		const removedClaimContentName = readdirSync(removedClaimJournal.epoch)
			.find((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name));
		const removedClaimContentBytes = readFileSync(join(removedClaimJournal.epoch, removedClaimContentName));
		assert.equal(removedClaimIndex.claimSha256, sha256Bytes(removedClaimContentBytes));
		assert.match(removedClaimContentName, new RegExp(`${removedClaimIndex.claimSha256}\\.json$`));
		const removedClaimReadReached = deferred();
		const releaseRemovedClaimRead = deferred();
		let claimReads = 0;
		let openedClaimPath;
		const removedClaimStale = rotateConsumerStateJournal(removedClaimHandoffPath, manualRuntime({ value: 2 }, {
			metadataRead: {
				afterInitialStat: async ({ path }) => {
					if (!/^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(basename(path))) return;
					claimReads += 1;
					if (claimReads !== 2) return;
					openedClaimPath = path;
					removedClaimReadReached.resolve();
					await releaseRemovedClaimRead.promise;
				},
			},
		}));
		await removedClaimReadReached.promise;
		const removedClaimWinner = await rotateConsumerStateJournal(removedClaimHandoffPath, manualRuntime({ value: 3 }));
		const removedClaimCleanup = await rotateConsumerStateJournal(removedClaimHandoffPath, manualRuntime({ value: 4 }));
		assert.deepEqual(removedClaimCleanup, removedClaimWinner);
		assert.equal(existsSync(openedClaimPath), false, "the authenticated old claim inode is removed while its read handle is pinned");
		releaseRemovedClaimRead.resolve();
		const removedClaimResumed = await removedClaimStale;
		assert.equal(Buffer.from(JSON.stringify(removedClaimResumed)).equals(
			Buffer.from(JSON.stringify(removedClaimWinner)),
		), true, "a same-inode old-claim removal converges only to the byte-identical winner receipt");
		assert.deepEqual(removedClaimWinner, {
			epoch: 2,
			tipSha256: sha256Bytes(bytes("removed-claim-handoff-anchor")),
		});


		const retainedCheckpointHandoffPath = join(fixture, "retained-checkpoint-completion-handoff.json");
		await withConsumerStateLock(retainedCheckpointHandoffPath, async (_path, transaction) => {
			await transaction.commitState(bytes("retained-checkpoint-handoff-anchor"));
		}, manualRuntime({ value: 1 }));
		const retainedCheckpointPublished = deferred();
		const releaseRetainedCheckpointWinner = deferred();
		const retainedCheckpointWinner = rotateConsumerStateJournal(
			retainedCheckpointHandoffPath,
			manualRuntime({ value: 2 }, {
				afterRotationCheckpoint: async () => {
					retainedCheckpointPublished.resolve();
					await releaseRetainedCheckpointWinner.promise;
				},
			}),
		);
		await retainedCheckpointPublished.promise;
		const retainedCheckpointJournal = `${retainedCheckpointHandoffPath}.journal`;
		const retainedCheckpointNames = readdirSync(retainedCheckpointJournal)
			.filter((name) => name.startsWith("checkpoint-"))
			.sort();
		assert.equal(retainedCheckpointNames.length, 2);
		const retainedCheckpointPath = join(retainedCheckpointJournal, retainedCheckpointNames[0]);
		const retainedCheckpointReadReached = deferred();
		const releaseRetainedCheckpointRead = deferred();
		let retainedCheckpointReads = 0;
		const retainedCheckpointStale = rotateConsumerStateJournal(
			retainedCheckpointHandoffPath,
			manualRuntime({ value: 3 }, {
				metadataRead: {
					afterInitialStat: async ({ path }) => {
						if (path !== retainedCheckpointPath) return;
						retainedCheckpointReads += 1;
						if (retainedCheckpointReads !== 2) return;
						retainedCheckpointReadReached.resolve();
						await releaseRetainedCheckpointRead.promise;
					},
				},
			}),
		);
		await retainedCheckpointReadReached.promise;
		const retainedCurrentCheckpoint = JSON.parse(readFileSync(join(
			retainedCheckpointJournal,
			retainedCheckpointNames[1],
		)));
		rmSync(join(retainedCheckpointJournal, retainedCurrentCheckpoint.retiredEpochDirectory), { recursive: true });
		rmSync(retainedCheckpointPath);
		assert.equal(existsSync(retainedCheckpointPath), false);
		releaseRetainedCheckpointRead.resolve();
		const retainedCheckpointStaleReceipt = await retainedCheckpointStale;
		releaseRetainedCheckpointWinner.resolve();
		const retainedCheckpointWinnerReceipt = await retainedCheckpointWinner;
		assert.equal(Buffer.from(JSON.stringify(retainedCheckpointStaleReceipt)).equals(
			Buffer.from(JSON.stringify(retainedCheckpointWinnerReceipt)),
		), true, "a removed digest-anchored retained checkpoint converges to the byte-identical epoch receipt");
		assert.deepEqual(
			readdirSync(retainedCheckpointJournal).filter((name) => !name.startsWith(".")).sort(),
			readdirSync(retainedCheckpointJournal).filter((name) => !name.startsWith(".")).filter(
				(name) => name.startsWith("checkpoint-") || name.startsWith("epoch-"),
			).sort(),
		);
		assert.equal(readdirSync(retainedCheckpointJournal).filter((name) => name.startsWith("checkpoint-")).length, 1);
		assert.equal(readdirSync(retainedCheckpointJournal).filter((name) => name.startsWith("epoch-")).length, 1);

		const currentCheckpointHandoffPath = join(fixture, "current-checkpoint-completion-handoff.json");
		await withConsumerStateLock(currentCheckpointHandoffPath, async (_path, transaction) => {
			await transaction.commitState(bytes("current-checkpoint-handoff-anchor"));
		}, manualRuntime({ value: 1 }));
		const currentCheckpointJournal = consumerJournal(currentCheckpointHandoffPath);
		const currentCheckpointReadReached = deferred();
		const releaseCurrentCheckpointRead = deferred();
		const currentCheckpointEarlyReached = deferred();
		const releaseCurrentCheckpointEarly = deferred();
		const currentCheckpointPublished = deferred();
		const releaseCurrentCheckpointWinner = deferred();
		let currentCheckpointWinnerPublished = false;
		let currentCheckpointEarlyWalks = 0;
		const currentCheckpointStale = rotateConsumerStateJournal(currentCheckpointHandoffPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				currentCheckpointEarlyWalks += 1;
				if (currentCheckpointEarlyWalks !== 1) return;
				currentCheckpointEarlyReached.resolve();
				await releaseCurrentCheckpointEarly.promise;
			},
			metadataRead: {
				afterInitialStat: async ({ path }) => {
					if (!currentCheckpointWinnerPublished || path !== currentCheckpointJournal.checkpoint) return;
					currentCheckpointReadReached.resolve();
					await releaseCurrentCheckpointRead.promise;
				},
			},
		}));
		await currentCheckpointEarlyReached.promise;
		const currentCheckpointWinner = rotateConsumerStateJournal(currentCheckpointHandoffPath, manualRuntime({ value: 3 }, {
			afterRotationCheckpoint: async () => {
				currentCheckpointWinnerPublished = true;
				currentCheckpointPublished.resolve();
				await releaseCurrentCheckpointWinner.promise;
			},
		}));
		await currentCheckpointPublished.promise;
		releaseCurrentCheckpointEarly.resolve();
		await currentCheckpointReadReached.promise;
		rmSync(currentCheckpointJournal.epoch, { recursive: true });
		rmSync(currentCheckpointJournal.checkpoint);
		assert.equal(existsSync(currentCheckpointJournal.checkpoint), false);
		releaseCurrentCheckpointRead.resolve();
		const currentCheckpointStaleReceipt = await currentCheckpointStale;
		releaseCurrentCheckpointWinner.resolve();
		const currentCheckpointWinnerReceipt = await currentCheckpointWinner;
		assert.equal(Buffer.from(JSON.stringify(currentCheckpointStaleReceipt)).equals(
			Buffer.from(JSON.stringify(currentCheckpointWinnerReceipt)),
		), true, "a removed digest-anchored current checkpoint authenticates exactly one immediate successor");
		assert.deepEqual(currentCheckpointWinnerReceipt, {
			epoch: 2,
			tipSha256: sha256Bytes(bytes("current-checkpoint-handoff-anchor")),
		});



		const forgedDescendantPath = join(fixture, "forged-descendant-checkpoint.json");
		await withConsumerStateLock(forgedDescendantPath, async (_path, transaction) => {
			await transaction.commitState(bytes("forged-descendant-base"));
		}, manualRuntime({ value: 1 }));
		let validDescendantCheckpoint;
		const capturedValidDescendant = new Error("capture exact valid descendant checkpoint");
		await assert.rejects(
			() => rotateConsumerStateJournal(forgedDescendantPath, manualRuntime({ value: 2 }, {
				beforeRotationDecision: async ({ claim }) => {
					validDescendantCheckpoint = claim.intent.checkpoint;
					throw capturedValidDescendant;
				},
			})),
			(error) => error === capturedValidDescendant,
		);
		const forgedDescendantJournal = consumerJournal(forgedDescendantPath);
		const validDescendantCheckpointPath = join(
			forgedDescendantJournal.journal,
			`checkpoint-${String(validDescendantCheckpoint.epoch).padStart(16, "0")}-${validDescendantCheckpoint.epochId}.json`,
		);
		const validDescendantEpochPath = join(
			forgedDescendantJournal.journal,
			`epoch-${String(validDescendantCheckpoint.epoch).padStart(16, "0")}-${validDescendantCheckpoint.epochId}`,
		);
		const forgedAnchorBytes = bytes("forged-descendant-anchor");
		const forgedSourceTipBytes = bytes("forged-descendant-source-tip");
		const forgedDescendantCheckpoint = {
			...validDescendantCheckpoint,
			epochId: randomUUID(),
			historySha256: "d".repeat(64),
			anchorDigest: sha256Bytes(forgedAnchorBytes),
			anchorBase64: forgedAnchorBytes.toString("base64"),
			previousTipSha256: sha256Bytes(forgedAnchorBytes),
			sourceAuthoritySha256: "e".repeat(64),
			sourceAuthorityTipDigest: sha256Bytes(forgedSourceTipBytes),
			sourceAuthorityTipBase64: forgedSourceTipBytes.toString("base64"),
		};
		assert.equal(
			forgedDescendantCheckpoint.previousCheckpointSha256,
			validDescendantCheckpoint.previousCheckpointSha256,
		);
		assert.equal(forgedDescendantCheckpoint.retiredEpochDirectory, validDescendantCheckpoint.retiredEpochDirectory);
		for (const field of ["epochId", "historySha256", "sourceAuthoritySha256", "anchorDigest"]) {
			assert.notEqual(forgedDescendantCheckpoint[field], validDescendantCheckpoint[field]);
		}
		const forgedDescendantCheckpointPath = join(
			forgedDescendantJournal.journal,
			`checkpoint-${String(forgedDescendantCheckpoint.epoch).padStart(16, "0")}-${forgedDescendantCheckpoint.epochId}.json`,
		);
		const forgedDescendantEpochPath = join(
			forgedDescendantJournal.journal,
			`epoch-${String(forgedDescendantCheckpoint.epoch).padStart(16, "0")}-${forgedDescendantCheckpoint.epochId}`,
		);
		const forgedProjectionReady = deferred();
		const releaseForgedProjection = deferred();
		let forgedProjectionHeld = false;
		let forgedSuccessorPublished = false;
		let forgedCurrentOperation = null;
		let forgedRetryRootReads = 0;
		const forgedDescendantWriter = withConsumerStateLock(
			forgedDescendantPath,
			async (_path, transaction) => {
				await transaction.commitState(bytes("forged-descendant-candidate"));
			},
			{
				...manualRuntime({ value: 3 }, {
					beforePathOperation: async ({ operation }) => {
						forgedCurrentOperation = operation;
					},
					afterProjectionFileSync: async () => {
						if (forgedProjectionHeld) return;
						forgedProjectionHeld = true;
						forgedProjectionReady.resolve();
						await releaseForgedProjection.promise;
					},
				}),
				readDirectory: async (path) => {
					const names = await readDirectoryEntries(path);
					if (
						forgedSuccessorPublished && path === forgedDescendantJournal.journal &&
						forgedCurrentOperation === "finish-commit-retry-authentication"
					) {
						forgedRetryRootReads += 1;
						if (forgedRetryRootReads === 4) {
							rmSync(validDescendantCheckpointPath);
							rmSync(validDescendantEpochPath, { recursive: true });
							mkdirSync(forgedDescendantEpochPath, { mode: 0o700 });
							writePrivate(forgedDescendantCheckpointPath, metadata(forgedDescendantCheckpoint));
						}
					}
					return names;
				},
			},
		);
		await forgedProjectionReady.promise;
		const forgedProjectionBefore = readFileSync(forgedDescendantPath);
		const forgedOperationEntriesBefore = new Map(
			readdirSync(forgedDescendantJournal.epoch)
				.filter((name) => /^(?:claim(?:-index)?-|terminal-|transition-|applied-)/.test(name))
				.sort()
				.map((name) => [name, readFileSync(join(forgedDescendantJournal.epoch, name))]),
		);
		assert.equal(
			[...forgedOperationEntriesBefore.keys()].some((name) => name.startsWith("terminal-")),
			true,
			"the forged-descendant barrier holds one terminal operation before any repair",
		);
		mkdirSync(validDescendantEpochPath, { mode: 0o700 });
		writePrivate(validDescendantCheckpointPath, metadata(validDescendantCheckpoint));
		rmSync(forgedDescendantJournal.checkpoint);
		forgedSuccessorPublished = true;
		releaseForgedProjection.resolve();
		await assert.rejects(
			forgedDescendantWriter,
			/journal root has neither its byte-exact current checkpoint nor one exact immediate successor/,
		);
		assert.equal(forgedRetryRootReads, 5, "the forged successor is rejected by the first classified scan after the valid final proof");
		assert.equal(readFileSync(forgedDescendantPath).equals(forgedProjectionBefore), true);
		const forgedOperationEntriesAfter = new Map(
			readdirSync(forgedDescendantJournal.epoch)
				.filter((name) => /^(?:claim(?:-index)?-|terminal-|transition-|applied-)/.test(name))
				.sort()
				.map((name) => [name, readFileSync(join(forgedDescendantJournal.epoch, name))]),
		);
		assert.deepEqual([...forgedOperationEntriesAfter.keys()], [...forgedOperationEntriesBefore.keys()]);
		for (const [name, entryBytes] of forgedOperationEntriesBefore) {
			assert.equal(forgedOperationEntriesAfter.get(name).equals(entryBytes), true, `${name} remains byte-exact`);
		}

		const startCheckpointReadRace = async (label, targetRole, runtime = {}) => {
			const statePath = join(fixture, `${label}.json`);
			await withConsumerStateLock(statePath, async (_path, transaction) => {
				await transaction.commitState(bytes(`${label}-anchor`));
			}, manualRuntime({ value: 1 }));
			const journal = consumerJournal(statePath);
			const earlyReached = deferred();
			const releaseEarly = deferred();
			const readReached = deferred();
			const releaseRead = deferred();
			let firstWalks = 0;
			let armed = false;
			let targetCheckpointPath = null;
			const stale = rotateConsumerStateJournal(statePath, {
				...manualRuntime({ value: 2 }, {
					beforePathOperation: async ({ operation }) => {
						if (operation !== "walk-transactions") return;
						firstWalks += 1;
						if (firstWalks !== 1) return;
						earlyReached.resolve();
						await releaseEarly.promise;
					},
					metadataRead: {
						afterInitialStat: async (event) => {
							await runtime.afterInitialStat?.(event, { armed, targetCheckpointPath });
							if (!armed || event.path !== targetCheckpointPath) return;
							readReached.resolve(event);
							await releaseRead.promise;
						},
						beforeFinalStat: runtime.beforeFinalStat,
					},
				}),
				...(runtime.readDirectory ? { readDirectory: runtime.readDirectory } : {}),
			});
			await earlyReached.promise;
			let successorCheckpoint;
			const capturedSuccessor = new Error(`capture ${label} successor`);
			await assert.rejects(
				() => rotateConsumerStateJournal(statePath, manualRuntime({ value: 3 }, {
					beforeRotationDecision: async ({ claim }) => {
						successorCheckpoint = claim.intent.checkpoint;
						throw capturedSuccessor;
					},
				})),
				(error) => error === capturedSuccessor,
			);
			const successorCheckpointName =
				`checkpoint-${String(successorCheckpoint.epoch).padStart(16, "0")}-${successorCheckpoint.epochId}.json`;
			const successorEpochName =
				`epoch-${String(successorCheckpoint.epoch).padStart(16, "0")}-${successorCheckpoint.epochId}`;
			const successorCheckpointPath = join(journal.journal, successorCheckpointName);
			const successorEpochPath = join(journal.journal, successorEpochName);
			mkdirSync(successorEpochPath, { mode: 0o700 });
			writePrivate(successorCheckpointPath, metadata(successorCheckpoint));
			targetCheckpointPath = targetRole === "current" ? journal.checkpoint : successorCheckpointPath;
			armed = true;
			releaseEarly.resolve();
			const readEvent = await readReached.promise;
			return {
				statePath,
				journal,
				stale,
				releaseRead,
				readEvent,
				targetCheckpointPath,
				successorCheckpointPath,
				successorEpochPath,
			};
		};

		const mutatedCheckpointRace = await startCheckpointReadRace(
			"canonical-mutated-checkpoint-read",
			"current",
		);
		const mutatedCheckpointBytes = readFileSync(mutatedCheckpointRace.targetCheckpointPath);
		const mutatedCheckpoint = JSON.parse(mutatedCheckpointBytes);
		mutatedCheckpoint.historySha256 = `${mutatedCheckpoint.historySha256[0] === "a" ? "b" : "a"}` +
			mutatedCheckpoint.historySha256.slice(1);
		const mutatedCanonicalBytes = metadata(mutatedCheckpoint);
		assert.equal(mutatedCanonicalBytes.length, mutatedCheckpointBytes.length);
		const mutatedCheckpointStat = statSync(mutatedCheckpointRace.targetCheckpointPath);
		writePrivate(mutatedCheckpointRace.targetCheckpointPath, mutatedCanonicalBytes);
		utimesSync(
			mutatedCheckpointRace.targetCheckpointPath,
			mutatedCheckpointStat.atime,
			mutatedCheckpointStat.mtime,
		);
		rmSync(mutatedCheckpointRace.targetCheckpointPath);
		mutatedCheckpointRace.releaseRead.resolve();
		await assert.rejects(mutatedCheckpointRace.stale, (error) => {
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read/);
			return true;
		});

		for (const operation of ["unlink", "replace"]) {
			const successorReadRace = await startCheckpointReadRace(
				`unanchored-successor-${operation}`,
				"successor",
			);
			if (operation === "unlink") {
				rmSync(successorReadRace.targetCheckpointPath);
			} else {
				const original = readFileSync(successorReadRace.targetCheckpointPath);
				renameSync(successorReadRace.targetCheckpointPath, `${successorReadRace.targetCheckpointPath}.replaced`);
				writePrivate(successorReadRace.targetCheckpointPath, original);
			}
			successorReadRace.releaseRead.resolve();
			await assert.rejects(successorReadRace.stale, (error) => {
				assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
				assert.match(error.message, /changed while it was read/);
				return true;
			});
		}

		const replacedAnchoredRace = await startCheckpointReadRace("anchored-checkpoint-replace", "current");
		const replacedAnchoredBytes = readFileSync(replacedAnchoredRace.targetCheckpointPath);
		renameSync(replacedAnchoredRace.targetCheckpointPath, `${replacedAnchoredRace.targetCheckpointPath}.replaced`);
		writePrivate(replacedAnchoredRace.targetCheckpointPath, replacedAnchoredBytes);
		replacedAnchoredRace.releaseRead.resolve();
		await assert.rejects(replacedAnchoredRace.stale, (error) => {
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read/);
			return true;
		});

		const checkpointIoFailure = new Error("injected exact checkpoint read I/O failure");
		let checkpointIoArmed = false;
		const checkpointIoRace = await startCheckpointReadRace("checkpoint-read-io-identity", "current", {
			beforeFinalStat: async ({ path }) => {
				if (checkpointIoArmed && path === checkpointIoRace.targetCheckpointPath) throw checkpointIoFailure;
			},
		});
		checkpointIoArmed = true;
		checkpointIoRace.releaseRead.resolve();
		await assert.rejects(checkpointIoRace.stale, (error) => error === checkpointIoFailure);

		for (const rootMutation of ["competing-checkpoint", "malformed-entry"]) {
			let proofRootReads = 0;
			let proofArmed = false;
			let rootPath;
			const finalProofRace = await startCheckpointReadRace(
				`checkpoint-final-proof-${rootMutation}`,
				"current",
				{
					readDirectory: async (path) => {
						if (proofArmed && path === rootPath) {
							proofRootReads += 1;
							if (proofRootReads === 2) {
								if (rootMutation === "competing-checkpoint") {
									writePrivate(
										join(rootPath, `checkpoint-0000000000000002-${randomUUID()}.json`),
										"{}\n",
									);
								} else {
									writePrivate(join(rootPath, "malformed-root-entry"), "{}\n");
								}
							}
						}
						return readDirectoryEntries(path);
					},
				},
			);
			rootPath = finalProofRace.journal.journal;
			const successorEntriesBefore = readdirSync(finalProofRace.successorEpochPath).sort();
			rmSync(finalProofRace.journal.epoch, { recursive: true });
			rmSync(finalProofRace.journal.checkpoint);
			proofArmed = true;
			finalProofRace.releaseRead.resolve();
			await assert.rejects(
				finalProofRace.stale,
				/journal root changed without one exact current or immediate-successor authority/,
			);
			assert.equal(proofRootReads, 2, "the changed root receives one bounded proof and one final subset recheck");
			assert.deepEqual(
				readdirSync(finalProofRace.successorEpochPath).sort(),
				successorEntriesBefore,
				"a root inserted before the final proof cannot publish any journal operation",
			);
		}

		const earlyFenceHandoffPath = join(fixture, "early-fence-completion-handoff.json");
		await withConsumerStateLock(earlyFenceHandoffPath, async (_path, transaction) => {
			await transaction.commitState(bytes("early-fence-handoff-anchor"));
		}, manualRuntime({ value: 1 }));
		const earlyFenceReached = deferred();
		const releaseEarlyFence = deferred();
		let preIntentWalks = 0;
		const earlyFenceStale = rotateConsumerStateJournal(earlyFenceHandoffPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				preIntentWalks += 1;
				if (preIntentWalks !== 1) return;
				earlyFenceReached.resolve();
				await releaseEarlyFence.promise;
			},
		}));
		await earlyFenceReached.promise;
		const earlyFenceWinner = await rotateConsumerStateJournal(earlyFenceHandoffPath, manualRuntime({ value: 3 }));
		releaseEarlyFence.resolve();
		const earlyFenceResumed = await earlyFenceStale;
		assert.equal(Buffer.from(JSON.stringify(earlyFenceResumed)).equals(
			Buffer.from(JSON.stringify(earlyFenceWinner)),
		), true, "a pre-intent epoch fence reauthenticates and returns the byte-identical winner receipt");
		assert.deepEqual(earlyFenceWinner, {
			epoch: 2,
			tipSha256: sha256Bytes(bytes("early-fence-handoff-anchor")),
		});

		const malformedSuccessorPath = join(fixture, "malformed-successor-fence.json");
		await withConsumerStateLock(malformedSuccessorPath, async (_path, transaction) => {
			await transaction.commitState(bytes("malformed-successor-anchor"));
		}, manualRuntime({ value: 1 }));
		const malformedSuccessorReached = deferred();
		const releaseMalformedSuccessor = deferred();
		let malformedSuccessorWalks = 0;
		const malformedSuccessorStale = rotateConsumerStateJournal(malformedSuccessorPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				malformedSuccessorWalks += 1;
				if (malformedSuccessorWalks !== 1) return;
				malformedSuccessorReached.resolve();
				await releaseMalformedSuccessor.promise;
			},
		}));
		await malformedSuccessorReached.promise;
		const malformedSuccessorJournal = consumerJournal(malformedSuccessorPath);
		writePrivate(
			join(malformedSuccessorJournal.journal, `checkpoint-0000000000000002-${randomUUID()}.json`),
			"{}\n",
		);
		releaseMalformedSuccessor.resolve();
		await assert.rejects(malformedSuccessorStale, /journal checkpoint is malformed/);

		const skippedSuccessorPath = join(fixture, "skipped-successor-fence.json");
		await withConsumerStateLock(skippedSuccessorPath, async (_path, transaction) => {
			await transaction.commitState(bytes("skipped-successor-anchor"));
		}, manualRuntime({ value: 1 }));
		const skippedSuccessorReached = deferred();
		const releaseSkippedSuccessor = deferred();
		let skippedSuccessorWalks = 0;
		const skippedSuccessorStale = rotateConsumerStateJournal(skippedSuccessorPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				skippedSuccessorWalks += 1;
				if (skippedSuccessorWalks !== 1) return;
				skippedSuccessorReached.resolve();
				await releaseSkippedSuccessor.promise;
			},
		}));
		await skippedSuccessorReached.promise;
		let skippedCheckpoint;
		const captureSkippedCheckpoint = new Error("capture skipped successor checkpoint");
		await assert.rejects(
			() => rotateConsumerStateJournal(skippedSuccessorPath, manualRuntime({ value: 3 }, {
				beforeRotationDecision: async ({ claim }) => {
					skippedCheckpoint = { ...claim.intent.checkpoint, epoch: claim.intent.checkpoint.epoch + 1 };
					throw captureSkippedCheckpoint;
				},
			})),
			(error) => error === captureSkippedCheckpoint,
		);
		const skippedSuccessorJournal = consumerJournal(skippedSuccessorPath);
		writePrivate(
			join(
				skippedSuccessorJournal.journal,
				`checkpoint-${String(skippedCheckpoint.epoch).padStart(16, "0")}-${skippedCheckpoint.epochId}.json`,
			),
			metadata(skippedCheckpoint),
		);
		releaseSkippedSuccessor.resolve();
		await assert.rejects(skippedSuccessorStale, /journal checkpoints are not contiguous/);

		const competingSuccessorPath = join(fixture, "competing-successor-fence.json");
		await withConsumerStateLock(competingSuccessorPath, async (_path, transaction) => {
			await transaction.commitState(bytes("competing-successor-anchor"));
		}, manualRuntime({ value: 1 }));
		const competingSuccessorReached = deferred();
		const releaseCompetingSuccessor = deferred();
		let competingSuccessorWalks = 0;
		const competingSuccessorStale = rotateConsumerStateJournal(competingSuccessorPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				competingSuccessorWalks += 1;
				if (competingSuccessorWalks !== 1) return;
				competingSuccessorReached.resolve();
				await releaseCompetingSuccessor.promise;
			},
		}));
		await competingSuccessorReached.promise;
		let competingCheckpoint;
		const captureCompetingCheckpoint = new Error("capture competing successor checkpoint");
		await assert.rejects(
			() => rotateConsumerStateJournal(competingSuccessorPath, manualRuntime({ value: 3 }, {
				beforeRotationDecision: async ({ claim }) => {
					competingCheckpoint = claim.intent.checkpoint;
					throw captureCompetingCheckpoint;
				},
			})),
			(error) => error === captureCompetingCheckpoint,
		);
		const competingSuccessorJournal = consumerJournal(competingSuccessorPath);
		const alternateCompetingCheckpoint = { ...competingCheckpoint, epochId: randomUUID() };
		for (const checkpoint of [competingCheckpoint, alternateCompetingCheckpoint]) {
			writePrivate(
				join(
					competingSuccessorJournal.journal,
					`checkpoint-${String(checkpoint.epoch).padStart(16, "0")}-${checkpoint.epochId}.json`,
				),
				metadata(checkpoint),
			);
		}
		releaseCompetingSuccessor.resolve();
		await assert.rejects(
			competingSuccessorStale,
			/journal (?:checkpoint set is malformed|root contains unbounded checkpoint metadata)/,
		);

		const publicationEntries = (journal) => readdirSync(journal.epoch)
			.filter((name) => /^(?:claim(?:-index)?-|terminal-|transition-)/.test(name))
			.sort();
		const sameEpochCheckpointPath = join(fixture, "same-epoch-checkpoint-root-competitor.json");
		await withConsumerStateLock(sameEpochCheckpointPath, async (_path, transaction) => {
			await transaction.commitState(bytes("same-epoch-checkpoint-anchor"));
		}, manualRuntime({ value: 1 }));
		const sameEpochCheckpointJournal = consumerJournal(sameEpochCheckpointPath);
		const sameEpochCheckpointBaseline = publicationEntries(sameEpochCheckpointJournal);
		const maliciousCheckpointName = `checkpoint-0000000000000001-${randomUUID()}.json`;
		const sameEpochCheckpointReached = deferred();
		const releaseSameEpochCheckpoint = deferred();
		let sameEpochCheckpointInjected = false;
		const sameEpochCheckpointWriter = withConsumerStateLock(sameEpochCheckpointPath, async () => {}, {
			...manualRuntime({ value: 2 }, {
				beforePathOperation: async ({ operation }) => {
					if (operation !== "claim") return;
					sameEpochCheckpointReached.resolve();
					await releaseSameEpochCheckpoint.promise;
				},
			}),
			readDirectory: async (path) => {
				const names = await readDirectoryEntries(path);
				if (!sameEpochCheckpointInjected || path !== sameEpochCheckpointJournal.journal) return names;
				return [maliciousCheckpointName, ...names.filter((name) => name !== maliciousCheckpointName)];
			},
		});
		await sameEpochCheckpointReached.promise;
		writePrivate(join(sameEpochCheckpointJournal.journal, maliciousCheckpointName), "{}\n");
		sameEpochCheckpointInjected = true;
		releaseSameEpochCheckpoint.resolve();
		await assert.rejects(sameEpochCheckpointWriter, /journal checkpoint is malformed/);
		assert.deepEqual(
			publicationEntries(sameEpochCheckpointJournal),
			sameEpochCheckpointBaseline,
			"a malicious same-epoch checkpoint cannot publish a claim, terminal, or transition",
		);

		const sameEpochDirectoryPath = join(fixture, "same-epoch-directory-root-competitor.json");
		await withConsumerStateLock(sameEpochDirectoryPath, async (_path, transaction) => {
			await transaction.commitState(bytes("same-epoch-directory-anchor"));
		}, manualRuntime({ value: 1 }));
		const sameEpochDirectoryJournal = consumerJournal(sameEpochDirectoryPath);
		const sameEpochDirectoryBaseline = publicationEntries(sameEpochDirectoryJournal);
		const sameEpochDirectoryReached = deferred();
		const releaseSameEpochDirectory = deferred();
		const sameEpochDirectoryWriter = withConsumerStateLock(sameEpochDirectoryPath, async () => {}, manualRuntime(
			{ value: 2 },
			{
				beforePathOperation: async ({ operation }) => {
					if (operation !== "claim") return;
					sameEpochDirectoryReached.resolve();
					await releaseSameEpochDirectory.promise;
				},
			},
		));
		await sameEpochDirectoryReached.promise;
		mkdirSync(
			join(sameEpochDirectoryJournal.journal, `epoch-0000000000000001-${randomUUID()}`),
			{ mode: 0o700 },
		);
		releaseSameEpochDirectory.resolve();
		await assert.rejects(sameEpochDirectoryWriter, /competing epoch directories for one parent epoch/);
		assert.deepEqual(
			publicationEntries(sameEpochDirectoryJournal),
			sameEpochDirectoryBaseline,
			"a sibling same-epoch directory cannot publish a claim, terminal, or transition",
		);

		const unindexedOrderPath = join(fixture, "unindexed-claim-validation-order.json");
		await withConsumerStateLock(unindexedOrderPath, async () => {}, manualRuntime({ value: 1 }));
		const unindexedOrderJournal = consumerJournal(unindexedOrderPath);
		const indexedClaimName = readdirSync(unindexedOrderJournal.epoch)
			.find((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name));
		const indexedClaimBytes = readFileSync(join(unindexedOrderJournal.epoch, indexedClaimName));
		const unorderedClaims = [
			`claim-0000000000000003-${"0".repeat(64)}.json`,
			`claim-0000000000000002-${"f".repeat(64)}.json`,
			`claim-0000000000000002-${"0".repeat(64)}.json`,
		];
		for (const name of unorderedClaims) writePrivate(join(unindexedOrderJournal.epoch, name), indexedClaimBytes);
		const unindexedReads = [];
		await assert.rejects(
			() => rotateConsumerStateJournal(unindexedOrderPath, {
				...manualRuntime({ value: 2 }, {
					metadataRead: {
						afterInitialStat: ({ path }) => {
							if (unorderedClaims.includes(basename(path))) unindexedReads.push(basename(path));
						},
					},
				}),
				readDirectory: async (path) => {
					const names = await readDirectoryEntries(path);
					if (path !== unindexedOrderJournal.epoch) return names;
					return [...unorderedClaims, ...names.filter((name) => !unorderedClaims.includes(name))];
				},
			}),
			/unindexed claim content differs from its exact canonical bytes/,
		);
		assert.deepEqual(
			unindexedReads,
			[`claim-0000000000000002-${"0".repeat(64)}.json`],
			"unindexed claims validate by numeric generation and then lexical content digest",
		);

		const missingSuccessorEpochPath = join(fixture, "missing-successor-epoch-fence.json");
		await withConsumerStateLock(missingSuccessorEpochPath, async (_path, transaction) => {
			await transaction.commitState(bytes("missing-successor-epoch-anchor"));
		}, manualRuntime({ value: 1 }));
		const missingSuccessorEpochReached = deferred();
		const releaseMissingSuccessorEpoch = deferred();
		let missingSuccessorEpochWalks = 0;
		const missingSuccessorEpochStale = rotateConsumerStateJournal(missingSuccessorEpochPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "walk-transactions") return;
				missingSuccessorEpochWalks += 1;
				if (missingSuccessorEpochWalks !== 1) return;
				missingSuccessorEpochReached.resolve();
				await releaseMissingSuccessorEpoch.promise;
			},
		}));
		await missingSuccessorEpochReached.promise;
		let missingEpochCheckpoint;
		const captureMissingEpoch = new Error("capture successor without publishing it");
		await assert.rejects(
			() => rotateConsumerStateJournal(missingSuccessorEpochPath, manualRuntime({ value: 3 }, {
				beforeRotationDecision: async ({ claim }) => {
					missingEpochCheckpoint = claim.intent.checkpoint;
					throw captureMissingEpoch;
				},
			})),
			(error) => error === captureMissingEpoch,
		);
		const missingSuccessorJournal = consumerJournal(missingSuccessorEpochPath);
		writePrivate(
			join(
				missingSuccessorJournal.journal,
				`checkpoint-${String(missingEpochCheckpoint.epoch).padStart(16, "0")}-${missingEpochCheckpoint.epochId}.json`,
			),
			metadata(missingEpochCheckpoint),
		);
		releaseMissingSuccessorEpoch.resolve();
		await assert.rejects(
			missingSuccessorEpochStale,
			/journal root changed without one exact current or immediate-successor authority/,
		);

		const successorIoPath = join(fixture, "successor-scan-io-fence.json");
		await withConsumerStateLock(successorIoPath, async (_path, transaction) => {
			await transaction.commitState(bytes("successor-io-anchor"));
		}, manualRuntime({ value: 1 }));
		const successorIoJournal = consumerJournal(successorIoPath);
		const successorIoReached = deferred();
		const releaseSuccessorIo = deferred();
		const successorIoFailure = new Error("injected successor scan I/O failure");
		let successorIoWalks = 0;
		let injectSuccessorIo = false;
		let successorIoRootReads = 0;
		const successorIoStale = rotateConsumerStateJournal(successorIoPath, {
			...manualRuntime({ value: 2 }, {
				beforePathOperation: async ({ operation }) => {
					if (operation !== "walk-transactions") return;
					successorIoWalks += 1;
					if (successorIoWalks !== 1) return;
					successorIoReached.resolve();
					await releaseSuccessorIo.promise;
				},
			}),
			readDirectory: async (path) => {
				if (injectSuccessorIo && path === successorIoJournal.journal) {
					successorIoRootReads += 1;
					if (successorIoRootReads === 2) throw successorIoFailure;
				}
				return readDirectoryEntries(path);
			},
		});
		await successorIoReached.promise;
		await rotateConsumerStateJournal(successorIoPath, manualRuntime({ value: 3 }));
		injectSuccessorIo = true;
		releaseSuccessorIo.resolve();
		await assert.rejects(successorIoStale, (error) => error === successorIoFailure);

		const falseFencePath = join(fixture, "false-typed-fence.json");
		await withConsumerStateLock(falseFencePath, async (_path, transaction) => {
			await transaction.commitState(bytes("false-fence-anchor"));
		}, manualRuntime({ value: 1 }));
		const falseFenceReached = deferred();
		const releaseFalseFence = deferred();
		const falseFence = new Error("Consumer high-water journal epoch changed and fenced a paused writer.");
		let falseFenceScans = 0;
		const falseFenceStale = rotateConsumerStateJournal(falseFencePath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation }) => {
				if (operation !== "scan-claims") return;
				falseFenceScans += 1;
				if (falseFenceScans !== 2) return;
				falseFenceReached.resolve();
				await releaseFalseFence.promise;
				throw falseFence;
			},
		}));
		await falseFenceReached.promise;
		await rotateConsumerStateJournal(falseFencePath, manualRuntime({ value: 3 }));
		releaseFalseFence.resolve();
		await assert.rejects(falseFenceStale, (error) => {
			assert.equal(error, falseFence);
			assert.equal(error.name, "Error");
			return true;
		});

		const escapedRemovalPath = join(fixture, "escaped-removal-signal.json");
		await withConsumerStateLock(escapedRemovalPath, async (_path, transaction) => {
			await transaction.commitState(bytes("escaped-removal-anchor"));
		}, manualRuntime({ value: 1 }));
		const escapedRemovalJournal = consumerJournal(escapedRemovalPath);
		const escapedClaimName = readdirSync(escapedRemovalJournal.epoch)
			.find((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name));
		const escapedClaimBytes = readFileSync(join(escapedRemovalJournal.epoch, escapedClaimName));
		const escapedClaimSha256 = /-([0-9a-f]{64})\.json$/.exec(escapedClaimName)[1];
		const escapedRemovalReached = deferred();
		const releaseEscapedRemoval = deferred();
		let escapedRemovalScans = 0;
		let escapedRemoval;
		const escapedRemovalStale = rotateConsumerStateJournal(escapedRemovalPath, manualRuntime({ value: 2 }, {
			beforePathOperation: async ({ operation, transactionDirectory }) => {
				if (operation !== "scan-claims") return;
				escapedRemovalScans += 1;
				if (escapedRemovalScans !== 2) return;
				const linkedStat = lstatSync(join(escapedRemovalJournal.epoch, escapedClaimName));
				const unlinkedStat = { ...linkedStat, ctimeMs: linkedStat.ctimeMs + 1, nlink: 0 };
				escapedRemoval = new BoundedFileUnlinkedDuringReadError(
					join(transactionDirectory, "..", escapedClaimName),
					"Consumer high-water operation claim",
					escapedClaimBytes,
					escapedClaimSha256,
					linkedStat,
					linkedStat,
					unlinkedStat,
				);
				escapedRemovalReached.resolve();
				await releaseEscapedRemoval.promise;
				throw escapedRemoval;
			},
		}));
		await escapedRemovalReached.promise;
		await rotateConsumerStateJournal(escapedRemovalPath, manualRuntime({ value: 3 }));
		releaseEscapedRemoval.resolve();
		await assert.rejects(escapedRemovalStale, (error) => {
			assert.equal(error, escapedRemoval);
			return true;
		});

		const changedClaimPath = join(fixture, "changed-claim-is-terminal.json");
		await withConsumerStateLock(changedClaimPath, async (_path, transaction) => {
			await transaction.commitState(bytes("changed-claim-anchor"));
		}, manualRuntime({ value: 1 }));
		const changedClaimReadReached = deferred();
		const releaseChangedClaimRead = deferred();
		let changedClaimReads = 0;
		let openedChangedClaimPath;
		const changedClaimStale = rotateConsumerStateJournal(changedClaimPath, manualRuntime({ value: 2 }, {
			metadataRead: {
				afterInitialStat: async ({ path }) => {
					if (!/^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(basename(path))) return;
					changedClaimReads += 1;
					if (changedClaimReads !== 2) return;
					openedChangedClaimPath = path;
					changedClaimReadReached.resolve();
					await releaseChangedClaimRead.promise;
				},
			},
		}));
		await changedClaimReadReached.promise;
		await rotateConsumerStateJournal(changedClaimPath, manualRuntime({ value: 3 }));
		writePrivate(openedChangedClaimPath, '{"malformed":true}\n');
		releaseChangedClaimRead.resolve();
		await assert.rejects(changedClaimStale, (error) => {
			assert.equal(error instanceof BoundedFileUnlinkedDuringReadError, false);
			assert.match(error.message, /changed while it was read/);
			return true;
		});

		const malformedClaimPath = join(fixture, "malformed-claim-is-terminal.json");
		await withConsumerStateLock(malformedClaimPath, async () => {}, manualRuntime({ value: 1 }));
		const malformedClaimJournal = consumerJournal(malformedClaimPath);
		const malformedClaimName = readdirSync(malformedClaimJournal.epoch)
			.find((name) => /^claim-[0-9]{16}-[0-9a-f]{64}\.json$/.test(name));
		writePrivate(join(malformedClaimJournal.epoch, malformedClaimName), "{}\n");
		await assert.rejects(
			() => rotateConsumerStateJournal(malformedClaimPath, manualRuntime({ value: 2 })),
			/operation claim is malformed/,
		);

		const concurrentRotationPath = join(fixture, "concurrent-rotation.json");
		await withConsumerStateLock(concurrentRotationPath, async (_path, transaction) => {
			await transaction.commitState(bytes("concurrent-anchor"));
		}, manualRuntime({ value: 1 }));
		const bothRotationsReady = deferred();
		const releaseRotations = deferred();
		let rotationEpochWriters = 0;
		const concurrentRotationHooks = {
			afterRotationEpochSync: async () => {
				rotationEpochWriters += 1;
				if (rotationEpochWriters === 2) bothRotationsReady.resolve();
				await releaseRotations.promise;
			},
		};
		const firstRotation = rotateConsumerStateJournal(concurrentRotationPath, {
			...manualRuntime({ value: 2 }, concurrentRotationHooks),
			maxLockGenerations: 2,
			maxTransactionDepth: 1,
		});
		const secondRotation = rotateConsumerStateJournal(concurrentRotationPath, {
			...manualRuntime({ value: 2 }, concurrentRotationHooks),
			maxLockGenerations: 19,
			maxTransactionDepth: 17,
		});
		await bothRotationsReady.promise;
		releaseRotations.resolve();
		const concurrentRotations = await Promise.all([firstRotation, secondRotation]);
		assert.deepEqual(concurrentRotations, [concurrentRotations[0], concurrentRotations[0]]);
		assert.equal(concurrentRotations[0].epoch, 2);
		await withConsumerStateLock(concurrentRotationPath, async (_path, transaction) => {
			assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "concurrent-anchor" });
			await transaction.commitState(bytes("concurrent-after-rotation"));
		}, { ...manualRuntime({ value: 3 }), maxLockGenerations: 2 });

		const rotationWavePath = join(fixture, "concurrent-rotation-process-wave.json");
		await withConsumerStateLock(rotationWavePath, async (_path, transaction) => {
			await transaction.commitState(bytes("wave-anchor"));
		}, manualRuntime({ value: 1 }));
		const rotationWave = await Promise.all(Array.from({ length: 12 }, () => runRotationChild(rotationWavePath)));
		assert.equal(rotationWave.every((result) => result.epoch === 2), true);
		assert.deepEqual(rotationWave, Array.from({ length: 12 }, () => rotationWave[0]));
		assert.equal(readdirSync(`${rotationWavePath}.journal`).filter((name) => name.startsWith("checkpoint-")).length, 1);
		assert.equal(readdirSync(`${rotationWavePath}.journal`).filter((name) => name.startsWith("epoch-")).length, 1);

		for (const competitorHook of ["afterRotationEpochSync", "afterMetadataLink"]) {
			const competingRotationPath = join(fixture, `competing-rotation-${competitorHook}.json`);
			await withConsumerStateLock(competingRotationPath, async (_path, transaction) => {
				await transaction.commitState(bytes("competing-anchor"));
			}, manualRuntime({ value: 1 }));
			let checkpoint;
			let injected = false;
			const injectCompetitor = () => {
				if (injected) return;
				injected = true;
				const competingEpochId = randomUUID();
				const competing = { ...checkpoint, epochId: competingEpochId };
				const root = `${competingRotationPath}.journal`;
				mkdirSync(join(root, `epoch-${String(competing.epoch).padStart(16, "0")}-${competingEpochId}`), { mode: 0o700 });
				writePrivate(
					join(root, `checkpoint-${String(competing.epoch).padStart(16, "0")}-${competingEpochId}.json`),
					metadata(competing),
				);
			};
			await assert.rejects(
				() => rotateConsumerStateJournal(competingRotationPath, manualRuntime({ value: 2 }, {
					beforeRotationDecision: async ({ claim }) => { checkpoint = claim.intent.checkpoint; },
					afterRotationEpochSync: async () => {
						if (competitorHook === "afterRotationEpochSync") injectCompetitor();
					},
					afterMetadataLink: async ({ kind }) => {
						if (competitorHook === "afterMetadataLink" && kind === "checkpoint") injectCompetitor();
					},
				})),
				/competing|checkpoint set|unbounded checkpoint metadata|fenced a paused writer/,
			);
			assert.equal(injected, true);
		}

		for (const [hookName, wantedKind] of [
			["afterMetadataLink", "checkpoint"],
			["afterMetadataDirectorySync", "checkpoint"],
			["afterRotationCheckpoint", null],
		]) {
			const responseLossPath = join(fixture, `rotation-response-loss-${hookName}.json`);
			await withConsumerStateLock(responseLossPath, async (_path, transaction) => {
				await transaction.commitState(bytes("response-loss-anchor"));
			}, manualRuntime({ value: 1 }));
			let armed = true;
			const firstResult = await rotateConsumerStateJournal(responseLossPath, {
				...manualRuntime({ value: 2 }, {
					[hookName]: async (event = {}) => {
						if (!armed || (wantedKind !== null && event.kind !== wantedKind)) return;
						armed = false;
						throw new Error(`simulated response loss at ${hookName}`);
					},
				}),
				maxLockGenerations: 2,
			});
			const retriedResult = await rotateConsumerStateJournal(responseLossPath, {
				...manualRuntime({ value: 3 }),
				maxLockGenerations: 23,
			});
			assert.deepEqual(retriedResult, firstResult);
			assert.equal(retriedResult.epoch, 2);
			await withConsumerStateLock(responseLossPath, async (_path, transaction) => {
				assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "response-loss-anchor" });
			}, manualRuntime({ value: 4 }));
		}

		for (const [hookName, wantedKind] of [
			["beforeRotationDecision", null],
			["afterFileSync", "claim"],
			["afterMetadataLink", "claim"],
			["afterMetadataDirectorySync", "claim"],
			["afterFileSync", "claim-index"],
			["afterMetadataLink", "claim-index"],
			["afterMetadataDirectorySync", "claim-index"],
			["afterRotationIntent", null],
		]) {
			const intentCrashPath = join(fixture, `rotation-operation-crash-${hookName}-${wantedKind ?? "rotation-decision"}.json`);
			await withConsumerStateLock(intentCrashPath, async (_path, transaction) => {
				await transaction.commitState(bytes("intent-anchor"));
			}, manualRuntime({ value: 1 }));
			let armed = true;
			await assert.rejects(
				() => rotateConsumerStateJournal(intentCrashPath, manualRuntime({ value: 2 }, {
					[hookName]: async (event = {}) => {
						if (!armed || (wantedKind !== null && event.kind !== wantedKind)) return;
						armed = false;
						throw new Error(`simulated rotation operation crash at ${hookName}`);
					},
				})),
				/simulated rotation operation crash/,
			);
			assert.equal(
				(await rotateConsumerStateJournal(intentCrashPath, manualRuntime({ value: 3 }))).epoch,
				2,
				`${hookName}/${wantedKind ?? "rotation-decision"} must recover the same rotation epoch`,
			);
		}

		for (const [hookName, wantedKind] of [
			["afterRotationEpochSync", null],
			["afterFileSync", "checkpoint"],
			["afterMetadataLink", "checkpoint"],
			["afterMetadataDirectorySync", "checkpoint"],
			["afterRotationCheckpoint", null],
		]) {
			const rotationCrashPath = join(fixture, `rotation-crash-${hookName}.json`);
			const rotationCrashClock = { value: 1 };
			await withConsumerStateLock(rotationCrashPath, async (_path, transaction) => {
				await transaction.commitState(bytes("anchored"));
			}, manualRuntime(rotationCrashClock));
			const reached = deferred();
			const resume = deferred();
			let armed = true;
			const interrupted = rotateConsumerStateJournal(rotationCrashPath, manualRuntime(rotationCrashClock, {
				[hookName]: async (event = {}) => {
					if (!armed || (wantedKind !== null && event.kind !== wantedKind)) return;
					armed = false;
					reached.resolve();
					await resume.promise;
				},
			}));
			await reached.promise;
			rotationCrashClock.value = 100;
			await withConsumerStateLock(rotationCrashPath, async () => {}, manualRuntime(rotationCrashClock));
			resume.resolve();
			await interrupted;
			await withConsumerStateLock(rotationCrashPath, async (_path, transaction) => {
				assert.deepEqual(JSON.parse(transaction.readStateBytes()), { value: "anchored" });
			}, manualRuntime(rotationCrashClock));
			const rootEntries = readdirSync(`${rotationCrashPath}.journal`);
			assert.equal(rootEntries.filter((name) => name.startsWith("checkpoint-")).length, 1);
			assert.equal(rootEntries.filter((name) => name.startsWith("epoch-")).length, 1);
			assert.deepEqual(rootEntries.filter((name) => name.startsWith(".")), [".owned-temporaries-v2"]);
		}

		const swapRoot = join(fixture, "swap-root");
		const swapDirectory = join(swapRoot, "state");
		const movedDirectory = join(swapRoot, "state-moved");
		mkdirSync(swapDirectory, { recursive: true, mode: 0o700 });
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
		await terminateCapturedChildren();
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("default-token source admission avoids Administration reads and every protected mutation has a fresh App audit", async () => {
	for (const [workflow, step] of [
		[".github/workflows/pylon-preview-release.yml", "Require the canonical protected push"],
		[".github/workflows/pylon-stable-release.yml", "Require protected pylon and an exact verified preview source"],
	]) {
		const script = githubScriptForStep(workflow, step);
		for (const value of ["pylon-code", "prime-agent", "refs/heads/pylon", "context.sha", "heads/pylon"]) {
			assert.ok(script.includes(value), `${workflow}:${step} lacks canonical source binding ${value}`);
		}
		assert.doesNotMatch(
			script,
			/branchProtectionRule|requiredStatusChecks|getBranchProtection|\/branches\/[^\s"'`]+\/protection|github\.graphql|rulesets\/\{ruleset_id\}/,
			"default GITHUB_TOKEN source admission must not request Administration or branch-protection data",
		);
	}

	for (const [workflow, step] of [
		[".github/workflows/pylon-preview-release.yml", "Verify exact checks and freeze the approved preview draft"],
		[".github/workflows/pylon-stable-release.yml", "Require protected pylon and an exact verified preview source"],
		[".github/workflows/pylon-stable-release.yml", "Re-download and validate the exact stable transaction"],
	]) {
		const script = githubScriptForStep(workflow, step);
		for (const value of [
			"Check changelog fragment", "build-check-test", "15368",
			".github/workflows/changelog-merged-proof.yml", ".github/workflows/ci.yml",
			"run.check_suite_id === suite.id", 'run.event === "push"',
		]) assert.ok(script.includes(value), `${workflow}:${step} lacks direct exact-check binding ${value}`);
		assert.match(script, /check\.head_sha === (?:sha|sourceSha)/);
		assert.match(script, /run\.head_sha === (?:sha|sourceSha)/);
		assert.doesNotMatch(script, /branchProtectionRule|requiredStatusChecks|getBranchProtection|github\.graphql/,
			"default-token exact-check proof must not depend on Administration data");
	}

	const authoritativeSteps = [
		[".github/workflows/pylon-preview-release.yml", "Require authoritative publication tag ruleset before preview tag CAS"],
		[".github/workflows/pylon-preview-release.yml", "Require authoritative publication tag ruleset before immutable preview publish"],
		[".github/workflows/pylon-stable-release.yml", "Require authoritative publication tag ruleset before reservation CAS"],
		[".github/workflows/pylon-stable-release.yml", "Require authoritative publication tag ruleset before final stable tag CAS"],
		[".github/workflows/pylon-stable-release.yml", "Require authoritative publication tag ruleset before immutable stable publish"],
	];
	const frozenValidators = new Set(authoritativeSteps.map(([workflow, step]) => githubScriptForStep(workflow, step)));
	assert.equal(frozenValidators.size, 1, "every protected mutation must use the same frozen combined validator bytes");
	for (const script of frozenValidators) {
		assert.match(script, /ruleset_id: 21950766, includes_parents: false/);
		assert.match(script, /restRuleset\.node_id !== "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4"/);
		assert.match(script, /Object\.hasOwn\(restRuleset, "bypass_actors"\)/);
		assert.match(script, /Object\.hasOwn\(restRuleset, "current_user_can_bypass"\)/);
		assert.match(script, /ruleset\(databaseId: \$rulesetDatabaseId, includeParents: false\)/);
		assert.ok(script.indexOf("await github.request") < script.indexOf("await github.graphql"));
		assert.doesNotMatch(script.slice(script.indexOf("await github.graphql") + 1), /await github\./,
			"GraphQL must remain the last authoritative GitHub read");
	}

	const validRest = exactPublicationTagRuleset();
	const validGraphql = exactPublicationTagRulesetGraphql();
	await (await inlinePublicationTagRulesetValidator([validRest], [validGraphql])).validate();
	const restWithoutVisibilityFields = structuredClone(validRest);
	delete restWithoutVisibilityFields.bypass_actors;
	delete restWithoutVisibilityFields.current_user_can_bypass;
	await (await inlinePublicationTagRulesetValidator([restWithoutVisibilityFields], [validGraphql])).validate();

	const restMutations = [
		(value) => delete value.id,
		(value) => (value.id = 1),
		(value) => delete value.node_id,
		(value) => (value.node_id = "RRS_wrong"),
		(value) => delete value.name,
		(value) => (value.name = "Other ruleset"),
		(value) => delete value.source_type,
		(value) => (value.source_type = "Organization"),
		(value) => delete value.source,
		(value) => (value.source = "fork/prime-agent"),
		(value) => delete value.target,
		(value) => (value.target = "branch"),
		(value) => delete value.enforcement,
		(value) => (value.enforcement = "disabled"),
		(value) => (value.bypass_actors = [{ actor_type: "RepositoryRole", actor_id: 5 }]),
		(value) => (value.bypass_actors = undefined),
		(value) => (value.current_user_can_bypass = "always"),
		(value) => (value.current_user_can_bypass = undefined),
		(value) => delete value.conditions,
		(value) => (value.conditions.extra = {}),
		(value) => delete value.conditions.ref_name,
		(value) => delete value.conditions.ref_name.exclude,
		(value) => value.conditions.ref_name.exclude.push("refs/tags/pylon-stable-sequence-*"),
		(value) => delete value.conditions.ref_name.include,
		(value) => (value.conditions.ref_name.include[1] = "refs/tags/pylon-stable-[0-9]*"),
		(value) => value.conditions.ref_name.include.pop(),
		(value) => delete value.rules,
		(value) => value.rules.pop(),
		(value) => delete value.rules[0].type,
		(value) => delete value.rules[0].parameters,
		(value) => delete value.rules[0].parameters.update_allows_fetch_and_merge,
		(value) => (value.rules[0].parameters.update_allows_fetch_and_merge = true),
		(value) => (value.rules[0].parameters.extra = false),
		(value) => delete value.rules[1].type,
		(value) => (value.rules[1].extra = false),
		(value) => value.rules.push({ type: "creation" }),
	];
	for (const mutate of restMutations) {
		const changed = structuredClone(validRest);
		mutate(changed);
		const rejected = await inlinePublicationTagRulesetValidator([changed], [validGraphql]);
		await assert.rejects(() => rejected.validate(), /REST ruleset-auditor response/);
	}
	for (const unavailable of [
		new Error("ruleset auth or endpoint unavailable"),
		{ status: 401, data: validRest },
		{ status: 403, data: { message: "Resource not accessible by integration" } },
	]) {
		const rejected = await inlinePublicationTagRulesetValidator([unavailable], [validGraphql]);
		await assert.rejects(() => rejected.validate(), /unavailable|REST ruleset-auditor response/);
	}

	const graphqlMutations = [
		() => null,
		() => ({ repository: null }),
		(value) => (value.errors = [{ message: "redacted" }]),
		(value) => delete value.repository.id,
		(value) => (value.repository.id = "R_wrong"),
		(value) => delete value.repository.databaseId,
		(value) => (value.repository.databaseId = 1),
		(value) => delete value.repository.nameWithOwner,
		(value) => (value.repository.nameWithOwner = "fork/prime-agent"),
		(value) => (value.repository.ruleset = null),
		(value) => delete value.repository.ruleset.id,
		(value) => (value.repository.ruleset.id = "RRS_wrong"),
		(value) => delete value.repository.ruleset.databaseId,
		(value) => (value.repository.ruleset.databaseId = 1),
		(value) => (value.repository.ruleset.name = "Other ruleset"),
		(value) => (value.repository.ruleset.enforcement = "DISABLED"),
		(value) => (value.repository.ruleset.target = "BRANCH"),
		(value) => (value.repository.ruleset.source = null),
		(value) => (value.repository.ruleset.source.id = "R_wrong"),
		(value) => (value.repository.ruleset.source.databaseId = 1),
		(value) => (value.repository.ruleset.source.nameWithOwner = "fork/prime-agent"),
		(value) => (value.repository.ruleset.source.__typename = "Organization"),
		(value) => (value.repository.ruleset.bypassActors = null),
		(value) => (value.repository.ruleset.bypassActors.totalCount = null),
		(value) => (value.repository.ruleset.bypassActors.totalCount = "0"),
		(value) => (value.repository.ruleset.bypassActors.totalCount = 1),
		(value) => (value.repository.ruleset.bypassActors.extra = 0),
		(value) => (value.repository.ruleset.conditions = null),
		(value) => (value.repository.ruleset.conditions.organizationProperty = { __typename: "OrganizationPropertyConditionTarget" }),
		(value) => (value.repository.ruleset.conditions.repositoryId = { __typename: "RepositoryIdConditionTarget" }),
		(value) => (value.repository.ruleset.conditions.extra = null),
		(value) => (value.repository.ruleset.conditions.refName = null),
		(value) => value.repository.ruleset.conditions.refName.include.pop(),
		(value) => value.repository.ruleset.conditions.refName.exclude.push("refs/tags/unsafe-*"),
		(value) => (value.repository.ruleset.rules = null),
		(value) => (value.repository.ruleset.rules.totalCount = null),
		(value) => (value.repository.ruleset.rules.totalCount = 3),
		(value) => (value.repository.ruleset.rules.nodes = null),
		(value) => (value.repository.ruleset.rules.nodes[0] = null),
		(value) => value.repository.ruleset.rules.nodes.push({ type: "CREATION", parameters: null }),
		(value) => (value.repository.ruleset.rules.nodes[0].type = "DELETION"),
		(value) => (value.repository.ruleset.rules.nodes[0].parameters = null),
		(value) => (value.repository.ruleset.rules.nodes[0].parameters.__typename = "OtherParameters"),
		(value) => (value.repository.ruleset.rules.nodes[0].parameters.updateAllowsFetchAndMerge = true),
		(value) => (value.repository.ruleset.rules.nodes[0].parameters.extra = false),
		(value) => (value.repository.ruleset.rules.nodes[1].parameters = {}),
		(value) => (value.repository.ruleset.rules.nodes[1].extra = null),
	];
	for (const mutate of graphqlMutations) {
		let changed = structuredClone(validGraphql);
		const replacement = mutate(changed);
		if (replacement !== undefined) changed = replacement;
		const rejected = await inlinePublicationTagRulesetValidator([validRest], [changed]);
		await assert.rejects(() => rejected.validate(), /GraphQL ruleset-auditor response|Cannot read/);
	}
	for (const unavailable of [new Error("GraphQL errors: forbidden"), new Error("GraphQL response was redacted")]) {
		const rejected = await inlinePublicationTagRulesetValidator([validRest], [unavailable]);
		await assert.rejects(() => rejected.validate(), /GraphQL/);
	}

	const staleRest = structuredClone(validRest);
	staleRest.enforcement = "disabled";
	const restPointInTime = await inlinePublicationTagRulesetValidator([validRest, staleRest], [validGraphql]);
	await restPointInTime.validate();
	await assert.rejects(() => restPointInTime.validate(), /REST ruleset-auditor response/);
	assert.deepEqual(restPointInTime.requests(), { rest: 2, graphql: 1, order: ["REST", "GraphQL", "REST"] });
	const staleGraphql = structuredClone(validGraphql);
	staleGraphql.repository.ruleset.bypassActors.totalCount = 1;
	const graphqlPointInTime = await inlinePublicationTagRulesetValidator([validRest], [validGraphql, staleGraphql]);
	await graphqlPointInTime.validate();
	await assert.rejects(() => graphqlPointInTime.validate(), /GraphQL ruleset-auditor response/);
	assert.deepEqual(graphqlPointInTime.requests(), {
		rest: 2, graphql: 2, order: ["REST", "GraphQL", "REST", "GraphQL"],
	});

	const executeProtectedMutation = async ({ appId, privateKey, mint, audit, mutate }) => {
		if (!appId || !privateKey) throw new Error("GitHub App credentials are required");
		const token = await mint({ appId, privateKey });
		if (!token) throw new Error("GitHub App token creation returned no token");
		await audit(token);
		await mutate();
	};
	let protectedMutationCalls = 0;
	const base = {
		appId: "1234",
		privateKey: "test-private-key",
		audit: async () => {},
		mutate: async () => { protectedMutationCalls += 1; },
	};
	await assert.rejects(() => executeProtectedMutation({ ...base, privateKey: "", mint: async () => "token" }), /credentials/);
	await assert.rejects(() => executeProtectedMutation({
		...base,
		mint: async () => { throw new Error("simulated create-github-app-token failure"); },
	}), /simulated create-github-app-token failure/);
	await assert.rejects(() => executeProtectedMutation({ ...base, mint: async () => "" }), /returned no token/);
	assert.equal(protectedMutationCalls, 0);
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

test("stable p1 zero-asset recovery carries the historical policy revision to attestation", async () => {
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
				assert.equal(JSON.parse(readFileSync(path, "utf8")).promotion.publicationPolicyRevision, 1);
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
		manifest.promotion.publicationPolicyRevision = 3;
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
	const reservationCas = publish.indexOf("- name: Create the exact protected stable reservation ref");
	const stableTagCas = publish.indexOf("- name: Create or refetch the exact protected stable tag");
	const immutablePublish = publish.indexOf("- name: Publish only the exact protected stable draft");
	assert.ok(publish.indexOf('POST /repos/{owner}/{repo}/releases/{release_id}/assets') < reservationCas);
	assert.ok(publish.indexOf("downloadedBytes.equals(bytes)") < reservationCas);
	assert.ok(reservationCas >= 0 && reservationCas < stableTagCas);
	assert.ok(stableTagCas < immutablePublish);
	assert.ok(immutablePublish < publish.indexOf("repos.updateRelease"));
});

test("release sandbox exports a closed Docker argv policy and workflows have exact parsed schemas", async () => {
	assert.equal(new Set(PYLON_SANDBOX_PUBLIC_FAIL_STAGES).size, PYLON_SANDBOX_PUBLIC_FAIL_STAGES.length);
	assert.ok(PYLON_SANDBOX_PUBLIC_FAIL_STAGES.every((stage) => /^E_[A-Z_]+$/.test(stage)));
	for (const stage of PYLON_SANDBOX_PUBLIC_FAIL_STAGES) {
		assert.equal(formatPublicFailure(stage), `::error::Pylon release sandbox failed closed [${stage}].`);
	}
	assert.equal(formatPublicFailure("/private/secret"), "::error::Pylon release sandbox failed closed [E_INTERNAL].");
	const rejectedArguments = spawnSync(process.execPath, [join(root, "scripts/run-pylon-release-sandbox.mjs"), "--unsafe"], {
		encoding: "utf8",
		env: {},
	});
	assert.equal(rejectedArguments.status, 1);
	assert.equal(rejectedArguments.stdout, "");
	assert.equal(rejectedArguments.stderr.trim(), "::error::Pylon release sandbox failed closed [E_ARGUMENTS].");

	const fixture = (phase, mode = "preview") => ({
		cidfile: `/control/${phase}.cid`,
		gid: "1001",
		mode,
		name: `pylon-${phase}-fixture`,
		paths: phase === "contracts" ? { candidate: "/scratch/contract-candidate", dependencies: "/scratch/build/node_modules", source: "/scratch/source" } :
			phase === "prepare" ? { candidate: "/scratch/real-candidate", source: "/scratch/source" } :
				phase === "final" ? { candidate: "/scratch/real-candidate", control: "/control/verification", source: "/scratch/source" } :
					["smoke-cache", "smoke"].includes(phase) ? { artifacts: "/scratch/artifacts", cache: "/scratch/cache", npm: "/scratch/npm.tgz", source: "/scratch/source" } : {
						build: "/scratch/build",
						...(phase === "pack" ? { dependencies: "/scratch/build/node_modules" } : {}),
						git: "/scratch/source/.git",
						npm: "/scratch/npm.tgz",
						source: "/scratch/source",
					},
		phase,
		publicationPolicyRevision: 3,
		runId: "123",
		runNumber: "456",
		uid: "1001",
	});
	const valueAfter = (args, flag) => {
		const indexes = args.flatMap((value, index) => value === flag ? [index] : []);
		assert.equal(indexes.length, 1, `${flag} must be singleton`);
		return args[indexes[0] + 1];
	};
	const expectedDestinations = {
		dependencies: ["/build", "/build/.git", "/input/npm.tgz", "/source"],
		pack: ["/build", "/build/.git", "/build/node_modules", "/input/npm.tgz", "/source"],
		contracts: ["/candidate", "/source", "/source/node_modules"],
		prepare: ["/candidate", "/source"],
		final: ["/candidate", "/control", "/source"],
		"smoke-cache": ["/artifacts", "/cache", "/input/npm.tgz", "/source"],
		smoke: ["/artifacts", "/cache", "/input/npm.tgz", "/source"],
	};
	const writableDestination = { dependencies: "/build", pack: "/build", contracts: "/candidate", prepare: "/candidate", final: "/control", "smoke-cache": "/cache", smoke: "/cache" };
	const validateArgs = (args, spec) => {
		assert.equal(assertExactDockerRunArgs(args, spec), true);
		assert.equal(args[0], "run");
		assert.equal(valueAfter(args, "--user"), "1001:1001");
		assert.equal(valueAfter(args, "--pids-limit"), "512");
		assert.equal(valueAfter(args, "--cpus"), "2");
		assert.equal(valueAfter(args, "--memory"), "3g");
		assert.equal(valueAfter(args, "--memory-swap"), "3g");
		assert.equal(valueAfter(args, "--cap-drop"), "ALL");
		assert.equal(valueAfter(args, "--security-opt"), "no-new-privileges");
		assert.equal(valueAfter(args, "--platform"), "linux/amd64");
		assert.equal(valueAfter(args, "--pull"), "never");
		assert.equal(valueAfter(args, "--entrypoint"), "/usr/bin/env");
		assert.equal(args.filter((value) => value === "--init").length, 1);
		assert.equal(args.filter((value) => value === "--read-only").length, 1);
		assert.equal(args.filter((value) => value === "--no-healthcheck").length, 1);
		assert.equal(valueAfter(args, "--network"), ["dependencies", "smoke-cache"].includes(spec.phase) ? "bridge" : "none");
		assert.equal(args.filter((value) => value === "--tmpfs").length, spec.phase === "smoke" ? 3 : 2);
		const imageIndex = args.indexOf(PYLON_SANDBOX_POLICY.image);
		assert.ok(imageIndex > 0);
		assert.equal(args[imageIndex + 1], "-i");
		assert.ok(args.slice(imageIndex + 1).includes("/usr/local/bin/node"));
		assert.doesNotMatch(args.join("\n"), /(?:docker\.sock|\/original|\/runner|\/home\/runner|GITHUB_ENV|GITHUB_PATH|GITHUB_TOKEN|GH_TOKEN|BASH_ENV|NODE_OPTIONS|LD_)/);
		assert.ok(!args.includes("--env"));
		assert.ok(!args.includes("--rm"));
		assert.ok(!args.includes("--privileged"));
		assert.ok(!args.includes("--cap-add"));
		assert.ok(!args.includes("--pid"));
		assert.doesNotMatch(args.slice(imageIndex + 1).join("\n"), /(?:\/bin\/sh|\/usr\/bin\/bash|^sh$|^bash$)/m);
		const mounts = args.flatMap((value, index) => args[index - 1] === "--mount" ? [value] : []);
		const destinations = mounts.map((value) => value.match(/(?:^|,)dst=([^,]+)/)?.[1]);
		assert.equal(new Set(destinations).size, destinations.length);
		assert.deepEqual([...destinations].sort(), [...expectedDestinations[spec.phase]].sort());
		for (const [index, mount] of mounts.entries()) {
			if (destinations[index] === writableDestination[spec.phase]) assert.doesNotMatch(mount, /,readonly$/);
			else assert.match(mount, /,readonly$/);
		}
	};
	for (const phase of ["dependencies", "pack", "contracts", "prepare", "final", "smoke-cache", "smoke"]) {
		const spec = fixture(phase);
		const args = buildDockerRunArgs(spec);
		validateArgs(args, spec);
		const imageIndex = args.indexOf(PYLON_SANDBOX_POLICY.image);
		const entrypointIndex = args.indexOf("--entrypoint");
		const healthcheckIndex = args.indexOf("--no-healthcheck");
		const mutations = [
			[...args.slice(0, imageIndex), "--network", "host", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--user", "0:0", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--pids-limit", "-1", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--cpus", "999", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--memory", "64g", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--cap-add", "ALL", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--pid", "host", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--privileged", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--mount", "type=bind,src=/original,dst=/build", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock", ...args.slice(imageIndex)],
			[...args.slice(0, entrypointIndex), ...args.slice(entrypointIndex + 2)],
			[...args.slice(0, imageIndex), "--entrypoint", "bin/env", ...args.slice(imageIndex)],
			[...args.slice(0, imageIndex), "--entrypoint", "/bin/sh", ...args.slice(imageIndex)],
			[...args.slice(0, healthcheckIndex), ...args.slice(healthcheckIndex + 1)],
			[...args.slice(0, imageIndex), "--no-healthcheck", ...args.slice(imageIndex)],
			["run", ...args],
			[...args.slice(0, imageIndex + 1), "/bin/sh", "-c", ...args.slice(imageIndex + 1)],
		];
		for (const mutation of mutations) assert.throws(() => assertExactDockerRunArgs(mutation, spec));
	}
	validateArgs(buildDockerRunArgs(fixture("smoke", "historical")), fixture("smoke", "historical"));

	const imageInspection = {
		Architecture: "amd64",
		Config: {
			Cmd: ["node"],
			Entrypoint: ["docker-entrypoint.sh"],
			Env: [
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"NODE_VERSION=22.23.2",
				"YARN_VERSION=1.22.22",
			],
		},
		Id: PYLON_SANDBOX_POLICY.imageConfigId,
		Os: "linux",
		RepoDigests: [PYLON_SANDBOX_POLICY.image],
	};
	const githubImageInspection = {
		Architecture: "amd64",
		Config: {
			ArgsEscaped: true,
			AttachStderr: false,
			AttachStdin: false,
			AttachStdout: false,
			Cmd: ["node"],
			Domainname: "",
			Entrypoint: ["docker-entrypoint.sh"],
			Env: [
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"NODE_VERSION=22.23.2",
				"YARN_VERSION=1.22.22",
			],
			ExposedPorts: null,
			Healthcheck: null,
			Hostname: "",
			Image: "",
			Labels: null,
			MacAddress: "",
			NetworkDisabled: false,
			OnBuild: null,
			OpenStdin: false,
			Shell: null,
			StdinOnce: false,
			StopSignal: "",
			StopTimeout: null,
			Tty: false,
			User: "",
			Volumes: null,
			WorkingDir: "",
		},
		Id: PYLON_SANDBOX_POLICY.imageConfigId,
		Os: "linux",
		RepoDigests: [`docker.io/library/${PYLON_SANDBOX_POLICY.image}`],
		Variant: null,
	};
	const emptyImageInspection = structuredClone(githubImageInspection);
	emptyImageInspection.Config.ExposedPorts = {};
	emptyImageInspection.Config.Healthcheck = {};
	emptyImageInspection.Config.OnBuild = [];
	emptyImageInspection.Config.Shell = [];
	emptyImageInspection.Config.Volumes = {};
	emptyImageInspection.RepoDigests.push(PYLON_SANDBOX_POLICY.image);
	for (const inspection of [imageInspection, githubImageInspection, emptyImageInspection]) {
		assert.equal(validateImageInspection(inspection), true);
	}
	for (const mutate of [
		(value) => { value.Config.Volumes = { "/data": {} }; },
		(value) => { value.Config.Healthcheck = { Test: ["CMD-SHELL", "true"] }; },
		(value) => { value.Config.User = "node"; },
		(value) => { value.Config.WorkingDir = "/app"; },
		(value) => { value.Config.ExposedPorts = { "3000/tcp": {} }; },
		(value) => { value.Config.OnBuild = ["RUN id"]; },
		(value) => { value.Config.Shell = ["/bin/sh", "-c"]; },
		(value) => { value.Config.StopSignal = "SIGTERM"; },
		(value) => { value.Config.StopTimeout = 1; },
		(value) => { value.Config.NetworkDisabled = true; },
		(value) => { value.Config.Hostname = "poison"; },
		(value) => { value.Config.Entrypoint = ["/bin/sh"]; },
		(value) => { value.Config.Cmd = ["-c", "id"]; },
		(value) => { value.Config.Env[1] = "NODE_VERSION=latest"; },
		(value) => { value.Id = `sha256:${"a".repeat(64)}`; },
		(value) => { value.Os = "darwin"; },
		(value) => { value.Architecture = "arm64"; },
		(value) => { value.Variant = "v8"; },
		(value) => { value.RepoDigests = []; },
		(value) => { value.RepoDigests = [`evil.example/node@${PYLON_SANDBOX_POLICY.image.split("@")[1]}`]; },
		(value) => { value.RepoDigests.push(value.RepoDigests[0]); },
		(value) => { value.Config = null; },
	]) {
		const changed = structuredClone(githubImageInspection);
		mutate(changed);
		assert.throws(() => validateImageInspection(changed));
	}

	const containerId = "b".repeat(64);
	const removal = { error: undefined, signal: null, status: 0, stdout: `${containerId}\n` };
	const inspection = { error: undefined, signal: null, status: 1, stderr: `Error response from daemon: No such container: ${containerId}\n` };
	assert.equal(assertContainerRemoved(containerId, removal, inspection), true);
	for (const [changedRemoval, changedInspection] of [
		[{ ...removal, status: 1 }, inspection],
		[{ ...removal, stdout: "" }, inspection],
		[removal, { ...inspection, status: 0 }],
		[removal, { ...inspection, status: null, signal: "SIGTERM" }],
		[removal, { ...inspection, error: new Error("daemon unavailable") }],
		[removal, { ...inspection, stderr: "daemon unavailable" }],
	]) assert.throws(() => assertContainerRemoved(containerId, changedRemoval, changedInspection));

	const packageManifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
	const packageLock = JSON.parse(readFileSync(join(root, "package-lock.json"), "utf8"));
	assert.equal(packageManifest.devDependencies.yaml, "2.9.0");
	assert.equal(packageLock.packages[""].devDependencies.yaml, "2.9.0");

	const expectedJobs = {
  "ciPack": {
    "name": "Pylon artifact pack (${{ matrix.copy }})",
    "needs": "trust",
    "if": "needs.trust.outputs.allowed == 'true'",
    "runs-on": "ubuntu-24.04",
    "timeout-minutes": 20,
    "strategy": {
      "fail-fast": false,
      "matrix": {
        "copy": [
          "a",
          "b"
        ]
      }
    },
    "env": {
      "PYLON_RELEASE_NODE": "22.23.2",
      "PYLON_RELEASE_MODE": "ci",
      "PYLON_RELEASE_NPM": "11.10.1"
    },
    "steps": [
      {
        "name": "Checkout exact source",
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {
          "ref": "${{ inputs.checkout_ref || github.ref }}",
          "persist-credentials": false,
          "fetch-depth": 1
        }
      },
      {
        "name": "Setup pinned Node.js",
        "uses": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
        "with": {
          "node-version": "${{ env.PYLON_RELEASE_NODE }}"
        }
      },
      {
        "name": "Run isolated release sandbox",
        "id": "sandbox",
        "shell": "/usr/bin/bash --noprofile --norc -euo pipefail {0}",
        "run": "node_path=\"$(node -e 'process.stdout.write(require(\"node:fs\").realpathSync.native(process.execPath))')\"\n/usr/bin/env -i \\\n  \"HOME=/tmp\" \\\n  \"PATH=${node_path%/*}:/usr/bin:/bin\" \\\n  \"$node_path\" \"$GITHUB_WORKSPACE/scripts/run-pylon-release-sandbox.mjs\" \\\n  --task pack \\\n  --mode \"$PYLON_RELEASE_MODE\" \\\n  --host-os \"${{ runner.os }}\" \\\n  --workspace \"$GITHUB_WORKSPACE\" \\\n  --github-output \"$GITHUB_OUTPUT\" \\\n  --run-id \"${{ github.run_id }}\" \\\n  --run-number \"${{ github.run_number }}\" \\\n  --publication-policy-revision 3\n"
      },
      {
        "name": "Upload isolated subjects",
        "uses": "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "with": {
          "name": "pylon-prime-pack-${{ matrix.copy }}",
          "path": "${{ steps.sandbox.outputs.artifact_dir }}",
          "include-hidden-files": true,
          "if-no-files-found": "error",
          "retention-days": 3
        }
      }
    ]
  },
  "ciInstall": {
    "name": "Pylon artifact install (${{ matrix.os }})",
    "needs": [
      "trust",
      "pylon-artifact-pack",
      "pylon-artifact-reproducibility"
    ],
    "if": "needs.trust.outputs.allowed == 'true'",
    "runs-on": "${{ matrix.os }}",
    "timeout-minutes": 15,
    "permissions": {
      "actions": "read",
      "contents": "read"
    },
    "strategy": {
      "fail-fast": false,
      "matrix": {
        "os": [
          "ubuntu-24.04",
          "macos-15"
        ]
      }
    },
    "env": {
      "PYLON_RELEASE_NODE": "22.23.2",
      "PYLON_RELEASE_MODE": "ci",
      "PYLON_RELEASE_NPM": "11.10.1"
    },
    "steps": [
      {
        "name": "Checkout exact source",
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {
          "ref": "${{ inputs.checkout_ref || github.ref }}",
          "persist-credentials": false,
          "fetch-depth": 1
        }
      },
      {
        "name": "Setup pinned Node.js",
        "uses": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
        "with": {
          "node-version": "${{ env.PYLON_RELEASE_NODE }}"
        }
      },
      {
        "name": "Download reproducible pack",
        "uses": "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        "with": {
          "name": "pylon-prime-pack-a",
          "path": ".npm/pylon-release/artifacts"
        }
      },
      {
        "name": "Run isolated artifact smoke",
        "shell": "/usr/bin/bash --noprofile --norc -euo pipefail {0}",
        "run": "node_path=\"$(node -e 'process.stdout.write(require(\"node:fs\").realpathSync.native(process.execPath))')\"\n/usr/bin/env -i \\\n  \"HOME=/tmp\" \\\n  \"PATH=${node_path%/*}:/usr/bin:/bin\" \\\n  \"$node_path\" \"$GITHUB_WORKSPACE/scripts/run-pylon-release-sandbox.mjs\" \\\n  --task smoke \\\n  --mode \"$PYLON_RELEASE_MODE\" \\\n  --host-os \"${{ runner.os }}\" \\\n  --workspace \"$GITHUB_WORKSPACE\" \\\n  --artifact-dir \"$GITHUB_WORKSPACE/.npm/pylon-release/artifacts\" \\\n  --run-id \"${{ github.run_id }}\" \\\n  --run-number \"${{ github.run_number }}\" \\\n  --publication-policy-revision 3\n"
      }
    ]
  },
  "previewPack": {
    "name": "Preview offline pack (${{ matrix.copy }})",
    "needs": "admission",
    "runs-on": "ubuntu-24.04",
    "timeout-minutes": 20,
    "env": {
      "PYLON_RELEASE_MODE": "preview"
    },
    "permissions": {
      "contents": "read"
    },
    "strategy": {
      "fail-fast": false,
      "matrix": {
        "copy": [
          "a",
          "b"
        ]
      }
    },
    "steps": [
      {
        "name": "Checkout exact source",
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {
          "ref": "${{ github.sha }}",
          "persist-credentials": false,
          "fetch-depth": 1
        }
      },
      {
        "name": "Setup pinned Node.js",
        "uses": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
        "with": {
          "node-version": "${{ env.PYLON_RELEASE_NODE }}"
        }
      },
      {
        "name": "Run isolated release sandbox",
        "id": "sandbox",
        "shell": "/usr/bin/bash --noprofile --norc -euo pipefail {0}",
        "run": "node_path=\"$(node -e 'process.stdout.write(require(\"node:fs\").realpathSync.native(process.execPath))')\"\n/usr/bin/env -i \\\n  \"HOME=/tmp\" \\\n  \"PATH=${node_path%/*}:/usr/bin:/bin\" \\\n  \"$node_path\" \"$GITHUB_WORKSPACE/scripts/run-pylon-release-sandbox.mjs\" \\\n  --task pack \\\n  --mode \"$PYLON_RELEASE_MODE\" \\\n  --host-os \"${{ runner.os }}\" \\\n  --workspace \"$GITHUB_WORKSPACE\" \\\n  --github-output \"$GITHUB_OUTPUT\" \\\n  --run-id \"${{ github.run_id }}\" \\\n  --run-number \"${{ github.run_number }}\" \\\n  --publication-policy-revision 3\n"
      },
      {
        "name": "Upload isolated subjects",
        "uses": "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "with": {
          "name": "pylon-preview-pack-${{ matrix.copy }}",
          "path": "${{ steps.sandbox.outputs.artifact_dir }}",
          "include-hidden-files": true,
          "if-no-files-found": "error",
          "retention-days": 3
        }
      }
    ]
  },
  "previewInstall": {
    "name": "Preview installed artifact (${{ matrix.os }})",
    "needs": [
      "pack",
      "reproducibility"
    ],
    "runs-on": "${{ matrix.os }}",
    "timeout-minutes": 15,
    "env": {
      "PYLON_RELEASE_MODE": "preview"
    },
    "permissions": {
      "actions": "read",
      "contents": "read"
    },
    "strategy": {
      "fail-fast": false,
      "matrix": {
        "os": [
          "ubuntu-24.04",
          "macos-15"
        ]
      }
    },
    "steps": [
      {
        "name": "Checkout exact source",
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {
          "ref": "${{ github.sha }}",
          "persist-credentials": false,
          "fetch-depth": 1
        }
      },
      {
        "name": "Setup pinned Node.js",
        "uses": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
        "with": {
          "node-version": "${{ env.PYLON_RELEASE_NODE }}"
        }
      },
      {
        "name": "Verify workflow artifact provenance",
        "uses": "actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea",
        "with": {
          "script": "const run = (await github.rest.actions.getWorkflowRun({ ...context.repo, run_id: context.runId })).data;\nconst workflow = (await github.rest.actions.getWorkflow({ ...context.repo, workflow_id: run.workflow_id })).data;\nif (\n  context.repo.owner !== \"pylon-code\" || context.repo.repo !== \"prime-agent\" ||\n  run.id !== context.runId || run.run_number !== Number(process.env.GITHUB_RUN_NUMBER) ||\n  String(run.run_attempt) !== process.env.GITHUB_RUN_ATTEMPT || run.repository?.id !== 1349002285 ||\n  run.repository?.full_name !== \"pylon-code/prime-agent\" || run.head_repository?.id !== 1349002285 ||\n  run.head_repository?.full_name !== \"pylon-code/prime-agent\" || run.event !== context.eventName ||\n  run.head_sha !== context.sha || run.head_branch !== \"pylon\" || context.ref !== \"refs/heads/pylon\" ||\n  workflow.path !== \".github/workflows/pylon-preview-release.yml\"\n) throw new Error(\"Artifact workflow provenance is not canonical.\");\nconst artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {\n  ...context.repo, run_id: context.runId, per_page: 100,\n});\nconst matches = artifacts.filter((artifact) => artifact.name === \"pylon-preview-pack-a\");\nif (matches.length !== 1 || matches[0].expired || !/^sha256:[0-9a-f]{64}$/.test(matches[0].digest ?? \"\")) {\n  throw new Error(\"Artifact is ambiguous, expired, or lacks a SHA-256 transport digest.\");\n}\n"
        }
      },
      {
        "name": "Download byte-identical preview subjects",
        "uses": "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        "with": {
          "name": "pylon-preview-pack-a",
          "path": ".npm/pylon-release/artifacts"
        }
      },
      {
        "name": "Run isolated artifact smoke",
        "shell": "/usr/bin/bash --noprofile --norc -euo pipefail {0}",
        "run": "node_path=\"$(node -e 'process.stdout.write(require(\"node:fs\").realpathSync.native(process.execPath))')\"\n/usr/bin/env -i \\\n  \"HOME=/tmp\" \\\n  \"PATH=${node_path%/*}:/usr/bin:/bin\" \\\n  \"$node_path\" \"$GITHUB_WORKSPACE/scripts/run-pylon-release-sandbox.mjs\" \\\n  --task smoke \\\n  --mode \"$PYLON_RELEASE_MODE\" \\\n  --host-os \"${{ runner.os }}\" \\\n  --workspace \"$GITHUB_WORKSPACE\" \\\n  --artifact-dir \"$GITHUB_WORKSPACE/.npm/pylon-release/artifacts\" \\\n  --run-id \"${{ github.run_id }}\" \\\n  --run-number \"${{ github.run_number }}\" \\\n  --publication-policy-revision 3\n"
      }
    ]
  },
  "stableInstall": {
    "name": "Stable candidate installed artifact (${{ matrix.os }})",
    "needs": [
      "admission",
      "verify-preview"
    ],
    "runs-on": "${{ matrix.os }}",
    "timeout-minutes": 15,
    "env": {
      "PYLON_RELEASE_MODE": "historical"
    },
    "permissions": {
      "actions": "read",
      "contents": "read"
    },
    "strategy": {
      "fail-fast": false,
      "matrix": {
        "os": [
          "ubuntu-24.04",
          "macos-15"
        ]
      }
    },
    "steps": [
      {
        "name": "Checkout exact source",
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {
          "ref": "${{ github.sha }}",
          "persist-credentials": false,
          "fetch-depth": 1
        }
      },
      {
        "name": "Setup pinned Node.js",
        "uses": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
        "with": {
          "node-version": "${{ env.PYLON_RELEASE_NODE }}"
        }
      },
      {
        "name": "Verify workflow artifact provenance",
        "uses": "actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea",
        "with": {
          "script": "const run = (await github.rest.actions.getWorkflowRun({ ...context.repo, run_id: context.runId })).data;\nconst workflow = (await github.rest.actions.getWorkflow({ ...context.repo, workflow_id: run.workflow_id })).data;\nif (\n  context.repo.owner !== \"pylon-code\" || context.repo.repo !== \"prime-agent\" || run.repository?.id !== 1349002285 ||\n  run.repository?.full_name !== \"pylon-code/prime-agent\" || run.head_repository?.id !== 1349002285 ||\n  run.head_repository?.full_name !== \"pylon-code/prime-agent\" || run.event !== context.eventName ||\n  run.head_sha !== context.sha || run.head_branch !== \"pylon\" || context.ref !== \"refs/heads/pylon\" ||\n  workflow.path !== \".github/workflows/pylon-stable-release.yml\"\n) throw new Error(\"Artifact workflow provenance is not canonical.\");\nconst artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {\n  ...context.repo, run_id: context.runId, per_page: 100,\n});\nconst matches = artifacts.filter((artifact) => artifact.name === \"verified-stable-preview\");\nif (matches.length !== 1 || matches[0].expired || !/^sha256:[0-9a-f]{64}$/.test(matches[0].digest ?? \"\")) {\n  throw new Error(\"Artifact is ambiguous, expired, or lacks a SHA-256 transport digest.\");\n}\n"
        }
      },
      {
        "name": "Download verified preview bytes",
        "uses": "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        "with": {
          "name": "verified-stable-preview",
          "path": ".npm/pylon-release/artifacts"
        }
      },
      {
        "name": "Run isolated artifact smoke",
        "shell": "/usr/bin/bash --noprofile --norc -euo pipefail {0}",
        "run": "node_path=\"$(node -e 'process.stdout.write(require(\"node:fs\").realpathSync.native(process.execPath))')\"\n/usr/bin/env -i \\\n  \"HOME=/tmp\" \\\n  \"PATH=${node_path%/*}:/usr/bin:/bin\" \\\n  \"$node_path\" \"$GITHUB_WORKSPACE/scripts/run-pylon-release-sandbox.mjs\" \\\n  --task smoke \\\n  --mode \"$PYLON_RELEASE_MODE\" \\\n  --host-os \"${{ runner.os }}\" \\\n  --workspace \"$GITHUB_WORKSPACE\" \\\n  --artifact-dir \"$GITHUB_WORKSPACE/.npm/pylon-release/artifacts\" \\\n  --run-id \"${{ github.run_id }}\" \\\n  --run-number \"${{ github.run_number }}\" \\\n  --publication-policy-revision 3\n"
      }
    ]
  }
};
	const expectedWorkflowRoots = {
		ci: {
			env: undefined,
			keys: ["concurrency", "jobs", "name", "on", "permissions"],
		},
		preview: {
			env: {
				PYLON_RELEASE_NODE: "22.23.2",
				PYLON_RELEASE_NPM: "11.10.1",
			},
			keys: ["concurrency", "env", "jobs", "name", "on", "permissions"],
		},
		stable: {
			env: {
				PYLON_RELEASE_NODE: "22.23.2",
				PYLON_RELEASE_NPM: "11.10.1",
			},
			keys: ["concurrency", "env", "jobs", "name", "on", "permissions"],
		},
	};
	const parseExactWorkflowRoot = (workflow, expected) => {
		const parsed = parseYaml(workflow, { merge: false, uniqueKeys: true });
		assert.ok(parsed && typeof parsed === "object" && !Array.isArray(parsed));
		assert.deepEqual(Object.keys(parsed).sort(), expected.keys);
		assert.deepEqual(parsed.env, expected.env);
		assert.ok(parsed.jobs && typeof parsed.jobs === "object" && !Array.isArray(parsed.jobs));
		return parsed;
	};
	const preview = readFileSync(join(root, ".github/workflows/pylon-preview-release.yml"), "utf8");
	const ci = readFileSync(join(root, ".github/workflows/ci.yml"), "utf8");
	const stable = readFileSync(join(root, ".github/workflows/pylon-stable-release.yml"), "utf8");
	const assertExactWorkflows = (ciWorkflow, previewWorkflow, stableWorkflow) => {
		const parsedCi = parseExactWorkflowRoot(ciWorkflow, expectedWorkflowRoots.ci);
		const parsedPreview = parseExactWorkflowRoot(previewWorkflow, expectedWorkflowRoots.preview);
		const parsedStable = parseExactWorkflowRoot(stableWorkflow, expectedWorkflowRoots.stable);
		assert.deepEqual(parsedCi.jobs["pylon-artifact-pack"], expectedJobs.ciPack);
		assert.deepEqual(parsedCi.jobs["pylon-artifact-install"], expectedJobs.ciInstall);
		assert.deepEqual(parsedPreview.jobs.pack, expectedJobs.previewPack);
		assert.deepEqual(parsedPreview.jobs.install, expectedJobs.previewInstall);
		assert.deepEqual(parsedStable.jobs.install, expectedJobs.stableInstall);
	};
	assertExactWorkflows(ci, preview, stable);
	assert.doesNotMatch(`${preview}\n${ci}\n${stable}`, /(?:npm install --global|\/usr\/bin\/realpath|\/usr\/bin\/dirname)/);
	for (const job of [expectedJobs.ciPack, expectedJobs.ciInstall, expectedJobs.previewPack, expectedJobs.previewInstall, expectedJobs.stableInstall]) {
		const sandbox = job.steps.find((step) => step.name.startsWith("Run isolated"));
		assert.match(sandbox.run, /^node_path="\$\(node -e 'process\.stdout\.write\(require\("node:fs"\)\.realpathSync\.native\(process\.execPath\)\)'\)"/);
		assert.match(sandbox.run, /"PATH=\$\{node_path%\/\*\}:\/usr\/bin:\/bin"/);
	}
	const capture = spawnSync(process.execPath, ["-e", 'process.stdout.write(require("node:fs").realpathSync.native(process.execPath))'], { encoding: "utf8", env: {} });
	assert.equal(capture.status, 0);
	assert.equal(capture.stdout, realpathSync.native(process.execPath));

	const workflowMutations = [
		[ci.replace("permissions:\n  contents: read", "arbitrary-root-policy: unsafe\n\npermissions:\n  contents: read"), preview, stable],
		[ci, preview.replace("  PYLON_RELEASE_NPM: 11.10.1", "  PYLON_RELEASE_NPM: 11.10.1\n  BASH_ENV: /tmp/poison"), stable],
		[ci, preview.replace("  PYLON_RELEASE_NPM: 11.10.1", "  PYLON_RELEASE_NPM: 11.10.1\n  NODE_OPTIONS: --require=/tmp/poison.cjs"), stable],
		[ci, preview.replace("  PYLON_RELEASE_NPM: 11.10.1", "  PYLON_RELEASE_NPM: 11.10.1\n  GITHUB_PATH: /tmp/poison"), stable],
		[ci, preview.replace("  PYLON_RELEASE_NODE: 22.23.2", "  PYLON_RELEASE_NODE: latest"), stable],
		[ci.replace("    name: Pylon artifact pack", "    unknown-policy: true\n    name: Pylon artifact pack"), preview, stable],
		[ci.replace("      PYLON_RELEASE_MODE: ci", "      PYLON_RELEASE_MODE: ci\n      POISON: yes"), preview, stable],
		[ci.replace("        id: sandbox", "        id: sandbox\n        env:\n          POISON: yes"), preview, stable],
		[ci.replace("      - name: Upload isolated subjects", "      - name: Extra host command\n        run: echo unsafe\n\n      - name: Upload isolated subjects"), preview, stable],
		[ci.replace("        id: sandbox", "        id: sandbox\n        run: echo duplicate"), preview, stable],
		[ci.replace("        id: sandbox", "        id: sandbox\n        mystery: unsafe"), preview, stable],
		[ci.replace("          node_path=", "          echo unsafe\n          node_path="), preview, stable],
		[ci.replace("            --publication-policy-revision 3", "            --publication-policy-revision 3\n          echo unsafe"), preview, stable],
		[ci.replace("          /usr/bin/env -i", "          sudo /usr/bin/env -i"), preview, stable],
	];
	for (const [changedCi, changedPreview, changedStable] of workflowMutations) {
		assert.throws(() => assertExactWorkflows(changedCi, changedPreview, changedStable));
	}

	const exerciseRepeatedSignal = async (primarySignal, repeatedSignals) => {
		const signalTarget = new EventEmitter();
		const signaledState = {
			activePgid: null,
			containers: [{ id: "d".repeat(64) }],
			control: "/private/control",
			scratch: "/private/scratch",
		};
		const signalCleanup = [];
		let cleanupCalls = 0;
		let exitCalls = 0;
		let capturedPgid;
		let resolveSignalExit;
		const signalExit = new Promise((resolveExit) => { resolveSignalExit = resolveExit; });
		const removeSignalCleanup = installSignalCleanup(signaledState, {
			cleanupContainers: (state) => {
				cleanupCalls += 1;
				assert.equal(state.containers.length, 1);
				state.containers.length = 0;
				signalCleanup.push("containers-absent");
				return true;
			},
			exit: (code) => {
				exitCalls += 1;
				assert.equal(signaledState.activePgid, null);
				assert.equal(signaledState.containers.length, 0);
				assert.throws(() => process.kill(-capturedPgid, 0), (error) => error?.code === "ESRCH");
				resolveSignalExit(code);
			},
			removeOwnedTree: (path) => signalCleanup.push(`removed:${path}`),
			signalTarget,
		});
		const interruptedMacSmoke = runDetachedProcessGroup(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
			cwd: root,
			env: {},
			lifecycle: signaledState,
			timeoutMs: 30_000,
		});
		const interruptedMacSmokeRejection = assert.rejects(interruptedMacSmoke);
		capturedPgid = signaledState.activePgid;
		assert.ok(Number.isSafeInteger(capturedPgid) && capturedPgid > 0);
		signalTarget.emit(primarySignal);
		assert.equal(signalTarget.listenerCount("SIGINT"), 1);
		assert.equal(signalTarget.listenerCount("SIGTERM"), 1);
		for (const repeatedSignal of repeatedSignals) signalTarget.emit(repeatedSignal);
		assert.equal(signalTarget.listenerCount("SIGINT"), 1);
		assert.equal(signalTarget.listenerCount("SIGTERM"), 1);
		const exitCode = await signalExit;
		await interruptedMacSmokeRejection;
		assert.equal(signaledState.activePgid, null);
		assert.equal(cleanupCalls, 1);
		assert.equal(exitCalls, 1);
		assert.deepEqual(signalCleanup, ["containers-absent", "removed:/private/scratch", "removed:/private/control"]);
		assert.equal(signalTarget.listenerCount("SIGINT"), 0);
		assert.equal(signalTarget.listenerCount("SIGTERM"), 0);
		await removeSignalCleanup();
		return exitCode;
	};
	assert.equal(await exerciseRepeatedSignal("SIGTERM", ["SIGTERM", "SIGINT"]), 143);
	assert.equal(await exerciseRepeatedSignal("SIGINT", ["SIGINT"]), 130);

	const cleanupBoundaryTarget = new EventEmitter();
	const cleanupBoundaryState = {
		activePgid: null,
		containers: [{ id: "c".repeat(64) }],
		control: "/private/final-control",
		scratch: "/private/final-scratch",
	};
	let absenceProved = false;
	let resolveBoundaryExit;
	const boundaryExit = new Promise((resolveExit) => { resolveBoundaryExit = resolveExit; });
	const removeBoundaryCleanup = installSignalCleanup(cleanupBoundaryState, {
		cleanupContainers: (state) => {
			assert.equal(state.containers.length, 1);
			state.containers.length = 0;
			absenceProved = true;
			return true;
		},
		exit: (code) => {
			assert.equal(absenceProved, true);
			resolveBoundaryExit(code);
		},
		removeOwnedTree: () => {},
		signalTarget: cleanupBoundaryTarget,
	});
	const finalHandlerRelease = removeBoundaryCleanup();
	queueMicrotask(() => {
		cleanupBoundaryTarget.emit("SIGINT");
		assert.equal(cleanupBoundaryTarget.listenerCount("SIGINT"), 1);
		assert.equal(cleanupBoundaryTarget.listenerCount("SIGTERM"), 1);
		cleanupBoundaryTarget.emit("SIGINT");
		cleanupBoundaryTarget.emit("SIGTERM");
	});
	await finalHandlerRelease;
	assert.equal(await boundaryExit, 130);
	assert.equal(cleanupBoundaryState.containers.length, 0);
	assert.equal(cleanupBoundaryTarget.listenerCount("SIGINT"), 0);
	assert.equal(cleanupBoundaryTarget.listenerCount("SIGTERM"), 0);

	const processReceipt = await runDetachedProcessGroup(process.execPath, ["-e", "process.exit(0)"], { cwd: root, env: {}, timeoutMs: 30_000 });
	assert.ok(Number.isSafeInteger(processReceipt.pid) && processReceipt.pid > 0);
	assert.equal(processReceipt.status, 0);
	assert.equal(processReceipt.signal, null);
	await assert.rejects(runDetachedProcessGroup(process.execPath, ["-e", `
const { spawn } = require("node:child_process");
const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
child.unref();
`], { cwd: root, env: {}, timeoutMs: 30_000 }));
});

test("bounded final subject copy binds receipt, metadata, sparse allocation, and disposable watcher isolation", async () => {
	const makeSource = () => {
		const parent = realpathSync.native(mkdtempSync(join(tmpdir(), "pylon-final-copy-")));
		const source = join(parent, "source");
		mkdirSync(source, { mode: 0o700 });
		for (let index = 0; index < 5; index += 1) {
			const file = join(source, `subject-${index}.tgz`);
			writeFileSync(file, `subject-${index}`, { mode: 0o644 });
			chmodSync(file, 0o644);
		}
		return { parent, source, destination: join(parent, "final") };
	};
	const clean = (parent) => {
		for (const directory of [join(parent, "historical"), join(parent, "preview"), join(parent, "disposable"), join(parent, "final"), join(parent, "source"), parent]) {
			try { chmodSync(directory, 0o700); } catch {}
		}
		rmSync(parent, { force: true, recursive: true });
	};
	const canonicalReceiptBytes = (files) => Buffer.from(`${JSON.stringify({ files, schemaVersion: 1 })}\n`);
	const writeReceipt = (path, files) => {
		writeFileSync(path, canonicalReceiptBytes(files), { flag: "wx", mode: 0o644 });
		chmodSync(path, 0o644);
	};
	{
		const fixture = makeSource();
		try {
			const initial = copyBoundedSubjects(fixture.source, fixture.destination, { expectedCount: 5, freeze: true, inputGid: null });
			assert.equal(initial.names.length, 5);
			assert.equal(lstatSync(fixture.destination).mode & 0o7777, 0o555);
			for (const name of initial.names) {
				const metadata = lstatSync(join(fixture.destination, name));
				assert.equal(metadata.mode & 0o7777, 0o444);
				assert.equal(metadata.nlink, 1);
			}
		} finally { clean(fixture.parent); }
	}
	const mutations = [
		({ source }) => { chmodSync(join(source, "subject-0.tgz"), 0o666); },
		({ source }) => { rmSync(join(source, "subject-0.tgz")); symlinkSync("subject-1.tgz", join(source, "subject-0.tgz")); },
		({ source }) => { rmSync(join(source, "subject-0.tgz")); linkSync(join(source, "subject-1.tgz"), join(source, "subject-0.tgz")); },
		({ source }) => { truncateSync(join(source, "subject-0.tgz"), 1024 * 1024); },
		({ source }) => {
			rmSync(join(source, "subject-0.tgz"));
			const result = spawnSync("/usr/bin/mkfifo", [join(source, "subject-0.tgz")], { stdio: "pipe" });
			assert.equal(result.status, 0);
		},
		({ source }) => { writeFileSync(join(source, "extra.tgz"), "extra", { mode: 0o644 }); },
	];
	for (const mutate of mutations) {
		const fixture = makeSource();
		try {
			mutate(fixture);
			assert.throws(() => copyBoundedSubjects(fixture.source, fixture.destination, { expectedCount: 5, freeze: true }));
			assert.equal(existsSync(fixture.destination), false);
		} finally { clean(fixture.parent); }
	}
	{
		const fixture = makeSource();
		const linked = join(fixture.parent, "linked-source");
		try {
			symlinkSync(fixture.source, linked);
			assert.throws(() => exactArtifactDirectory(linked));
			assert.throws(() => copyBoundedSubjects(linked, fixture.destination, { expectedCount: 5, freeze: true }));
		} finally { clean(fixture.parent); }
	}
	{
		const fixture = makeSource();
		try {
			const probe = copyBoundedSubjects(fixture.source, join(fixture.parent, "probe"), { expectedCount: 5 });
			const receiptPath = join(fixture.parent, "receipt.json");
			writeReceipt(receiptPath, probe.files);
			const receipt = readFinalReceipt(receiptPath, 5);
			copyBoundedSubjects(fixture.source, fixture.destination, { expectedCount: 5, expectedReceipt: receipt, freeze: true });
			assert.throws(() => readFinalReceipt(join(fixture.parent, "missing-receipt.json"), 5));
			const wrongPath = join(fixture.parent, "wrong.json");
			const wrong = structuredClone(probe.files);
			wrong[0].sha256 = "f".repeat(64);
			writeReceipt(wrongPath, wrong);
			const wrongReceipt = readFinalReceipt(wrongPath, 5);
			assert.throws(() => copyBoundedSubjects(fixture.source, join(fixture.parent, "wrong-final"), { expectedCount: 5, expectedReceipt: wrongReceipt, freeze: true }));
			writeFileSync(join(fixture.source, "subject-0.tgz"), "stale-source-bytes");
			assert.throws(() => copyBoundedSubjects(fixture.source, join(fixture.parent, "stale-final"), { expectedCount: 5, expectedReceipt: receipt, freeze: true }));
			const unsorted = join(fixture.parent, "unsorted.json");
			writeReceipt(unsorted, [...probe.files].reverse());
			assert.throws(() => readFinalReceipt(unsorted, 5));
			const noncanonical = join(fixture.parent, "noncanonical.json");
			writeFileSync(noncanonical, `${JSON.stringify({ schemaVersion: 1, files: probe.files }, null, 2)}\n`, { mode: 0o644 });
			chmodSync(noncanonical, 0o644);
			assert.throws(() => readFinalReceipt(noncanonical, 5));
		} finally { clean(fixture.parent); }
	}
	{
		const fixture = makeSource();
		let watcher;
		try {
			const receipt = copyBoundedSubjects(fixture.source, join(fixture.parent, "probe"), { expectedCount: 5 });
			const disposable = join(fixture.parent, "disposable");
			copyBoundedSubjects(fixture.source, disposable, { expectedCount: 5 });
			const properLockfileWatcher = `
const { writeFileSync } = require("node:fs");
const target = process.argv[1];
let running = true;
process.stdin.once("data", () => { running = false; });
process.stdout.write("ready\\n");
const mutate = () => {
  writeFileSync(target, "malicious-proper-lockfile-watcher");
  if (running) setImmediate(mutate);
  else process.exit(0);
};
mutate();`;
			watcher = spawn(process.execPath, ["-e", properLockfileWatcher, join(disposable, "subject-0.tgz")], { stdio: ["pipe", "pipe", "inherit"] });
			await new Promise((resolveReady, rejectReady) => {
				const timer = setTimeout(() => rejectReady(new Error("watcher readiness timed out")), 10_000);
				watcher.once("error", rejectReady);
				watcher.stdout.once("data", (chunk) => {
					clearTimeout(timer);
					if (!chunk.toString("utf8").includes("ready")) rejectReady(new Error("watcher readiness malformed"));
					else resolveReady();
				});
			});
			copyBoundedSubjects(fixture.source, fixture.destination, { expectedCount: 5, expectedReceipt: { files: receipt.files, schemaVersion: 1 }, freeze: true });
			watcher.stdin.end("stop");
			await new Promise((resolveClose, rejectClose) => {
				const timer = setTimeout(() => rejectClose(new Error("watcher exit timed out")), 10_000);
				watcher.once("error", rejectClose);
				watcher.once("close", (status) => {
					clearTimeout(timer);
					if (status !== 0) rejectClose(new Error("watcher failed"));
					else resolveClose();
				});
			});
			watcher = undefined;
			assert.equal(readFileSync(join(fixture.source, "subject-0.tgz"), "utf8"), "subject-0");
			assert.equal(readFileSync(join(fixture.destination, "subject-0.tgz"), "utf8"), "subject-0");
			assert.equal(readFileSync(join(disposable, "subject-0.tgz"), "utf8"), "malicious-proper-lockfile-watcher");
		} finally {
			if (watcher && watcher.exitCode === null) watcher.kill("SIGKILL");
			clean(fixture.parent);
		}
	}
	{
		const parent = realpathSync.native(mkdtempSync(join(tmpdir(), "pylon-historical-copy-")));
		const source = join(parent, "source");
		mkdirSync(source, { mode: 0o700 });
		const assets = ["pylon-prime-agent-0.8.1.tgz", "pylon-prime-agent-ai-0.8.1.tgz", "pylon-prime-agent-core-0.8.1.tgz", "pylon-prime-agent-tui-0.8.1.tgz"];
		for (const name of assets) writeFileSync(join(source, name), name, { mode: 0o444 });
		writeFileSync(join(source, "pylon-prime-agent-release-v1.json"), `${JSON.stringify({ assets: assets.map((file) => ({ file })) })}\n`, { mode: 0o444 });
		writeFileSync(join(source, "pylon-preview-channel-v1.json"), "channel\n", { mode: 0o444 });
		for (const name of readdirSync(source)) chmodSync(join(source, name), 0o444);
		try {
			const preview = releaseOnlyCopy(source, join(parent, "preview"), { historical: false });
			assert.equal(preview.names.length, 5);
			assert.equal(preview.names.includes("pylon-preview-channel-v1.json"), false);
			const historical = releaseOnlyCopy(source, join(parent, "historical"), { historical: true });
			assert.equal(historical.names.length, 6);
			assert.equal(historical.names.includes("pylon-preview-channel-v1.json"), true);
		} finally { clean(parent); }
	}
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
	assert.equal(CREATE_GITHUB_APP_TOKEN_ACTION, "actions/create-github-app-token@fee1f7d63c2ff003460e3d139729b119787bc349");
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
		preview.replace("private-key: ${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}", "private-key: ''"),
		preview.replace("private-key: ${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}", "private-key: ${{ env.APP_KEY }}"),
		preview.replace("permission-administration: read", "permission-administration: write"),
		preview.replace("permission-administration: read", "permission-administration: read\n          permission-contents: read"),
		preview.replace("permission-administration: read", "permission-administration: read\n          github-api-url: https://api.github.com"),
		preview.replace("permission-administration: read", "permission-administration: read\n          skip-token-revoke: true"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor\n        id: decoy"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor\n        continue-on-error: true"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor\n        env:\n          APP_KEY: ${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor\n        run: echo ${{ steps.ruleset-auditor.outputs.token }}"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor\n        outputs:\n          token: ${{ steps.ruleset-auditor.outputs.token }}"),
		preview.replace("id: ruleset-auditor", "id: ruleset-auditor # trusted"),
		preview.replace("        with:\n          app-id:", "        with: &auditor\n          app-id:"),
		preview.replace("        with:\n          app-id:", "        with: *auditor\n          app-id:"),
		preview.replace("          owner: pylon-code", "          owner: pylon-code\n          owner: attacker"),
		preview.replace(CREATE_GITHUB_APP_TOKEN_ACTION, `${CREATE_GITHUB_APP_TOKEN_ACTION} # mutable comment`),
		preview.replace(CREATE_GITHUB_APP_TOKEN_ACTION, "actions/create-github-app-token@" + "f".repeat(40)),
		preview.replace("      - name: Require live pylon immediately", `      # decoy ${CREATE_GITHUB_APP_TOKEN_ACTION}\n      - name: Require live pylon immediately`),
		preview.replace("github-token: ${{ steps.ruleset-auditor.outputs.token }}", "github-token: ${{ fromJSON(steps.ruleset-auditor.outputs.token) }}"),
		preview.replace("github-token: ${{ steps.ruleset-auditor.outputs.token }}", "github-token: ${{ steps.ruleset-auditor.outputs.token }}\n          token: ${{ steps.ruleset-auditor.outputs.token }}"),
		preview.replace("      - name: Create or refetch the exact protected preview tag", "      - name: Token exposure\n        env:\n          TOKEN: ${{ steps.ruleset-auditor.outputs.token }}\n        run: node scripts/untrusted.mjs\n      - name: Create or refetch the exact protected preview tag"),
		preview.replace("      - name: Create or refetch the exact protected preview tag", "      - name: Indirect step exposure\n        env:\n          STEPS: ${{ toJSON(steps) }}\n      - name: Create or refetch the exact protected preview tag"),
		preview.replace("      - name: Create or refetch the exact protected preview tag", "      - name: Bracket step exposure\n        env:\n          TOKEN: ${{ steps['ruleset-' + 'auditor'].outputs.token }}\n      - name: Create or refetch the exact protected preview tag"),
		preview.replace("github-token: ${{ steps.ruleset-auditor.outputs.token }}", "github-token: ${{\n            steps.ruleset-auditor.outputs.token\n          }}"),
		preview.replace("      - name: Require authoritative publication tag ruleset before preview tag CAS\n        uses:", "      - name: Require authoritative publication tag ruleset before preview tag CAS\n        continue-on-error: true\n        uses:"),
		preview.replace("      - name: Require authoritative publication tag ruleset before preview tag CAS\n        uses:", "      - name: Require authoritative publication tag ruleset before preview tag CAS\n        if: false\n        uses:"),
		preview.replace("          script: |\n            const response = await github.request", "          script: |\n            core.setOutput('token', await github.auth());\n            const response = await github.request"),
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
			const protectedMutationWriter = new Set([
				".github/workflows/pylon-preview-release.yml:stage-draft",
				".github/workflows/pylon-preview-release.yml:publish",
				".github/workflows/pylon-stable-release.yml:publish",
			]).has(identity);
			if (protectedMutationWriter) {
				assert.match(block, new RegExp(`^    environment: pylon-${file.includes("preview") ? "preview" : "stable"}$`, "m"));
				assert.match(block, new RegExp(CREATE_GITHUB_APP_TOKEN_ACTION.replaceAll("/", "\\/")));
				assert.match(block, /app-id: \$\{\{ vars\.PYLON_RULESET_AUDITOR_APP_ID \}\}/);
				assert.match(block, /private-key: \$\{\{ secrets\.PYLON_RULESET_AUDITOR_PRIVATE_KEY \}\}/);
				assert.match(block, /permission-administration: read/);
				assert.doesNotMatch(block, /skip-token-revoke:|permission-contents:|continue-on-error:/);
			}
		}
	}
	assert.deepEqual(foundWriters, approvedWriters);
	const previewJobs = blocks(preview);
	assert.ok(needs(previewJobs.get("stage-draft"), "verify-attestation"));
	assert.match(previewJobs.get("stage-draft"), /^    environment: pylon-preview$/m);
	assert.ok(needs(previewJobs.get("publish"), "stage-draft"));
	assert.match(previewJobs.get("publish"), /^    environment: pylon-preview$/m);
	assert.ok(needs(previewJobs.get("publish"), "verify-attestation"));
	const stableJobs = blocks(stable);
	assert.ok(needs(stableJobs.get("stage-draft"), "verify-attestation"));
	assert.ok(needs(stableJobs.get("publish"), "stage-draft"));
	assert.ok(needs(stableJobs.get("publish"), "authorize-stable-resume"));
	assert.match(stableJobs.get("publish"), /^    environment: pylon-stable$/m);
	assert.match(stableJobs.get("authorize-stable-resume"), /environment: pylon-stable/);
	assert.match(stableJobs.get("authorize-stable-resume"), /permissions: \{\}/);
	assert.match(stableJobs.get("attest"), /if: .*mode == 'normal'/);
	assert.match(stableJobs.get("authorize-stable-resume"), /if: .*mode == 'resume'/);
	const upstream = blocks(workflows.get(".github/workflows/pylon-upstream-sync.yml"));
	assert.match(upstream.get("sync"), /environment: pylon-upstream-sync/);
	assert.doesNotMatch(workflows.get(".github/workflows/pylon-upstream-sync.yml"), /authorize-upstream-sync/);
	assert.match(stable, /group: pylon-stable-publication[\s\S]*cancel-in-progress: false/);
	assert.match(stable, /final publish ruleset audit/i);
	assert.equal((stable.match(/github-token: \$\{\{ steps\.ruleset-auditor\.outputs\.token \}\}/g) ?? []).length, 3);
	assert.equal((preview.match(/github-token: \$\{\{ steps\.ruleset-auditor\.outputs\.token \}\}/g) ?? []).length, 2);
	assert.match(stable, /refetched state and stopped without N\+1, move, or delete/);
	assert.match(stable, /Draft release: \$\{draft\.id\}/);
	assert.match(stable, /Withdraw build tag:/);
	assert.match(stable, /final stable tag CAS/);
	assert.doesNotMatch(stable, /manifest\.sequence\s*\+\+|updateRef|deleteRef|deleteRelease|deleteReleaseAsset/);
	const stepBlocks = (job) => {
		const matches = [...job.matchAll(/^      - name: (.+)$/gm)];
		return matches.map((match, index) => ({
			name: match[1],
			block: job.slice(match.index, matches[index + 1]?.index ?? job.length),
		}));
	};
	const mutationInventory = [];
	for (const [workflow, jobs] of [["preview", previewJobs], ["stable", stableJobs]]) {
		for (const [jobName, job] of jobs) {
			const steps = stepBlocks(job);
			for (let index = 0; index < steps.length; index += 1) {
				if (!/github\.rest\.git\.createRef|github\.rest\.repos\.updateRelease/.test(steps[index].block)) continue;
				mutationInventory.push(`${workflow}:${jobName}:${steps[index].name}`);
				assert.match(steps[index - 1].block, /github-token: \$\{\{ steps\.ruleset-auditor\.outputs\.token \}\}/,
					"each protected mutation needs an adjacent fresh App-authenticated audit");
				assert.doesNotMatch(steps[index].block, /ruleset-auditor\.outputs\.token|PYLON_RULESET_AUDITOR/,
					"the App token must not enter a contents-write step");
			}
		}
	}
	assert.deepEqual(mutationInventory.sort(), [
		"preview:publish:Publish the exact approved preview draft",
		"preview:stage-draft:Create or refetch the exact protected preview tag",
		"stable:publish:Create or refetch the exact protected stable tag",
		"stable:publish:Create the exact protected stable reservation ref",
		"stable:publish:Publish only the exact protected stable draft",
	].sort());
	const attestationVerifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	for (const flag of ["--cert-identity", "--signer-digest", "--source-ref", "--source-digest", "--cert-oidc-issuer", "--predicate-type", "--deny-self-hosted-runners"]) {
		assert.match(attestationVerifier, new RegExp(flag));
	}
	assert.match(attestationVerifier, /verifyApprovedWorkflowAtSignerDigest/);
});
