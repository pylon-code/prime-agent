import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
	publicationReleaseBody,
	PYLON_PREVIEW_MANIFEST,
	PYLON_PREVIEW_WORKFLOW,
	PYLON_PUBLICATION_REF,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_MANIFEST,
	sha256Bytes,
	stableSequenceReservationTag,
	parseStableSequenceReservationTag,
	stableTag,
	validateAttestationEvidence,
	validateMergedChangelogProof,
	validatePreviewManifest,
	validateRequiredChecks,
	validateStableHistory,
	validateStableManifest,
	validateWorkflowArtifactProvenance,
} from "./lib/pylon-publication.mjs";
import { verifyGhAttestationResult } from "./verify-pylon-publication-attestations.mjs";
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";

const root = resolve(import.meta.dirname, "..");
const source = {
	repository: "https://github.com/pylon-code/prime-agent",
	commit: "0123456789abcdef0123456789abcdef01234567",
	tree: "89abcdef0123456789abcdef0123456789abcdef",
};
const version = "0.8.1";

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
	const preview = createPreviewManifest(release, releaseBytes);
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
		promotion: { kind: "promote", policyCommit: source.commit, policyTree: source.tree },
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
		promotion: options.withdraw ? { kind: "withdraw", policyCommit: source.commit, policyTree: source.tree, revocation } : { kind: "promote", policyCommit: source.commit, policyTree: source.tree },
	});
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

test("stable history is contiguous, previous-digest chained, high-water marked, sorted, and append-only", () => {
	const first = firstStable();
	const second = secondStable(first, { withdraw: true });
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

test("stable manifest nested schema rejects extras, malformed identities, unsafe assets, duplicates, and ordering changes", () => {
	const stable = firstStable();
	assert.equal(validateStableManifest(stable), stable);
	for (const mutate of [
		(value) => (value.build.extra = true),
		(value) => (value.build.source.extra = true),
		(value) => (value.build.source.repository = "https://github.com/fork/prime-agent"),
		(value) => (value.build.source.commit = "abc"),
		(value) => (value.build.source.tree = "abc"),
		(value) => (value.build.releaseManifest.file = "other.json"),
		(value) => (value.build.previewManifest.file = "other.json"),
		(value) => (value.build.assets[0].file = "../escape.tgz"),
		(value) => (value.build.assets[0].file = "pylon-prime-agent-ai-9.9.9.tgz"),
		(value) => (value.build.assets[0].size = 0),
		(value) => value.build.assets.push(structuredClone(value.build.assets[0])),
		(value) => value.build.assets.reverse(),
		(value) => (value.promotion.policyTree = "abc"),
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
		head_sha: source.commit,
		app: { id: GITHUB_ACTIONS_APP_ID },
		status: "completed",
		conclusion: "success",
	}));
	assert.equal(validateRequiredChecks({ sourceSha: source.commit, requiredChecks, checkRuns }), true);
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

test("gh verification result requires the exact subject digest and Rekor inclusion", () => {
	const subject = { name: "artifact.tgz", sha256: "a".repeat(64) };
	const output = JSON.stringify([{ verificationResult: {
		statement: { predicateType: "https://slsa.dev/provenance/v1", subject: [{ name: subject.name, digest: { sha256: subject.sha256 } }] },
		verifiedTimestamps: [{ type: "Tlog", uri: "https://rekor.sigstore.dev", timestamp: "2026-01-01T00:00:00Z" }],
	} }]);
	assert.equal(verifyGhAttestationResult(output, subject), true);
	assert.throws(() => verifyGhAttestationResult(output, { ...subject, sha256: "f".repeat(64) }), /subject/);
	const noRekor = output.replace("Tlog", "TimestampAuthority");
	assert.throws(() => verifyGhAttestationResult(noRekor, subject), /Rekor/);
	const parsed = JSON.parse(output);
	parsed[0].verificationResult.statement.subject.push({ name: "extra", digest: { sha256: "b".repeat(64) } });
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(parsed), subject), /subject set/);
	parsed[0].verificationResult.statement.subject[1] = structuredClone(parsed[0].verificationResult.statement.subject[0]);
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(parsed), subject), /duplicate|subject set/);
	parsed[0].verificationResult.statement.subject = [{ name: subject.name, digest: { sha256: subject.sha256, sha512: "c".repeat(128) } }];
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(parsed), subject), /malformed subject/);
	parsed[0].verificationResult.statement.subject = [{ name: subject.name, digest: { sha256: subject.sha256 } }];
	parsed[0].verificationResult.statement.predicateType = "https://example.invalid/predicate";
	assert.throws(() => verifyGhAttestationResult(JSON.stringify(parsed), subject), /predicate/);
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
		const preview = createPreviewManifest(release, releaseBytes);
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

test("workflow static policy pins actions, splits write and OIDC, and never executes source in publishers", () => {
	const workflows = [
		".github/workflows/changelog-merged-proof.yml",
		".github/workflows/pylon-preview-release.yml",
		".github/workflows/pylon-stable-release.yml",
	].map((file) => [file, readFileSync(join(root, file), "utf8")]);
	for (const [file, workflow] of workflows) {
		for (const match of workflow.matchAll(/^\s*uses:\s*[^\s@]+@([^\s#]+)/gm)) {
			assert.match(match[1], /^[0-9a-f]{40}$/, `${file} contains an unpinned action`);
		}
		assert.doesNotMatch(workflow, /secrets\./);
	}
	const preview = workflows[1][1];
	const stable = workflows[2][1];
	assert.match(preview, /environment: pylon-preview/);
	assert.match(stable, /environment: pylon-stable/);
	assert.match(preview, /actions\/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8/);
	assert.match(stable, /actions\/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8/);
	assert.match(preview, /subject-path: \.npm\/pylon-release\/artifacts\/\*/);
	const previewPublisher = preview.slice(preview.indexOf("  publish:"));
	const stablePublisher = stable.slice(stable.lastIndexOf("\n  publish:\n"));
	for (const publisher of [previewPublisher, stablePublisher]) {
		assert.doesNotMatch(publisher, /actions\/checkout|actions\/setup-node|npm (?:ci|run|install)|node scripts\/|tar -|\.tgz\b.*(?:exec|run)/);
		assert.match(publisher, /contents: write/);
		assert.doesNotMatch(publisher, /id-token: write|attestations: write/);
	}
	assert.match(preview, /attestations: write[\s\S]*id-token: write|id-token: write[\s\S]*attestations: write/);
	assert.match(stable, /concurrency:[\s\S]*group: pylon-stable-publication[\s\S]*cancel-in-progress: false/);
	assert.match(stable, /if \(error\.status !== 404\) throw error;/);
	assert.match(preview, /Validate exact subjects before signing[\s\S]*Generate build provenance for exactly six subjects/);
	assert.match(stable, /Validate exact stable subject before signing[\s\S]*Generate stable manifest provenance/);
	assert.match(preview, /verify-attestation:[\s\S]*Verify exact signer, subjects, source, and Rekor evidence/);
	assert.match(preview, /Preview publication became stale while verification ran/);
	assert.match(stable, /Immutable preview identity changed before stable publication/);
	const attestationVerifier = readFileSync(join(root, "scripts/verify-pylon-publication-attestations.mjs"), "utf8");
	for (const flag of ["--cert-identity", "--signer-digest", "--source-ref", "--source-digest", "--cert-oidc-issuer", "--predicate-type", "--deny-self-hosted-runners"]) {
		assert.match(attestationVerifier, new RegExp(flag));
	}
	assert.match(attestationVerifier, /source-tree/);
	assert.match(attestationVerifier, /statement subject set/);
	assert.match(stable, /pylon-stable-sequence-\$\{String\(manifest\.sequence\)/);
	assert.match(stable, /github\.rest\.git\.createRef/);
	assert.match(stable, /Stable sequence reservation raced \(422\)/);
	assert.match(stable, /target_commitish: manifest\.promotion\.policyCommit/);
	assert.match(stable, /github\.rest\.git\.createTag/);
	assert.match(stable, /annotation\.object\.sha !== manifest\.promotion\.policyCommit/);
	assert.match(stable, /different policy, build, or manifest identity/);
	assert.match(stable, /globally unique sequence/);
	assert.doesNotMatch(stable, /manifest\.sequence\s*\+\+/);
});
