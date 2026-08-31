#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	canonicalJson,
	PYLON_PREVIEW_WORKFLOW,
	PYLON_PUBLICATION_REF,
	PYLON_PUBLICATION_REPOSITORY,
} from "./lib/pylon-publication.mjs";
import { verifyApprovedWorkflowAtSignerDigest } from "./lib/pylon-workflow-policy.mjs";
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(args) {
	const historicalIndex = args.indexOf("--historical");
	const historical = historicalIndex !== -1;
	const remaining = args.filter((_, index) => index !== historicalIndex);
	const values = new Map();
	for (let index = 0; index < remaining.length; index += 2) {
		const name = remaining[index];
		const value = remaining[index + 1];
		if (!name?.startsWith("--") || value === undefined) throw new Error("Attestation verification arguments must be name/value pairs.");
		values.set(name, value);
	}
	const artifactDir = resolve(root, values.get("--artifact-dir") ?? ".npm/pylon-stable/preview");
	const sourceSha = values.get("--source-sha") ?? "";
	const sourceTree = values.get("--source-tree") ?? "";
	if (!/^[0-9a-f]{40}$/.test(sourceSha)) throw new Error("--source-sha must be a full lowercase Git SHA.");
	if (!/^[0-9a-f]{40}$/.test(sourceTree)) throw new Error("--source-tree must be a full lowercase Git tree SHA.");
	return { artifactDir, sourceSha, sourceTree, historical };
}

export function verifyGhAttestationResult(output, expectedSubjects, expectedInvocation) {
	const expected = Array.isArray(expectedSubjects) ? expectedSubjects : [expectedSubjects];
	if (expected.length === 0) throw new Error("Attestation policy needs at least one exact subject.");
	const expectedSet = expected
		.map((subject) => ({ name: subject.name, digest: { sha256: subject.sha256 } }))
		.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
	if (new Set(expectedSet.map((subject) => subject.name)).size !== expectedSet.length) {
		throw new Error("Expected attestation subject set contains a duplicate name.");
	}
	const results = JSON.parse(output.replace(/\u001b\[[0-9;]*m/g, ""));
	if (!Array.isArray(results) || results.length === 0) throw new Error("No verified attestation for the exact subject set.");
	const verifiedAttempts = new Set();
	for (const entry of results) {
		const verification = entry.verificationResult;
		const statement = verification?.statement;
		const subjects = statement?.subject;
		if (statement?.predicateType !== "https://slsa.dev/provenance/v1" || !Array.isArray(subjects)) {
			throw new Error("Attestation predicate is not exact SLSA provenance.");
		}
		if (expectedInvocation) {
			const predicate = statement.predicate;
			const definition = predicate?.buildDefinition;
			const workflow = definition?.externalParameters?.workflow;
			const github = definition?.internalParameters?.github;
			const dependency = definition?.resolvedDependencies;
			const invocationId = predicate?.runDetails?.metadata?.invocationId;
			const invocation = new RegExp(`^https://github\\.com/${expectedInvocation.repository.replace("/", "\\/")}/actions/runs/([1-9][0-9]*)/attempts/([1-9][0-9]*)$`).exec(invocationId ?? "");
			if (
				definition?.buildType !== "https://actions.github.io/buildtypes/workflow/v1" ||
				workflow?.repository !== `https://github.com/${expectedInvocation.repository}` ||
				workflow?.path !== expectedInvocation.workflow || workflow?.ref !== PYLON_PUBLICATION_REF ||
				github?.event_name !== expectedInvocation.event || String(github?.repository_id) !== "1349002285" ||
				github?.runner_environment !== "github-hosted" || !/^[1-9][0-9]*$/.test(String(github?.repository_owner_id ?? "")) ||
				!Array.isArray(dependency) || dependency.length !== 1 ||
				dependency[0]?.uri !== `git+https://github.com/${expectedInvocation.repository}@${PYLON_PUBLICATION_REF}` ||
				dependency[0]?.digest?.gitCommit !== expectedInvocation.sourceSha ||
				predicate?.runDetails?.builder?.id !== `https://github.com/${expectedInvocation.repository}/${expectedInvocation.workflow}@${PYLON_PUBLICATION_REF}` ||
				!invocation || invocation[1] !== expectedInvocation.workflowRunId
			) throw new Error("Attestation SLSA invocation does not bind the exact workflow run and source.");
			verifiedAttempts.add(invocation[2]);
		}
		const actualSet = subjects
			.map((subject) => {
				if (
					!subject || Object.keys(subject).sort().join(",") !== "digest,name" ||
					typeof subject.name !== "string" ||
					!subject.digest || Object.keys(subject.digest).join(",") !== "sha256" ||
					!/^[0-9a-f]{64}$/.test(subject.digest.sha256)
				) throw new Error("Attestation statement contains a malformed subject.");
				return { name: subject.name, digest: { sha256: subject.digest.sha256 } };
			})
			.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
		if (
			new Set(actualSet.map((subject) => subject.name)).size !== actualSet.length ||
			canonicalJson(actualSet) !== canonicalJson(expectedSet)
		) throw new Error("Attestation statement subject set has an extra, missing, duplicate, or changed subject.");
		const hasRekor =
			Array.isArray(verification?.verifiedTimestamps) &&
			verification.verifiedTimestamps.some(
				(timestamp) => timestamp.type === "Tlog" && /^https:\/\/rekor\.sigstore\.dev(?:\/|$)/.test(timestamp.uri ?? ""),
			);
		if (!hasRekor) throw new Error("Attestation lacks Sigstore public-good Rekor evidence.");
	}
	return expectedInvocation ? [...verifiedAttempts].sort((left, right) => Number(left) - Number(right)) : true;
}

function verifySubject(path, subject, allSubjects, sourceSha, expectedInvocation) {
	const result = spawnSync(
		"gh",
		[
			"attestation",
			"verify",
			path,
			"--repo",
			PYLON_PUBLICATION_REPOSITORY,
			"--cert-identity",
			`https://github.com/${PYLON_PUBLICATION_REPOSITORY}/${PYLON_PREVIEW_WORKFLOW}@${PYLON_PUBLICATION_REF}`,
			"--signer-digest",
			sourceSha,
			"--source-ref",
			PYLON_PUBLICATION_REF,
			"--source-digest",
			sourceSha,
			"--cert-oidc-issuer",
			"https://token.actions.githubusercontent.com",
			"--predicate-type",
			"https://slsa.dev/provenance/v1",
			"--deny-self-hosted-runners",
			"--limit",
			"100",
			"--format",
			"json",
		],
		{ encoding: "utf8", timeout: 120_000, maxBuffer: 16 * 1024 * 1024 },
	);
	if (result.error) throw result.error;
	if (result.status !== 0) throw new Error(`gh attestation verify failed for ${subject.name}: ${result.stderr}`);
	return verifyGhAttestationResult(result.stdout, allSubjects, expectedInvocation);
}

function ghJson(path) {
	const token = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
	if (!token) throw new Error("GH_TOKEN or GITHUB_TOKEN is required to verify workflow-run sequence evidence.");
	const result = spawnSync("gh", ["api", path], {
		encoding: "utf8", timeout: 120_000, maxBuffer: 16 * 1024 * 1024,
		env: { ...process.env, GH_TOKEN: token },
	});
	if (result.error) throw result.error;
	if (result.status !== 0) throw new Error(`Could not verify workflow-run evidence: ${result.stderr}`);
	return JSON.parse(result.stdout.replace(/\u001b\[[0-9;]*m/g, ""));
}

export function validatePreviewWorkflowRunEvidence({ run, suite, jobs }, previewManifest, attestedAttempts) {
	const expected = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		workflow: PYLON_PREVIEW_WORKFLOW,
		event: "push",
		sourceSha: previewManifest.build.source.commit,
		workflowRunId: previewManifest.workflowRunId,
	};
	if (
		String(run.id) !== expected.workflowRunId || run.run_number !== previewManifest.sequence ||
		run.event !== expected.event || run.head_branch !== "pylon" || run.head_sha !== expected.sourceSha ||
		run.path !== expected.workflow || run.repository?.id !== 1_349_002_285 || run.repository?.full_name !== expected.repository ||
		run.head_repository?.id !== 1_349_002_285 || run.head_repository?.full_name !== expected.repository ||
		!run.check_suite_id || !["in_progress", "completed"].includes(run.status) ||
		(run.status === "completed" && run.conclusion !== "success")
	) throw new Error("Preview sequence does not match the exact canonical workflow run.");
	if (suite.app?.id !== 15368 || suite.head_sha !== expected.sourceSha || suite.id !== run.check_suite_id) {
		throw new Error("Preview workflow run is not owned by the GitHub Actions app on the exact source.");
	}
	if (!Array.isArray(attestedAttempts) || attestedAttempts.length === 0 || attestedAttempts.some((attempt) => !/^[1-9][0-9]*$/.test(attempt))) {
		throw new Error("Preview attestation has no exact workflow attempt evidence.");
	}
	for (const attempt of attestedAttempts) {
		const attesters = jobs?.filter((job) => job.name === "Approve and attest six preview subjects" && job.run_attempt === Number(attempt));
		if (attesters?.length !== 1 || attesters[0].status !== "completed" || attesters[0].conclusion !== "success") {
			throw new Error("Preview workflow run lacks its one successful directly approved attester job for the signed attempt.");
		}
	}
	return expected;
}

export function verifyPreviewWorkflowRun(previewManifest, attestedAttempts) {
	const run = ghJson(`repos/${PYLON_PUBLICATION_REPOSITORY}/actions/runs/${previewManifest.workflowRunId}`);
	const suite = ghJson(`repos/${PYLON_PUBLICATION_REPOSITORY}/check-suites/${run.check_suite_id}`);
	const jobs = ghJson(`repos/${PYLON_PUBLICATION_REPOSITORY}/actions/runs/${previewManifest.workflowRunId}/jobs?filter=all&per_page=100`).jobs;
	return validatePreviewWorkflowRunEvidence({ run, suite, jobs }, previewManifest, attestedAttempts);
}

export function verifyPreviewAttestations({ artifactDir, sourceSha, sourceTree, historical = false }) {
	const verified = verifyPreviewPublication(artifactDir, { historical });
	if (verified.previewManifest.build.source.commit !== sourceSha || verified.previewManifest.build.source.tree !== sourceTree) {
		throw new Error("Requested attestation source commit/tree does not match the preview manifest.");
	}
	const invocation = {
		repository: PYLON_PUBLICATION_REPOSITORY,
		workflow: PYLON_PREVIEW_WORKFLOW,
		event: "push",
		sourceSha,
		workflowRunId: verified.previewManifest.workflowRunId,
	};
	verifyApprovedWorkflowAtSignerDigest(PYLON_PREVIEW_WORKFLOW, sourceSha, "preview");
	const attempts = new Set();
	for (const subject of verified.subjects) {
		for (const attempt of verifySubject(join(artifactDir, subject.name), subject, verified.subjects, sourceSha, invocation)) attempts.add(attempt);
	}
	verifyPreviewWorkflowRun(verified.previewManifest, [...attempts]);
	return verified;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const verified = verifyPreviewAttestations(args);
		console.log(`Verified ${verified.subjects.length} exact preview attestations for ${args.sourceSha}.`);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
