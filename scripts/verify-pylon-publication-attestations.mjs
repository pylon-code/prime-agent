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
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(args) {
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) {
		const name = args[index];
		const value = args[index + 1];
		if (!name?.startsWith("--") || value === undefined) throw new Error("Attestation verification arguments must be name/value pairs.");
		values.set(name, value);
	}
	const artifactDir = resolve(root, values.get("--artifact-dir") ?? ".npm/pylon-stable/preview");
	const sourceSha = values.get("--source-sha") ?? "";
	const sourceTree = values.get("--source-tree") ?? "";
	if (!/^[0-9a-f]{40}$/.test(sourceSha)) throw new Error("--source-sha must be a full lowercase Git SHA.");
	if (!/^[0-9a-f]{40}$/.test(sourceTree)) throw new Error("--source-tree must be a full lowercase Git tree SHA.");
	return { artifactDir, sourceSha, sourceTree };
}

export function verifyGhAttestationResult(output, expectedSubjects) {
	const expected = Array.isArray(expectedSubjects) ? expectedSubjects : [expectedSubjects];
	if (expected.length === 0) throw new Error("Attestation policy needs at least one exact subject.");
	const expectedSet = expected
		.map((subject) => ({ name: subject.name, digest: { sha256: subject.sha256 } }))
		.sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0);
	if (new Set(expectedSet.map((subject) => subject.name)).size !== expectedSet.length) {
		throw new Error("Expected attestation subject set contains a duplicate name.");
	}
	const results = JSON.parse(output);
	if (!Array.isArray(results) || results.length === 0) throw new Error("No verified attestation for the exact subject set.");
	for (const entry of results) {
		const verification = entry.verificationResult;
		const statement = verification?.statement;
		const subjects = statement?.subject;
		if (statement?.predicateType !== "https://slsa.dev/provenance/v1" || !Array.isArray(subjects)) {
			throw new Error("Attestation predicate is not exact SLSA provenance.");
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
	return true;
}

function verifySubject(path, subject, allSubjects, sourceSha) {
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
	verifyGhAttestationResult(result.stdout, allSubjects);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const verified = verifyPreviewPublication(args.artifactDir);
		if (
			verified.previewManifest.build.source.commit !== args.sourceSha ||
			verified.previewManifest.build.source.tree !== args.sourceTree
		) {
			throw new Error("Requested attestation source commit/tree does not match the preview manifest.");
		}
		for (const subject of verified.subjects) {
			verifySubject(join(args.artifactDir, subject.name), subject, verified.subjects, args.sourceSha);
		}
		console.log(`Verified ${verified.subjects.length} exact preview attestations for ${args.sourceSha}.`);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
