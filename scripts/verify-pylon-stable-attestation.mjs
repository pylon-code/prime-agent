#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
	readBoundedRegularFileSync,
} from "./lib/pylon-bounded-file.mjs";
import {
	canonicalJson,
	PYLON_PUBLICATION_REF,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_MANIFEST,
	PYLON_STABLE_WORKFLOW,
	sha256Bytes,
	validateStableManifest,
} from "./lib/pylon-publication.mjs";
import { verifyApprovedWorkflowAtSignerDigest } from "./lib/pylon-workflow-policy.mjs";
import { verifyGhAttestationResult } from "./verify-pylon-publication-attestations.mjs";

function parseArgs(args) {
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) values.set(args[index], args[index + 1]);
	const path = resolve(values.get("--manifest") ?? PYLON_STABLE_MANIFEST);
	const sourceSha = values.get("--promotion-sha") ?? "";
	const sourceTree = values.get("--promotion-tree") ?? "";
	if (!/^[0-9a-f]{40}$/.test(sourceSha)) throw new Error("--promotion-sha must be a full lowercase Git SHA.");
	if (!/^[0-9a-f]{40}$/.test(sourceTree)) throw new Error("--promotion-tree must be a full lowercase Git tree SHA.");
	return { path, sourceSha, sourceTree };
}

export function verifyStableAttestation(path, sourceSha, sourceTree) {
	const bytes = readBoundedRegularFileSync(path, {
		maxBytes: PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
		description: "Stable attestation manifest",
	});
	if (bytes === null) throw new Error("Stable attestation manifest does not exist.");
	const manifest = validateStableManifest(JSON.parse(bytes));
	if (canonicalJson(manifest) !== bytes.toString("utf8")) throw new Error("Stable manifest is not canonical publication JSON.");
	if (manifest.promotion.policyCommit !== sourceSha || manifest.promotion.policyTree !== sourceTree) {
		throw new Error("Promotion commit/tree does not match the signed stable policy identity.");
	}
	const subject = { name: PYLON_STABLE_MANIFEST, sha256: sha256Bytes(bytes) };
	verifyApprovedWorkflowAtSignerDigest(
		PYLON_STABLE_WORKFLOW,
		sourceSha,
		"stable",
		manifest.promotion.publicationPolicyRevision,
	);
	const result = spawnSync(
		"gh",
		[
			"attestation", "verify", path,
			"--repo", PYLON_PUBLICATION_REPOSITORY,
			"--cert-identity", `https://github.com/${PYLON_PUBLICATION_REPOSITORY}/${PYLON_STABLE_WORKFLOW}@${PYLON_PUBLICATION_REF}`,
			"--signer-digest", sourceSha,
			"--source-ref", PYLON_PUBLICATION_REF,
			"--source-digest", sourceSha,
			"--cert-oidc-issuer", "https://token.actions.githubusercontent.com",
			"--predicate-type", "https://slsa.dev/provenance/v1",
			"--deny-self-hosted-runners",
			"--limit", "100",
			"--format", "json",
		],
		{ encoding: "utf8", timeout: 120_000, maxBuffer: 16 * 1024 * 1024 },
	);
	if (result.error) throw result.error;
	if (result.status !== 0) throw new Error(`gh attestation verify failed for the stable manifest: ${result.stderr}`);
	verifyGhAttestationResult(result.stdout, [subject], {
		repository: PYLON_PUBLICATION_REPOSITORY,
		workflow: PYLON_STABLE_WORKFLOW,
		event: "workflow_dispatch",
		sourceSha,
	});
	return manifest;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const manifest = verifyStableAttestation(args.path, args.sourceSha, args.sourceTree);
		console.log(`Verified stable manifest provenance for ${manifest.tag}.`);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
