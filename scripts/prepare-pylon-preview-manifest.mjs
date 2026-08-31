#!/usr/bin/env node

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { PYLON_RELEASE_MANIFEST } from "./lib/pylon-release.mjs";
import {
	canonicalJson,
	createPreviewManifest,
	PYLON_PREVIEW_MANIFEST,
} from "./lib/pylon-publication.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultArtifacts = join(root, ".npm", "pylon-release", "artifacts");

function parseArgs(args) {
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) {
		const name = args[index];
		const value = args[index + 1];
		if (!name?.startsWith("--") || value === undefined) throw new Error("Preview preparation arguments must be name/value pairs.");
		values.set(name, value);
	}
	if ([...values.keys()].some((name) => !["--artifact-dir", "--publication-policy-revision"].includes(name))) {
		throw new Error("Unknown preview preparation argument.");
	}
	const publicationPolicyRevision = Number(values.get("--publication-policy-revision"));
	if (!Number.isSafeInteger(publicationPolicyRevision) || publicationPolicyRevision < 1) {
		throw new Error("Preview preparation requires an exact positive --publication-policy-revision.");
	}
	return {
		artifactsDir: resolve(root, values.get("--artifact-dir") ?? defaultArtifacts),
		publicationPolicyRevision,
	};
}

try {
	const { artifactsDir, publicationPolicyRevision } = parseArgs(process.argv.slice(2));
	const releaseManifestBytes = readFileSync(join(artifactsDir, PYLON_RELEASE_MANIFEST));
	const releaseManifest = JSON.parse(releaseManifestBytes);
	const previewManifest = createPreviewManifest(releaseManifest, releaseManifestBytes, {
		sequenceEpoch: 1,
		sequence: Number(process.env.GITHUB_RUN_NUMBER),
		workflowRunId: process.env.GITHUB_RUN_ID ?? "",
		publicationPolicyRevision,
	});
	writeFileSync(join(artifactsDir, PYLON_PREVIEW_MANIFEST), canonicalJson(previewManifest));
	console.log(`Created ${join(artifactsDir, PYLON_PREVIEW_MANIFEST)}`);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
