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

function artifactDirectory(args) {
	if (args.length === 0) return defaultArtifacts;
	if (args.length === 2 && args[0] === "--artifact-dir") return resolve(root, args[1]);
	throw new Error("Usage: node scripts/prepare-pylon-preview-manifest.mjs [--artifact-dir path]");
}

try {
	const artifactsDir = artifactDirectory(process.argv.slice(2));
	const releaseManifestBytes = readFileSync(join(artifactsDir, PYLON_RELEASE_MANIFEST));
	const releaseManifest = JSON.parse(releaseManifestBytes);
	const previewManifest = createPreviewManifest(releaseManifest, releaseManifestBytes, {
		sequenceEpoch: 1,
		sequence: Number(process.env.GITHUB_RUN_NUMBER),
		workflowRunId: process.env.GITHUB_RUN_ID ?? "",
	});
	writeFileSync(join(artifactsDir, PYLON_PREVIEW_MANIFEST), canonicalJson(previewManifest));
	console.log(`Created ${join(artifactsDir, PYLON_PREVIEW_MANIFEST)}`);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
