#!/usr/bin/env node

import { lstatSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
	readBoundedRegularFileSync,
} from "./lib/pylon-bounded-file.mjs";
import {
	hashBytes,
	PYLON_RELEASE_MANIFEST,
	validateReleaseManifest,
} from "./lib/pylon-release.mjs";
import {
	canonicalJson,
	PYLON_PREVIEW_MANIFEST,
	sha256Bytes,
	validatePreviewManifest,
	validatePublishedReleaseManifest,
} from "./lib/pylon-publication.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultArtifacts = join(root, ".npm", "pylon-release", "artifacts");

function parseArgs(args) {
	const historicalIndex = args.indexOf("--historical");
	const historical = historicalIndex !== -1;
	const remaining = args.filter((_, index) => index !== historicalIndex);
	if (remaining.length === 0) return { artifactsDir: defaultArtifacts, historical };
	if (remaining.length === 2 && remaining[0] === "--artifact-dir") {
		return { artifactsDir: resolve(root, remaining[1]), historical };
	}
	throw new Error("Usage: node scripts/verify-pylon-preview-publication.mjs [--historical] [--artifact-dir path]");
}

export function verifyPreviewPublication(artifactsDir, { historical = false } = {}) {
	const releaseBytes = readBoundedRegularFileSync(join(artifactsDir, PYLON_RELEASE_MANIFEST), {
		maxBytes: PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
		description: "Release manifest",
	});
	if (releaseBytes === null) throw new Error("Release manifest does not exist.");
	const releaseManifest = JSON.parse(releaseBytes);
	if (historical) validatePublishedReleaseManifest(releaseManifest);
	else validateReleaseManifest(releaseManifest);
	const previewBytes = readBoundedRegularFileSync(join(artifactsDir, PYLON_PREVIEW_MANIFEST), {
		maxBytes: PYLON_PUBLICATION_MANIFEST_MAX_BYTES,
		description: "Preview manifest",
	});
	if (previewBytes === null) throw new Error("Preview manifest does not exist.");
	const previewManifest = JSON.parse(previewBytes);
	if (canonicalJson(previewManifest) !== previewBytes.toString("utf8")) {
		throw new Error("Preview manifest is not canonical publication JSON.");
	}
	validatePreviewManifest(previewManifest, releaseManifest, releaseBytes, { historical });
	const expectedFiles = new Set([
		PYLON_RELEASE_MANIFEST,
		PYLON_PREVIEW_MANIFEST,
		...releaseManifest.assets.map((asset) => asset.file),
	]);
	for (const file of readdirSync(artifactsDir)) {
		if (!expectedFiles.delete(file)) throw new Error(`Unexpected preview publication subject: ${file}`);
		if (!lstatSync(join(artifactsDir, file)).isFile()) {
			throw new Error(`Preview publication subject is not one regular file: ${file}`);
		}
	}
	if (expectedFiles.size > 0) throw new Error(`Missing preview publication subject: ${[...expectedFiles].join(", ")}`);
	for (const asset of releaseManifest.assets) {
		const bytes = readFileSync(join(artifactsDir, asset.file));
		if (
			statSync(join(artifactsDir, asset.file)).size !== asset.size ||
			hashBytes(bytes, "sha256") !== asset.sha256 ||
			hashBytes(bytes, "sha512") !== asset.sha512
		) {
			throw new Error(`Preview artifact digest mismatch for ${asset.file}.`);
		}
	}
	return {
		releaseManifest,
		previewManifest,
		subjects: [
		...releaseManifest.assets.map((asset) => ({ name: asset.file, sha256: asset.sha256 })),
		{ name: PYLON_RELEASE_MANIFEST, sha256: sha256Bytes(releaseBytes) },
		{ name: PYLON_PREVIEW_MANIFEST, sha256: sha256Bytes(previewBytes) },
	].sort((left, right) => left.name.localeCompare(right.name)),
	};
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const verified = verifyPreviewPublication(args.artifactsDir, { historical: args.historical });
		console.log(
			JSON.stringify({
				tag: verified.previewManifest.build.tag,
				source: verified.previewManifest.build.source,
				recipeRevision: verified.previewManifest.build.recipeRevision,
				publicationPolicyRevision: verified.previewManifest.publicationPolicyRevision,
				subjects: verified.subjects,
			}),
		);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
