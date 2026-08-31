#!/usr/bin/env node

import { lstatSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

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
} from "./lib/pylon-publication.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultArtifacts = join(root, ".npm", "pylon-release", "artifacts");

function artifactDirectory(args) {
	if (args.length === 0) return defaultArtifacts;
	if (args.length === 2 && args[0] === "--artifact-dir") return resolve(root, args[1]);
	throw new Error("Usage: node scripts/verify-pylon-preview-publication.mjs [--artifact-dir path]");
}

export function verifyPreviewPublication(artifactsDir) {
	const releaseBytes = readFileSync(join(artifactsDir, PYLON_RELEASE_MANIFEST));
	const releaseManifest = validateReleaseManifest(JSON.parse(releaseBytes));
	const previewBytes = readFileSync(join(artifactsDir, PYLON_PREVIEW_MANIFEST));
	const previewManifest = JSON.parse(previewBytes);
	if (canonicalJson(previewManifest) !== previewBytes.toString("utf8")) {
		throw new Error("Preview manifest is not canonical publication JSON.");
	}
	validatePreviewManifest(previewManifest, releaseManifest, releaseBytes);
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
		const verified = verifyPreviewPublication(artifactDirectory(process.argv.slice(2)));
		console.log(
			JSON.stringify({
				tag: verified.previewManifest.build.tag,
				source: verified.previewManifest.build.source,
				recipeRevision: verified.previewManifest.build.recipeRevision,
				subjects: verified.subjects,
			}),
		);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
