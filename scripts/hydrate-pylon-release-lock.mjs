#!/usr/bin/env node

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	assertCleanSource,
	assertPinnedToolchain,
	assertPylonRepository,
	isCanonicalSha512Integrity,
	readCompletePackageLock,
	runNpm,
} from "./lib/pylon-release.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const lockPath = join(root, "package-lock.json");

function packageNameFromLockPath(path) {
	const tail = path.slice(path.lastIndexOf("node_modules/") + "node_modules/".length);
	const parts = tail.split("/");
	return parts[0].startsWith("@") ? `${parts[0]}/${parts[1]}` : parts[0];
}

function withoutDistributionMetadata(lock) {
	const copy = structuredClone(lock);
	for (const entry of Object.values(copy.packages ?? {})) {
		delete entry.resolved;
		delete entry.integrity;
	}
	return copy;
}

function insertDistributionMetadata(entry, resolved, integrity) {
	const hydrated = {};
	for (const [key, value] of Object.entries(entry)) {
		hydrated[key] = value;
		if (key === "version") {
			hydrated.resolved = resolved;
			hydrated.integrity = integrity;
		}
	}
	return hydrated;
}

try {
	assertCleanSource(root, { rejectIgnoredInputs: true });
	assertPylonRepository(root);
	assertPinnedToolchain(root);
	const lock = JSON.parse(readFileSync(lockPath, "utf8"));
	const beforeGraph = JSON.stringify(withoutDistributionMetadata(lock));
	const missing = Object.entries(lock.packages ?? {}).filter(
		([path, entry]) =>
			path.startsWith("node_modules/") &&
			!entry.link &&
			typeof entry.version === "string" &&
			(typeof entry.resolved !== "string" || typeof entry.integrity !== "string"),
	);
	for (const [path, entry] of missing) {
		const packageName = packageNameFromLockPath(path);
		const output = runNpm(["view", `${packageName}@${entry.version}`, "dist", "--json"], { cwd: root });
		const dist = JSON.parse(output);
		if (
			typeof dist.tarball !== "string" ||
			!dist.tarball.startsWith("https://registry.npmjs.org/") ||
			typeof dist.integrity !== "string" ||
			!isCanonicalSha512Integrity(dist.integrity) ||
			(entry.resolved !== undefined && entry.resolved !== dist.tarball) ||
			(entry.integrity !== undefined && entry.integrity !== dist.integrity)
		) {
			throw new Error(`Registry metadata mismatch for ${packageName}@${entry.version}.`);
		}
		lock.packages[path] = insertDistributionMetadata(entry, dist.tarball, dist.integrity);
	}
	if (JSON.stringify(withoutDistributionMetadata(lock)) !== beforeGraph) {
		throw new Error("Lock hydration changed the package graph.");
	}
	if (missing.length > 0) writeFileSync(lockPath, `${JSON.stringify(lock, null, "\t")}\n`);
	readCompletePackageLock(root);
	console.log(`Hydrated ${missing.length} exact lockfile package records.`);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
