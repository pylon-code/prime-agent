#!/usr/bin/env node

import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	assertCleanSource,
	assertPylonRepository,
	gitSource,
	hashBytes,
	hashFile,
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_REPOSITORY_GIT,
	readCompletePackageLock,
	readTarJson,
	validateInternalPackageGraph,
	validateLockedRegistryGraph,
	validateReleaseManifest,
} from "./lib/pylon-release.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultArtifacts = join(root, ".npm", "pylon-release", "artifacts");

function parseArgs(args) {
	if (args.length === 0) return defaultArtifacts;
	if (args.length === 2 && args[0] === "--artifact-dir") return resolve(root, args[1]);
	throw new Error("Usage: node scripts/verify-pylon-prime-agent-release.mjs [--artifact-dir path]");
}

export function verifyPylonPrimeAgentRelease(artifactsDir) {
	assertCleanSource(root, { rejectIgnoredInputs: true });
	assertPylonRepository(root);
	const manifestPath = join(artifactsDir, PYLON_RELEASE_MANIFEST);
	const manifest = validateReleaseManifest(JSON.parse(readFileSync(manifestPath, "utf8")));
	const checkoutSource = gitSource(root);
	if (
		manifest.source.repository !== checkoutSource.repository ||
		manifest.source.commit !== checkoutSource.commit ||
		manifest.source.tree !== checkoutSource.tree
	) {
		throw new Error("Release manifest does not match the exact source checkout.");
	}
	const packageLock = readCompletePackageLock(root);
	if (manifest.build.lockfile.sha256 !== hashFile(join(root, "package-lock.json"), "sha256")) {
		throw new Error("Release manifest does not match the committed package lock.");
	}
	const expectedFiles = new Set([PYLON_RELEASE_MANIFEST, ...manifest.assets.map((asset) => asset.file)]);
	for (const file of readdirSync(artifactsDir)) {
		if (!expectedFiles.delete(file)) throw new Error(`Unexpected release output: ${file}`);
	}
	if (expectedFiles.size > 0) throw new Error(`Missing release output: ${[...expectedFiles].join(", ")}`);

	const packageArtifacts = new Map();
	for (const asset of manifest.assets) {
		const archive = join(artifactsDir, asset.file);
		const bytes = readFileSync(archive);
		if (
			statSync(archive).size !== asset.size ||
			hashBytes(bytes, "sha256") !== asset.sha256 ||
			hashBytes(bytes, "sha512") !== asset.sha512
		) {
			throw new Error(`Release digest mismatch for ${asset.file}.`);
		}
		const packageJson = readTarJson(archive, "package/package.json");
		const shrinkwrap = readTarJson(archive, "package/npm-shrinkwrap.json");
		if (
			packageJson.name !== asset.package ||
			packageJson.version !== manifest.package.version ||
			packageJson.repository?.url !== PYLON_RELEASE_REPOSITORY_GIT ||
			JSON.stringify(packageJson.pylonDistribution) !==
				JSON.stringify({
					schemaVersion: manifest.schemaVersion,
					repository: manifest.source.repository,
					sourceCommit: manifest.source.commit,
					sourceTree: manifest.source.tree,
					buildId: manifest.build.id,
					recipeRevision: manifest.build.recipeRevision,
					node: manifest.build.node,
					npm: manifest.build.npm,
					packageLockSha256: manifest.build.lockfile.sha256,
				})
		) {
			throw new Error(`Fork provenance mismatch in ${asset.file}.`);
		}
		packageArtifacts.set(packageJson.name, {
			packageName: packageJson.name,
			file: asset.file,
			version: packageJson.version,
			url: `${manifest.build.assetBaseUrl}/${asset.file}`,
			integrity: `sha512-${hashBytes(bytes, "sha512", "base64")}`,
			packageJson,
			shrinkwrap,
		});
	}

	const rootPackage = packageArtifacts.get("prime-agent")?.packageJson;
	if (
		!rootPackage ||
		rootPackage.version !== manifest.package.version ||
		rootPackage.bin?.["prime-agent"] !== "dist/bundle/cli.js" ||
		Object.keys(rootPackage.bin).length !== 1
	) {
		throw new Error("Public package or command identity changed.");
	}

	const internalPackageNames = new Set([...packageArtifacts.keys()].filter((name) => name !== "prime-agent"));
	for (const artifact of packageArtifacts.values()) {
		const internalArtifacts = new Map();
		for (const [dependencyName, dependency] of Object.entries({
			...artifact.packageJson.dependencies,
			...artifact.packageJson.optionalDependencies,
		})) {
			if (internalPackageNames.has(dependencyName)) {
				const target = packageArtifacts.get(dependencyName);
				if (!target) throw new Error(`Missing internal package asset for ${dependencyName}.`);
				internalArtifacts.set(dependencyName, target);
			} else if (typeof dependency === "string" && dependency.startsWith(`${manifest.build.assetBaseUrl}/`)) {
				throw new Error(`Unexpected Pylon asset URL for external dependency ${dependencyName}.`);
			}
		}
		validateInternalPackageGraph(
			artifact.packageJson,
			artifact.shrinkwrap,
			internalArtifacts,
			manifest.source.commit,
		);
		validateLockedRegistryGraph(artifact.shrinkwrap, packageLock.packages, internalArtifacts);
	}
	return manifest;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const artifactsDir = parseArgs(process.argv.slice(2));
		const manifest = verifyPylonPrimeAgentRelease(artifactsDir);
		console.log(`Verified ${manifest.build.id} (${manifest.assets.length} packages).`);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
