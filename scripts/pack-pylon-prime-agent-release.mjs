#!/usr/bin/env node

import { existsSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	assertCleanSource,
	assertPinnedToolchain,
	assertPylonRepository,
	copyReleasePackage,
	createInternalShrinkwrap,
	createReleaseManifest,
	createReleasePackageJson,
	gitSource,
	hashFile,
	packStagingPackage,
	packageDirectory,
	prepareOutputDirectory,
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_PACKAGES,
	readCompletePackageLock,
	readPackageJson,
	releaseAssetFile,
	releaseAssetUrl,
	validateInternalPackageGraph,
	validateLockedRegistryGraph,
	validateReleaseBuildReceipt,
	validateReleaseManifest,
	writeJson,
} from "./lib/pylon-release.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultOutput = join(root, ".npm", "pylon-release");

function parseArgs(args) {
	let outDir = defaultOutput;
	for (let index = 0; index < args.length; index += 1) {
		const argument = args[index];
		if (argument === "--out-dir") {
			const value = args[index + 1];
			if (!value) throw new Error("--out-dir requires a value");
			outDir = resolve(root, value);
			index += 1;
		} else if (argument === "--help" || argument === "-h") {
			console.log("Usage: node scripts/pack-pylon-prime-agent-release.mjs [--out-dir path]");
			process.exit(0);
		} else {
			throw new Error(`Unknown argument: ${argument}`);
		}
	}
	return { outDir };
}

function requireBuiltPackages() {
	for (const releasePackage of PYLON_RELEASE_PACKAGES) {
		const dist = join(packageDirectory(root, releasePackage.packageDir), "dist");
		if (!existsSync(dist)) throw new Error(`Missing ${dist}. Run npm run release:pylon:build first.`);
	}
}

export function packPylonPrimeAgentRelease(args = process.argv.slice(2), environment = process.env) {
	const { outDir } = parseArgs(args);
	assertCleanSource(root, { rejectIgnoredInputs: true });
	assertPylonRepository(root);
	const toolchain = assertPinnedToolchain(root, environment);
	const source = gitSource(root);
	const version = readPackageJson(root, "coding-agent").version;
	const packageLock = readCompletePackageLock(root);
	const lockfileSha256 = hashFile(join(root, "package-lock.json"), "sha256");
	for (const releasePackage of PYLON_RELEASE_PACKAGES) {
		if (readPackageJson(root, releasePackage.packageDir).version !== version) {
			throw new Error("Pylon release package versions must remain in lockstep.");
		}
	}
	requireBuiltPackages();
	validateReleaseBuildReceipt(root, source, toolchain, lockfileSha256);
	const output = prepareOutputDirectory(outDir);
	const stagingRoot = join(output, "staging");
	const artifactsDir = join(output, "artifacts");
	const internalArtifacts = new Map();
	const artifacts = [];

	for (const releasePackage of PYLON_RELEASE_PACKAGES) {
		const sourcePackage = readPackageJson(root, releasePackage.packageDir);
		const packageJson = createReleasePackageJson({
			sourcePackage,
			releasePackage,
			version,
			source,
			toolchain,
			lockfileSha256,
			internalArtifacts,
		});
		const shrinkwrap = createInternalShrinkwrap(packageJson, internalArtifacts, packageLock.packages);
		validateInternalPackageGraph(packageJson, shrinkwrap, internalArtifacts, source.commit);
		validateLockedRegistryGraph(shrinkwrap, packageLock.packages, internalArtifacts);
		const stagingDir = join(stagingRoot, releasePackage.packageDir);
		copyReleasePackage(packageDirectory(root, releasePackage.packageDir), stagingDir, packageJson, shrinkwrap);
		const file = releaseAssetFile(releasePackage.assetStem, version);
		const packed = packStagingPackage({ root, stagingDir, artifactsDir, assetFile: file, environment });
		const artifact = { package: packageJson.name, file, version, ...packed };
		artifacts.push(artifact);
		internalArtifacts.set(sourcePackage.name, {
			packageName: packageJson.name,
			file,
			version,
			url: releaseAssetUrl(source.commit, file),
			integrity: packed.integrity,
		});
	}

	artifacts.sort((left, right) => left.file.localeCompare(right.file));
	const manifest = createReleaseManifest({ source, version, toolchain, lockfileSha256, artifacts });
	validateReleaseManifest(manifest);
	writeJson(join(artifactsDir, PYLON_RELEASE_MANIFEST), manifest);
	rmSync(stagingRoot, { recursive: true, force: true });
	for (const artifact of artifacts) console.log(`Created ${artifact.path}`);
	console.log(`Created ${join(artifactsDir, PYLON_RELEASE_MANIFEST)}`);
	return { artifactsDir, manifest };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		packPylonPrimeAgentRelease();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
