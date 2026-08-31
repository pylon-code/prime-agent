#!/usr/bin/env node

import { rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	assertCleanSource,
	assertPinnedToolchain,
	assertPylonRepository,
	gitSource,
	hashFile,
	PYLON_RELEASE_BUILD_RECEIPT,
	readCompletePackageLock,
	releaseBuildId,
	runNpm,
	writeReleaseBuildReceipt,
} from "./lib/pylon-release.mjs";
import { packPylonPrimeAgentRelease } from "./pack-pylon-prime-agent-release.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const generatedModels = join(root, "packages", "ai", "src", "models.generated.ts");

function parseArgs(args) {
	let pack = false;
	let outDir;
	for (let index = 0; index < args.length; index += 1) {
		const argument = args[index];
		if (argument === "--pack") {
			pack = true;
		} else if (argument === "--out-dir") {
			outDir = args[index + 1];
			if (!outDir) throw new Error("--out-dir requires a value");
			index += 1;
		} else if (argument === "--help" || argument === "-h") {
			console.log("Usage: node scripts/build-pylon-prime-agent-release.mjs [--pack] [--out-dir path]");
			process.exit(0);
		} else {
			throw new Error(`Unknown argument: ${argument}`);
		}
	}
	return { pack, outDir };
}

function buildOffline(environment) {
	runNpm(["run", "clean", "--workspaces", "--if-present"], { cwd: root, env: environment, stdio: "inherit" });
	for (const [workspace, script] of [
		["@earendil-works/pi-tui", "build"],
		["@earendil-works/pi-ai", "build:offline"],
		["@earendil-works/pi-agent-core", "build"],
		["@earendil-works/pi-coding-agent", "build"],
	]) {
		runNpm(["run", script, "--workspace", workspace], { cwd: root, env: environment, stdio: "inherit" });
	}
}

try {
	const args = parseArgs(process.argv.slice(2));
	rmSync(join(root, PYLON_RELEASE_BUILD_RECEIPT), { force: true });
	assertCleanSource(root, { rejectIgnoredInputs: true });
	assertPylonRepository(root);
	const toolchain = assertPinnedToolchain(root);
	readCompletePackageLock(root);
	const source = gitSource(root);
	const lockfileSha256 = hashFile(join(root, "package-lock.json"), "sha256");
	const modelsBefore = hashFile(generatedModels, "sha256");
	const environment = {
		...process.env,
		PI_OFFLINE: "1",
		PYLON_RELEASE_OFFLINE: "1",
		npm_config_offline: "true",
		PRIME_AGENT_RELEASE_BUILD_ID: releaseBuildId(source.commit),
	};
	buildOffline(environment);
	const modelsAfter = hashFile(generatedModels, "sha256");
	if (modelsAfter !== modelsBefore) throw new Error("Offline release build changed committed model data.");
	assertCleanSource(root, { rejectIgnoredInputs: true });
	writeReleaseBuildReceipt(root, source, toolchain, lockfileSha256);
	if (args.pack) {
		packPylonPrimeAgentRelease(args.outDir ? ["--out-dir", args.outDir] : [], environment);
	}
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
