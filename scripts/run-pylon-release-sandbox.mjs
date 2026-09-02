#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	appendFileSync,
	chmodSync,
	chownSync,
	closeSync,
	constants,
	createWriteStream,
	fchmodSync,
	fchownSync,
	fstatSync,
	fsyncSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	openSync,
	opendirSync,
	readFileSync,
	readSync,
	realpathSync,
	rmSync,
	statSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { get } from "node:https";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const PYLON_SANDBOX_POLICY = Object.freeze({
	image: "node@sha256:87a4f951f28b85d189df365d24c479d3bdb70be77c1ff5c9029db2ef67e251ac",
	imageConfigId: "sha256:6622b5ce13429346f91fcdb936ec2e026ccc28465409fd25b7f79c623e7a20af",
	platform: "linux/amd64",
	nodeVersion: "22.23.2",
	npmVersion: "11.10.1",
	npmUrl: "https://registry.npmjs.org/npm/-/npm-11.10.1.tgz",
	npmMaxBytes: 4 * 1024 * 1024,
	npmSha256: "2190945151842685142f5085b3c5dd356b1021ab390d7d02c2bb2c580f0c4840",
	npmSha512: "c286afb98d8e803150d4afed07d407b14b96f7cf6729fbec29337f879a86c92fb7f8186f5cdfc30364cdcf1e7af496857d3aab113e5b110347c1584593e53582",
	npmSri: "sha512-woavuY2OgDFQ1K/tB9QHsUuW989nKfvsKTN/h5qGyS+3+BhvXN/DA2TNzx569JaFfTqrET5bEQNHwVhFk+U1gg==",
	pidsLimit: "512",
	cpus: "2",
	memory: "3g",
	maxSubjectBytes: 256 * 1024 * 1024,
	maxTotalBytes: 512 * 1024 * 1024,
});

export const PYLON_SANDBOX_PUBLIC_FAIL_STAGES = Object.freeze([
	"E_ARGUMENTS",
	"E_INITIALIZE",
	"E_HOST",
	"E_SOURCE",
	"E_NPM",
	"E_IMAGE_PULL",
	"E_IMAGE_CONFIG",
	"E_DEPENDENCIES",
	"E_PACK",
	"E_CONTRACTS",
	"E_PREPARE",
	"E_FINAL",
	"E_ARTIFACT_INPUT",
	"E_SMOKE_CACHE",
	"E_SMOKE",
	"E_RECEIPT",
	"E_FINAL_COPY",
	"E_CLEANUP",
	"E_OUTPUT",
	"E_INTERNAL",
]);
const publicFailStages = new Set(PYLON_SANDBOX_PUBLIC_FAIL_STAGES);
const containerFailStages = Object.freeze({
	contracts: "E_CONTRACTS",
	dependencies: "E_DEPENDENCIES",
	final: "E_FINAL",
	pack: "E_PACK",
	prepare: "E_PREPARE",
	smoke: "E_SMOKE",
	"smoke-cache": "E_SMOKE_CACHE",
});
let publicFailStage = "E_ARGUMENTS";

function setPublicFailStage(stage) {
	if (!publicFailStages.has(stage)) throw new Error();
	publicFailStage = stage;
}

function setContainerFailStage(phase) {
	const stage = containerFailStages[phase];
	if (stage === undefined) throw new Error();
	setPublicFailStage(stage);
}

export function formatPublicFailure(stage) {
	const safeStage = publicFailStages.has(stage) ? stage : "E_INTERNAL";
	return `::error::Pylon release sandbox failed closed [${safeStage}].`;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const driverPath = "/source/scripts/run-pylon-release-sandbox.mjs";
const hostTools = Object.freeze({
	docker: "/usr/bin/docker",
	git: "/usr/bin/git",
	tar: "/usr/bin/tar",
});
const forbiddenEnvironment = /^(?:GITHUB_ENV|GITHUB_PATH|GITHUB_TOKEN|GH_TOKEN|BASH_ENV|NODE_OPTIONS|LD_.+|NPM_CONFIG_USERCONFIG)$/;

function reject(message = "Release sandbox invariant failed.") {
	throw new Error(message);
}

function exactKeys(value, keys) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) reject();
	const actual = Object.keys(value).sort();
	const expected = [...keys].sort();
	if (JSON.stringify(actual) !== JSON.stringify(expected)) reject();
}

function safeAbsolute(value) {
	return typeof value === "string" && /^\/[A-Za-z0-9._/+:-]+$/.test(value) && resolve(value) === value;
}

function phaseMounts(spec) {
	if (["dependencies", "pack"].includes(spec.phase)) {
		const mounts = [
			`type=bind,src=${spec.paths.build},dst=/build`,
			`type=bind,src=${spec.paths.source},dst=/source,readonly`,
			`type=bind,src=${spec.paths.git},dst=/build/.git,readonly`,
			`type=bind,src=${spec.paths.npm},dst=/input/npm.tgz,readonly`,
		];
		if (spec.phase === "pack") mounts.push(`type=bind,src=${spec.paths.dependencies},dst=/build/node_modules,readonly`);
		return mounts;
	}
	if (spec.phase === "contracts") {
		return [
			`type=bind,src=${spec.paths.source},dst=/source,readonly`,
			`type=bind,src=${spec.paths.dependencies},dst=/source/node_modules,readonly`,
			`type=bind,src=${spec.paths.candidate},dst=/candidate`,
		];
	}
	if (spec.phase === "prepare") {
		return [
			`type=bind,src=${spec.paths.source},dst=/source,readonly`,
			`type=bind,src=${spec.paths.candidate},dst=/candidate`,
		];
	}
	if (spec.phase === "final") {
		return [
			`type=bind,src=${spec.paths.source},dst=/source,readonly`,
			`type=bind,src=${spec.paths.candidate},dst=/candidate,readonly`,
			`type=bind,src=${spec.paths.control},dst=/control`,
		];
	}
	if (["smoke-cache", "smoke"].includes(spec.phase)) {
		return [
			`type=bind,src=${spec.paths.source},dst=/source,readonly`,
			`type=bind,src=${spec.paths.artifacts},dst=/artifacts,readonly`,
			`type=bind,src=${spec.paths.npm},dst=/input/npm.tgz,readonly`,
			`type=bind,src=${spec.paths.cache},dst=/cache`,
		];
	}
	reject();
}

function phaseEnvironment(spec) {
	const safeDirectory = ["dependencies", "pack"].includes(spec.phase) ? "/build" : "/source";
	const values = [
		"HOME=/home/build",
		`TMPDIR=${spec.phase === "smoke" ? "/smoke" : "/tmp"}`,
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"CI=true",
		"GITHUB_ACTIONS=true",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_COUNT=3",
		"GIT_CONFIG_KEY_0=safe.directory",
		`GIT_CONFIG_VALUE_0=${safeDirectory}`,
		"GIT_CONFIG_KEY_1=core.fsmonitor",
		"GIT_CONFIG_VALUE_1=false",
		"GIT_CONFIG_KEY_2=core.hooksPath",
		"GIT_CONFIG_VALUE_2=/dev/null",
	];
	if (["dependencies", "pack", "smoke-cache", "smoke"].includes(spec.phase)) values.push("npm_config_userconfig=/dev/null");
	if (["pack", "smoke-cache", "smoke"].includes(spec.phase)) values.push("PYLON_RELEASE_NPM_CLI=/tmp/npm-runtime/package/bin/npm-cli.js");
	if (["contracts", "prepare"].includes(spec.phase)) values.push(`GITHUB_RUN_ID=${spec.runId}`, `GITHUB_RUN_NUMBER=${spec.runNumber}`);
	if (["smoke-cache", "smoke"].includes(spec.phase)) values.push("npm_config_cache=/cache");
	if (spec.phase === "smoke") values.push("npm_config_offline=true");
	return values;
}

function phaseCommand(spec) {
	return [
		"-i",
		...phaseEnvironment(spec),
		"/usr/local/bin/node",
		driverPath,
		"--container-phase",
		spec.phase,
		"--mode",
		spec.mode,
		"--publication-policy-revision",
		String(spec.publicationPolicyRevision),
	];
}

export function buildDockerRunArgs(spec) {
	exactKeys(spec, ["cidfile", "gid", "mode", "name", "paths", "phase", "publicationPolicyRevision", "runId", "runNumber", "uid"]);
	if (!/^[1-9][0-9]*$/.test(String(spec.uid)) || !/^[1-9][0-9]*$/.test(String(spec.gid))) reject();
	if (!/^[a-z0-9][a-z0-9_.-]{0,62}$/.test(spec.name) || !safeAbsolute(spec.cidfile)) reject();
	if (!["ci", "preview", "historical"].includes(spec.mode) || !["dependencies", "pack", "contracts", "prepare", "final", "smoke-cache", "smoke"].includes(spec.phase)) reject();
	if (spec.mode === "historical" && !["smoke-cache", "smoke"].includes(spec.phase)) reject();
	if (!Number.isSafeInteger(spec.publicationPolicyRevision) || spec.publicationPolicyRevision < 1) reject();
	if (!/^[0-9]+$/.test(spec.runId) || !/^[0-9]+$/.test(spec.runNumber)) reject();
	const pathKeys = spec.phase === "dependencies" ? ["build", "git", "npm", "source"] :
		spec.phase === "pack" ? ["build", "dependencies", "git", "npm", "source"] :
			spec.phase === "contracts" ? ["candidate", "dependencies", "source"] :
				spec.phase === "prepare" ? ["candidate", "source"] :
					spec.phase === "final" ? ["candidate", "control", "source"] : ["artifacts", "cache", "npm", "source"];
	exactKeys(spec.paths, pathKeys);
	for (const value of Object.values(spec.paths)) if (!safeAbsolute(value) || value.includes(",")) reject();
	const network = ["dependencies", "smoke-cache"].includes(spec.phase) ? "bridge" : "none";
	const args = [
		"run",
		"--name",
		spec.name,
		"--cidfile",
		spec.cidfile,
		"--pull",
		"never",
		"--platform",
		PYLON_SANDBOX_POLICY.platform,
		"--entrypoint",
		"/usr/bin/env",
		"--no-healthcheck",
		"--init",
		"--user",
		`${spec.uid}:${spec.gid}`,
		"--cap-drop",
		"ALL",
		"--security-opt",
		"no-new-privileges",
		"--pids-limit",
		PYLON_SANDBOX_POLICY.pidsLimit,
		"--cpus",
		PYLON_SANDBOX_POLICY.cpus,
		"--memory",
		PYLON_SANDBOX_POLICY.memory,
		"--memory-swap",
		PYLON_SANDBOX_POLICY.memory,
		"--read-only",
		"--network",
		network,
		"--tmpfs",
		`/home/build:rw,nosuid,nodev,noexec,size=256m,uid=${spec.uid},gid=${spec.gid},mode=700`,
		"--tmpfs",
		`/tmp:rw,nosuid,nodev,noexec,size=512m,uid=${spec.uid},gid=${spec.gid},mode=1777`,
	];
	if (spec.phase === "smoke") args.push("--tmpfs", `/smoke:rw,nosuid,nodev,exec,size=1024m,uid=${spec.uid},gid=${spec.gid},mode=1777`);
	for (const mount of phaseMounts(spec)) args.push("--mount", mount);
	args.push("--workdir", ["dependencies", "pack"].includes(spec.phase) ? "/build" : "/source");
	args.push(PYLON_SANDBOX_POLICY.image, ...phaseCommand(spec));
	return args;
}

export function assertExactDockerRunArgs(args, spec) {
	if (!Array.isArray(args) || args.some((value) => typeof value !== "string")) reject();
	const expected = buildDockerRunArgs(spec);
	if (JSON.stringify(args) !== JSON.stringify(expected)) reject("Docker argv differs from the closed sandbox policy.");
	const singletonValueFlags = ["--name", "--cidfile", "--pull", "--platform", "--entrypoint", "--user", "--cap-drop", "--security-opt", "--pids-limit", "--cpus", "--memory", "--memory-swap", "--network", "--workdir"];
	for (const flag of singletonValueFlags) if (args.filter((value) => value === flag).length !== 1) reject();
	for (const flag of ["--init", "--read-only", "--no-healthcheck"]) if (args.filter((value) => value === flag).length !== 1) reject();
	if (args.filter((value) => value === "run").length !== 1 || args[0] !== "run") reject();
	if (args.includes("--privileged") || args.includes("--cap-add") || args.includes("--pid") || args.includes("--env") || args.includes("--rm")) reject();
	const imageIndex = args.indexOf(PYLON_SANDBOX_POLICY.image);
	if (imageIndex < 1 || args[imageIndex + 1] !== "-i" || args.slice(imageIndex + 1).some((value) => ["/bin/sh", "/usr/bin/bash", "sh", "bash"].includes(value))) reject();
	const mounts = args.flatMap((value, index) => args[index - 1] === "--mount" ? [value] : []);
	const destinations = mounts.map((value) => value.match(/(?:^|,)dst=([^,]+)/)?.[1]);
	if (destinations.some((value) => value === undefined) || new Set(destinations).size !== destinations.length) reject();
	if (mounts.some((value) => value.includes("docker.sock"))) reject();
	return true;
}

function sanitizedHostEnvironment(home, nodePath = process.execPath) {
	return {
		HOME: home,
		PATH: `${dirname(nodePath)}:/usr/bin:/bin`,
	};
}

function runExact(command, args, options = {}) {
	const result = spawnSync(command, args, {
		cwd: options.cwd,
		env: options.env,
		encoding: options.capture ? "utf8" : undefined,
		stdio: options.capture ? "pipe" : "inherit",
		timeout: options.timeoutMs ?? 120_000,
		maxBuffer: options.capture ? 1024 * 1024 : undefined,
	});
	if (result.error || result.signal !== null || result.status !== 0) {
		reject(options.description ?? "Exact subprocess failed.");
	}
	return options.capture ? result.stdout.trim() : undefined;
}

function validateTool(path, { rootOwned = true } = {}) {
	const link = lstatSync(path);
	const resolved = realpathSync.native(path);
	const metadata = lstatSync(resolved);
	if (!metadata.isFile() || metadata.isSymbolicLink() || (metadata.mode & 0o111) === 0) reject();
	if (rootOwned && (
		link.uid !== 0 || link.gid !== 0 || (!link.isSymbolicLink() && (link.mode & 0o022) !== 0) ||
		metadata.uid !== 0 || metadata.gid !== 0 || (metadata.mode & 0o022) !== 0
	)) reject();
}

function gitArgs(worktree, args) {
	return [
		"-c", "core.fsmonitor=false",
		"-c", "core.hooksPath=/dev/null",
		"-c", `safe.directory=${worktree}`,
		"-C", worktree,
		...args,
	];
}

function gitEnvironment(home) {
	return {
		HOME: home,
		PATH: "/usr/bin:/bin",
		GIT_CONFIG_GLOBAL: "/dev/null",
		GIT_CONFIG_NOSYSTEM: "1",
		GIT_OPTIONAL_LOCKS: "0",
	};
}

function gitValue(worktree, args, home) {
	return runExact(hostTools.git, gitArgs(worktree, args), {
		capture: true,
		env: gitEnvironment(home),
		description: "Trusted Git validation failed.",
	});
}

function validateOriginal(workspace, expected, home) {
	const dotGit = join(workspace, ".git");
	const gitMetadata = lstatSync(dotGit);
	if (!gitMetadata.isDirectory() || gitMetadata.isSymbolicLink() || realpathSync.native(dotGit) !== dotGit) reject();
	const head = gitValue(workspace, ["rev-parse", "HEAD"], home);
	const tree = gitValue(workspace, ["rev-parse", "HEAD^{tree}"], home);
	const status = gitValue(workspace, ["status", "--porcelain=v1", "--untracked-files=all"], home);
	if (!/^[0-9a-f]{40}$/.test(head) || !/^[0-9a-f]{40}$/.test(tree) || status !== "") reject();
	if (expected && (head !== expected.head || tree !== expected.tree)) reject();
	return { head, tree };
}

function chmodTreeReadOnly(path, traversal = { entries: 0 }, depth = 0) {
	traversal.entries += 1;
	if (traversal.entries > 200_000 || depth > 64) reject();
	const metadata = lstatSync(path);
	if (metadata.isSymbolicLink()) return;
	if (metadata.isDirectory()) {
		const directory = opendirSync(path);
		try {
			for (let entry = directory.readSync(); entry !== null; entry = directory.readSync()) {
				chmodTreeReadOnly(join(path, entry.name), traversal, depth + 1);
			}
		} finally {
			directory.closeSync();
		}
		chmodSync(path, 0o555);
		return;
	}
	if (!metadata.isFile()) reject();
	chmodSync(path, (metadata.mode & 0o111) === 0 ? 0o444 : 0o555);
}

function preparePristineSource(workspace, scratch, home, identity, { withBuild }) {
	const source = join(scratch, "source");
	const build = join(scratch, "build");
	const archive = join(scratch, "source.tar");
	const env = gitEnvironment(home);
	runExact(hostTools.git, ["-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", "clone", "--no-hardlinks", "--no-checkout", "--", workspace, source], {
		env,
		description: "Trusted source clone failed.",
	});
	runExact(hostTools.git, gitArgs(source, ["checkout", "--detach", identity.head]), { env, description: "Trusted source checkout failed." });
	runExact(hostTools.git, gitArgs(source, ["remote", "set-url", "origin", "https://github.com/pylon-code/prime-agent.git"]), { env });
	if (gitValue(source, ["rev-parse", "HEAD"], home) !== identity.head || gitValue(source, ["rev-parse", "HEAD^{tree}"], home) !== identity.tree) reject();
	if (gitValue(source, ["status", "--porcelain=v1", "--untracked-files=all"], home) !== "") reject();
	const dotGit = join(source, ".git");
	if (!lstatSync(dotGit).isDirectory() || realpathSync.native(dotGit) !== dotGit) reject();
	mkdirSync(join(source, "node_modules"), { mode: 0o700 });
	if (withBuild) {
		mkdirSync(build, { mode: 0o700 });
		runExact(hostTools.git, gitArgs(source, ["archive", "--format=tar", `--output=${archive}`, "HEAD"]), { env });
		runExact(hostTools.tar, ["--extract", "--file", archive, "--directory", build, "--no-same-owner"], {
			env: sanitizedHostEnvironment(home),
			description: "Trusted source archive extraction failed.",
		});
		mkdirSync(join(build, ".git"), { mode: 0o555 });
		rmSync(archive, { force: true });
	}
	chmodTreeReadOnly(source);
	return { source, git: dotGit, build: withBuild ? build : undefined };
}

async function downloadNpmArchive(destination) {
	const url = new URL(PYLON_SANDBOX_POLICY.npmUrl);
	if (url.protocol !== "https:" || url.username !== "" || url.password !== "" || url.hash !== "") reject();
	await new Promise((resolveDownload, rejectDownload) => {
		const sha256 = createHash("sha256");
		const sha512 = createHash("sha512");
		let bytes = 0;
		let overallTimeout;
		let settled = false;
		const output = createWriteStream(destination, { flags: "wx", mode: 0o400 });
		const finish = (error) => {
			if (settled) return;
			settled = true;
			clearTimeout(overallTimeout);
			if (error) {
				output.destroy();
				rejectDownload(error);
			} else {
				resolveDownload();
			}
		};
		output.on("error", finish);
		const request = get(url, { headers: { accept: "application/octet-stream", "user-agent": "pylon-release-sandbox-v1" }, timeout: 20_000 }, (response) => {
			if (response.statusCode !== 200 || response.headers.location !== undefined) {
				response.resume();
				finish(new Error());
				return;
			}
			const length = Number(response.headers["content-length"]);
			if (!Number.isSafeInteger(length) || length < 1 || length > PYLON_SANDBOX_POLICY.npmMaxBytes) {
				response.destroy();
				finish(new Error());
				return;
			}
			response.on("data", (chunk) => {
				bytes += chunk.length;
				if (bytes > PYLON_SANDBOX_POLICY.npmMaxBytes) {
					response.destroy();
					finish(new Error());
					return;
				}
				sha256.update(chunk);
				sha512.update(chunk);
				if (!output.write(chunk)) response.pause();
			});
			output.on("drain", () => response.resume());
			response.on("end", () => {
				output.end(() => {
					const digest256 = sha256.digest("hex");
					const digest512 = sha512.digest();
					if (
						bytes !== length || digest256 !== PYLON_SANDBOX_POLICY.npmSha256 ||
						digest512.toString("hex") !== PYLON_SANDBOX_POLICY.npmSha512 ||
						`sha512-${digest512.toString("base64")}` !== PYLON_SANDBOX_POLICY.npmSri
					) finish(new Error());
					else finish();
				});
			});
			response.on("error", finish);
		});
		overallTimeout = setTimeout(() => request.destroy(new Error()), 120_000);
		request.on("timeout", () => request.destroy(new Error()));
		request.on("error", finish);
	});
	chmodSync(destination, 0o444);
}

function streamEntries(directory, expectedCount) {
	const entries = [];
	const handle = opendirSync(directory);
	try {
		for (let entry = handle.readSync(); entry !== null; entry = handle.readSync()) {
			entries.push(entry.name);
			if (entries.length > expectedCount) reject();
		}
	} finally {
		handle.closeSync();
	}
	if (entries.length !== expectedCount || entries.some((name) => !/^[A-Za-z0-9._-]+$/.test(name))) reject();
	return entries.sort();
}

const PINNED_STAT_KEYS = Object.freeze(["dev", "ino", "nlink", "size", "mode", "uid", "gid", "mtimeNs", "ctimeNs"]);

function samePinnedStat(left, right) {
	return PINNED_STAT_KEYS.every((key) => left[key] === right[key]);
}

function preflightPinnedRegular(path, uid, gid, requiredMode, maxBytes = PYLON_SANDBOX_POLICY.maxSubjectBytes) {
	const metadata = lstatSync(path, { bigint: true });
	if (
		!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1n ||
		metadata.uid !== BigInt(uid) || (gid !== null && metadata.gid !== BigInt(gid)) ||
		(metadata.mode & 0o7777n) !== BigInt(requiredMode) || metadata.size < 1n ||
		metadata.size > BigInt(maxBytes) || metadata.blocks * 512n < metadata.size
	) reject();
	return metadata;
}

function readExactChunks(descriptor, size, onChunk) {
	const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, Number(size)));
	let offset = 0n;
	while (offset < size) {
		const wanted = Math.min(buffer.length, Number(size - offset));
		const count = readSync(descriptor, buffer, 0, wanted, null);
		if (count < 1 || count > wanted) reject();
		onChunk(buffer.subarray(0, count));
		offset += BigInt(count);
	}
	const extra = Buffer.allocUnsafe(1);
	if (readSync(descriptor, extra, 0, 1, null) !== 0) reject();
}

function writeExactChunk(descriptor, chunk) {
	let offset = 0;
	while (offset < chunk.length) {
		const count = writeSync(descriptor, chunk, offset, chunk.length - offset, null);
		if (count < 1 || count > chunk.length - offset) reject();
		offset += count;
	}
}

function digestPinnedRegular(path, before, uid, gid, requiredMode) {
	const descriptor = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
	try {
		const opened = fstatSync(descriptor, { bigint: true });
		if (!samePinnedStat(before, opened)) reject();
		const sha256 = createHash("sha256");
		readExactChunks(descriptor, opened.size, (chunk) => sha256.update(chunk));
		const after = fstatSync(descriptor, { bigint: true });
		const pathname = preflightPinnedRegular(path, uid, gid, requiredMode);
		if (!samePinnedStat(opened, after) || !samePinnedStat(after, pathname)) reject();
		return sha256.digest("hex");
	} finally {
		closeSync(descriptor);
	}
}

function copyPinnedRegular(input, output, before, uid, inputGid, outputGid, inputMode, outputMode, expected) {
	const inputDescriptor = openSync(input, constants.O_RDONLY | constants.O_NOFOLLOW);
	let outputDescriptor;
	try {
		const opened = fstatSync(inputDescriptor, { bigint: true });
		if (!samePinnedStat(before, opened)) reject();
		outputDescriptor = openSync(output, constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
		const sha256 = createHash("sha256");
		readExactChunks(inputDescriptor, opened.size, (chunk) => {
			sha256.update(chunk);
			writeExactChunk(outputDescriptor, chunk);
		});
		const digest = sha256.digest("hex");
		const after = fstatSync(inputDescriptor, { bigint: true });
		const pathname = preflightPinnedRegular(input, uid, inputGid, inputMode);
		if (!samePinnedStat(opened, after) || !samePinnedStat(after, pathname)) reject();
		if (expected && (expected.size !== Number(opened.size) || expected.sha256 !== digest)) reject();
		fchownSync(outputDescriptor, uid, outputGid);
		fchmodSync(outputDescriptor, outputMode);
		fsyncSync(outputDescriptor);
		const copied = fstatSync(outputDescriptor, { bigint: true });
		if (
			!copied.isFile() || copied.nlink !== 1n || copied.uid !== BigInt(uid) || copied.gid !== BigInt(outputGid) ||
			(copied.mode & 0o7777n) !== BigInt(outputMode) || copied.size !== opened.size
		) reject();
		return { dev: copied.dev, ino: copied.ino, size: Number(copied.size), sha256: digest };
	} finally {
		if (outputDescriptor !== undefined) closeSync(outputDescriptor);
		closeSync(inputDescriptor);
	}
}

function validateReceipt(receipt, names) {
	exactKeys(receipt, ["files", "schemaVersion"]);
	if (receipt.schemaVersion !== 1 || !Array.isArray(receipt.files) || receipt.files.length !== names.length) reject();
	const files = new Map();
	for (const entry of receipt.files) {
		exactKeys(entry, ["file", "sha256", "size"]);
		if (
			typeof entry.file !== "string" || !/^[A-Za-z0-9._-]+$/.test(entry.file) ||
			!names.includes(entry.file) || files.has(entry.file) || !Number.isSafeInteger(entry.size) ||
			entry.size < 1 || entry.size > PYLON_SANDBOX_POLICY.maxSubjectBytes || !/^[0-9a-f]{64}$/.test(entry.sha256)
		) reject();
		files.set(entry.file, entry);
	}
	if (JSON.stringify([...files.keys()]) !== JSON.stringify(names)) reject();
	return files;
}

function validateCopiedSubjects(source, destination, names, sourceStats, outputReceipts, { inputMode, outputMode, uid, gid, inputGid, expectedFiles }) {
	if (JSON.stringify(streamEntries(destination, names.length)) !== JSON.stringify(names)) reject();
	for (const name of names) {
		const input = join(source, name);
		const output = join(destination, name);
		const sourceBefore = sourceStats.get(name);
		const sourceDigest = digestPinnedRegular(input, sourceBefore, uid, inputGid, inputMode);
		const expected = expectedFiles?.get(name);
		if (expected && (expected.size !== Number(sourceBefore.size) || expected.sha256 !== sourceDigest)) reject();
		const outputBefore = preflightPinnedRegular(output, uid, gid, outputMode);
		const outputDigest = digestPinnedRegular(output, outputBefore, uid, gid, outputMode);
		const copied = outputReceipts.get(name);
		if (
			!copied || outputBefore.dev !== copied.dev || outputBefore.ino !== copied.ino ||
			Number(outputBefore.size) !== copied.size || outputDigest !== copied.sha256 || outputDigest !== sourceDigest
		) reject();
	}
}

/**
 * Workflow-private bridge. Its caller must own private source/destination parents and must prove
 * every captured container and process group absent before copying a real publication candidate.
 * This is not a general concurrent-writer boundary.
 */
export function copyBoundedSubjects(source, destination, {
	expectedCount,
	expectedNames,
	expectedReceipt,
	freeze = false,
	gid = process.getgid(),
	inputGid = gid,
	inputMode = 0o644,
	outputMode = 0o644,
	sourceExpectedCount = expectedCount,
	uid = process.getuid(),
} = {}) {
	const sourceTop = lstatSync(source, { bigint: true });
	if (!sourceTop.isDirectory() || sourceTop.isSymbolicLink() || realpathSync.native(source) !== source) reject();
	const sourceNames = streamEntries(source, sourceExpectedCount);
	const names = expectedNames === undefined ? sourceNames : [...expectedNames].sort();
	if (
		!Number.isSafeInteger(expectedCount) || names.length !== expectedCount || new Set(names).size !== names.length ||
		names.some((name) => !sourceNames.includes(name) || !/^[A-Za-z0-9._-]+$/.test(name))
	) reject();
	const sourceStats = new Map();
	let total = 0;
	for (const name of names) {
		const metadata = preflightPinnedRegular(join(source, name), uid, inputGid, inputMode);
		total += Number(metadata.size);
		if (!Number.isSafeInteger(total) || total > PYLON_SANDBOX_POLICY.maxTotalBytes) reject();
		sourceStats.set(name, metadata);
	}
	const expectedFiles = expectedReceipt === undefined ? undefined : validateReceipt(expectedReceipt, names);
	if (expectedFiles) {
		const expectedTotal = [...expectedFiles.values()].reduce((sum, entry) => sum + entry.size, 0);
		if (expectedTotal !== total) reject();
	}
	mkdirSync(destination, { mode: 0o700 });
	const directoryDescriptor = openSync(destination, constants.O_RDONLY | constants.O_NOFOLLOW);
	const outputReceipts = new Map();
	try {
		fchownSync(directoryDescriptor, uid, gid);
		fchmodSync(directoryDescriptor, 0o700);
		const destinationTop = fstatSync(directoryDescriptor, { bigint: true });
		if (!destinationTop.isDirectory() || destinationTop.uid !== BigInt(uid) || destinationTop.gid !== BigInt(gid) || (destinationTop.mode & 0o7777n) !== 0o700n) reject();
		for (const name of names) {
			const receipt = copyPinnedRegular(
				join(source, name), join(destination, name), sourceStats.get(name), uid, inputGid, gid, inputMode, outputMode, expectedFiles?.get(name),
			);
			outputReceipts.set(name, receipt);
		}
		validateCopiedSubjects(source, destination, names, sourceStats, outputReceipts, { inputMode, outputMode, uid, gid, inputGid, expectedFiles });
		if (freeze) {
			for (const name of names) {
				const descriptor = openSync(join(destination, name), constants.O_RDWR | constants.O_NOFOLLOW);
				try { fchmodSync(descriptor, 0o444); fsyncSync(descriptor); } finally { closeSync(descriptor); }
				const metadata = preflightPinnedRegular(join(destination, name), uid, gid, 0o444);
				const receipt = outputReceipts.get(name);
				outputReceipts.set(name, { ...receipt, dev: metadata.dev, ino: metadata.ino, size: Number(metadata.size) });
			}
			fchmodSync(directoryDescriptor, 0o555);
		}
		fsyncSync(directoryDescriptor);
	} finally {
		closeSync(directoryDescriptor);
	}
	const finalMode = freeze ? 0o444 : outputMode;
	validateCopiedSubjects(source, destination, names, sourceStats, outputReceipts, { inputMode, outputMode: finalMode, uid, gid, inputGid, expectedFiles });
	const finalTop = lstatSync(destination, { bigint: true });
	if (
		!finalTop.isDirectory() || finalTop.isSymbolicLink() || finalTop.uid !== BigInt(uid) || finalTop.gid !== BigInt(gid) ||
		(finalTop.mode & 0o7777n) !== BigInt(freeze ? 0o555 : 0o700)
	) reject();
	return Object.freeze({
		files: Object.freeze(names.map((name) => Object.freeze({ file: name, sha256: outputReceipts.get(name).sha256, size: outputReceipts.get(name).size }))),
		names: Object.freeze(names),
		totalBytes: total,
	});
}

function freezeSubjectDirectory(path, expectedCount, uid = process.getuid(), gid = process.getgid()) {
	const names = streamEntries(path, expectedCount);
	for (const name of names) {
		preflightPinnedRegular(join(path, name), uid, gid, 0o644);
		const descriptor = openSync(join(path, name), constants.O_RDWR | constants.O_NOFOLLOW);
		try { fchmodSync(descriptor, 0o444); fsyncSync(descriptor); } finally { closeSync(descriptor); }
		const frozen = preflightPinnedRegular(join(path, name), uid, gid, 0o444);
		digestPinnedRegular(join(path, name), frozen, uid, gid, 0o444);
	}
	const descriptor = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
	try { fchmodSync(descriptor, 0o555); fsyncSync(descriptor); } finally { closeSync(descriptor); }
	if ((lstatSync(path, { bigint: true }).mode & 0o7777n) !== 0o555n) reject();
}

function readPinnedSmall(path, uid, gid, requiredMode, maxBytes = 64 * 1024) {
	const before = preflightPinnedRegular(path, uid, gid, requiredMode, maxBytes);
	const descriptor = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
	try {
		const opened = fstatSync(descriptor, { bigint: true });
		if (!samePinnedStat(before, opened)) reject();
		const chunks = [];
		readExactChunks(descriptor, opened.size, (chunk) => chunks.push(Buffer.from(chunk)));
		const after = fstatSync(descriptor, { bigint: true });
		const pathname = preflightPinnedRegular(path, uid, gid, requiredMode, maxBytes);
		if (!samePinnedStat(opened, after) || !samePinnedStat(after, pathname)) reject();
		return Buffer.concat(chunks, Number(opened.size));
	} finally {
		closeSync(descriptor);
	}
}

function canonicalReceipt(receipt) {
	return `${JSON.stringify({ files: receipt.files.map((entry) => ({ file: entry.file, sha256: entry.sha256, size: entry.size })), schemaVersion: 1 })}\n`;
}

function writeFinalReceipt(candidate, control, expectedCount) {
	const names = streamEntries(candidate, expectedCount);
	const files = names.map((name) => {
		const metadata = preflightPinnedRegular(join(candidate, name), process.getuid(), process.getgid(), 0o444);
		return { file: name, sha256: digestPinnedRegular(join(candidate, name), metadata, process.getuid(), process.getgid(), 0o444), size: Number(metadata.size) };
	});
	const bytes = Buffer.from(canonicalReceipt({ files }), "utf8");
	const path = join(control, "final-receipt.json");
	const descriptor = openSync(path, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
	try {
		writeExactChunk(descriptor, bytes);
		fchownSync(descriptor, process.getuid(), process.getgid());
		fchmodSync(descriptor, 0o644);
		fsyncSync(descriptor);
	} finally {
		closeSync(descriptor);
	}
	const directoryDescriptor = openSync(control, constants.O_RDONLY | constants.O_NOFOLLOW);
	try { fsyncSync(directoryDescriptor); } finally { closeSync(directoryDescriptor); }
}

export function readFinalReceipt(path, expectedCount, uid = process.getuid(), gid = process.getgid()) {
	const bytes = readPinnedSmall(path, uid, gid, 0o644);
	let receipt;
	try { receipt = JSON.parse(bytes.toString("utf8")); } catch { reject(); }
	if (!Array.isArray(receipt?.files) || receipt.files.length !== expectedCount) reject();
	const names = receipt.files.map((entry) => entry?.file);
	if (names.some((name) => typeof name !== "string" || !/^[A-Za-z0-9._-]+$/.test(name))) reject();
	const sortedNames = [...names].sort();
	if (JSON.stringify(names) !== JSON.stringify(sortedNames)) reject();
	validateReceipt(receipt, names);
	if (!bytes.equals(Buffer.from(canonicalReceipt(receipt), "utf8"))) reject();
	return receipt;
}

function parseCid(path) {
	const metadata = lstatSync(path);
	if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1 || metadata.uid !== process.getuid() || (metadata.mode & 0o022) !== 0 || metadata.size < 64 || metadata.size > 65) reject();
	const descriptor = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
	try {
		const opened = fstatSync(descriptor);
		if (opened.dev !== metadata.dev || opened.ino !== metadata.ino || opened.size !== metadata.size) reject();
		const buffer = Buffer.alloc(65);
		const count = readSync(descriptor, buffer, 0, buffer.length, null);
		if (count !== metadata.size || readSync(descriptor, buffer, 0, 1, null) !== 0) reject();
		const value = buffer.subarray(0, count).toString("utf8").trim();
		const after = fstatSync(descriptor);
		const pathname = lstatSync(path);
		if (after.dev !== opened.dev || after.ino !== opened.ino || after.size !== opened.size || pathname.dev !== opened.dev || pathname.ino !== opened.ino || pathname.size !== opened.size || !/^[0-9a-f]{64}$/.test(value)) reject();
		return value;
	} finally {
		closeSync(descriptor);
	}
}

function dockerEnvironment(home) {
	return { HOME: home, PATH: "/usr/bin:/bin" };
}

function dockerCaptured(state, args, timeout = 30_000) {
	return spawnSync(hostTools.docker, args, {
		encoding: "utf8",
		env: dockerEnvironment(state.home),
		maxBuffer: 1024 * 1024,
		stdio: "pipe",
		timeout,
	});
}

export function assertContainerRemoved(id, removal, inspection) {
	if (!/^[0-9a-f]{64}$/.test(id)) reject();
	const missing = !inspection.error && inspection.signal === null && inspection.status === 1 &&
		typeof inspection.stderr === "string" && inspection.stderr.includes("No such container") && inspection.stderr.includes(id);
	if (removal.error || removal.signal !== null || removal.status !== 0 || removal.stdout?.trim() !== id || !missing) reject();
	return true;
}

function captureContainerId(state, receipt) {
	if (lstatExists(receipt.cidfile)) return parseCid(receipt.cidfile);
	const result = dockerCaptured(state, ["container", "inspect", "--format", "{{.Id}}", receipt.name], 20_000);
	const id = result.stdout?.trim();
	if (result.error || result.signal !== null || result.status !== 0 || !/^[0-9a-f]{64}$/.test(id ?? "")) reject();
	return id;
}

function runDockerPhase(state, phase, paths, options) {
	setContainerFailStage(phase);
	for (const value of Object.values(paths)) {
		if (realpathSync.native(value) !== value || (!value.startsWith(`${state.scratch}/`) && !value.startsWith(`${state.control}/`))) reject();
	}
	const name = `pylon-${phase}-${state.nonce}`;
	const cidfile = join(state.cidDirectory, `${phase}.cid`);
	const spec = {
		cidfile,
		gid: String(process.getgid()),
		mode: options.mode,
		name,
		paths,
		phase,
		publicationPolicyRevision: options.publicationPolicyRevision,
		runId: options.runId,
		runNumber: options.runNumber,
		uid: String(process.getuid()),
	};
	const args = buildDockerRunArgs(spec);
	assertExactDockerRunArgs(args, spec);
	const receipt = { cidfile, id: undefined, name };
	state.containers.push(receipt);
	const result = spawnSync(hostTools.docker, args, {
		env: dockerEnvironment(state.home),
		stdio: "inherit",
		timeout: ["smoke", "contracts"].includes(phase) ? 12 * 60_000 : 10 * 60_000,
	});
	receipt.id = captureContainerId(state, receipt);
	if (result.error || result.signal !== null || result.status !== 0) reject(`Container phase ${phase} failed.`);
}

function lstatExists(path) {
	try { lstatSync(path); return true; } catch (error) { if (error?.code === "ENOENT") return false; throw error; }
}

function cleanupContainers(state) {
	let clean = true;
	for (const receipt of [...state.containers].reverse()) {
		let id = receipt.id;
		if (id === undefined) {
			try { id = captureContainerId(state, receipt); } catch { clean = false; continue; }
		}
		const removal = dockerCaptured(state, ["container", "rm", "--force", id], 30_000);
		const inspection = dockerCaptured(state, ["container", "inspect", id], 20_000);
		try { assertContainerRemoved(id, removal, inspection); } catch { clean = false; }
	}
	if (clean) state.containers.length = 0;
	return clean;
}

async function terminateActiveProcessGroup(state) {
	const pgid = state.activePgid;
	if (pgid === null) return true;
	if (!Number.isSafeInteger(pgid) || pgid < 1) return false;
	try { process.kill(-pgid, "SIGKILL"); } catch (error) { if (error?.code !== "ESRCH") return false; }
	const drained = await waitForProcessGroupDrain(pgid, 3_000);
	if (drained && state.activePgid === pgid) state.activePgid = null;
	return drained;
}

export function installSignalCleanup(state, hooks = {}) {
	const signalTarget = hooks.signalTarget ?? process;
	const cleanupContainersFn = hooks.cleanupContainers ?? cleanupContainers;
	const removeOwnedTreeFn = hooks.removeOwnedTree ?? removeOwnedTree;
	const exit = hooks.exit ?? ((code) => process.exit(code));
	const yieldForPendingSignal = hooks.yieldForPendingSignal ?? (() => new Promise((resolveYield) => setImmediate(resolveYield)));
	const handlers = new Map();
	let handlingPromise;
	const removeHandlers = () => {
		for (const [signal, handler] of handlers) signalTarget.off(signal, handler);
	};
	for (const signal of ["SIGINT", "SIGTERM"]) {
		const handler = () => {
			if (handlingPromise) return;
			handlingPromise = (async () => {
				let clean = await terminateActiveProcessGroup(state);
				try { clean = cleanupContainersFn(state) && clean; } catch { clean = false; }
				try { removeOwnedTreeFn(state.scratch); } catch { clean = false; }
				try { removeOwnedTreeFn(state.control); } catch { clean = false; }
				removeHandlers();
				exit(clean ? (signal === "SIGINT" ? 130 : 143) : 1);
			})().catch(() => {
				removeHandlers();
				exit(1);
			});
		};
		handlers.set(signal, handler);
		signalTarget.on(signal, handler);
	}
	return async () => {
		await yieldForPendingSignal();
		if (handlingPromise) await handlingPromise;
		else removeHandlers();
	};
}

function absentNullOrEmptyString(value) {
	return value === undefined || value === null || value === "";
}

function absentNullOrEmptyArray(value) {
	return value === undefined || value === null || (Array.isArray(value) && value.length === 0);
}

function absentNullOrEmptyObject(value) {
	return value === undefined || value === null || (
		typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0
	);
}

export function validateImageInspection(value) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) reject();
	if (
		value.Os !== "linux" || value.Architecture !== "amd64" ||
		!absentNullOrEmptyString(value.Variant) || value.Id !== PYLON_SANDBOX_POLICY.imageConfigId
	) reject();
	const allowedRepoDigests = new Set([
		PYLON_SANDBOX_POLICY.image,
		`docker.io/library/${PYLON_SANDBOX_POLICY.image}`,
	]);
	if (
		!Array.isArray(value.RepoDigests) || value.RepoDigests.length < 1 || value.RepoDigests.length > allowedRepoDigests.size ||
		new Set(value.RepoDigests).size !== value.RepoDigests.length ||
		value.RepoDigests.some((digest) => !allowedRepoDigests.has(digest))
	) reject();
	const config = value.Config;
	if (config === null || typeof config !== "object" || Array.isArray(config)) reject();
	if (
		JSON.stringify(config.Entrypoint) !== JSON.stringify(["docker-entrypoint.sh"]) ||
		JSON.stringify(config.Cmd) !== JSON.stringify(["node"]) ||
		JSON.stringify(config.Env) !== JSON.stringify([
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			`NODE_VERSION=${PYLON_SANDBOX_POLICY.nodeVersion}`,
			"YARN_VERSION=1.22.22",
		]) ||
		!absentNullOrEmptyObject(config.Volumes) ||
		!absentNullOrEmptyObject(config.Healthcheck) ||
		!absentNullOrEmptyString(config.User) ||
		!absentNullOrEmptyString(config.WorkingDir) ||
		!absentNullOrEmptyObject(config.ExposedPorts) ||
		!absentNullOrEmptyArray(config.OnBuild) ||
		!absentNullOrEmptyArray(config.Shell) ||
		!absentNullOrEmptyString(config.StopSignal) ||
		!(config.StopTimeout === undefined || config.StopTimeout === null) ||
		!(config.NetworkDisabled === undefined || config.NetworkDisabled === null || config.NetworkDisabled === false) ||
		!absentNullOrEmptyString(config.Hostname) ||
		!absentNullOrEmptyString(config.Domainname) ||
		!absentNullOrEmptyString(config.MacAddress)
	) reject();
	return true;
}

function pullImage(state) {
	setPublicFailStage("E_IMAGE_PULL");
	validateTool(hostTools.docker);
	runExact(hostTools.docker, ["pull", "--platform", PYLON_SANDBOX_POLICY.platform, PYLON_SANDBOX_POLICY.image], {
		env: dockerEnvironment(state.home), timeoutMs: 5 * 60_000, description: "Pinned image pull failed.",
	});
	setPublicFailStage("E_IMAGE_CONFIG");
	const inspected = runExact(hostTools.docker, ["image", "inspect", "--format", "{{json .}}", PYLON_SANDBOX_POLICY.image], {
		capture: true, env: dockerEnvironment(state.home), description: "Pinned image inspection failed.",
	});
	let value;
	try { value = JSON.parse(inspected); } catch { reject(); }
	validateImageInspection(value);
}

function verifyNpmArchive(path) {
	const metadata = lstatSync(path);
	if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size < 1 || metadata.size > PYLON_SANDBOX_POLICY.npmMaxBytes) reject();
	const bytes = readFileSync(path);
	const sha256 = createHash("sha256").update(bytes).digest("hex");
	const sha512 = createHash("sha512").update(bytes).digest();
	if (sha256 !== PYLON_SANDBOX_POLICY.npmSha256 || sha512.toString("hex") !== PYLON_SANDBOX_POLICY.npmSha512 || `sha512-${sha512.toString("base64")}` !== PYLON_SANDBOX_POLICY.npmSri) reject();
}

function extractNpmRuntime(archive, destination, env) {
	verifyNpmArchive(archive);
	mkdirSync(destination, { mode: 0o700 });
	runExact(hostTools.tar, ["--extract", "--gzip", "--file", archive, "--directory", destination, "--no-same-owner"], { env });
	const cli = join(destination, "package", "bin", "npm-cli.js");
	if (!lstatSync(cli).isFile()) reject();
	const version = runExact(process.execPath, [cli, "--version"], { capture: true, env, description: "Pinned npm validation failed." });
	if (version !== PYLON_SANDBOX_POLICY.npmVersion) reject();
	return cli;
}

function verifySmokeArtifacts(options) {
	if (options.mode === "ci") {
		runExact(process.execPath, ["/source/scripts/verify-pylon-prime-agent-release.mjs", "--artifact-dir", "/artifacts"], { cwd: "/source", env: process.env });
		return JSON.parse(readFileSync("/artifacts/pylon-prime-agent-release-v1.json", "utf8"));
	}
	const historical = options.mode === "historical" ? ["--historical"] : [];
	runExact(process.execPath, ["/source/scripts/verify-pylon-preview-publication.mjs", ...historical, "--artifact-dir", "/artifacts"], { cwd: "/source", env: process.env });
	return JSON.parse(readFileSync("/artifacts/pylon-prime-agent-release-v1.json", "utf8"));
}

function prepareSmokeCache(options, npmCli) {
	const release = verifySmokeArtifacts(options);
	if (!Array.isArray(release.assets) || release.assets.length !== 4) reject();
	const packages = new Map();
	for (const asset of release.assets) {
		if (
			asset === null || typeof asset !== "object" || typeof asset.package !== "string" ||
			typeof asset.file !== "string" || !/^[A-Za-z0-9._-]+$/.test(asset.file) || packages.has(asset.package)
		) reject();
		packages.set(asset.package, `file:///artifacts/${asset.file}`);
	}
	const rootArchive = packages.get("prime-agent");
	if (rootArchive === undefined) reject();
	const overrides = Object.fromEntries([...packages.entries()].filter(([name]) => name !== "prime-agent"));
	const consumer = "/tmp/cache-consumer";
	mkdirSync(consumer, { mode: 0o700 });
	writeFileSync(join(consumer, "package.json"), `${JSON.stringify({ name: "pylon-prime-artifact-cache", private: true, dependencies: { "prime-agent": rootArchive }, overrides }, null, "\t")}\n`, { flag: "wx", mode: 0o600 });
	runExact(process.execPath, [npmCli, "install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false"], {
		cwd: consumer,
		env: process.env,
		timeoutMs: 10 * 60_000,
	});
}

function containerPhase(options) {
	process.umask(0o022);
	if (process.execPath !== "/usr/local/bin/node" || process.version !== `v${PYLON_SANDBOX_POLICY.nodeVersion}`) reject();
	for (const name of Object.keys(process.env)) if (forbiddenEnvironment.test(name)) reject();
	if (["dependencies", "pack", "smoke-cache", "smoke"].includes(options.containerPhase)) {
		const npmCli = extractNpmRuntime("/input/npm.tgz", "/tmp/npm-runtime", process.env);
		if (options.containerPhase === "dependencies") {
			runExact(process.execPath, [npmCli, "ci", "--ignore-scripts", "--no-audit", "--no-fund"], { cwd: "/build", env: process.env, timeoutMs: 8 * 60_000 });
			return;
		}
		if (options.containerPhase === "pack") {
			runExact(process.execPath, [npmCli, "run", "release:pylon:pack"], { cwd: "/build", env: process.env, timeoutMs: 8 * 60_000 });
			return;
		}
		if (options.containerPhase === "smoke-cache") {
			prepareSmokeCache(options, npmCli);
			return;
		}
	}
	if (options.containerPhase === "contracts") {
		const contractTemp = "/candidate/.contract-tmp";
		mkdirSync(contractTemp, { mode: 0o700 });
		const contractEnvironment = { ...process.env, TMPDIR: contractTemp };
		try {
			runExact(process.execPath, ["--test", "/source/scripts/pylon-prime-agent-release.test.mjs"], { cwd: "/source", env: contractEnvironment, timeoutMs: 3 * 60_000 });
			runExact(process.execPath, ["--test", "/source/scripts/pylon-publication.test.mjs"], { cwd: "/source", env: contractEnvironment, timeoutMs: 3 * 60_000 });
		} finally {
			removeOwnedTree(contractTemp);
		}
		return;
	}
	if (options.containerPhase === "prepare") {
		runExact(process.execPath, ["/source/scripts/verify-pylon-prime-agent-release.mjs", "--artifact-dir", "/candidate"], { cwd: "/source", env: process.env });
		if (options.mode === "preview") {
			runExact(process.execPath, ["/source/scripts/prepare-pylon-preview-manifest.mjs", "--artifact-dir", "/candidate", "--publication-policy-revision", String(options.publicationPolicyRevision)], { cwd: "/source", env: process.env });
		}
		return;
	}
	if (options.containerPhase === "final") {
		const verifier = options.mode === "preview" ? "/source/scripts/verify-pylon-preview-publication.mjs" : "/source/scripts/verify-pylon-prime-agent-release.mjs";
		runExact(process.execPath, [verifier, "--artifact-dir", "/candidate"], { cwd: "/source", env: process.env });
		writeFinalReceipt("/candidate", "/control", options.mode === "preview" ? 6 : 5);
		return;
	}
	if (options.containerPhase === "smoke") {
		verifySmokeArtifacts(options);
		const historical = options.mode === "historical";
		let artifacts = "/artifacts";
		if (options.mode !== "ci") {
			artifacts = "/tmp/release-artifacts";
			releaseOnlyCopy("/artifacts", artifacts, { historical });
		}
		runExact(process.execPath, ["/source/scripts/smoke-pylon-prime-agent-release.mjs", ...(historical ? ["--historical"] : []), "--artifact-dir", artifacts], { cwd: "/source", env: process.env, timeoutMs: 10 * 60_000 });
		return;
	}
	reject();
}

function parseArguments(args) {
	if (args.length % 2 !== 0) reject();
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) {
		const key = args[index];
		const value = args[index + 1];
		if (!key.startsWith("--") || values.has(key)) reject();
		values.set(key, value);
	}
	if (values.has("--container-phase")) {
		const allowed = new Set(["--container-phase", "--mode", "--publication-policy-revision"]);
		if ([...values.keys()].some((key) => !allowed.has(key))) reject();
		const publicationPolicyRevision = Number(values.get("--publication-policy-revision"));
		const result = { containerPhase: values.get("--container-phase"), mode: values.get("--mode"), publicationPolicyRevision };
		if (!["dependencies", "pack", "contracts", "prepare", "final", "smoke-cache", "smoke"].includes(result.containerPhase) || !["ci", "preview", "historical"].includes(result.mode) || !Number.isSafeInteger(publicationPolicyRevision) || publicationPolicyRevision < 1) reject();
		if (result.mode === "historical" && !["smoke-cache", "smoke"].includes(result.containerPhase)) reject();
		return result;
	}
	const allowed = new Set(["--artifact-dir", "--github-output", "--host-os", "--mode", "--publication-policy-revision", "--run-id", "--run-number", "--task", "--workspace"]);
	if ([...values.keys()].some((key) => !allowed.has(key))) reject();
	const result = {
		artifactDir: values.get("--artifact-dir"),
		githubOutput: values.get("--github-output"),
		hostOs: values.get("--host-os"),
		mode: values.get("--mode"),
		publicationPolicyRevision: Number(values.get("--publication-policy-revision")),
		runId: values.get("--run-id"),
		runNumber: values.get("--run-number"),
		task: values.get("--task"),
		workspace: values.get("--workspace"),
	};
	if (!["pack", "smoke"].includes(result.task) || !["ci", "preview", "historical"].includes(result.mode) || !["Linux", "macOS"].includes(result.hostOs)) reject();
	if (result.mode === "historical" && result.task !== "smoke") reject();
	if (!Number.isSafeInteger(result.publicationPolicyRevision) || result.publicationPolicyRevision < 1 || !/^[0-9]+$/.test(result.runId ?? "") || !/^[0-9]+$/.test(result.runNumber ?? "")) reject();
	if (!safeAbsolute(result.workspace) || (result.task === "pack" && (!safeAbsolute(result.githubOutput) || result.hostOs !== "Linux")) || (result.task === "smoke" && !safeAbsolute(result.artifactDir))) reject();
	return result;
}

function removeOwnedTree(path) {
	if (!lstatExists(path)) return;
	const metadata = lstatSync(path);
	if (metadata.isDirectory() && !metadata.isSymbolicLink()) {
		chmodSync(path, 0o700);
		const directory = opendirSync(path);
		try {
			for (let entry = directory.readSync(); entry !== null; entry = directory.readSync()) removeOwnedTree(join(path, entry.name));
		} finally {
			directory.closeSync();
		}
	}
	rmSync(path, { force: true, recursive: true });
}

function makeState() {
	const control = realpathSync.native(mkdtempSync("/tmp/pylon-release-control."));
	const scratch = realpathSync.native(mkdtempSync("/tmp/pylon-release-scratch."));
	chmodSync(control, 0o700);
	chmodSync(scratch, 0o700);
	const home = join(control, "home");
	const cidDirectory = join(control, "cids");
	mkdirSync(home, { mode: 0o700 });
	mkdirSync(cidDirectory, { mode: 0o700 });
	return { activePgid: null, cidDirectory, containers: [], control, home, nonce: control.split(".").at(-1).toLowerCase(), scratch };
}

function validateHost(options) {
	if (process.version !== `v${PYLON_SANDBOX_POLICY.nodeVersion}` || process.getuid() === 0 || process.getgid() === 0) reject();
	const workspace = realpathSync.native(options.workspace);
	if (workspace !== options.workspace || workspace !== root) reject();
	validateTool(hostTools.git);
	validateTool(hostTools.tar);
	validateTool(process.execPath, { rootOwned: false });
	return workspace;
}

function processGroupIsAlive(pid) {
	try { process.kill(-pid, 0); return true; } catch (error) { return error?.code !== "ESRCH"; }
}

async function waitForProcessGroupDrain(pid, timeoutMs) {
	const deadline = Date.now() + timeoutMs;
	while (processGroupIsAlive(pid) && Date.now() < deadline) {
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
	}
	return !processGroupIsAlive(pid);
}

export async function runDetachedProcessGroup(command, args, options) {
	const lifecycle = options.lifecycle ?? { activePgid: null };
	if (lifecycle.activePgid !== null) reject();
	return await new Promise((resolveRun, rejectRun) => {
		const child = spawn(command, args, { cwd: options.cwd, env: options.env, detached: true, stdio: "inherit" });
		if (!Number.isSafeInteger(child.pid) || child.pid < 1) {
			child.once("error", rejectRun);
			child.once("close", () => rejectRun(new Error()));
			return;
		}
		const pgid = child.pid;
		lifecycle.activePgid = pgid;
		const receipt = { pid: pgid, status: undefined, signal: undefined };
		let settled = false;
		let timedOut = false;
		const timer = setTimeout(() => {
			timedOut = true;
			try { process.kill(-pgid, "SIGKILL"); } catch {}
		}, options.timeoutMs);
		child.once("error", async (error) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			await terminateActiveProcessGroup(lifecycle);
			rejectRun(error);
		});
		child.once("close", async (status, signal) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			receipt.status = status;
			receipt.signal = signal;
			const groupAlive = processGroupIsAlive(pgid);
			if (groupAlive) {
				try { process.kill(-pgid, "SIGKILL"); } catch {}
			}
			const drained = !groupAlive || await waitForProcessGroupDrain(pgid, 3_000);
			if (drained && lifecycle.activePgid === pgid) lifecycle.activePgid = null;
			if (!drained || timedOut || groupAlive || status !== 0 || signal !== null) rejectRun(new Error());
			else resolveRun(receipt);
		});
	});
}

export function releaseOnlyCopy(source, destination, { historical }) {
	const releaseBytes = readPinnedSmall(join(source, "pylon-prime-agent-release-v1.json"), process.getuid(), process.getgid(), 0o444);
	let release;
	try { release = JSON.parse(releaseBytes.toString("utf8")); } catch { reject(); }
	if (!Array.isArray(release.assets) || release.assets.length !== 4) reject();
	const expected = new Set([...release.assets.map((asset) => asset.file), "pylon-prime-agent-release-v1.json"]);
	if (historical) expected.add("pylon-preview-channel-v1.json");
	if (expected.size !== (historical ? 6 : 5)) reject();
	return copyBoundedSubjects(source, destination, {
		expectedCount: expected.size,
		expectedNames: [...expected],
		inputMode: 0o444,
		sourceExpectedCount: 6,
	});
}

async function runMacSmoke(state, options, source, artifacts, npmArchive) {
	const runtime = join(state.scratch, "npm-runtime");
	const temp = join(state.control, "tmp");
	mkdirSync(temp, { mode: 0o700 });
	const env = {
		HOME: state.home,
		TMPDIR: temp,
		PATH: `${dirname(process.execPath)}:/usr/bin:/bin`,
		CI: "true",
		GITHUB_ACTIONS: "true",
		GIT_OPTIONAL_LOCKS: "0",
		GIT_CONFIG_NOSYSTEM: "1",
		GIT_CONFIG_COUNT: "3",
		GIT_CONFIG_KEY_0: "safe.directory",
		GIT_CONFIG_VALUE_0: source,
		GIT_CONFIG_KEY_1: "core.fsmonitor",
		GIT_CONFIG_VALUE_1: "false",
		GIT_CONFIG_KEY_2: "core.hooksPath",
		GIT_CONFIG_VALUE_2: "/dev/null",
		npm_config_userconfig: "/dev/null",
	};
	const npmCli = extractNpmRuntime(npmArchive, runtime, env);
	env.PYLON_RELEASE_NPM_CLI = npmCli;
	let smokeArtifacts = artifacts;
	if (options.mode !== "ci") {
		const historical = options.mode === "historical";
		runExact(process.execPath, [join(source, "scripts/verify-pylon-preview-publication.mjs"), ...(historical ? ["--historical"] : []), "--artifact-dir", artifacts], { cwd: source, env });
		smokeArtifacts = join(state.scratch, "release-artifacts");
		releaseOnlyCopy(artifacts, smokeArtifacts, { historical });
	} else {
		runExact(process.execPath, [join(source, "scripts/verify-pylon-prime-agent-release.mjs"), "--artifact-dir", artifacts], { cwd: source, env });
	}
	const historical = options.mode === "historical" ? ["--historical"] : [];
	const receipt = await runDetachedProcessGroup(process.execPath, [join(source, "scripts/smoke-pylon-prime-agent-release.mjs"), ...historical, "--artifact-dir", smokeArtifacts], {
		cwd: source, env, lifecycle: state, timeoutMs: 10 * 60_000,
	});
	if (!Number.isSafeInteger(receipt.pid) || receipt.pid < 1 || receipt.status !== 0 || receipt.signal !== null) reject();
}

export function exactArtifactDirectory(path) {
	const metadata = lstatSync(path);
	if (!metadata.isDirectory() || metadata.isSymbolicLink()) reject();
	const resolved = realpathSync.native(path);
	if (resolved !== path) reject();
	return resolved;
}

async function orchestrate(options) {
	setPublicFailStage("E_INITIALIZE");
	const state = makeState();
	const removeSignalHandlers = installSignalCleanup(state);
	let success = false;
	let operationFailureStage;
	let finalOutput;
	try {
		setPublicFailStage("E_HOST");
		const workspace = validateHost(options);
		const identity = validateOriginal(workspace, undefined, state.home);
		setPublicFailStage("E_SOURCE");
		const pristine = preparePristineSource(workspace, state.scratch, state.home, identity, { withBuild: options.task === "pack" });
		const npmArchive = join(state.scratch, "npm-11.10.1.tgz");
		setPublicFailStage("E_NPM");
		await downloadNpmArchive(npmArchive);
		if (options.task === "pack") {
			pullImage(state);
			runDockerPhase(state, "dependencies", { build: pristine.build, git: pristine.git, npm: npmArchive, source: pristine.source }, options);
			const dependencies = join(pristine.build, "node_modules");
			if (!lstatSync(dependencies).isDirectory() || realpathSync.native(dependencies) !== dependencies) reject();
			chmodTreeReadOnly(dependencies);
			runDockerPhase(state, "pack", { build: pristine.build, dependencies, git: pristine.git, npm: npmArchive, source: pristine.source }, options);
			const candidate = join(state.scratch, "candidate");
			copyBoundedSubjects(join(pristine.build, ".npm", "pylon-release", "artifacts"), candidate, { expectedCount: 5 });
			const contractCandidate = join(state.scratch, "contract-candidate");
			copyBoundedSubjects(candidate, contractCandidate, { expectedCount: 5 });
			runDockerPhase(state, "contracts", { candidate: contractCandidate, dependencies, source: pristine.source }, options);
			setPublicFailStage("E_CLEANUP");
			if (!cleanupContainers(state)) reject("Captured container cleanup failed.");
			removeOwnedTree(contractCandidate);
			runDockerPhase(state, "prepare", { candidate, source: pristine.source }, options);
			freezeSubjectDirectory(candidate, options.mode === "preview" ? 6 : 5);
			const receiptControl = join(state.control, "verification");
			mkdirSync(receiptControl, { mode: 0o700 });
			runDockerPhase(state, "final", { candidate, control: receiptControl, source: pristine.source }, options);
			setPublicFailStage("E_CLEANUP");
			if (!cleanupContainers(state)) reject("Captured container cleanup failed.");
			const expectedCount = options.mode === "preview" ? 6 : 5;
			setPublicFailStage("E_RECEIPT");
			const finalReceipt = readFinalReceipt(join(receiptControl, "final-receipt.json"), expectedCount);
			setPublicFailStage("E_SOURCE");
			validateOriginal(workspace, identity, state.home);
			setPublicFailStage("E_FINAL_COPY");
			finalOutput = join(state.control, "final-artifacts");
			copyBoundedSubjects(candidate, finalOutput, { expectedCount, expectedReceipt: finalReceipt, freeze: true, inputMode: 0o444 });
		} else {
			setPublicFailStage("E_ARTIFACT_INPUT");
			const artifactInput = join(state.scratch, "artifact-input");
			copyBoundedSubjects(exactArtifactDirectory(options.artifactDir), artifactInput, {
				expectedCount: options.mode === "ci" ? 5 : 6,
				freeze: true,
				inputGid: options.hostOs === "macOS" ? null : process.getgid(),
			});
			if (options.hostOs === "Linux") {
				pullImage(state);
				const cache = join(state.scratch, "smoke-cache");
				mkdirSync(cache, { mode: 0o700 });
				runDockerPhase(state, "smoke-cache", { artifacts: artifactInput, cache, npm: npmArchive, source: pristine.source }, options);
				runDockerPhase(state, "smoke", { artifacts: artifactInput, cache, npm: npmArchive, source: pristine.source }, options);
			} else {
				setPublicFailStage("E_SMOKE");
				await runMacSmoke(state, options, pristine.source, artifactInput, npmArchive);
			}
		}
		success = true;
	} catch (error) {
		operationFailureStage = publicFailStage;
		throw error;
	} finally {
		setPublicFailStage("E_CLEANUP");
		let processGroupClean = false;
		let containersClean = false;
		try {
			processGroupClean = await terminateActiveProcessGroup(state);
			containersClean = options.hostOs !== "Linux" || cleanupContainers(state);
			removeOwnedTree(state.scratch);
			if (!success || options.task === "smoke") removeOwnedTree(state.control);
			else {
				removeOwnedTree(state.home);
				removeOwnedTree(state.cidDirectory);
			}
		} finally {
			await removeSignalHandlers();
		}
		if (!processGroupClean) reject("Captured process-group cleanup failed.");
		if (!containersClean) reject("Captured container cleanup failed.");
		if (operationFailureStage !== undefined) setPublicFailStage(operationFailureStage);
	}
	if (options.task === "pack") {
		setPublicFailStage("E_OUTPUT");
		appendFileSync(options.githubOutput, `artifact_dir=${finalOutput}
`, { encoding: "utf8" });
	}
}

const isMain = process.argv[1] !== undefined && realpathSync.native(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
	try {
		setPublicFailStage("E_ARGUMENTS");
		const options = parseArguments(process.argv.slice(2));
		if ("containerPhase" in options) {
			setContainerFailStage(options.containerPhase);
			containerPhase(options);
		} else await orchestrate(options);
	} catch {
		console.error(formatPublicFailure(publicFailStage));
		process.exitCode = 1;
	}
}
