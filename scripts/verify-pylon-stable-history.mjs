#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import { lstat, open, readFile, rename, rm } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { PYLON_RELEASE_REPOSITORY } from "./lib/pylon-release.mjs";
import { withConsumerStateLock } from "./lib/pylon-consumer-lock.mjs";
import {
	canonicalJson,
	parseStableTag,
	sha256Bytes,
	validateStableHistory,
	validateStableManifest,
} from "./lib/pylon-publication.mjs";

const STATE_SCHEMA_VERSION = 1;
const STATE_MAX_BYTES = 4 * 1024;

function exactKeys(value, keys) {
	return (
		value !== null &&
		typeof value === "object" &&
		!Array.isArray(value) &&
		Object.keys(value).sort().join(",") === [...keys].sort().join(",")
	);
}

function validateConsumerState(state) {
	if (
		!exactKeys(state, ["schemaVersion", "repository", "channel", "highWater"]) ||
		state.schemaVersion !== STATE_SCHEMA_VERSION ||
		state.repository !== PYLON_RELEASE_REPOSITORY ||
		state.channel !== "stable" ||
		!exactKeys(state.highWater, ["sequence", "tag", "sha256"]) ||
		!Number.isSafeInteger(state.highWater.sequence) ||
		parseStableTag(state.highWater.tag).sequence !== state.highWater.sequence ||
		!/^[0-9a-f]{64}$/.test(state.highWater.sha256 ?? "")
	) {
		throw new Error("Consumer stable high-water state is malformed.");
	}
	return state;
}

async function readCanonicalState(statePath) {
	const stat = await lstat(statePath);
	if (!stat.isFile()) throw new Error("Consumer stable high-water state is not one regular file.");
	if (stat.size < 1 || stat.size > STATE_MAX_BYTES) throw new Error("Consumer stable high-water state is malformed.");
	const bytes = await readFile(statePath);
	const state = validateConsumerState(JSON.parse(bytes));
	if (bytes.toString("utf8") !== canonicalJson(state)) {
		throw new Error("Consumer stable high-water state is not canonical JSON.");
	}
	return state;
}

async function syncDirectory(path) {
	let handle;
	try {
		handle = await open(path, "r");
		await handle.sync();
	} catch (error) {
		if (!["EINVAL", "EPERM", "EISDIR"].includes(error?.code)) throw error;
	} finally {
		if (handle !== undefined) await handle.close();
	}
}

async function writeStateAtomically(statePath, state) {
	const directory = dirname(statePath);
	const temporary = resolve(directory, `.${basename(statePath)}.${process.pid}.${randomUUID()}.tmp`);
	let handle;
	try {
		handle = await open(temporary, "wx", 0o600);
		await handle.writeFile(canonicalJson(state));
		await handle.sync();
		await handle.close();
		handle = undefined;
		await rename(temporary, statePath);
		await syncDirectory(directory);
	} finally {
		if (handle !== undefined) await handle.close();
		await rm(temporary, { force: true });
	}
}

function verifiedManifestFiles(paths) {
	if (!Array.isArray(paths) || paths.length === 0) throw new Error("Provide every stable manifest from sequence 1 through current high-water.");
	return paths.map((input) => {
		const path = resolve(input);
		if (!lstatSync(path).isFile()) throw new Error(`Stable manifest is not a regular file: ${path}`);
		const bytes = readFileSync(path);
		const manifest = validateStableManifest(JSON.parse(bytes));
		if (bytes.toString("utf8") !== canonicalJson(manifest)) throw new Error(`Stable manifest is not canonical: ${path}`);
		return manifest;
	});
}

export async function verifyStableHistoryWithState(paths, { statePath, initialize = false }) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local --state path is required.");
	const absoluteStatePath = resolve(statePath);
	const history = validateStableHistory(verifiedManifestFiles(paths));
	const witnessed = new Map(history.map((manifest) => [manifest.sequence, {
		tag: manifest.tag,
		sha256: sha256Bytes(Buffer.from(canonicalJson(manifest))),
	}]));
	const latest = history.at(-1);
	const highWater = {
		sequence: latest.sequence,
		tag: latest.tag,
		sha256: witnessed.get(latest.sequence).sha256,
	};
	return withConsumerStateLock(absoluteStatePath, async () => {
		let stateEntry;
		try {
			stateEntry = await lstat(absoluteStatePath);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
		}
		const stateExists = stateEntry !== undefined;
		if (stateExists && !stateEntry.isFile()) {
			throw new Error("Consumer stable high-water state is not one regular file.");
		}
		if (!stateExists && !initialize) {
			throw new Error("No consumer high-water state exists. Inspect the full history, then use --initialize once to accept its witnessed high-water.");
		}
		if (stateExists && initialize) throw new Error("Consumer high-water state already exists; --initialize cannot reset it.");
		const priorState = stateExists ? await readCanonicalState(absoluteStatePath) : null;
		if (priorState) {
			if (latest.sequence < priorState.highWater.sequence) {
				throw new Error("Verified stable history is older than the persisted consumer high-water mark.");
			}
			const priorWitness = witnessed.get(priorState.highWater.sequence);
			if (
				!priorWitness ||
				priorWitness.tag !== priorState.highWater.tag ||
				priorWitness.sha256 !== priorState.highWater.sha256
			) {
				throw new Error("Verified stable history rewrites the consumer's persisted high-water sequence.");
			}
		}
		const state = {
			schemaVersion: STATE_SCHEMA_VERSION,
			repository: PYLON_RELEASE_REPOSITORY,
			channel: "stable",
			highWater,
		};
		const advanced = !priorState || highWater.sequence > priorState.highWater.sequence;
		if (advanced) await writeStateAtomically(absoluteStatePath, state);
		return { history, state: advanced ? state : priorState, advanced };
	});
}

function parseArgs(args) {
	const remaining = [...args];
	const stateIndex = remaining.indexOf("--state");
	if (stateIndex === -1 || !remaining[stateIndex + 1] || remaining[stateIndex + 1].startsWith("--")) {
		throw new Error("Usage: verify-pylon-stable-history --state <path> [--initialize] <manifest...>");
	}
	const statePath = remaining[stateIndex + 1];
	remaining.splice(stateIndex, 2);
	const initializeIndex = remaining.indexOf("--initialize");
	const initialize = initializeIndex !== -1;
	if (initialize) remaining.splice(initializeIndex, 1);
	if (remaining.some((value) => value.startsWith("--"))) throw new Error("Unknown stable history verifier option.");
	return { statePath, initialize, paths: remaining };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const verified = await verifyStableHistoryWithState(args.paths, args);
		console.log(JSON.stringify({
			sequences: verified.history.length,
			highWater: verified.state.highWater,
			advanced: verified.advanced,
		}));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
