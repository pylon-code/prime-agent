#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import {
	closeSync,
	fsyncSync,
	lstatSync,
	openSync,
	readFileSync,
	renameSync,
	rmSync,
	writeFileSync,
} from "node:fs";
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

function readCanonicalState(statePath) {
	if (!lstatSync(statePath).isFile()) throw new Error("Consumer stable high-water state is not one regular file.");
	const bytes = readFileSync(statePath);
	const state = validateConsumerState(JSON.parse(bytes));
	if (bytes.toString("utf8") !== canonicalJson(state)) {
		throw new Error("Consumer stable high-water state is not canonical JSON.");
	}
	return state;
}

function syncDirectory(path) {
	let descriptor;
	try {
		descriptor = openSync(path, "r");
		fsyncSync(descriptor);
	} catch (error) {
		if (!(["EINVAL", "EPERM", "EISDIR"].includes(error?.code))) throw error;
	} finally {
		if (descriptor !== undefined) closeSync(descriptor);
	}
}

function writeStateAtomically(statePath, state) {
	const directory = dirname(statePath);
	const temporary = resolve(directory, `.${basename(statePath)}.${process.pid}.${randomUUID()}.tmp`);
	let descriptor;
	try {
		descriptor = openSync(temporary, "wx", 0o600);
		writeFileSync(descriptor, canonicalJson(state));
		fsyncSync(descriptor);
		closeSync(descriptor);
		descriptor = undefined;
		renameSync(temporary, statePath);
		syncDirectory(directory);
	} finally {
		if (descriptor !== undefined) closeSync(descriptor);
		rmSync(temporary, { force: true });
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

export function verifyStableHistoryWithState(paths, { statePath, initialize = false }) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local --state path is required.");
	const absoluteStatePath = resolve(statePath);
	return withConsumerStateLock(absoluteStatePath, () => {
		const stateEntry = lstatSync(absoluteStatePath, { throwIfNoEntry: false });
		const stateExists = stateEntry !== undefined;
		if (stateExists && !stateEntry.isFile()) {
			throw new Error("Consumer stable high-water state is not one regular file.");
		}
		if (!stateExists && !initialize) {
			throw new Error("No consumer high-water state exists. Inspect the full history, then use --initialize once to accept its witnessed high-water.");
		}
		if (stateExists && initialize) throw new Error("Consumer high-water state already exists; --initialize cannot reset it.");
		const priorState = stateExists ? readCanonicalState(absoluteStatePath) : null;
		const history = validateStableHistory(verifiedManifestFiles(paths));
		const latest = history.at(-1);
		const highWater = {
			sequence: latest.sequence,
			tag: latest.tag,
			sha256: sha256Bytes(Buffer.from(canonicalJson(latest))),
		};
		if (priorState) {
			if (latest.sequence < priorState.highWater.sequence) {
				throw new Error("Verified stable history is older than the persisted consumer high-water mark.");
			}
			const witnessed = history.find((manifest) => manifest.sequence === priorState.highWater.sequence);
			if (
				!witnessed ||
				witnessed.tag !== priorState.highWater.tag ||
				sha256Bytes(Buffer.from(canonicalJson(witnessed))) !== priorState.highWater.sha256
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
		if (advanced) writeStateAtomically(absoluteStatePath, state);
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
		const verified = verifyStableHistoryWithState(args.paths, args);
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
