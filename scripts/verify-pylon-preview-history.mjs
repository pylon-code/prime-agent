#!/usr/bin/env node

import { lstatSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	canonicalJson,
	parsePreviewTag,
	PYLON_PREVIEW_MANIFEST,
	sha256Bytes,
} from "./lib/pylon-publication.mjs";
import { PYLON_RELEASE_REPOSITORY } from "./lib/pylon-release.mjs";
import { withConsumerStateLock } from "./lib/pylon-consumer-lock.mjs";
import { verifyPreviewAttestations } from "./verify-pylon-publication-attestations.mjs";

const STATE_SCHEMA_VERSION = 1;
const STATE_MAX_BYTES = 4 * 1024;
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function exactKeys(value, keys) {
	return value !== null && typeof value === "object" && !Array.isArray(value) &&
		Object.keys(value).sort().join(",") === [...keys].sort().join(",");
}

function validateState(state) {
	if (
		!exactKeys(state, ["schemaVersion", "repository", "channel", "sequenceEpoch", "highWater"]) ||
		state.schemaVersion !== STATE_SCHEMA_VERSION || state.repository !== PYLON_RELEASE_REPOSITORY ||
		state.channel !== "preview" || state.sequenceEpoch !== 1 ||
		!exactKeys(state.highWater, ["sequence", "tag", "sha256", "workflowRunId"]) ||
		!Number.isSafeInteger(state.highWater.sequence) || state.highWater.sequence < 1 ||
		parsePreviewTag(state.highWater.tag).recipeRevision < 1 || !/^[0-9a-f]{64}$/.test(state.highWater.sha256 ?? "") ||
		!/^[1-9][0-9]*$/.test(state.highWater.workflowRunId ?? "")
	) throw new Error("Consumer preview high-water state is malformed.");
	return state;
}

function readState(bytes) {
	if (!Buffer.isBuffer(bytes) || bytes.length < 1 || bytes.length > STATE_MAX_BYTES) {
		throw new Error("Consumer preview high-water state is malformed.");
	}
	const state = validateState(JSON.parse(bytes));
	if (bytes.toString("utf8") !== canonicalJson(state)) throw new Error("Consumer preview high-water state is not canonical JSON.");
	return state;
}

export async function recordPreviewHighWater(previewManifest, previewBytes, { statePath, initialize = false }) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local --state path is required.");
	if (!Buffer.isBuffer(previewBytes) || previewBytes.toString("utf8") !== canonicalJson(previewManifest)) {
		throw new Error("Preview high-water requires exact canonical verified manifest bytes.");
	}
	if (
		previewManifest.sequenceEpoch !== 1 || !Number.isSafeInteger(previewManifest.sequence) || previewManifest.sequence < 1 ||
		!/^[1-9][0-9]*$/.test(previewManifest.workflowRunId ?? "")
	) throw new Error("Verified preview has a malformed monotonic sequence identity.");
	const path = resolve(statePath);
	const highWater = {
		sequence: previewManifest.sequence,
		tag: previewManifest.build.tag,
		sha256: sha256Bytes(previewBytes),
		workflowRunId: previewManifest.workflowRunId,
	};
	return withConsumerStateLock(path, async (_lockedPath, transaction) => {
		const priorBytes = transaction.readStateBytes();
		if (priorBytes === null && !initialize) throw new Error("No consumer preview high-water exists. Verify the release, then use --initialize once.");
		if (priorBytes !== null && initialize) throw new Error("Consumer preview high-water already exists; --initialize cannot reset it.");
		const prior = priorBytes === null ? null : readState(priorBytes);
		if (prior) {
			if (prior.sequenceEpoch !== previewManifest.sequenceEpoch) throw new Error("Preview sequence epoch changed without a new signed state schema.");
			if (highWater.sequence < prior.highWater.sequence) throw new Error("Verified preview is older than the consumer high-water sequence.");
			if (highWater.sequence === prior.highWater.sequence) {
				if (canonicalJson(highWater) !== canonicalJson(prior.highWater)) {
					throw new Error("Verified preview equivocates at the consumer high-water sequence.");
				}
				return { state: prior, advanced: false };
			}
		}
		const state = {
			schemaVersion: STATE_SCHEMA_VERSION,
			repository: PYLON_RELEASE_REPOSITORY,
			channel: "preview",
			sequenceEpoch: previewManifest.sequenceEpoch,
			highWater,
		};
		await transaction.commitState(Buffer.from(canonicalJson(state)));
		return { state, advanced: true };
	}, { stateMaxBytes: STATE_MAX_BYTES });
}

function parseArgs(args) {
	const remaining = [...args];
	const flag = (name) => {
		const index = remaining.indexOf(name);
		if (index === -1) return false;
		remaining.splice(index, 1);
		return true;
	};
	const initialize = flag("--initialize");
	const historical = flag("--historical");
	const value = (name, fallback) => {
		const index = remaining.indexOf(name);
		if (index === -1) return fallback;
		const result = remaining[index + 1];
		if (!result || result.startsWith("--")) throw new Error(`Missing value for ${name}.`);
		remaining.splice(index, 2);
		return result;
	};
	const statePath = value("--state", "");
	const artifactDir = resolve(root, value("--artifact-dir", ".npm/pylon-release/artifacts"));
	if (remaining.length > 0 || !statePath) throw new Error("Usage: verify-pylon-preview-history --state <path> [--initialize] [--historical] [--artifact-dir path]");
	return { statePath, artifactDir, initialize, historical };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const previewPath = join(args.artifactDir, PYLON_PREVIEW_MANIFEST);
		if (!lstatSync(previewPath).isFile()) throw new Error("Preview manifest is not one regular file.");
		const previewBytes = readFileSync(previewPath);
		const untrusted = JSON.parse(previewBytes);
		const verified = verifyPreviewAttestations({
			artifactDir: args.artifactDir,
			sourceSha: untrusted.build?.source?.commit ?? "",
			sourceTree: untrusted.build?.source?.tree ?? "",
			historical: args.historical,
		});
		const result = await recordPreviewHighWater(verified.previewManifest, previewBytes, args);
		console.log(JSON.stringify({ highWater: result.state.highWater, advanced: result.advanced }));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
