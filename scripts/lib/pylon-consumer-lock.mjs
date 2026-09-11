import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, lstat, mkdir, open, readdir, rename, rm } from "node:fs/promises";
import { basename, dirname, join, parse, relative, resolve, sep } from "node:path";

import {
	BoundedFileLinkRetiredBeforeReadError,
	BoundedFileLinkRetiredDuringReadError,
	BoundedFileUnlinkedDuringReadError,
	readBoundedRegularFile,
} from "./pylon-bounded-file.mjs";

import {
	consumerGenerationGenesisCheckpoint,
	consumerGenerationName,
	consumerGenerationRotationClaim,
	consumerGenerationSuccessorCheckpoint,
	generationRecordMaxBytes,
	GENERATION_EPOCH_MAX_ENTRIES,
	GENERATION_JOURNAL_MAX_BYTES,
	GENERATION_RECEIPT_MAX_ENTRIES,
	GENERATION_ROOT_MAX_ENTRIES,
	GENERATION_STATE_MAX_BYTES,
	validateGenerationCheckpoint,
} from "./pylon-generation-format.mjs";

export { consumerGenerationGenesisCheckpoint, consumerGenerationName, consumerGenerationRotationClaim, consumerGenerationSuccessorCheckpoint };

class ConsumerEpochAdvancedError extends Error {
	constructor() {
		super("Consumer high-water journal epoch changed and fenced a paused writer.");
		this.name = "ConsumerEpochAdvancedError";
	}
}

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
export const PYLON_CONSUMER_ROTATE_CLAIM_TRIGGER = 60_000;
export const PYLON_CONSUMER_ROTATE_TRANSITION_TRIGGER = 3_800;
const LOCK_SCHEMA_VERSION = 2;
const CLAIM_INDEX_SCHEMA_VERSION = 1;
const LEGACY_LOCK_SCHEMA_VERSION = 1;
const TRANSACTION_SCHEMA_VERSION = 1;
const CHECKPOINT_SCHEMA_VERSION = 2;
const ROTATION_INTENT_SCHEMA_VERSION = 2;
const LEGACY_GUARD_SCHEMA_VERSION = 1;
const LEGACY_RETIREMENT_SCHEMA_VERSION = 1;
const GENESIS_DIGEST = "0".repeat(64);
const DEFAULT_STATE_MAX_BYTES = 1024 * 1024;
const MAX_STATE_BYTES = 16 * 1024 * 1024;
const DEFAULT_JOURNAL_MAX_BYTES = 64 * 1024 * 1024;
const MAX_JOURNAL_BYTES = 256 * 1024 * 1024;
const MAX_TRANSACTION_DEPTH = 4096;
const MAX_LOCK_GENERATIONS = 65_536;
const MAX_OPERATION_GENERATIONS = MAX_LOCK_GENERATIONS + 1;
const MAX_JOURNAL_ROOT_ENTRIES = 16;
const MAX_TEMPORARY_ENTRIES = 65_536;
const PROJECTION_RETRY_LIMIT = 32;
const TEMPORARY_DIRECTORY_NAME = ".owned-temporaries-v2";
const LEGACY_RETIREMENT_MARKER_NAME = ".pylon-consumer-v1-retired.json";
const uuidSource = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const uuidPattern = new RegExp(`^${uuidSource}$`);
const claimPattern = /^claim-([0-9]{16})-([0-9a-f]{64})\.json$/;
const claimIndexPattern = /^claim-index-([0-9]{16})\.json$/;
const undigestedClaimPattern = /^claim-([0-9]{16})\.json$/;
const transitionPattern = /^transition-([0-9a-f]{64})\.json$/;
const legacyTransitionPattern = /^([0-9a-f]{64})\.json$/;
const checkpointPattern = new RegExp(`^checkpoint-([0-9]{16})-(${uuidSource})\\.json$`);
const epochPattern = new RegExp(`^epoch-([0-9]{16})-(${uuidSource})$`);
const heartbeatPattern = new RegExp(`^heartbeat-([0-9]{16})-(${uuidSource})\\.json$`);
const terminalPattern = new RegExp(`^terminal-([0-9]{16})-(${uuidSource})\\.json$`);
const appliedPattern = new RegExp(`^applied-([0-9]{16})-(${uuidSource})\\.json$`);
const temporaryPattern = new RegExp(
	`^\\.pylon-consumer-tmp-v1-p([1-9][0-9]*)-e(${uuidSource})-g([0-9]{16})-w(${uuidSource})-n([0-9a-f]{12})-k([a-z0-9-]{1,40})-t([0-9a-f]{64})\\.tmp$`,
);

function exactKeys(value, keys) {
	return value !== null && typeof value === "object" && !Array.isArray(value) &&
		Object.keys(value).sort().join(",") === [...keys].sort().join(",");
}

function metadataBytes(value) {
	return Buffer.from(`${JSON.stringify(value)}\n`);
}

function digest(bytes) {
	return createHash("sha256").update(bytes).digest("hex");
}

function generationName(generation) {
	if (!Number.isSafeInteger(generation) || generation < 0 || generation > 9_999_999_999_999_999) {
		throw new Error("Consumer high-water lock generation is exhausted or malformed.");
	}
	return String(generation).padStart(16, "0");
}

function deterministicUuid(value) {
	const hex = digest(Buffer.from(value));
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

function checkpointName(checkpoint) {
	return `checkpoint-${generationName(checkpoint.epoch)}-${checkpoint.epochId}.json`;
}

function epochName(checkpoint) {
	return `epoch-${generationName(checkpoint.epoch)}-${checkpoint.epochId}`;
}

function claimPath(context, claim) {
	return join(context.epochDirectory, `claim-${generationName(claim.generation)}-${digest(metadataBytes(claim))}.json`);
}

function claimIndexPath(context, generation) {
	return join(context.epochDirectory, `claim-index-${generationName(generation)}.json`);
}

function heartbeatPath(context, claim) {
	return join(context.epochDirectory, `heartbeat-${generationName(claim.generation)}-${claim.token}.json`);
}

function terminalPath(context, claim) {
	return join(context.epochDirectory, `terminal-${generationName(claim.generation)}-${claim.token}.json`);
}

function appliedPath(context, claim) {
	return join(context.epochDirectory, `applied-${generationName(claim.generation)}-${claim.token}.json`);
}

function transitionPath(context, baseDigest) {
	return join(context.epochDirectory, `transition-${baseDigest}.json`);
}

function validateClaim(value, context, stateMaxBytes) {
	if (
		!value || value.schemaVersion !== LOCK_SCHEMA_VERSION || !Number.isSafeInteger(value.generation) ||
		value.generation < 1 || value.generation > MAX_OPERATION_GENERATIONS || !uuidPattern.test(value.token ?? "") ||
		!["normal", "rotation"].includes(value.type)
	) throw new Error("Consumer high-water operation claim is malformed.");
	if (value.type === "normal") {
		if (
			!exactKeys(value, ["schemaVersion", "generation", "token", "type", "ownerPid", "createdAtMs"]) ||
			!Number.isSafeInteger(value.ownerPid) || value.ownerPid < 1 ||
			!Number.isSafeInteger(value.createdAtMs) || value.createdAtMs < 0
		) throw new Error("Consumer high-water normal operation claim is malformed.");
		return value;
	}
	if (!exactKeys(value, ["schemaVersion", "generation", "token", "type", "intent"]) || !context) {
		throw new Error("Consumer high-water rotation operation claim is malformed.");
	}
	const intent = validateRotationIntent(value.intent, context, stateMaxBytes);
	if (value.token !== intent.checkpoint.epochId) {
		throw new Error("Consumer high-water rotation operation claim differs from its deterministic intent.");
	}
	return value;
}

function claimIndexFor(claim) {
	return {
		schemaVersion: CLAIM_INDEX_SCHEMA_VERSION,
		generation: claim.generation,
		claimSha256: digest(metadataBytes(claim)),
	};
}

function validateClaimIndex(value, generation) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "claimSha256"]) ||
		value.schemaVersion !== CLAIM_INDEX_SCHEMA_VERSION || value.generation !== generation ||
		!/^[0-9a-f]{64}$/.test(value.claimSha256 ?? "")
	) throw new Error("Consumer high-water claim index is malformed.");
	return value;
}

function validateHeartbeat(value, claim) {
	if (
		claim.type !== "normal" ||
		!exactKeys(value, ["schemaVersion", "generation", "token", "refreshedAtMs"]) ||
		value.schemaVersion !== LOCK_SCHEMA_VERSION || value.generation !== claim.generation || value.token !== claim.token ||
		!Number.isSafeInteger(value.refreshedAtMs) || value.refreshedAtMs < claim.createdAtMs
	) throw new Error("Consumer high-water lock heartbeat is malformed.");
	return value;
}

function transactionFor(baseDigest, candidateBytes) {
	return {
		schemaVersion: TRANSACTION_SCHEMA_VERSION,
		baseDigest,
		candidateDigest: digest(candidateBytes),
		candidateBase64: candidateBytes.toString("base64"),
	};
}

function validateTransaction(value, expectedBaseDigest, stateMaxBytes) {
	if (
		!exactKeys(value, ["schemaVersion", "baseDigest", "candidateDigest", "candidateBase64"]) ||
		value.schemaVersion !== TRANSACTION_SCHEMA_VERSION || value.baseDigest !== expectedBaseDigest ||
		!/^[0-9a-f]{64}$/.test(value.candidateDigest ?? "") || typeof value.candidateBase64 !== "string" ||
		!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.candidateBase64)
	) throw new Error("Consumer high-water transaction is malformed.");
	const candidateBytes = Buffer.from(value.candidateBase64, "base64");
	if (
		candidateBytes.length < 1 || candidateBytes.length > stateMaxBytes ||
		candidateBytes.toString("base64") !== value.candidateBase64 || digest(candidateBytes) !== value.candidateDigest ||
		value.candidateDigest === value.baseDigest
	) throw new Error("Consumer high-water transaction payload is malformed.");
	return { value, candidateBytes };
}

function validateCheckpoint(value, stateMaxBytes) {
	if (
		!exactKeys(value, [
			"schemaVersion", "epoch", "epochId", "previousCheckpointSha256", "previousTipSha256",
			"historySha256", "anchorDigest", "anchorBase64", "retiredEpochDirectory", "sourceAuthoritySha256",
			"sourceAuthorityTipDigest", "sourceAuthorityTipBase64",
		]) || value.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !Number.isSafeInteger(value.epoch) || value.epoch < 1 ||
		!uuidPattern.test(value.epochId ?? "") || !/^[0-9a-f]{64}$/.test(value.previousCheckpointSha256 ?? "") ||
		!/^[0-9a-f]{64}$/.test(value.previousTipSha256 ?? "") || !/^[0-9a-f]{64}$/.test(value.historySha256 ?? "") ||
		!/^[0-9a-f]{64}$/.test(value.anchorDigest ?? "") || !/^[0-9a-f]{64}$/.test(value.sourceAuthoritySha256 ?? "") ||
		!/^[0-9a-f]{64}$/.test(value.sourceAuthorityTipDigest ?? "") ||
		!(value.retiredEpochDirectory === null || epochPattern.test(value.retiredEpochDirectory)) ||
		!(value.anchorBase64 === null || typeof value.anchorBase64 === "string") ||
		!(value.sourceAuthorityTipBase64 === null || typeof value.sourceAuthorityTipBase64 === "string")
	) throw new Error("Consumer high-water journal checkpoint is malformed.");
	let anchorBytes = null;
	if (value.anchorBase64 !== null) {
		if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.anchorBase64)) {
			throw new Error("Consumer high-water journal checkpoint is malformed.");
		}
		anchorBytes = Buffer.from(value.anchorBase64, "base64");
		if (
			anchorBytes.length < 1 || anchorBytes.length > stateMaxBytes || anchorBytes.toString("base64") !== value.anchorBase64 ||
			digest(anchorBytes) !== value.anchorDigest
		) throw new Error("Consumer high-water journal checkpoint anchor is malformed.");
	} else if (value.anchorDigest !== GENESIS_DIGEST) {
		throw new Error("Consumer high-water journal checkpoint anchor is malformed.");
	}
	if (value.sourceAuthorityTipBase64 === null) {
		if (value.sourceAuthorityTipDigest !== GENESIS_DIGEST) {
			throw new Error("Consumer high-water checkpoint source-authority tip is malformed.");
		}
	} else {
		if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.sourceAuthorityTipBase64)) {
			throw new Error("Consumer high-water checkpoint source-authority tip is malformed.");
		}
		const sourceTip = Buffer.from(value.sourceAuthorityTipBase64, "base64");
		if (
			sourceTip.length < 1 || sourceTip.length > stateMaxBytes ||
			sourceTip.toString("base64") !== value.sourceAuthorityTipBase64 || digest(sourceTip) !== value.sourceAuthorityTipDigest
		) throw new Error("Consumer high-water checkpoint source-authority tip is malformed.");
	}
	if (value.epoch === 1) {
		if (
			value.previousCheckpointSha256 !== GENESIS_DIGEST || value.previousTipSha256 !== GENESIS_DIGEST ||
			value.retiredEpochDirectory !== null
		) throw new Error("Consumer high-water genesis checkpoint is malformed.");
	} else if (value.retiredEpochDirectory === null || value.previousTipSha256 !== value.anchorDigest) {
		throw new Error("Consumer high-water rotated checkpoint is malformed.");
	}
	return { value, anchorBytes };
}

function validateRotationIntent(value, context, stateMaxBytes) {
	if (
		!exactKeys(value, ["schemaVersion", "epoch", "epochId", "checkpointSha256", "tipSha256", "checkpoint"]) ||
		value.schemaVersion !== ROTATION_INTENT_SCHEMA_VERSION || value.epoch !== context.checkpoint.epoch ||
		value.epochId !== context.checkpoint.epochId || value.checkpointSha256 !== context.checkpointDigest ||
		!/^[0-9a-f]{64}$/.test(value.tipSha256 ?? "")
	) throw new Error("Consumer high-water rotation intent is malformed.");
	const checkpoint = validateCheckpoint(value.checkpoint, stateMaxBytes).value;
	if (
		checkpoint.epoch !== context.checkpoint.epoch + 1 ||
		checkpoint.previousCheckpointSha256 !== context.checkpointDigest ||
		checkpoint.previousTipSha256 !== value.tipSha256 || checkpoint.anchorDigest !== value.tipSha256 ||
		checkpoint.retiredEpochDirectory !== basename(context.epochDirectory) ||
		checkpoint.sourceAuthoritySha256 !== context.checkpoint.sourceAuthoritySha256 ||
		checkpoint.sourceAuthorityTipDigest !== context.checkpoint.sourceAuthorityTipDigest ||
		checkpoint.sourceAuthorityTipBase64 !== context.checkpoint.sourceAuthorityTipBase64 ||
		checkpoint.historySha256 !== digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${value.tipSha256}`,
		))
	) throw new Error("Consumer high-water rotation intent does not anchor the exact epoch and tip.");
	return value;
}

function validateTerminal(value, claim, stateMaxBytes, validatePayload = validateTransaction) {
	const common = ["schemaVersion", "generation", "token", "outcome"];
	if (
		claim.type !== "normal" || !value || value.schemaVersion !== LOCK_SCHEMA_VERSION || value.generation !== claim.generation ||
		value.token !== claim.token || !["released", "retired", "commit"].includes(value.outcome)
	) throw new Error("Consumer high-water lock terminal marker is malformed.");
	if (["released", "retired"].includes(value.outcome)) {
		if (!exactKeys(value, common)) throw new Error("Consumer high-water lock terminal marker is malformed.");
		return value;
	}
	if (
		!exactKeys(value, [...common, "transactions"]) || !Array.isArray(value.transactions) ||
		value.transactions.length < 1 || value.transactions.length > 2
	) throw new Error("Consumer high-water lock commit marker is malformed.");
	let expectedBase = value.transactions[0]?.baseDigest;
	if (!/^[0-9a-f]{64}$/.test(expectedBase ?? "")) throw new Error("Consumer high-water lock commit marker is malformed.");
	for (const transaction of value.transactions) {
		validatePayload(transaction, expectedBase, stateMaxBytes);
		expectedBase = transaction.candidateDigest;
	}
	return value;
}

function validateApplied(value, claim, terminal) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "terminalSha256"]) ||
		value.schemaVersion !== LOCK_SCHEMA_VERSION || value.generation !== claim.generation || value.token !== claim.token ||
		terminal?.outcome !== "commit" || value.terminalSha256 !== digest(metadataBytes(terminal))
	) throw new Error("Consumer high-water lock applied marker is malformed.");
	return value;
}

function validateLegacyClaim(value) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "ownerPid", "createdAtMs"]) ||
		value.schemaVersion !== LEGACY_LOCK_SCHEMA_VERSION || !Number.isSafeInteger(value.generation) || value.generation < 1 ||
		!uuidPattern.test(value.token ?? "") || !Number.isSafeInteger(value.ownerPid) || value.ownerPid < 1 ||
		!Number.isSafeInteger(value.createdAtMs) || value.createdAtMs < 0
	) throw new Error("Legacy consumer high-water lock claim is malformed.");
	return value;
}

function validateLegacyHeartbeat(value, claim) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "refreshedAtMs"]) ||
		value.schemaVersion !== LEGACY_LOCK_SCHEMA_VERSION || value.generation !== claim.generation ||
		value.token !== claim.token || !Number.isSafeInteger(value.refreshedAtMs) || value.refreshedAtMs < claim.createdAtMs
	) throw new Error("Legacy consumer high-water heartbeat is malformed.");
	return value;
}

function validateLegacyTerminal(value, claim, stateMaxBytes) {
	const common = ["schemaVersion", "generation", "token", "outcome"];
	if (
		!value || value.schemaVersion !== LEGACY_LOCK_SCHEMA_VERSION || value.generation !== claim.generation ||
		value.token !== claim.token || !["released", "retired", "commit"].includes(value.outcome)
	) throw new Error("Legacy consumer high-water terminal marker is malformed.");
	if (value.outcome !== "commit") {
		if (!exactKeys(value, common)) throw new Error("Legacy consumer high-water terminal marker is malformed.");
		return value;
	}
	if (
		!exactKeys(value, [...common, "transactions"]) || !Array.isArray(value.transactions) ||
		value.transactions.length < 1 || value.transactions.length > 2
	) throw new Error("Legacy consumer high-water commit marker is malformed.");
	let expectedBase = value.transactions[0]?.baseDigest;
	if (!/^[0-9a-f]{64}$/.test(expectedBase ?? "")) throw new Error("Legacy consumer high-water commit marker is malformed.");
	for (const transaction of value.transactions) {
		validateTransaction(transaction, expectedBase, stateMaxBytes);
		expectedBase = transaction.candidateDigest;
	}
	return value;
}

function validateLegacyApplied(value, claim, terminal) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "terminalSha256"]) ||
		value.schemaVersion !== LEGACY_LOCK_SCHEMA_VERSION || value.generation !== claim.generation ||
		value.token !== claim.token || terminal?.outcome !== "commit" ||
		value.terminalSha256 !== digest(metadataBytes(terminal))
	) throw new Error("Legacy consumer high-water applied marker is malformed.");
	return value;
}

function legacyGuardFor(statePath) {
	return {
		schemaVersion: LEGACY_GUARD_SCHEMA_VERSION,
		kind: "pylon-consumer-legacy-lock-guard",
		statePathSha256: digest(Buffer.from(statePath)),
	};
}

function legacyRetirementMarkerFor(statePath, legacy) {
	return {
		schemaVersion: LEGACY_RETIREMENT_SCHEMA_VERSION,
		kind: "pylon-consumer-v1-retirement",
		statePathSha256: digest(Buffer.from(statePath)),
		authoritySha256: legacy.authoritySha256,
		tipSha256: legacy.tipDigest,
	};
}

function validateLegacyRetirementMarker(value, statePath) {
	if (
		!exactKeys(value, ["schemaVersion", "kind", "statePathSha256", "authoritySha256", "tipSha256"]) ||
		value.schemaVersion !== LEGACY_RETIREMENT_SCHEMA_VERSION || value.kind !== "pylon-consumer-v1-retirement" ||
		value.statePathSha256 !== digest(Buffer.from(statePath)) ||
		!/^[0-9a-f]{64}$/.test(value.authoritySha256 ?? "") || !/^[0-9a-f]{64}$/.test(value.tipSha256 ?? "")
	) throw new Error("Legacy consumer high-water retirement marker is malformed.");
	return value;
}

async function secureHandle(handle, stat, description, type, options) {
	if ((type === "file" && !stat.isFile()) || (type === "directory" && !stat.isDirectory())) {
		throw new Error(`${description} must be one real ${type}.`);
	}
	if (stat.uid !== options.currentUid) throw new Error(`${description} must be owned by the current uid.`);
	const requiredMode = type === "directory" ? 0o700 : 0o600;
	if ((stat.mode & 0o7777) !== requiredMode) {
		throw new Error(`${description} must already have exact ${requiredMode.toString(8)} permissions before use.`);
	}
	return stat;
}

async function secureDirectory(path, description, options) {
	let handle;
	try {
		handle = await options.openFile(
			path,
			constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0),
		);
	} catch (error) {
		if (["ELOOP", "ENOTDIR"].includes(error?.code)) throw new Error(`${description} must be one real directory.`);
		throw error;
	}
	try {
		await secureHandle(handle, await handle.stat(), description, "directory", options);
	} finally {
		await handle.close();
	}
}

export async function syncConsumerStateDirectory(path, { openDirectory = open } = {}) {
	let handle;
	try {
		handle = await openDirectory(path, "r");
		await handle.sync();
	} catch (error) {
		if (!["EINVAL", "EPERM", "EISDIR"].includes(error?.code)) throw error;
	} finally {
		if (handle !== undefined) await handle.close();
	}
}

export async function ensureDurableConsumerStateDirectory(
	directory,
	{ lstatEntry = lstat, makeDirectory = mkdir, syncDirectory = syncConsumerStateDirectory, create = true } = {},
) {
	const absolute = resolve(directory);
	const root = parse(absolute).root;
	let parent = root;
	const rootEntry = await lstatEntry(root);
	if (!rootEntry.isDirectory()) throw new Error("Consumer high-water state directory must be one canonical real directory.");
	const remainder = relative(root, absolute);
	for (const component of remainder ? remainder.split(sep) : []) {
		const current = join(parent, component);
		let entry;
		try {
			entry = await lstatEntry(current);
		} catch (error) {
			if (error?.code !== "ENOENT" || !create) throw error;
			try {
				await makeDirectory(current, { mode: 0o700 });
			} catch (mkdirError) {
				if (mkdirError?.code !== "EEXIST") throw mkdirError;
			}
			entry = await lstatEntry(current);
		}
		if (!entry.isDirectory() || entry.isSymbolicLink?.()) {
			throw new Error("Consumer high-water state directory must be one canonical real directory.");
		}
		await syncDirectory(parent);
		parent = current;
	}
	return absolute;
}

async function ensureDirectory(path, description, options) {
	try {
		await options.makeDirectory(path, { mode: 0o700 });
	} catch (error) {
		if (error?.code !== "EEXIST") throw error;
	}
	const entry = await options.lstatEntry(path);
	if (!entry.isDirectory() || entry.isSymbolicLink?.()) throw new Error(`${description} must be one real directory.`);
	await secureDirectory(path, description, options);
	await options.syncDirectory(path);
	await options.syncDirectory(dirname(path));
}

async function readSecureFile(path, maxBytes, description, options, minBytes = 1, hooks, expectedSha256 = null) {
	return readBoundedRegularFile(path, {
		maxBytes,
		minBytes,
		description,
		openFile: options.openFile,
		lstatEntry: options.lstatEntry,
		hooks: {
			...hooks,
			afterInitialPathStat: async (observation) => {
				await options.afterInitialPathStat?.(observation);
				await hooks?.afterInitialPathStat?.(observation);
			},
		},
		expectedSha256,
		validateHandle: (handle, stat) => secureHandle(handle, stat, description, "file", options),
	});
}

async function readExactMetadata(path, maxBytes, validate, description, options, budget, expectedSha256 = null) {
	const bytes = await readSecureFile(
		path,
		maxBytes,
		description,
		options,
		1,
		options.hooks?.metadataRead,
		expectedSha256,
	);
	if (bytes === null) return null;
	if (budget) {
		budget.bytes += bytes.length;
		if (budget.bytes > options.maxJournalBytes) throw new Error("Consumer high-water journal exceeds its safe byte bound.");
	}
	let value;
	try {
		value = validate(JSON.parse(bytes));
	} catch (error) {
		if (error instanceof SyntaxError) throw new Error(`${description} is malformed.`);
		throw error;
	}
	if (!bytes.equals(metadataBytes(value))) throw new Error(`${description} is not canonical.`);
	return value;
}

function temporaryName(targetPath, kind, writer, context) {
	if (!/^[a-z0-9-]{1,40}$/.test(kind)) throw new Error("Consumer high-water temporary kind is malformed.");
	const attempt = randomUUID().replaceAll("-", "").slice(0, 12);
	return `.pylon-consumer-tmp-v1-p${process.pid}-e${context.checkpoint.epochId}-g${generationName(writer.generation)}` +
		`-w${writer.token}-n${attempt}-k${kind}-t${digest(Buffer.from(resolve(targetPath)))}.tmp`;
}

async function inspectTemporary(path, options) {
	const match = temporaryPattern.exec(basename(path));
	if (!match) throw new Error("Consumer high-water journal contains an unexpected hidden entry.");
	let handle;
	try {
		handle = await options.openFile(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		if (["ELOOP", "EISDIR"].includes(error?.code)) {
			throw new Error("Consumer high-water owned temporary is not one regular non-symlink file.");
		}
		throw error;
	}
	try {
		const stat = await secureHandle(
			handle,
			await handle.stat(),
			"Consumer high-water owned temporary",
			"file",
			options,
		);
		if (stat.size > options.metadataMaxBytes) throw new Error("Consumer high-water owned temporary exceeds its safe byte bound.");
	} finally {
		await handle.close();
	}
	const kind = match[6];
	const allowedKinds = new Set([
		"checkpoint", "projection", "transition", "claim", "claim-index", "initial-heartbeat", "heartbeat",
		"terminal-released", "terminal-retired", "terminal-commit", "applied", "legacy-guard",
		"legacy-retirement",
	]);
	if (!allowedKinds.has(kind)) throw new Error("Consumer high-water owned temporary target metadata is malformed.");
	return {
		path,
		pid: Number(match[1]),
		epochId: match[2],
		generation: Number(match[3]),
		token: match[4],
		attempt: match[5],
		kind,
		targetSha256: match[7],
	};
}

function isImmediateSuccessorCheckpoint(context, checkpoint) {
	return checkpoint.epoch === context.checkpoint.epoch + 1 &&
		checkpoint.epochId === deterministicUuid(
			`pylon-consumer-rotation-v2:${context.checkpointDigest}:${checkpoint.anchorDigest}`,
		) &&
		checkpoint.previousCheckpointSha256 === context.checkpointDigest &&
		checkpoint.previousTipSha256 === checkpoint.anchorDigest &&
		checkpoint.retiredEpochDirectory === basename(context.epochDirectory) &&
		checkpoint.sourceAuthoritySha256 === context.checkpoint.sourceAuthoritySha256 &&
		checkpoint.sourceAuthorityTipDigest === context.checkpoint.sourceAuthorityTipDigest &&
		checkpoint.sourceAuthorityTipBase64 === context.checkpoint.sourceAuthorityTipBase64 &&
		checkpoint.historySha256 === digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${checkpoint.anchorDigest}`,
		));
}

function retainedCheckpointPath(context) {
	if (context.checkpoint.retiredEpochDirectory === null) return null;
	const match = epochPattern.exec(context.checkpoint.retiredEpochDirectory);
	if (!match) throw new Error("Consumer high-water journal checkpoint context is malformed.");
	return join(context.journalDirectory, `checkpoint-${match[1]}-${match[2]}.json`);
}

function contextCheckpointAnchors(context) {
	const anchors = new Map([[context.checkpointPath, context.checkpointDigest]]);
	const retainedPath = retainedCheckpointPath(context);
	if (retainedPath !== null) anchors.set(retainedPath, context.checkpoint.previousCheckpointSha256);
	return anchors;
}

function isContextAnchoredCheckpoint(context, path, checkpoint, expectedSha256) {
	if (path === context.checkpointPath) {
		return expectedSha256 === context.checkpointDigest &&
			metadataBytes(checkpoint).equals(metadataBytes(context.checkpoint));
	}
	const retainedPath = retainedCheckpointPath(context);
	return retainedPath !== null && path === retainedPath &&
		expectedSha256 === context.checkpoint.previousCheckpointSha256 &&
		checkpoint.epoch + 1 === context.checkpoint.epoch &&
		epochName(checkpoint) === context.checkpoint.retiredEpochDirectory &&
		checkpoint.sourceAuthoritySha256 === context.checkpoint.sourceAuthoritySha256 &&
		checkpoint.sourceAuthorityTipDigest === context.checkpoint.sourceAuthorityTipDigest &&
		checkpoint.sourceAuthorityTipBase64 === context.checkpoint.sourceAuthorityTipBase64 &&
		context.checkpoint.historySha256 === digest(Buffer.from(
			`${checkpoint.historySha256}:${expectedSha256}:${context.checkpoint.anchorDigest}`,
		));
}

function isAuthenticatedCheckpointAnchor(context, path, checkpoint, expectedSha256, additionalAnchor = null) {
	return isContextAnchoredCheckpoint(context, path, checkpoint, expectedSha256) || (
		additionalAnchor !== null && path === additionalAnchor.path && expectedSha256 === additionalAnchor.digest &&
		metadataBytes(checkpoint).equals(metadataBytes(additionalAnchor.checkpoint)) &&
		isImmediateSuccessorCheckpoint(context, checkpoint)
	);
}

function canonicalCheckpointNameEpoch(name) {
	const match = checkpointPattern.exec(name);
	if (!match) return null;
	const epoch = Number(match[1]);
	return Number.isSafeInteger(epoch) && generationName(epoch) === match[1] ? epoch : null;
}

function isProvisionallyRemovedContextCurrentCheckpoint(path, context, anchors, rootNames) {
	const name = basename(path);
	if (
		path !== context.checkpointPath || path !== join(context.journalDirectory, name) ||
		name !== checkpointName(context.checkpoint) || anchors.get(path) !== context.checkpointDigest ||
		canonicalCheckpointNameEpoch(name) !== context.checkpoint.epoch
	) return false;
	return rootNames.some((candidate) => {
		const candidateEpoch = canonicalCheckpointNameEpoch(candidate);
		return candidateEpoch !== null && candidateEpoch > context.checkpoint.epoch;
	});
}

function isVanishedRetainedCheckpoint(path, context, anchors, rootNames) {
	const retainedPath = retainedCheckpointPath(context);
	if (
		retainedPath === null || path !== retainedPath ||
		anchors.get(path) !== context.checkpoint.previousCheckpointSha256
	) return false;
	const retainedMatch = checkpointPattern.exec(basename(path));
	if (!retainedMatch || Number(retainedMatch[1]) + 1 !== context.checkpoint.epoch) return false;
	return rootNames.some((candidate) => {
		const candidateEpoch = canonicalCheckpointNameEpoch(candidate);
		return candidateEpoch !== null && candidateEpoch > Number(retainedMatch[1]);
	});
}

const checkpointStatEvidenceKeys = ["dev", "ino", "size", "mtimeMs", "ctimeMs", "nlink"];

function isFrozenRecord(value) {
	return value !== null && typeof value === "object" && Object.isFrozen(value);
}

function isExactCheckpointStatEvidence(value) {
	return isFrozenRecord(value) && exactKeys(value, checkpointStatEvidenceKeys) &&
		checkpointStatEvidenceKeys.every((key) => Number.isFinite(value[key])) &&
		Number.isSafeInteger(value.size) && value.size >= 0 && Number.isSafeInteger(value.nlink) && value.nlink >= 0;
}

function exactEvidenceMonotoneCut(observations, fromLinks, toLinks, byteLength) {
	if (
		observations.length < 2 || observations.some((stat) => !isExactCheckpointStatEvidence(stat)) ||
		observations[0].size !== byteLength || observations[0].nlink !== fromLinks ||
		observations.at(-1).nlink !== toLinks ||
		observations.some((stat) => (
			stat.dev !== observations[0].dev || stat.ino !== observations[0].ino ||
			stat.size !== observations[0].size || stat.mtimeMs !== observations[0].mtimeMs
		))
	) return null;
	let cut = null;
	for (let index = 1; index < observations.length; index += 1) {
		const previous = observations[index - 1];
		const current = observations[index];
		if (previous.nlink === current.nlink) {
			if (previous.ctimeMs !== current.ctimeMs) return null;
			continue;
		}
		if (
			cut !== null || previous.nlink !== fromLinks || current.nlink !== toLinks ||
			previous.ctimeMs === current.ctimeMs
		) return null;
		cut = index;
	}
	return cut;
}

function isExactLinkRetiredBeforeReadEvidence(error) {
	const transition = error.statTransition;
	return isFrozenRecord(transition) && exactKeys(transition, ["pathEntry", "openedHandle"]) &&
		exactEvidenceMonotoneCut(
			[transition.pathEntry, transition.openedHandle],
			2,
			1,
			error.bytes.length,
		) === 1;
}

function isExactLinkRetiredDuringReadEvidence(error) {
	const transition = error.statTransition;
	return isFrozenRecord(transition) && exactKeys(transition, ["pathEntry", "before", "after", "finalPathEntry"]) &&
		[2, 3].includes(exactEvidenceMonotoneCut(
			[transition.pathEntry, transition.before, transition.after, transition.finalPathEntry],
			2,
			1,
			error.bytes.length,
		));
}

function isExactUnlinkedDuringReadEvidence(error) {
	const transition = error.statTransition;
	if (
		!isFrozenRecord(transition) ||
		!exactKeys(transition, ["pathEntry", "before", "after", "confirmedHandle"]) ||
		!(transition.confirmedHandle === null || isExactCheckpointStatEvidence(transition.confirmedHandle))
	) return false;
	const observations = [transition.pathEntry, transition.before, transition.after];
	if (transition.confirmedHandle !== null) observations.push(transition.confirmedHandle);
	return exactEvidenceMonotoneCut(observations, 1, 0, error.bytes.length) !== null;
}

function authenticatedChangedCheckpointRead(error, context, options, anchors, rootNames, additionalAnchor = null) {
	const linkRetiredBeforeRead = error instanceof BoundedFileLinkRetiredBeforeReadError &&
		error.constructor === BoundedFileLinkRetiredBeforeReadError &&
		error.name === "BoundedFileLinkRetiredBeforeReadError";
	const linkRetiredDuringRead = error instanceof BoundedFileLinkRetiredDuringReadError &&
		error.constructor === BoundedFileLinkRetiredDuringReadError &&
		error.name === "BoundedFileLinkRetiredDuringReadError";
	const unlinkedDuringRead = error instanceof BoundedFileUnlinkedDuringReadError &&
		error.constructor === BoundedFileUnlinkedDuringReadError &&
		error.name === "BoundedFileUnlinkedDuringReadError";
	const linkRetiredDuringOrBeforeRead = linkRetiredBeforeRead || linkRetiredDuringRead;
	if (
		(!linkRetiredDuringOrBeforeRead && !unlinkedDuringRead) ||
		error.description !== "Consumer high-water journal checkpoint" || typeof error.path !== "string" ||
		!Buffer.isBuffer(error.bytes) || error.bytes.length < 1 || error.bytes.length > options.metadataMaxBytes ||
		(linkRetiredBeforeRead && !isExactLinkRetiredBeforeReadEvidence(error)) ||
		(linkRetiredDuringRead && !isExactLinkRetiredDuringReadEvidence(error)) ||
		(unlinkedDuringRead && !isExactUnlinkedDuringReadEvidence(error))
	) return null;
	const expectedSha256 = anchors.get(error.path);
	if (
		expectedSha256 === undefined || error.expectedSha256 !== expectedSha256 ||
		digest(error.bytes) !== expectedSha256 || error.sha256 !== expectedSha256 ||
		dirname(error.path) !== context.journalDirectory
	) return null;
	const name = basename(error.path);
	const match = checkpointPattern.exec(name);
	if (!match || error.path !== join(context.journalDirectory, name)) return null;
	let checkpoint;
	try {
		checkpoint = validateCheckpoint(JSON.parse(error.bytes.toString("utf8")), options.stateMaxBytes).value;
	} catch {
		return null;
	}
	if (
		!metadataBytes(checkpoint).equals(error.bytes) || checkpointName(checkpoint) !== name ||
		checkpoint.epoch !== Number(match[1]) ||
		!isAuthenticatedCheckpointAnchor(context, error.path, checkpoint, expectedSha256, additionalAnchor)
	) return null;
	if (unlinkedDuringRead) {
		const hasLaterCheckpoint = rootNames.some((candidate) => {
			const candidateEpoch = canonicalCheckpointNameEpoch(candidate);
			return candidateEpoch !== null && candidateEpoch > checkpoint.epoch;
		});
		if (!hasLaterCheckpoint) return null;
	}
	const linkRetirementStat = linkRetiredBeforeRead
		? error.statTransition.openedHandle
		: linkRetiredDuringRead ? error.statTransition.finalPathEntry : null;
	return {
		checkpoint,
		linkRetiredBeforeRead: linkRetiredDuringOrBeforeRead,
		linkRetirementStat,
		unlinkedDuringRead,
	};
}

function sameRetiredLinkStat(left, right) {
	return left !== null && right !== null &&
		left.dev === right.dev && left.ino === right.ino && left.size === right.size &&
		left.mtimeMs === right.mtimeMs && left.ctimeMs === right.ctimeMs && left.nlink === right.nlink;
}

function checkpointProofOptions(entry, options, invalidRoot) {
	if (entry.checkpointStat === null) return options;
	let initialPathStat = true;
	return {
		...options,
		afterInitialPathStat: async ({ path, stat }) => {
			await options.afterInitialPathStat?.({ path, stat });
			if (path === entry.path && initialPathStat) {
				initialPathStat = false;
				if (!sameRetiredLinkStat(stat, entry.checkpointStat)) throw invalidRoot();
			}
		},
	};
}

async function authenticateStableChangedRoot(scan, context, options, anchors, target, invalidRoot) {
	const initialNames = new Set(scan.rootNames);
	if (initialNames.size !== scan.rootNames.length) throw invalidRoot();
	const targetCheckpointName = basename(target.path);
	const targetEpochName = epochName(target.checkpoint);
	const proofAnchors = new Map(anchors);
	proofAnchors.set(target.path, target.digest);
	const optionalCheckpointNames = new Set([
		...scan.checkpointEntries
			.filter((entry) => entry.path !== target.path && entry.checkpoint.epoch < target.checkpoint.epoch)
			.map((entry) => entry.name),
		...scan.vanishedRetainedCheckpointNames,
	]);
	const optionalEpochNames = new Set(
		scan.epochEntries
			.filter((entry) => entry.epoch < target.checkpoint.epoch)
			.map((entry) => entry.name),
	);
	const optionalTemporaryNames = new Set(
		scan.temporaries
			.filter((temporary) => dirname(temporary.path) === context.journalDirectory)
			.map((temporary) => basename(temporary.path)),
	);
	const optionalNames = new Set([
		...optionalCheckpointNames,
		...optionalEpochNames,
		...optionalTemporaryNames,
	]);
	const requiredNames = new Set([TEMPORARY_DIRECTORY_NAME, targetCheckpointName, targetEpochName]);
	const removedBeforeProof = new Set(scan.removedCheckpointEntries.map((entry) => entry.name));
	const proofNamesArray = await options.readDirectory(context.journalDirectory);
	const proofNames = new Set(proofNamesArray);
	if (
		proofNamesArray.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES ||
		proofNames.size !== proofNamesArray.length ||
		[...proofNames].some((name) => !initialNames.has(name)) ||
		[...removedBeforeProof].some((name) => proofNames.has(name)) ||
		scan.vanishedRetainedCheckpointNames.some((name) => proofNames.has(name)) ||
		[...initialNames].some((name) => !optionalNames.has(name) && !proofNames.has(name)) ||
		[...requiredNames].some((name) => !proofNames.has(name))
	) throw invalidRoot();
	const removedDuringProof = new Set();
	let targetAuthenticated = false;
	for (const entry of scan.checkpointEntries) {
		if (!proofNames.has(entry.name)) continue;
		const optional = optionalCheckpointNames.has(entry.name);
		await options.hooks?.beforeStableCheckpointProofRead?.({
			name: entry.name,
			path: entry.path,
			target: entry.path === target.path,
		});
		const expectedSha256 = proofAnchors.get(entry.path) ?? null;
		let checkpoint;
		let changedRead = null;
		try {
			checkpoint = await readExactMetadata(
				entry.path,
				options.metadataMaxBytes,
				(value) => validateCheckpoint(value, options.stateMaxBytes).value,
				"Consumer high-water journal checkpoint",
				checkpointProofOptions(entry, options, invalidRoot),
				undefined,
				expectedSha256,
			);
		} catch (error) {
			changedRead = authenticatedChangedCheckpointRead(
				error,
				context,
				options,
				proofAnchors,
				proofNamesArray,
				target,
			);
			if (changedRead === null) throw error;
			checkpoint = changedRead.checkpoint;
		}
		if (checkpoint === null) {
			if (!optional) throw invalidRoot();
			removedDuringProof.add(entry.name);
			continue;
		}
		if (
			!metadataBytes(checkpoint).equals(metadataBytes(entry.checkpoint)) ||
			(expectedSha256 !== null && !isAuthenticatedCheckpointAnchor(
				context,
				entry.path,
				checkpoint,
				expectedSha256,
				target,
			))
		) throw invalidRoot();
		if (changedRead?.unlinkedDuringRead) {
			if (!optional) throw invalidRoot();
			removedDuringProof.add(entry.name);
			continue;
		}
		if (entry.path === target.path) targetAuthenticated = true;
	}
	if (!targetAuthenticated) throw invalidRoot();
	await secureDirectory(
		join(context.journalDirectory, TEMPORARY_DIRECTORY_NAME),
		"Consumer high-water temporary directory",
		options,
	);
	const targetEpoch = scan.epochEntries.find((entry) => entry.name === targetEpochName);
	if (targetEpoch === undefined) throw invalidRoot();
	await secureDirectory(targetEpoch.path, "Consumer high-water epoch directory", options);
	for (const temporary of scan.temporaries) {
		if (dirname(temporary.path) === context.journalDirectory && proofNames.has(basename(temporary.path))) {
			if ((await inspectTemporary(temporary.path, options)) === null) throw invalidRoot();
		}
	}
	const finalNamesArray = await options.readDirectory(context.journalDirectory);
	const finalNames = new Set(finalNamesArray);
	if (
		finalNamesArray.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES ||
		finalNames.size !== finalNamesArray.length ||
		[...finalNames].some((name) => !proofNames.has(name)) ||
		[...proofNames].some((name) => !optionalNames.has(name) && !finalNames.has(name)) ||
		[...removedBeforeProof].some((name) => finalNames.has(name)) ||
		[...removedDuringProof].some((name) => finalNames.has(name)) ||
		[...requiredNames].some((name) => !finalNames.has(name))
	) throw invalidRoot();
}

async function inProgressDirectoryStats(context, nextEpochPath, options, invalidRoot) {
	const [temporaryDirectory, currentEpoch, nextEpoch] = await Promise.all([
		options.lstatEntry(context.temporaryDirectory),
		options.lstatEntry(context.epochDirectory),
		options.lstatEntry(nextEpochPath),
	]);
	if (
		!temporaryDirectory.isDirectory() || temporaryDirectory.isSymbolicLink?.() ||
		!currentEpoch.isDirectory() || currentEpoch.isSymbolicLink?.() ||
		!nextEpoch.isDirectory() || nextEpoch.isSymbolicLink?.()
	) throw invalidRoot();
	return { temporaryDirectory, currentEpoch, nextEpoch };
}

async function authenticateLinkRetiredInProgressRoot(
	scan,
	context,
	options,
	anchors,
	currentCheckpoint,
	nextEpochPath,
	kind,
	invalidRoot,
) {
	if (scan.linkRetiredCheckpointEntries.length === 0) return;
	if (
		currentCheckpoint === undefined || scan.linkRetiredCheckpointEntries.length !== 1 ||
		scan.linkRetiredCheckpointEntries[0] !== currentCheckpoint || currentCheckpoint.path !== context.checkpointPath ||
		currentCheckpoint.linkRetirementStat === null
	) throw invalidRoot();
	const permittedNames = new Set([
		TEMPORARY_DIRECTORY_NAME,
		basename(context.checkpointPath),
		basename(context.epochDirectory),
		basename(nextEpochPath),
	]);
	if (
		scan.rootNames.length !== permittedNames.size ||
		scan.rootNames.some((name) => !permittedNames.has(name))
	) throw invalidRoot();
	const beforeDirectories = await inProgressDirectoryStats(context, nextEpochPath, options, invalidRoot);
	await options.hooks?.beforeInProgressStableRootProof?.({ kind });
	await authenticateStableChangedRoot(scan, context, options, anchors, currentCheckpoint, invalidRoot);
	const afterDirectories = await inProgressDirectoryStats(context, nextEpochPath, options, invalidRoot);
	if (
		!sameRetiredLinkStat(beforeDirectories.temporaryDirectory, afterDirectories.temporaryDirectory) ||
		!sameRetiredLinkStat(beforeDirectories.currentEpoch, afterDirectories.currentEpoch) ||
		!sameRetiredLinkStat(beforeDirectories.nextEpoch, afterDirectories.nextEpoch) ||
		(await options.readDirectory(nextEpochPath)).length !== 0
	) throw invalidRoot();
}

async function authenticateChangedRoot(
	context,
	options,
	inProgressCheckpoint = null,
	allowInProgressDiscovery = false,
) {
	const anchors = contextCheckpointAnchors(context);
	const scan = await scanJournalRoot(context.statePath, context.journalDirectory, options, {
		checkpointAnchors: anchors,
		checkpointContext: context,
	});
	const invalidRoot = () => new Error(
		"Consumer high-water journal root changed without one exact current or immediate-successor authority.",
	);
	const currentCheckpoint = scan.checkpointEntries.find((entry) => entry.path === context.checkpointPath);
	if (currentCheckpoint && currentCheckpoint.digest !== context.checkpointDigest) throw invalidRoot();

	if (inProgressCheckpoint !== null && scan.checkpointEntries.length === 1) {
		const checkpoint = validateCheckpoint(inProgressCheckpoint, options.stateMaxBytes).value;
		const nextEpochPath = join(context.journalDirectory, epochName(checkpoint));
		await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
		await secureDirectory(nextEpochPath, "Consumer high-water epoch directory", options);
		if (
			!isImmediateSuccessorCheckpoint(context, checkpoint) ||
			scan.checkpointEntries.length !== 1 || scan.head?.path !== context.checkpointPath || scan.missingHeadEpoch ||
			scan.epochEntries.length !== 2 ||
			scan.epochEntries.some((entry) => ![context.epochDirectory, nextEpochPath].includes(entry.path)) ||
			!scan.epochEntries.some((entry) => entry.path === nextEpochPath) ||
			(await options.readDirectory(nextEpochPath)).length !== 0
		) throw invalidRoot();
		if ((await options.readDirectory(nextEpochPath)).length !== 0) throw invalidRoot();
		await authenticateLinkRetiredInProgressRoot(
			scan,
			context,
			options,
			anchors,
			currentCheckpoint,
			nextEpochPath,
			"known",
			invalidRoot,
		);
		return false;
	}

	const discoveredNextEpoch = scan.epochEntries.find((entry) => entry.path !== context.epochDirectory);
	if (
		allowInProgressDiscovery && inProgressCheckpoint === null &&
		scan.checkpointEntries.length === 1 && scan.head?.path === context.checkpointPath && !scan.missingHeadEpoch &&
		scan.epochEntries.length === 2 && discoveredNextEpoch?.epoch === context.checkpoint.epoch + 1 &&
		(await options.readDirectory(discoveredNextEpoch.path)).length === 0
	) {
		await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
		await secureDirectory(discoveredNextEpoch.path, "Consumer high-water epoch directory", options);
		if ((await options.readDirectory(discoveredNextEpoch.path)).length !== 0) throw invalidRoot();
		await authenticateLinkRetiredInProgressRoot(
			scan,
			context,
			options,
			anchors,
			currentCheckpoint,
			discoveredNextEpoch.path,
			"discovered",
			invalidRoot,
		);
		return discoveredNextEpoch.path;
	}

	const retainedCheckpoint = scan.checkpointEntries.find((entry) => entry.path !== context.checkpointPath);
	const retiredEpochPath = context.checkpoint.retiredEpochDirectory === null
		? null
		: join(context.journalDirectory, context.checkpoint.retiredEpochDirectory);
	if (
		currentCheckpoint && scan.head?.path === context.checkpointPath && !scan.missingHeadEpoch &&
		scan.checkpointEntries.length <= 2 && scan.epochEntries.length <= 2 &&
		(!retainedCheckpoint || (
			retainedCheckpoint.digest === context.checkpoint.previousCheckpointSha256 &&
			epochName(retainedCheckpoint.checkpoint) === context.checkpoint.retiredEpochDirectory
		)) &&
		scan.epochEntries.every((entry) => [context.epochDirectory, retiredEpochPath].includes(entry.path))
	) {
		if (
			scan.removedCheckpointEntries.length > 0 || scan.linkRetiredCheckpointEntries.length > 0 ||
			scan.vanishedRetainedCheckpointNames.length > 0
		) {
			await authenticateStableChangedRoot(scan, context, options, anchors, currentCheckpoint, invalidRoot);
		} else {
			await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
		}
		return false;
	}

	const successor = scan.head;
	const expectedCheckpointPath = successor
		? join(context.journalDirectory, checkpointName(successor.checkpoint))
		: null;
	const expectedEpochPath = successor
		? join(context.journalDirectory, epochName(successor.checkpoint))
		: null;
	if (
		!successor || scan.missingHeadEpoch || successor.path !== expectedCheckpointPath ||
		!isImmediateSuccessorCheckpoint(context, successor.checkpoint) ||
		scan.checkpointEntries.length < 1 || scan.checkpointEntries.length > 2 ||
		scan.epochEntries.length < 1 || scan.epochEntries.length > 2 ||
		scan.checkpointEntries.some((entry) => ![context.checkpointPath, expectedCheckpointPath].includes(entry.path)) ||
		scan.epochEntries.some((entry) => ![context.epochDirectory, expectedEpochPath].includes(entry.path)) ||
		!scan.epochEntries.some((entry) => entry.path === expectedEpochPath)
	) throw invalidRoot();
	await authenticateStableChangedRoot(scan, context, options, anchors, successor, invalidRoot);
	return true;
}

async function revalidateAuthority(
	context,
	operation,
	options,
	inProgressCheckpoint = options.inProgressCheckpoint ?? null,
	allowInProgressDiscovery = false,
) {
	await options.hooks?.beforePathOperation?.({
		operation,
		statePath: context.statePath,
		lockDirectory: context.journalDirectory,
		transactionDirectory: context.epochDirectory,
		inProgressCheckpoint: inProgressCheckpoint === null ? null : structuredClone(inProgressCheckpoint),
	});
	await ensureDurableConsumerStateDirectory(dirname(context.statePath), {
		...options.directoryOperations,
		create: false,
	});
	await secureDirectory(dirname(context.statePath), "Consumer high-water state directory", options);
	await secureDirectory(context.journalDirectory, "Consumer high-water journal directory", options);
	await secureDirectory(context.temporaryDirectory, "Consumer high-water temporary directory", options);
	let oldEpochError = null;
	try {
		await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
	} catch (error) {
		if (error?.code !== "ENOENT") throw error;
		oldEpochError = error;
	}
	const entries = await options.readDirectory(context.journalDirectory);
	if (entries.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe allocation bound.");
	}
	const expectedRootNames = new Set([
		TEMPORARY_DIRECTORY_NAME,
		basename(context.checkpointPath),
		basename(context.epochDirectory),
	]);
	let changedRoot = false;
	if (entries.some((name) => !expectedRootNames.has(name))) {
		changedRoot = await authenticateChangedRoot(
			context,
			options,
			inProgressCheckpoint,
			allowInProgressDiscovery,
		);
		if (changedRoot === true) throw new ConsumerEpochAdvancedError();
	}
	if (oldEpochError) throw oldEpochError;
	const current = await readExactMetadata(
		context.checkpointPath,
		options.metadataMaxBytes,
		(value) => validateCheckpoint(value, options.stateMaxBytes).value,
		"Consumer high-water journal checkpoint",
		options,
	);
	if (digest(metadataBytes(current)) !== context.checkpointDigest) {
		throw new Error("Consumer high-water journal checkpoint changed and fenced a paused writer.");
	}
	return typeof changedRoot === "string" ? changedRoot : null;
}

async function publishImmutable({
	path,
	bytes,
	directory,
	kind,
	context,
	writer,
	options,
	revalidate = true,
	beforeLink,
	inProgressCheckpoint = null,
}) {
	if (revalidate) await revalidateAuthority(context, kind, options, inProgressCheckpoint);
	const temporary = join(context.temporaryDirectory, temporaryName(path, kind, writer, context));
	let handle;
	let linked = false;
	try {
		handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
		await handle.chmod?.(0o600);
		await handle.writeFile(bytes);
		await handle.sync();
		await handle.close();
		handle = undefined;
		await options.hooks?.afterFileSync?.({ kind, path, temporary });
		await beforeLink?.();
		if (revalidate) await revalidateAuthority(context, `${kind}-link`, options, inProgressCheckpoint);
		try {
			await options.linkFile(temporary, path);
			linked = true;
		} catch (error) {
			if (error?.code !== "EEXIST") throw error;
		}
		if (linked) await options.hooks?.afterMetadataLink?.({ kind, path });
		await options.syncDirectory(directory);
		await options.hooks?.afterMetadataDirectorySync?.({ kind, path, linked });
		return linked;
	} finally {
		if (handle !== undefined) await handle.close();
		await options.removeFile(temporary, { force: true });
		await options.syncDirectory(context.temporaryDirectory);
	}
}

async function publishMetadata(path, value, kind, context, writer, options, inProgressCheckpoint = null) {
	const created = await publishImmutable({
		path,
		bytes: metadataBytes(value),
		directory: dirname(path),
		kind,
		context,
		writer,
		options,
		inProgressCheckpoint,
	});
	if (created) return { value, created: true };
	await revalidateAuthority(context, `${kind}-existing`, options, inProgressCheckpoint);
	const existing = await readExactMetadata(
		path,
		options.metadataMaxBytes,
		(candidate) => candidate,
		"Consumer high-water lock metadata",
		options,
	);
	return { value: existing, created: false };
}

function genesisCheckpoint(statePath) {
	const epochId = deterministicUuid(`pylon-consumer-journal:${statePath}`);
	return {
		schemaVersion: CHECKPOINT_SCHEMA_VERSION,
		epoch: 1,
		epochId,
		previousCheckpointSha256: GENESIS_DIGEST,
		previousTipSha256: GENESIS_DIGEST,
		historySha256: digest(Buffer.from(`pylon-consumer-history:${digest(Buffer.from(statePath))}`)),
		anchorDigest: GENESIS_DIGEST,
		anchorBase64: null,
		retiredEpochDirectory: null,
		sourceAuthoritySha256: GENESIS_DIGEST,
		sourceAuthorityTipDigest: GENESIS_DIGEST,
		sourceAuthorityTipBase64: null,
	};
}

async function scanJournalRoot(
	statePath,
	journalDirectory,
	options,
	{ checkpointAnchors = null, checkpointContext = null } = {},
) {
	await secureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	await options.syncDirectory(journalDirectory);
	const names = await options.readDirectory(journalDirectory);
	if (names.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe allocation bound.");
	}
	const checkpointEntries = [];
	const removedCheckpointEntries = [];
	const linkRetiredCheckpointEntries = [];
	const vanishedRetainedCheckpointNames = [];
	const epochEntries = [];
	const temporaries = [];
	let temporaryDirectorySeen = false;
	for (const name of names) {
		const path = join(journalDirectory, name);
		if (name === TEMPORARY_DIRECTORY_NAME) {
			if (temporaryDirectorySeen) throw new Error("Consumer high-water temporary namespace is duplicated.");
			temporaryDirectorySeen = true;
			const entry = await options.lstatEntry(path);
			if (!entry.isDirectory() || entry.isSymbolicLink?.()) {
				throw new Error("Consumer high-water temporary namespace must be one real directory.");
			}
			await secureDirectory(path, "Consumer high-water temporary directory", options);
			const temporaryNames = await options.readDirectory(path);
			if (temporaryNames.length > MAX_TEMPORARY_ENTRIES) {
				throw new Error("Consumer high-water temporary namespace exceeds its safe allocation bound.");
			}
			for (const temporaryName of temporaryNames) {
				const temporary = await inspectTemporary(join(path, temporaryName), options);
				if (temporary) temporaries.push(temporary);
			}
			continue;
		}
		const checkpointMatch = checkpointPattern.exec(name);
		if (checkpointMatch) {
			const expectedSha256 = checkpointAnchors?.get(path) ?? null;
			let checkpoint;
			let removedDuringRead = false;
			let linkRetiredBeforeRead = false;
			let linkRetirementStat = null;
			try {
				checkpoint = await readExactMetadata(
					path,
					options.metadataMaxBytes,
					(value) => validateCheckpoint(value, options.stateMaxBytes).value,
					"Consumer high-water journal checkpoint",
					options,
					undefined,
					expectedSha256,
				);
			} catch (error) {
				const authenticated = checkpointContext === null || checkpointAnchors === null
					? null
					: authenticatedChangedCheckpointRead(error, checkpointContext, options, checkpointAnchors, names);
				if (authenticated === null) throw error;
				checkpoint = authenticated.checkpoint;
				linkRetiredBeforeRead = authenticated.linkRetiredBeforeRead;
				linkRetirementStat = authenticated.linkRetirementStat;
				removedDuringRead = !linkRetiredBeforeRead;
			}
			if (checkpoint === null) {
				if (checkpointContext === null || checkpointAnchors === null) {
					throw new Error("Consumer high-water journal lost its current checkpoint during an authenticated scan.");
				}
				if (isProvisionallyRemovedContextCurrentCheckpoint(path, checkpointContext, checkpointAnchors, names)) {
					checkpoint = checkpointContext.checkpoint;
					removedDuringRead = true;
				} else {
					if (!isVanishedRetainedCheckpoint(path, checkpointContext, checkpointAnchors, names)) {
						throw new Error("Consumer high-water journal lost its current checkpoint during an authenticated scan.");
					}
					vanishedRetainedCheckpointNames.push(name);
					continue;
				}
			}
			let checkpointStat = linkRetirementStat;
			if (!removedDuringRead && checkpointStat === null) {
				await options.hooks?.beforeCheckpointIdentityStat?.({ name, path });
				try {
					checkpointStat = await options.lstatEntry(path);
				} catch (error) {
					const allowedRemoval = checkpointContext !== null && checkpointAnchors !== null && (
						isProvisionallyRemovedContextCurrentCheckpoint(path, checkpointContext, checkpointAnchors, names) ||
						isVanishedRetainedCheckpoint(path, checkpointContext, checkpointAnchors, names)
					);
					if (error?.code !== "ENOENT" || !allowedRemoval) throw error;
					removedDuringRead = true;
				}
				if (
					checkpointStat !== null &&
					(checkpointStat.isSymbolicLink?.() || !checkpointStat.isFile())
				) throw new Error("Consumer high-water journal checkpoint must remain one regular non-symlink file.");
			}
			if (checkpointName(checkpoint) !== name || checkpoint.epoch !== Number(checkpointMatch[1])) {
				throw new Error("Consumer high-water journal checkpoint name is malformed.");
			}
			const entry = {
				name,
				path,
				checkpoint,
				digest: digest(metadataBytes(checkpoint)),
				removedDuringRead,
				linkRetiredBeforeRead,
				linkRetirementStat,
				checkpointStat,
			};
			checkpointEntries.push(entry);
			if (removedDuringRead) removedCheckpointEntries.push(entry);
			if (linkRetiredBeforeRead) linkRetiredCheckpointEntries.push(entry);
			continue;
		}
		const epochMatch = epochPattern.exec(name);
		if (epochMatch) {
			epochEntries.push({ name, path, epoch: Number(epochMatch[1]), epochId: epochMatch[2] });
			continue;
		}
		if (name.startsWith(".")) {
			const temporary = await inspectTemporary(path, options);
			if (temporary?.kind !== "checkpoint") {
				throw new Error("Consumer high-water journal root contains an unexpected owned temporary.");
			}
			if (temporary) temporaries.push(temporary);
			continue;
		}
		throw new Error("Consumer high-water journal root contains an unexpected entry.");
	}
	if (!temporaryDirectorySeen) throw new Error("Consumer high-water journal lacks its exact temporary namespace.");
	const checkpointNameCount = names.filter((name) => checkpointPattern.test(name)).length;
	const authoritativeEntries = checkpointNameCount + epochEntries.length + 1;
	if (authoritativeEntries > MAX_JOURNAL_ROOT_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe entry bound.");
	}
	checkpointEntries.sort((left, right) => left.checkpoint.epoch - right.checkpoint.epoch);
	epochEntries.sort((left, right) => left.epoch - right.epoch);
	if (checkpointNameCount > 2 || epochEntries.length > 2) {
		throw new Error("Consumer high-water journal root contains unbounded checkpoint metadata.");
	}
	for (let index = 1; index < checkpointEntries.length; index += 1) {
		if (checkpointEntries[index - 1].checkpoint.epoch === checkpointEntries[index].checkpoint.epoch) {
			throw new Error("Consumer high-water journal contains competing checkpoints for one parent epoch.");
		}
		if (checkpointEntries[index - 1].checkpoint.epoch + 1 !== checkpointEntries[index].checkpoint.epoch) {
			throw new Error("Consumer high-water journal checkpoints are not contiguous.");
		}
	}
	for (let index = 1; index < epochEntries.length; index += 1) {
		if (epochEntries[index - 1].epoch === epochEntries[index].epoch) {
			throw new Error("Consumer high-water journal contains competing epoch directories for one parent epoch.");
		}
	}
	const head = checkpointEntries.at(-1) ?? null;
	if (head) {
		const previous = checkpointEntries.at(-2);
		if (previous && (
			head.checkpoint.previousCheckpointSha256 !== previous.digest ||
			head.checkpoint.retiredEpochDirectory !== epochName(previous.checkpoint) ||
			head.checkpoint.sourceAuthoritySha256 !== previous.checkpoint.sourceAuthoritySha256 ||
			head.checkpoint.sourceAuthorityTipDigest !== previous.checkpoint.sourceAuthorityTipDigest ||
			head.checkpoint.sourceAuthorityTipBase64 !== previous.checkpoint.sourceAuthorityTipBase64 ||
			head.checkpoint.historySha256 !== digest(Buffer.from(
				`${previous.checkpoint.historySha256}:${previous.digest}:${head.checkpoint.anchorDigest}`,
			))
		)) throw new Error("Consumer high-water journal checkpoint does not anchor its exact predecessor.");
	}
	const missingHeadEpoch = head ? !epochEntries.some((entry) => entry.name === epochName(head.checkpoint)) : false;
	if (checkpointContext === null) {
		for (const entry of epochEntries) {
			const pathEntry = await options.lstatEntry(entry.path);
			if (!pathEntry.isDirectory() || pathEntry.isSymbolicLink?.()) {
				throw new Error("Consumer high-water epoch entry must be one real directory.");
			}
			await secureDirectory(entry.path, "Consumer high-water epoch directory", options);
		}
	}
	return {
		checkpointEntries,
		removedCheckpointEntries,
		linkRetiredCheckpointEntries,
		vanishedRetainedCheckpointNames,
		epochEntries,
		temporaries,
		head,
		missingHeadEpoch,
		rootNames: names,
	};
}

function classifyContextCheckpointAuthority(scan, context, anchors, invalidRoot) {
	const current = scan.checkpointEntries.find((entry) => entry.path === context.checkpointPath);
	if (current && !isContextAnchoredCheckpoint(context, current.path, current.checkpoint, current.digest)) {
		throw invalidRoot();
	}
	const successors = scan.checkpointEntries.filter((entry) => entry.checkpoint.epoch > context.checkpoint.epoch);
	if (successors.length === 0) {
		if (
			!current || scan.head?.path !== current.path ||
			scan.checkpointEntries.some((entry) => {
				const expectedSha256 = anchors.get(entry.path);
				return expectedSha256 === undefined ||
					!isContextAnchoredCheckpoint(context, entry.path, entry.checkpoint, expectedSha256);
			})
		) throw invalidRoot();
		return { kind: "current", entry: current };
	}
	const successor = successors[0];
	const successorEpochPath = join(context.journalDirectory, epochName(successor.checkpoint));
	if (
		successors.length !== 1 || scan.head?.path !== successor.path ||
		!isImmediateSuccessorCheckpoint(context, successor.checkpoint) ||
		scan.checkpointEntries.some((entry) => ![context.checkpointPath, successor.path].includes(entry.path)) ||
		scan.epochEntries.some((entry) => ![context.epochDirectory, successorEpochPath].includes(entry.path)) ||
		!scan.epochEntries.some((entry) => entry.path === successorEpochPath)
	) throw invalidRoot();
	return { kind: "successor", entry: successor };
}

async function scanAuthenticatedContextRoot(context, options) {
	const anchors = contextCheckpointAnchors(context);
	const scan = await scanJournalRoot(context.statePath, context.journalDirectory, options, {
		checkpointAnchors: anchors,
		checkpointContext: context,
	});
	const invalidRoot = () => new Error(
		"Consumer high-water journal root has neither its byte-exact current checkpoint nor one exact immediate successor.",
	);
	const authority = classifyContextCheckpointAuthority(scan, context, anchors, invalidRoot);
	await authenticateStableChangedRoot(scan, context, options, anchors, authority.entry, invalidRoot);
	return { scan, authority };
}

async function initializeJournal(
	statePath,
	journalDirectory,
	options,
	bootstrapCheckpoint = genesisCheckpoint(statePath),
	beforeCheckpointLink,
) {
	let scan = await scanJournalRoot(statePath, journalDirectory, options);
	if (scan.head) {
		if (!scan.missingHeadEpoch) return scan;
		if (
			scan.head.checkpoint.epoch !== 1 || !metadataBytes(scan.head.checkpoint).equals(metadataBytes(bootstrapCheckpoint)) ||
			scan.epochEntries.length !== 0
		) throw new Error("Consumer high-water journal checkpoint lacks its exact epoch directory.");
		await ensureDirectory(
			join(journalDirectory, epochName(scan.head.checkpoint)),
			"Consumer high-water epoch directory",
			options,
		);
		return scanJournalRoot(statePath, journalDirectory, options);
	}
	if (scan.epochEntries.length > 0) throw new Error("Consumer high-water journal contains an orphan epoch directory.");
	const checkpoint = bootstrapCheckpoint;
	const bootstrap = { generation: 0, token: checkpoint.epochId };
	const bootstrapContext = {
		statePath,
		journalDirectory,
		checkpoint,
		checkpointPath: join(journalDirectory, checkpointName(checkpoint)),
		checkpointDigest: digest(metadataBytes(checkpoint)),
		epochDirectory: join(journalDirectory, epochName(checkpoint)),
		temporaryDirectory: join(journalDirectory, TEMPORARY_DIRECTORY_NAME),
	};
	await publishImmutable({
		path: bootstrapContext.checkpointPath,
		bytes: metadataBytes(checkpoint),
		directory: journalDirectory,
		kind: "checkpoint",
		context: bootstrapContext,
		writer: bootstrap,
		options,
		revalidate: false,
		beforeLink: beforeCheckpointLink,
	});
	await ensureDirectory(bootstrapContext.epochDirectory, "Consumer high-water epoch directory", options);
	scan = await scanJournalRoot(statePath, journalDirectory, options);
	if (!scan.head) throw new Error("Consumer high-water journal initialization did not publish a checkpoint.");
	return scan;
}

function contextFromHead(statePath, guardPath, journalDirectory, head) {
	return {
		statePath,
		guardPath,
		journalDirectory,
		checkpoint: head.checkpoint,
		checkpointPath: head.path,
		checkpointDigest: head.digest,
		epochDirectory: join(journalDirectory, epochName(head.checkpoint)),
		temporaryDirectory: join(journalDirectory, TEMPORARY_DIRECTORY_NAME),
	};
}

async function readProjection(context, operation, options) {
	await revalidateAuthority(context, operation, options);
	const bytes = await readSecureFile(
		context.statePath,
		options.stateMaxBytes,
		"Consumer high-water state",
		options,
		0,
		options.hooks?.projectionRead,
	);
	if (bytes === null) return { exists: false, bytes: null, sha256: null, malformed: false };
	if (bytes.length < 1) return { exists: true, bytes: null, sha256: null, malformed: true };
	return { exists: true, bytes, sha256: digest(bytes), malformed: false };
}

async function walkTransactions(context, options) {
	await revalidateAuthority(context, "walk-transactions", options);
	await options.syncDirectory(context.epochDirectory);
	const entries = await options.readDirectory(context.epochDirectory);
	if (entries.length > options.maxJournalEntries + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water epoch exceeds its safe allocation bound.");
	}
	const named = new Map();
	for (const name of entries) {
		const match = transitionPattern.exec(name);
		if (match) {
			if (named.has(match[1])) throw new Error("Consumer high-water journal contains a duplicate transition.");
			named.set(match[1], name);
			if (named.size > options.maxTransactionDepth) {
				throw new Error("Consumer high-water transaction chain exceeds its safe entry bound.");
			}
		}
	}
	const visited = new Set();
	let tipDigest = context.checkpoint.anchorDigest;
	let tipBytes = validateCheckpoint(context.checkpoint, options.stateMaxBytes).anchorBytes;
	const budget = { bytes: 0 };
	for (let depth = 0; named.has(tipDigest); depth += 1) {
		if (depth >= options.maxTransactionDepth || visited.has(tipDigest)) {
			throw new Error("Consumer high-water transaction chain is cyclic or exceeds its safe bound.");
		}
		visited.add(tipDigest);
		const path = transitionPath(context, tipDigest);
		await revalidateAuthority(context, "read-transition", options);
		const value = await readExactMetadata(
			path,
			options.metadataMaxBytes,
			(candidate) => validateTransaction(candidate, tipDigest, options.stateMaxBytes).value,
			"Consumer high-water transaction",
			options,
			budget,
		);
		const validated = validateTransaction(value, tipDigest, options.stateMaxBytes);
		tipDigest = value.candidateDigest;
		tipBytes = validated.candidateBytes;
	}
	if (visited.size !== named.size) throw new Error("Consumer high-water transaction chain contains an unreachable transition.");
	return { tipDigest, tipBytes, length: visited.size };
}

function isProjectionReplacementTransient(error) {
	return error?.code === "ENOENT" || error?.message === "Consumer high-water state changed while it was read.";
}

function isCommitHelperReplacementTransient(error) {
	return isProjectionReplacementTransient(error) || error instanceof ConsumerEpochAdvancedError;
}

async function repairProjection(context, initialTip, options, writer = options.activeWriter) {
	let tip = initialTip;
	for (let attempt = 0; attempt < PROJECTION_RETRY_LIMIT; attempt += 1) {
		if (tip.tipBytes === null) return tip;
		try {
			const projection = await readProjection(context, "projection-read", options);
			if (projection.sha256 !== tip.tipDigest) {
				await options.hooks?.beforeProjectionWrite?.({ tipDigest: tip.tipDigest });
				await revalidateAuthority(context, "projection-write", options);
				const temporary = join(context.temporaryDirectory, temporaryName(context.statePath, "projection", writer, context));
				let handle;
				try {
					handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
					await handle.chmod?.(0o600);
					await handle.writeFile(tip.tipBytes);
					await handle.sync();
					await handle.close();
					handle = undefined;
					await options.hooks?.afterProjectionFileSync?.({ tipDigest: tip.tipDigest, temporary });
					await revalidateAuthority(context, "projection-rename", options);
					await options.renameFile(temporary, context.statePath);
					await options.hooks?.afterProjectionRename?.({ tipDigest: tip.tipDigest });
					await options.syncDirectory(context.temporaryDirectory);
					await options.syncDirectory(dirname(context.statePath));
					await options.hooks?.afterProjectionDirectorySync?.({ tipDigest: tip.tipDigest });
				} finally {
					if (handle !== undefined) await handle.close();
					await options.removeFile(temporary, { force: true });
					await options.syncDirectory(context.temporaryDirectory);
				}
			}
		} catch (error) {
			if (!isProjectionReplacementTransient(error)) throw error;
			await revalidateAuthority(context, "projection-retry-authentication", options);
			tip = await walkTransactions(context, options);
			continue;
		}
		const latest = await walkTransactions(context, options);
		if (latest.tipDigest === tip.tipDigest) return latest;
		tip = latest;
	}
	throw new Error("Consumer high-water projection could not catch up with its immutable transaction tip.");
}

async function publishTransition(context, transaction, claim, options) {
	validateTransaction(transaction, transaction.baseDigest, options.stateMaxBytes);
	const path = transitionPath(context, transaction.baseDigest);
	const result = await publishMetadata(path, transaction, "transition", context, claim, options);
	const existing = validateTransaction(result.value, transaction.baseDigest, options.stateMaxBytes).value;
	if (!metadataBytes(existing).equals(metadataBytes(transaction))) {
		throw new Error("Consumer high-water transaction lost its immutable base-digest compare-and-set.");
	}
}

async function scanEpoch(context, options) {
	const discoveredNextEpoch = await revalidateAuthority(
		context,
		"scan-claims",
		options,
		options.inProgressCheckpoint ?? null,
		true,
	);
	await options.syncDirectory(context.epochDirectory);
	const names = await options.readDirectory(context.epochDirectory);
	if (names.length > options.maxJournalEntries + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water epoch exceeds its safe allocation bound.");
	}
	const claimContentNames = new Map();
	const claimIndexNames = new Map();
	const legacyClaimNames = new Map();
	const heartbeatNames = new Map();
	const terminalNames = new Map();
	const appliedNames = new Map();
	const temporaries = [];
	let authoritativeEntryCount = 0;
	for (const name of names) {
		let match;
		if ((match = claimPattern.exec(name))) {
			const generation = Number(match[1]);
			const contents = claimContentNames.get(generation) ?? new Map();
			contents.set(match[2], name);
			claimContentNames.set(generation, contents);
			authoritativeEntryCount += 1;
		} else if ((match = claimIndexPattern.exec(name))) {
			const generation = Number(match[1]);
			if (claimIndexNames.has(generation)) throw new Error("Consumer high-water lock contains a duplicate claim index.");
			claimIndexNames.set(generation, name);
			authoritativeEntryCount += 1;
		} else if ((match = undigestedClaimPattern.exec(name))) {
			const generation = Number(match[1]);
			if (legacyClaimNames.has(generation)) throw new Error("Consumer high-water lock contains a duplicate legacy claim.");
			legacyClaimNames.set(generation, name);
			authoritativeEntryCount += 1;
		} else if ((match = heartbeatPattern.exec(name))) {
			heartbeatNames.set(`${Number(match[1])}:${match[2]}`, name);
			authoritativeEntryCount += 1;
		} else if ((match = terminalPattern.exec(name))) {
			terminalNames.set(`${Number(match[1])}:${match[2]}`, name);
			authoritativeEntryCount += 1;
		} else if ((match = appliedPattern.exec(name))) {
			appliedNames.set(`${Number(match[1])}:${match[2]}`, name);
			authoritativeEntryCount += 1;
		} else if (transitionPattern.test(name)) {
			// Validated by the transaction walk before any state decision.
			authoritativeEntryCount += 1;
		} else if (name.startsWith(".")) {
			const temporary = await inspectTemporary(join(context.epochDirectory, name), options);
			if (temporary && ["checkpoint", "projection", "legacy-guard"].includes(temporary.kind)) {
				throw new Error("Consumer high-water epoch contains an unexpected owned temporary.");
			}
			if (temporary) temporaries.push(temporary);
		} else {
			throw new Error("Consumer high-water epoch contains a malformed or unexpected entry.");
		}
	}
	if (authoritativeEntryCount > options.maxJournalEntries) {
		throw new Error("Consumer high-water epoch exceeds its safe entry bound.");
	}
	const budget = { bytes: 0 };
	const claims = [];
	const byKey = new Map();
	const referencedClaimContents = new Set();
	const generations = new Set([...claimIndexNames.keys(), ...legacyClaimNames.keys()]);
	for (const generation of [...generations].sort((left, right) => left - right)) {
		if (claimIndexNames.has(generation) && legacyClaimNames.has(generation)) {
			throw new Error("Consumer high-water lock contains competing indexed and legacy claims.");
		}
		let claim;
		if (claimIndexNames.has(generation)) {
			const index = await readExactMetadata(
				join(context.epochDirectory, claimIndexNames.get(generation)),
				options.metadataMaxBytes,
				(value) => validateClaimIndex(value, generation),
				"Consumer high-water claim index",
				options,
				budget,
			);
			const name = claimContentNames.get(generation)?.get(index.claimSha256);
			if (!name) throw new Error("Consumer high-water claim index lacks its exact digest-bound claim bytes.");
			referencedClaimContents.add(name);
			claim = await readExactMetadata(
				join(context.epochDirectory, name),
				options.metadataMaxBytes,
				(value) => validateClaim(value, context, options.stateMaxBytes),
				"Consumer high-water operation claim",
				options,
				budget,
				index.claimSha256,
			);
			if (
				claim.generation !== generation || digest(metadataBytes(claim)) !== index.claimSha256 ||
				name !== basename(claimPath(context, claim))
			) throw new Error("Consumer high-water claim index differs from its exact canonical claim bytes.");
		} else {
			const name = legacyClaimNames.get(generation);
			claim = await readExactMetadata(
				join(context.epochDirectory, name),
				options.metadataMaxBytes,
				(value) => validateClaim(value, context, options.stateMaxBytes),
				"Consumer high-water legacy operation claim",
				options,
				budget,
			);
			if (claim.generation !== generation || name !== `claim-${generationName(generation)}.json`) {
				throw new Error("Consumer high-water legacy claim name differs from its exact generation.");
			}
		}
		claims.push(claim);
		byKey.set(`${generation}:${claim.token}`, claim);
	}
	for (const [generation, contents] of [...claimContentNames].sort((left, right) => left[0] - right[0])) {
		for (const [claimSha256, name] of [...contents].sort((left, right) => left[0].localeCompare(right[0]))) {
			if (referencedClaimContents.has(name)) continue;
			const claim = await readExactMetadata(
				join(context.epochDirectory, name),
				options.metadataMaxBytes,
				(value) => validateClaim(value, context, options.stateMaxBytes),
				"Consumer high-water unindexed claim content",
				options,
				budget,
			);
			if (
				claim.generation !== generation || digest(metadataBytes(claim)) !== claimSha256 ||
				name !== basename(claimPath(context, claim))
			) throw new Error("Consumer high-water unindexed claim content differs from its exact canonical bytes.");
		}
	}
	if (claims.length > MAX_OPERATION_GENERATIONS) throw new Error("Consumer high-water operation generation bound is exhausted.");
	for (let index = 0; index < claims.length; index += 1) {
		if (claims[index].generation !== index + 1) throw new Error("Consumer high-water lock generations are not contiguous.");
	}
	for (const [key, name] of heartbeatNames) {
		const claim = byKey.get(key);
		if (!claim) throw new Error("Consumer high-water epoch contains an orphan heartbeat entry.");
		await readExactMetadata(
			join(context.epochDirectory, name),
			options.metadataMaxBytes,
			(value) => validateHeartbeat(value, claim),
			"Consumer high-water lock heartbeat",
			options,
			budget,
		);
	}
	const terminals = new Map();
	for (const [key, name] of terminalNames) {
		const claim = byKey.get(key);
		if (!claim) throw new Error("Consumer high-water epoch contains an orphan terminal entry.");
		terminals.set(key, await readExactMetadata(
			join(context.epochDirectory, name),
			options.metadataMaxBytes,
			(value) => validateTerminal(value, claim, options.stateMaxBytes),
			"Consumer high-water lock terminal marker",
			options,
			budget,
		));
	}
	const appliedClaims = new Set();
	for (const [key, name] of appliedNames) {
		const claim = byKey.get(key);
		const terminal = terminals.get(key);
		if (!claim || !terminal) throw new Error("Consumer high-water epoch contains an orphan applied entry.");
		await readExactMetadata(
			join(context.epochDirectory, name),
			options.metadataMaxBytes,
			(value) => validateApplied(value, claim, terminal),
			"Consumer high-water lock applied marker",
			options,
			budget,
		);
		appliedClaims.add(key);
	}
	for (const claim of claims.slice(0, -1)) {
		const key = `${claim.generation}:${claim.token}`;
		const terminal = terminals.get(key);
		if (claim.type === "rotation" || !terminal || (terminal.outcome === "commit" && !appliedClaims.has(key))) {
			throw new Error("Consumer high-water operation generations crossed an unresolved earlier slot.");
		}
	}
	if (discoveredNextEpoch !== null) {
		const latest = claims.at(-1);
		if (latest?.type !== "rotation") {
			throw new Error("Consumer high-water in-progress next epoch lacks its exact published rotation intent.");
		}
		const intent = validateRotationIntent(latest.intent, context, options.stateMaxBytes);
		if (
			!isImmediateSuccessorCheckpoint(context, intent.checkpoint) ||
			discoveredNextEpoch !== join(context.journalDirectory, epochName(intent.checkpoint))
		) throw new Error("Consumer high-water in-progress next epoch differs from its exact published rotation intent.");
		if (await authenticateChangedRoot(context, options, intent.checkpoint)) {
			throw new ConsumerEpochAdvancedError();
		}
	}
	return { claims, terminals, temporaries };
}

async function readTerminal(context, claim, options) {
	await revalidateAuthority(context, "read-terminal", options);
	return readExactMetadata(
		terminalPath(context, claim),
		options.metadataMaxBytes,
		(value) => validateTerminal(value, claim, options.stateMaxBytes),
		"Consumer high-water lock terminal marker",
		options,
	);
}

async function readHeartbeat(context, claim, options) {
	await revalidateAuthority(context, "read-heartbeat", options);
	const heartbeat = await readExactMetadata(
		heartbeatPath(context, claim),
		options.metadataMaxBytes,
		(value) => validateHeartbeat(value, claim),
		"Consumer high-water lock heartbeat",
		options,
	);
	return heartbeat ?? { ...claim, refreshedAtMs: claim.createdAtMs };
}

async function publishTerminal(context, claim, wanted, options) {
	const result = await publishMetadata(
		terminalPath(context, claim),
		wanted,
		`terminal-${wanted.outcome}`,
		context,
		claim,
		options,
	);
	return validateTerminal(result.value, claim, options.stateMaxBytes);
}

async function refreshHeartbeat(context, claim, options) {
	if (await readTerminal(context, claim, options) !== null) return false;
	const value = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		refreshedAtMs: options.now(),
	};
	const path = heartbeatPath(context, claim);
	await revalidateAuthority(context, "heartbeat", options);
	const temporary = join(context.temporaryDirectory, temporaryName(path, "heartbeat", claim, context));
	let handle;
	try {
		handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
		await handle.chmod?.(0o600);
		await handle.writeFile(metadataBytes(value));
		await handle.sync();
		await handle.close();
		handle = undefined;
		if (await readTerminal(context, claim, options) !== null) return false;
		await revalidateAuthority(context, "heartbeat-rename", options);
		await options.renameFile(temporary, path);
		await options.syncDirectory(context.temporaryDirectory);
		await options.syncDirectory(context.epochDirectory);
		return true;
	} finally {
		if (handle !== undefined) await handle.close();
		await options.removeFile(temporary, { force: true });
		await options.syncDirectory(context.temporaryDirectory);
	}
}

function defaultHeartbeatScheduler({ interval, beat }) {
	let stopped = false;
	let timer;
	let pending = Promise.resolve();
	const arm = () => {
		if (stopped) return;
		timer = setTimeout(() => {
			pending = beat().catch(() => false).finally(arm);
		}, interval);
		timer.unref?.();
	};
	arm();
	return async () => {
		stopped = true;
		clearTimeout(timer);
		await pending;
	};
}

async function publishApplied(context, claim, terminal, options) {
	const value = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		terminalSha256: digest(metadataBytes(terminal)),
	};
	const result = await publishMetadata(appliedPath(context, claim), value, "applied", context, claim, options);
	validateApplied(result.value, claim, terminal);
	await options.hooks?.afterApplied?.({ claim, terminal });
}

async function readApplied(context, claim, terminal, options) {
	await revalidateAuthority(context, "read-applied", options);
	return readExactMetadata(
		appliedPath(context, claim),
		options.metadataMaxBytes,
		(value) => validateApplied(value, claim, terminal),
		"Consumer high-water lock applied marker",
		options,
	);
}

async function authenticateAppliedCommit(context, claim, terminal, options) {
	for (const transaction of terminal.transactions) {
		await revalidateAuthority(context, "read-applied-transition", options);
		const existing = await readExactMetadata(
			transitionPath(context, transaction.baseDigest),
			options.metadataMaxBytes,
			(value) => validateTransaction(value, transaction.baseDigest, options.stateMaxBytes).value,
			"Consumer high-water applied transaction",
			options,
		);
		if (existing === null || !metadataBytes(existing).equals(metadataBytes(transaction))) {
			throw new Error("Consumer high-water applied marker does not authenticate its exact immutable transaction chain.");
		}
	}
	const tip = await walkTransactions(context, options);
	const terminalDigest = terminal.transactions.at(-1).candidateDigest;
	if (tip.tipDigest !== terminalDigest) {
		await revalidateAuthority(context, "read-applied-continuation", options);
		const continuation = await readExactMetadata(
			transitionPath(context, terminalDigest),
			options.metadataMaxBytes,
			(value) => validateTransaction(value, terminalDigest, options.stateMaxBytes).value,
			"Consumer high-water applied transaction continuation",
			options,
		);
		if (continuation === null) {
			throw new Error("Consumer high-water applied marker does not authenticate its exact terminal digest.");
		}
	}
	const repairedTip = await repairProjection(context, tip, options, claim);
	const projection = await readProjection(context, "read-applied-projection", options);
	if (projection.malformed || projection.sha256 !== repairedTip.tipDigest) {
		throw new Error("Consumer high-water applied marker does not authenticate the current immutable tip and projection.");
	}
}

async function finishCommitAtAuthenticatedDescendant(context, options) {
	const { authority } = await scanAuthenticatedContextRoot(context, options);
	if (authority.kind !== "successor") return false;
	const descendant = contextFromHead(context.statePath, context.guardPath, context.journalDirectory, authority.entry);
	const tip = await walkTransactions(descendant, options);
	await repairProjection(descendant, tip, options, {
		generation: 0,
		token: descendant.checkpoint.epochId,
		type: "rotation",
	});
	return true;
}

async function finishCommit(context, claim, terminal, options) {
	for (let attempt = 0; attempt < PROJECTION_RETRY_LIMIT; attempt += 1) {
		try {
			const applied = await readApplied(context, claim, terminal, options);
			if (applied !== null) {
				await authenticateAppliedCommit(context, claim, terminal, options);
				return;
			}
			for (const transaction of terminal.transactions) await publishTransition(context, transaction, claim, options);
			const tip = await walkTransactions(context, options);
			await repairProjection(context, tip, options, claim);
			await publishApplied(context, claim, terminal, options);
			return;
		} catch (error) {
			if (!isCommitHelperReplacementTransient(error)) throw error;
			try {
				await revalidateAuthority(context, "finish-commit-retry-authentication", options);
				await walkTransactions(context, options);
			} catch (authenticationError) {
				if (!isCommitHelperReplacementTransient(authenticationError)) throw authenticationError;
				if (await finishCommitAtAuthenticatedDescendant(context, options)) return;
				throw authenticationError;
			}
		}
	}
	throw new Error("Consumer high-water commit helper could not converge after bounded projection replacement retries.");
}

function rotationCheckpoint(context, tip) {
	const epochId = deterministicUuid(
		`pylon-consumer-rotation-v2:${context.checkpointDigest}:${tip.tipDigest}`,
	);
	const checkpoint = {
		schemaVersion: CHECKPOINT_SCHEMA_VERSION,
		epoch: context.checkpoint.epoch + 1,
		epochId,
		previousCheckpointSha256: context.checkpointDigest,
		previousTipSha256: tip.tipDigest,
		historySha256: digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${tip.tipDigest}`,
		)),
		anchorDigest: tip.tipDigest,
		anchorBase64: tip.tipBytes === null ? null : tip.tipBytes.toString("base64"),
		retiredEpochDirectory: basename(context.epochDirectory),
		sourceAuthoritySha256: context.checkpoint.sourceAuthoritySha256,
		sourceAuthorityTipDigest: context.checkpoint.sourceAuthorityTipDigest,
		sourceAuthorityTipBase64: context.checkpoint.sourceAuthorityTipBase64,
	};
	validateCheckpoint(checkpoint, Number.MAX_SAFE_INTEGER);
	return checkpoint;
}

function rotationIntentFor(context, tip) {
	return {
		schemaVersion: ROTATION_INTENT_SCHEMA_VERSION,
		epoch: context.checkpoint.epoch,
		epochId: context.checkpoint.epochId,
		checkpointSha256: context.checkpointDigest,
		tipSha256: tip.tipDigest,
		checkpoint: rotationCheckpoint(context, tip),
	};
}

function rotationClaimFor(context, generation, tip) {
	const intent = rotationIntentFor(context, tip);
	return {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: intent.checkpoint.epochId,
		type: "rotation",
		intent,
	};
}

async function effectiveTip(context, options) {
	const chain = await walkTransactions(context, options);
	if (chain.tipBytes !== null) return chain;
	const projection = await readProjection(context, "rotation-legacy-state-read", options);
	if (projection.malformed) throw new Error("Consumer high-water rotation cannot authenticate its legacy projection anchor.");
	if (projection.bytes === null) return chain;
	return { tipDigest: digest(projection.bytes), tipBytes: projection.bytes, length: chain.length };
}

async function scanRotationPublicationSet(context, checkpoint, options, requirePublished) {
	const { scan, authority } = await scanAuthenticatedContextRoot(context, options);
	const nextCheckpointPath = join(context.journalDirectory, checkpointName(checkpoint));
	const nextEpochPath = join(context.journalDirectory, epochName(checkpoint));
	const allowedCheckpointPaths = new Set([context.checkpointPath, nextCheckpointPath]);
	const allowedEpochPaths = new Set([context.epochDirectory, nextEpochPath]);
	const currentCheckpoint = scan.checkpointEntries.find((entry) => entry.path === context.checkpointPath);
	const currentEpoch = scan.epochEntries.find((entry) => entry.path === context.epochDirectory);
	const published = scan.checkpointEntries.find((entry) => entry.path === nextCheckpointPath);
	if (
		scan.checkpointEntries.some((entry) => !allowedCheckpointPaths.has(entry.path)) ||
		scan.epochEntries.some((entry) => !allowedEpochPaths.has(entry.path)) ||
		(currentCheckpoint && currentCheckpoint.digest !== context.checkpointDigest) ||
		!scan.epochEntries.some((entry) => entry.path === nextEpochPath) ||
		(!requirePublished && (!currentCheckpoint || !currentEpoch)) ||
		(published === undefined
			? authority.kind !== "current"
			: authority.kind !== "successor" || authority.entry.path !== published.path)
	) throw new Error("Consumer high-water rotation found a competing root or epoch publication.");
	if (published && !metadataBytes(published.checkpoint).equals(metadataBytes(checkpoint))) {
		throw new Error("Consumer high-water rotation found a competing checkpoint for the same epoch.");
	}
	if (requirePublished && (!published || scan.head?.path !== nextCheckpointPath || scan.missingHeadEpoch)) {
		throw new Error("Consumer high-water rotation checkpoint did not become the unique complete journal head.");
	}
	return scan;
}

async function finishRotationCheckpoint(context, checkpoint, writer, options) {
	validateCheckpoint(checkpoint, options.stateMaxBytes);
	if (
		checkpoint.epoch !== context.checkpoint.epoch + 1 ||
		checkpoint.previousCheckpointSha256 !== context.checkpointDigest ||
		checkpoint.retiredEpochDirectory !== basename(context.epochDirectory) ||
		checkpoint.sourceAuthoritySha256 !== context.checkpoint.sourceAuthoritySha256 ||
		checkpoint.sourceAuthorityTipDigest !== context.checkpoint.sourceAuthorityTipDigest ||
		checkpoint.sourceAuthorityTipBase64 !== context.checkpoint.sourceAuthorityTipBase64 ||
		checkpoint.historySha256 !== digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${checkpoint.anchorDigest}`,
		))
	) throw new Error("Consumer high-water rotation does not anchor the exact current epoch.");
	const tip = await effectiveTip(context, options);
	const anchorBytes = validateCheckpoint(checkpoint, options.stateMaxBytes).anchorBytes;
	if (
		checkpoint.previousTipSha256 !== tip.tipDigest || checkpoint.anchorDigest !== tip.tipDigest ||
		(anchorBytes === null ? tip.tipBytes !== null : !anchorBytes.equals(tip.tipBytes))
	) throw new Error("Consumer high-water rotation does not anchor the exact immutable tip.");
	const nextEpoch = join(context.journalDirectory, epochName(checkpoint));
	await ensureDirectory(nextEpoch, "Consumer high-water epoch directory", options);
	await options.hooks?.afterRotationEpochSync?.({ checkpoint: structuredClone(checkpoint), nextEpoch });
	await secureDirectory(nextEpoch, "Consumer high-water next epoch directory", options);
	await options.syncDirectory(nextEpoch);
	if ((await options.readDirectory(nextEpoch)).length !== 0) {
		throw new Error("Consumer high-water rotation found a competing next-epoch directory for the same parent.");
	}
	const nextPath = join(context.journalDirectory, checkpointName(checkpoint));
	await publishImmutable({
		path: nextPath,
		bytes: metadataBytes(checkpoint),
		directory: context.journalDirectory,
		kind: "checkpoint",
		context,
		writer,
		options,
		inProgressCheckpoint: checkpoint,
		beforeLink: () => scanRotationPublicationSet(context, checkpoint, options, false),
	});
	await options.hooks?.afterRotationCheckpoint?.({ checkpoint: structuredClone(checkpoint), nextPath });
	await scanRotationPublicationSet(context, checkpoint, options, true);
}

async function resolveLatestOperation(context, claim, options) {
	if (claim.type === "rotation") {
		await helpRotationOperation(context, claim, options);
		return "rotated";
	}
	const terminal = await readTerminal(context, claim, options);
	if (terminal?.outcome === "commit") {
		await finishCommit(context, claim, terminal, options);
		return "resolved";
	}
	if (terminal !== null) return "resolved";
	const heartbeat = await readHeartbeat(context, claim, options);
	if (options.now() - heartbeat.refreshedAtMs < options.stale) return "active";
	await options.hooks?.afterObserveStale?.({ claim, heartbeat });
	const retired = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		outcome: "retired",
	};
	const decision = await publishTerminal(context, claim, retired, options);
	await options.hooks?.afterRetire?.({ claim, decision });
	if (decision.outcome === "commit") await finishCommit(context, claim, decision, options);
	return "resolved";
}

function operationIdentity(claim) {
	return claim ? `${claim.generation}:${claim.token}:${claim.type}` : null;
}

function sameOperationClaim(left, right) {
	return left === null
		? right === null
		: right !== null && operationIdentity(left) === operationIdentity(right) && metadataBytes(left).equals(metadataBytes(right));
}

async function resolveOperationFrontier(context, options) {
	const initial = await scanEpoch(context, options);
	const latest = initial.claims.at(-1) ?? null;
	if (latest) {
		const outcome = await resolveLatestOperation(context, latest, options);
		if (outcome === "rotated") return { rotated: true };
		if (outcome === "active") return { active: true };
	}
	const scan = await scanEpoch(context, options);
	if (!sameOperationClaim(scan.claims.at(-1) ?? null, latest)) return { retry: true };
	return { scan, frontier: latest, rotated: false, active: false };
}

function inProgressCheckpointForClaim(context, claim, options) {
	if (claim.type !== "rotation") return null;
	const validatedClaim = validateClaim(claim, context, options.stateMaxBytes);
	const intent = validateRotationIntent(validatedClaim.intent, context, options.stateMaxBytes);
	const { anchorBytes } = validateCheckpoint(intent.checkpoint, options.stateMaxBytes);
	const expectedClaim = rotationClaimFor(context, validatedClaim.generation, {
		tipDigest: intent.tipSha256,
		tipBytes: anchorBytes,
	});
	if (!metadataBytes(validatedClaim).equals(metadataBytes(expectedClaim))) {
		throw new Error("Consumer high-water rotation claim does not match its exact authenticated intent.");
	}
	return intent.checkpoint;
}

async function tryPublishClaim(context, claim, options) {
	const inProgressCheckpoint = inProgressCheckpointForClaim(context, claim, options);
	const contentPath = claimPath(context, claim);
	const contentResult = await publishMetadata(
		contentPath,
		claim,
		"claim",
		context,
		claim,
		options,
		inProgressCheckpoint,
	);
	const existingClaim = validateClaim(contentResult.value, context, options.stateMaxBytes);
	if (!metadataBytes(existingClaim).equals(metadataBytes(claim))) {
		throw new Error("Consumer high-water claim content lost its exact digest-bound publication.");
	}
	const index = claimIndexFor(claim);
	const indexResult = await publishMetadata(
		claimIndexPath(context, claim.generation),
		index,
		"claim-index",
		context,
		claim,
		options,
		inProgressCheckpoint,
	);
	const existingIndex = validateClaimIndex(indexResult.value, claim.generation);
	if (!metadataBytes(existingIndex).equals(metadataBytes(index))) return false;
	return indexResult.created;
}

async function tryCreateNormalClaim(context, generation, options) {
	const claim = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: randomUUID(),
		type: "normal",
		ownerPid: process.pid,
		createdAtMs: options.now(),
	};
	if (!(await tryPublishClaim(context, claim, options))) return null;
	const heartbeat = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: claim.token,
		refreshedAtMs: claim.createdAtMs,
	};
	await publishMetadata(heartbeatPath(context, claim), heartbeat, "initial-heartbeat", context, claim, options);
	await options.hooks?.afterClaim?.({ claim });
	return claim;
}

async function tryCreateRotationClaim(context, generation, tip, options) {
	const claim = rotationClaimFor(context, generation, tip);
	await options.hooks?.beforeRotationDecision?.({ intent: structuredClone(claim.intent), claim: structuredClone(claim) });
	if (!(await tryPublishClaim(context, claim, options))) return null;
	await options.hooks?.afterRotationIntent?.({ intent: structuredClone(claim.intent), claim: structuredClone(claim) });
	return claim;
}

async function acquireNormalOperation(context, options) {
	for (;;) {
		const frontier = await resolveOperationFrontier(context, options);
		if (frontier.rotated) return { rotated: true };
		if (frontier.active) throw new Error(`Consumer high-water state is actively locked: ${context.journalDirectory}`);
		if (frontier.retry) continue;
		const nextGeneration = (frontier.scan.claims.at(-1)?.generation ?? 0) + 1;
		if (nextGeneration > options.maxLockGenerations) {
			throw new Error("Consumer high-water claim epoch is exhausted; run the consumer journal rotation command.");
		}
		const confirmation = await scanEpoch(context, options);
		if (!sameOperationClaim(confirmation.claims.at(-1) ?? null, frontier.frontier)) continue;
		const claim = await tryCreateNormalClaim(context, nextGeneration, options);
		if (!claim) continue;
		const afterClaim = await scanEpoch(context, options);
		if (!sameOperationClaim(afterClaim.claims.at(-1) ?? null, claim)) {
			throw new Error("Consumer high-water normal operation did not remain the unique latest slot.");
		}
		return { claim, temporaries: afterClaim.temporaries, rotated: false };
	}
}

function temporaryIsFenced(temporary, context, writer) {
	if (temporary.epochId !== context.checkpoint.epochId) return true;
	if (writer.generation === 0) {
		return temporary.generation !== 0 || temporary.token !== writer.token;
	}
	if (temporary.generation === 0 || temporary.generation < writer.generation) return true;
	return temporary.generation === writer.generation && temporary.token !== writer.token;
}

function temporaryBelongsToRetiredClaim(temporary, context, epochAuthority) {
	if (!epochAuthority || temporary.epochId !== context.checkpoint.epochId || temporary.generation === 0) return false;
	const claim = epochAuthority.claims.find((candidate) => (
		candidate.generation === temporary.generation && candidate.token === temporary.token
	));
	return claim !== undefined && epochAuthority.terminals.has(`${claim.generation}:${claim.token}`);
}

function temporaryProcessIsAlive(temporary, options) {
	try {
		options.processKill(temporary.pid, 0);
		return true;
	} catch (error) {
		if (error?.code === "ESRCH") return false;
		if (error?.code === "EPERM") return true;
		throw error;
	}
}

async function cleanupAuthority(
	context,
	writer,
	rootScan,
	epochTemporaries,
	options,
	requireQuiescent,
	epochAuthority = null,
	allowedNextEpoch = null,
) {
	await revalidateAuthority(context, "cleanup", options);
	const candidatesByPath = new Map(
		[...rootScan.temporaries, ...epochTemporaries].map((temporary) => [temporary.path, temporary]),
	);
	const parentNames = await options.readDirectory(dirname(context.statePath));
	const targetDigests = new Set([digest(Buffer.from(resolve(context.statePath))), digest(Buffer.from(resolve(context.guardPath)))]);
	for (const name of parentNames) {
		if (!name.startsWith(".pylon-consumer-tmp-v1-")) continue;
		const temporary = await inspectTemporary(join(dirname(context.statePath), name), options);
		if (!temporary || !targetDigests.has(temporary.targetSha256)) continue;
		const expectedKind = temporary.targetSha256 === digest(Buffer.from(resolve(context.statePath)))
			? "projection"
			: "legacy-guard";
		if (temporary.kind !== expectedKind) {
			throw new Error("Consumer high-water state directory contains an unexpected owned temporary.");
		}
		candidatesByPath.set(temporary.path, temporary);
	}
	for (const temporary of candidatesByPath.values()) {
		const fenced = temporaryIsFenced(temporary, context, writer);
		if (!fenced && temporary.token !== writer.token) {
			throw new Error("Consumer high-water journal contains a live or future owned temporary.");
		}
		if (!fenced) {
			if (!requireQuiescent) continue;
			if (temporaryProcessIsAlive(temporary, options)) {
				if (writer.type === "rotation" && temporary.generation === writer.generation && temporary.token === writer.token) {
					continue;
				}
				throw new Error("Consumer high-water journal rotation operation is pending until every prior owned temporary writer quiesces.");
			}
			await options.removeFile(temporary.path, { force: true });
			await options.syncDirectory(dirname(temporary.path));
			continue;
		}
		const retiredClaimTemporary = writer.generation === 0 &&
			temporaryBelongsToRetiredClaim(temporary, context, epochAuthority);
		if (!retiredClaimTemporary && temporaryProcessIsAlive(temporary, options)) {
			if (requireQuiescent) {
				throw new Error("Consumer high-water journal rotation operation is pending until every prior owned temporary writer quiesces.");
			}
			continue;
		}
		await options.removeFile(temporary.path, { force: true });
		await options.syncDirectory(dirname(temporary.path));
	}
	let retiredEpochDeferred = false;
	for (const epoch of rootScan.epochEntries) {
		if (epoch.name === basename(context.epochDirectory)) continue;
		if (allowedNextEpoch !== null && epoch.name === allowedNextEpoch) continue;
		if (epoch.name !== context.checkpoint.retiredEpochDirectory) {
			throw new Error("Consumer high-water journal contains an orphan epoch directory.");
		}
		let retiredNames;
		try {
			retiredNames = await options.readDirectory(epoch.path);
		} catch (error) {
			if (error?.code === "ENOENT") continue;
			throw error;
		}
		if (retiredNames.length > options.maxJournalEntries + MAX_TEMPORARY_ENTRIES) {
			throw new Error("Consumer high-water retired epoch exceeds its safe allocation bound.");
		}
		const retiredTemporaries = [];
		for (const name of retiredNames) {
			const path = join(epoch.path, name);
			let entry;
			try {
				entry = await options.lstatEntry(path);
			} catch (error) {
				if (error?.code === "ENOENT") continue;
				throw error;
			}
			if (entry.isSymbolicLink?.() || (!entry.isFile() && !entry.isDirectory())) {
				throw new Error("Consumer high-water retired epoch contains an unsafe entry.");
			}
			if (name.startsWith(".")) {
				const temporary = await inspectTemporary(path, options);
				if (temporary) retiredTemporaries.push(temporary);
			} else if (
				!claimPattern.test(name) && !claimIndexPattern.test(name) && !undigestedClaimPattern.test(name) &&
				!heartbeatPattern.test(name) &&
				!terminalPattern.test(name) &&
				!appliedPattern.test(name) && !transitionPattern.test(name)
			) {
				throw new Error("Consumer high-water retired epoch contains an unexpected entry.");
			} else if (!entry.isFile()) {
				throw new Error("Consumer high-water retired epoch metadata must be regular files.");
			}
		}
		if (retiredTemporaries.some((temporary) => temporaryProcessIsAlive(temporary, options))) {
			if (requireQuiescent) {
				throw new Error("Consumer high-water journal rotation operation is pending until every retired temporary writer quiesces.");
			}
			retiredEpochDeferred = true;
			continue;
		}
		await options.removeFile(epoch.path, { recursive: true, force: true });
		await options.syncDirectory(context.journalDirectory);
	}
	for (const entry of rootScan.checkpointEntries) {
		if (entry.path === context.checkpointPath) continue;
		if (
			entry.digest !== context.checkpoint.previousCheckpointSha256 ||
			epochName(entry.checkpoint) !== context.checkpoint.retiredEpochDirectory
		) throw new Error("Consumer high-water journal contains an orphan checkpoint entry.");
		if (retiredEpochDeferred) continue;
		await options.removeFile(entry.path, { force: true });
		await options.syncDirectory(context.journalDirectory);
	}
	const { scan: final, authority: finalAuthority } = await scanAuthenticatedContextRoot(context, options);
	const allowedCheckpoints = retiredEpochDeferred ? 2 : 1;
	const expectedEpochs = new Set([context.epochDirectory]);
	if (retiredEpochDeferred) {
		expectedEpochs.add(join(context.journalDirectory, context.checkpoint.retiredEpochDirectory));
	}
	if (
		allowedNextEpoch !== null &&
		final.epochEntries.some((entry) => entry.name === allowedNextEpoch)
	) expectedEpochs.add(join(context.journalDirectory, allowedNextEpoch));
	if (
		finalAuthority.kind !== "current" ||
		final.checkpointEntries.length !== allowedCheckpoints || final.epochEntries.length !== expectedEpochs.size ||
		final.temporaries.some((temporary) => !temporaryProcessIsAlive(temporary, options)) ||
		final.head?.path !== context.checkpointPath ||
		final.epochEntries.some((entry) => !expectedEpochs.has(entry.path))
	) throw new Error("Consumer high-water journal did not converge to one bounded current epoch.");
}

async function helpRotationOperation(context, claim, options) {
	if (claim.type !== "rotation") throw new Error("Consumer high-water rotation helper requires one rotation operation slot.");
	const intent = validateRotationIntent(claim.intent, context, options.stateMaxBytes);
	const helperOptions = { ...options, inProgressCheckpoint: intent.checkpoint };
	const completedBeforeHelp = await completedRotationResult(context, intent, helperOptions).catch(() => null);
	if (completedBeforeHelp) return true;
	try {
		const scan = await scanEpoch(context, helperOptions);
		const latest = scan.claims.at(-1);
		if (operationIdentity(latest) !== operationIdentity(claim) || !metadataBytes(latest).equals(metadataBytes(claim))) {
			throw new Error("Consumer high-water rotation operation is not the unique latest slot.");
		}
		const tip = await effectiveTip(context, helperOptions);
		if (tip.tipDigest !== intent.tipSha256) {
			throw new Error("Consumer high-water rotation operation no longer matches its exact authoritative tip.");
		}
		const { scan: rootScan, authority: rootAuthority } = await scanAuthenticatedContextRoot(context, helperOptions);
		if (rootAuthority.kind !== "current") {
			throw new Error("Consumer high-water rotation helper lost its exact current checkpoint authority.");
		}
		const nextEpochName = epochName(intent.checkpoint);
		await cleanupAuthority(
			context,
			claim,
			rootScan,
			scan.temporaries,
			helperOptions,
			true,
			scan,
			nextEpochName,
		);
		await finishRotationCheckpoint(context, intent.checkpoint, claim, helperOptions);
		return true;
	} catch (error) {
		const completed = await completedRotationResult(context, intent, helperOptions).catch(() => null);
		if (completed) return true;
		throw error;
	}
}

async function inspectLegacyGuard(context, options) {
	let entry;
	try {
		entry = await options.lstatEntry(context.guardPath);
	} catch (error) {
		if (error?.code === "ENOENT") return "absent";
		throw error;
	}
	if (entry.isDirectory() && !entry.isSymbolicLink?.()) {
		if (await lstatOrNull(join(context.guardPath, LEGACY_RETIREMENT_MARKER_NAME), options) === null) {
			throw new Error(
				`Legacy consumer lock directory exists at ${context.guardPath}. Stop every legacy proper-lockfile client, ` +
				"confirm that no owner remains, remove that directory manually, and retry.",
			);
		}
		await secureDirectory(context.guardPath, "Legacy consumer high-water lock directory", options);
		const marker = await readExactMetadata(
			join(context.guardPath, LEGACY_RETIREMENT_MARKER_NAME),
			options.metadataMaxBytes,
			(value) => validateLegacyRetirementMarker(value, context.statePath),
			"Legacy consumer high-water retirement marker",
			options,
		);
		await options.syncDirectory(context.guardPath);
		await options.syncDirectory(dirname(context.guardPath));
		return "retirement-marker";
	}
	if (!entry.isFile() || entry.isSymbolicLink?.()) {
		throw new Error("Legacy consumer lock guard is not one exact regular non-symlink file.");
	}
	const expected = legacyGuardFor(context.statePath);
	const actual = await readExactMetadata(
		context.guardPath,
		options.metadataMaxBytes,
		(value) => value,
		"Legacy consumer lock guard",
		options,
	);
	if (!metadataBytes(actual).equals(metadataBytes(expected))) {
		throw new Error("Legacy consumer lock guard differs from the exact durable handoff guard.");
	}
	await options.syncDirectory(dirname(context.guardPath));
	return "guard";
}

async function ensureLegacyGuard(context, claim, options) {
	if (["guard", "retirement-marker"].includes(await inspectLegacyGuard(context, options))) return;
	const expected = legacyGuardFor(context.statePath);
	await publishImmutable({
		path: context.guardPath,
		bytes: metadataBytes(expected),
		directory: dirname(context.guardPath),
		kind: "legacy-guard",
		context,
		writer: claim,
		options,
	});
	if (await inspectLegacyGuard(context, options) !== "guard") {
		throw new Error("Legacy consumer lock handoff did not publish the exact durable guard.");
	}
}

function normalizeOptions({
	stale = PYLON_CONSUMER_LOCK_STALE_MS,
	update = PYLON_CONSUMER_LOCK_UPDATE_MS,
	stateMaxBytes = DEFAULT_STATE_MAX_BYTES,
	maxTransactionDepth = MAX_TRANSACTION_DEPTH,
	maxLockGenerations = MAX_LOCK_GENERATIONS,
	maxJournalBytes = DEFAULT_JOURNAL_MAX_BYTES,
	now = Date.now,
	startHeartbeat = defaultHeartbeatScheduler,
	hooks,
	directoryOperations = {},
	lstatEntry = lstat,
	makeDirectory = mkdir,
	syncDirectory = syncConsumerStateDirectory,
	openFile = open,
	linkFile = link,
	readDirectory = readdir,
	renameFile = rename,
	removeFile = rm,
	processKill = process.kill.bind(process),
	currentUid = typeof process.getuid === "function" ? process.getuid() : null,
} = {}) {
	if (
		!Number.isSafeInteger(stale) || !Number.isSafeInteger(update) || update < 1 || stale <= update ||
		!Number.isSafeInteger(stateMaxBytes) || stateMaxBytes < 1 || stateMaxBytes > MAX_STATE_BYTES ||
		!Number.isSafeInteger(maxTransactionDepth) || maxTransactionDepth < 1 || maxTransactionDepth > MAX_TRANSACTION_DEPTH ||
		!Number.isSafeInteger(maxLockGenerations) || maxLockGenerations < 2 || maxLockGenerations > MAX_LOCK_GENERATIONS ||
		!Number.isSafeInteger(maxJournalBytes) || maxJournalBytes < stateMaxBytes || maxJournalBytes > MAX_JOURNAL_BYTES ||
		!Number.isSafeInteger(currentUid) || currentUid < 0
	) throw new Error("Consumer high-water lock timing, state-size, journal, or transaction bound is invalid.");
	return {
		stale,
		update,
		stateMaxBytes,
		maxTransactionDepth,
		maxLockGenerations,
		maxJournalBytes,
		maxJournalEntries: MAX_OPERATION_GENERATIONS * 5 + MAX_TRANSACTION_DEPTH + 32,
		metadataMaxBytes: stateMaxBytes * 3 + 8192,
		now,
		startHeartbeat,
		hooks,
		directoryOperations,
		lstatEntry,
		makeDirectory,
		syncDirectory,
		openFile,
		linkFile,
		readDirectory,
		renameFile,
		removeFile,
		processKill,
		currentUid,
		activeWriter: null,
	};
}

function normalizeRotationOptions(rawOptions) {
	const options = normalizeOptions(rawOptions);
	options.stateMaxBytes = MAX_STATE_BYTES;
	options.maxTransactionDepth = MAX_TRANSACTION_DEPTH;
	options.maxLockGenerations = MAX_LOCK_GENERATIONS;
	options.maxJournalBytes = MAX_JOURNAL_BYTES;
	options.maxJournalEntries = MAX_OPERATION_GENERATIONS * 5 + MAX_TRANSACTION_DEPTH + 32;
	options.metadataMaxBytes = MAX_STATE_BYTES * 3 + 8192;
	return options;
}

async function lstatOrNull(path, options) {
	try {
		return await options.lstatEntry(path);
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		throw error;
	}
}

function legacyTerminalFileName(claim) {
	return `terminal-${generationName(claim.generation)}-${claim.token}.json`;
}

function legacyHeartbeatFileName(claim) {
	return `heartbeat-${generationName(claim.generation)}-${claim.token}.json`;
}

function legacyAppliedFileName(claim) {
	return `applied-${generationName(claim.generation)}-${claim.token}.json`;
}

function authorityDigest(entries, tipDigest, tipBytes) {
	const hash = createHash("sha256");
	hash.update("pylon-consumer-v1-authority\0");
	const sorted = [...entries].sort((left, right) => {
		if (left[0] < right[0]) return -1;
		if (left[0] > right[0]) return 1;
		return 0;
	});
	for (const [name, bytes] of sorted) {
		const nameBytes = Buffer.from(name);
		const header = Buffer.alloc(12);
		header.writeUInt32BE(nameBytes.length, 0);
		header.writeBigUInt64BE(BigInt(bytes.length), 4);
		hash.update(header);
		hash.update(nameBytes);
		hash.update(bytes);
	}
	hash.update(Buffer.from(`tip:${tipDigest}:`));
	if (tipBytes !== null) hash.update(tipBytes);
	return hash.digest("hex");
}

async function readLegacyAuthority(statePath, lockDirectory, transactionDirectory, options) {
	await secureDirectory(lockDirectory, "Legacy consumer high-water lock directory", options);
	await secureDirectory(transactionDirectory, "Legacy consumer high-water transaction directory", options);
	await options.syncDirectory(lockDirectory);
	await options.syncDirectory(transactionDirectory);
	const transactionNames = await options.readDirectory(transactionDirectory);
	if (transactionNames.length > options.maxTransactionDepth) {
		throw new Error("Legacy consumer high-water transaction directory exceeds its safe entry bound.");
	}
	const budget = { bytes: 0 };
	const authorityEntries = [];
	const actualTransactions = new Map();
	for (const name of transactionNames) {
		const match = legacyTransitionPattern.exec(name);
		if (!match || actualTransactions.has(match[1])) {
			throw new Error("Legacy consumer high-water transaction directory contains a malformed or extra entry.");
		}
		const value = await readExactMetadata(
			join(transactionDirectory, name),
			options.metadataMaxBytes,
			(candidate) => validateTransaction(candidate, match[1], options.stateMaxBytes).value,
			"Legacy consumer high-water transaction",
			options,
			budget,
		);
		actualTransactions.set(match[1], value);
		authorityEntries.push([`transactions/${name}`, metadataBytes(value)]);
	}
	const lockNames = await options.readDirectory(lockDirectory);
	if (lockNames.length > MAX_OPERATION_GENERATIONS * 4 + 1) {
		throw new Error("Legacy consumer high-water lock directory exceeds its safe entry bound.");
	}
	const claimNames = new Map();
	const heartbeatNames = new Map();
	const terminalNames = new Map();
	const appliedNames = new Map();
	let retirementMarker = null;
	for (const name of lockNames) {
		let match;
		if (name === LEGACY_RETIREMENT_MARKER_NAME) {
			if (retirementMarker !== null) throw new Error("Legacy consumer high-water retirement marker is duplicated.");
			retirementMarker = await readExactMetadata(
				join(lockDirectory, name),
				options.metadataMaxBytes,
				(value) => validateLegacyRetirementMarker(value, statePath),
				"Legacy consumer high-water retirement marker",
				options,
				budget,
			);
			continue;
		}
		if ((match = undigestedClaimPattern.exec(name))) claimNames.set(Number(match[1]), name);
		else if ((match = heartbeatPattern.exec(name))) heartbeatNames.set(`${Number(match[1])}:${match[2]}`, name);
		else if ((match = terminalPattern.exec(name))) terminalNames.set(`${Number(match[1])}:${match[2]}`, name);
		else if ((match = appliedPattern.exec(name))) appliedNames.set(`${Number(match[1])}:${match[2]}`, name);
		else throw new Error("Legacy consumer high-water lock directory contains a malformed or extra entry.");
	}
	const claims = [];
	const byKey = new Map();
	for (const [generation, name] of [...claimNames].sort((left, right) => left[0] - right[0])) {
		const claim = await readExactMetadata(
			join(lockDirectory, name),
			options.metadataMaxBytes,
			validateLegacyClaim,
			"Legacy consumer high-water lock claim",
			options,
			budget,
		);
		if (claim.generation !== generation || name !== `claim-${generationName(generation)}.json`) {
			throw new Error("Legacy consumer high-water claim name differs from its exact generation.");
		}
		claims.push(claim);
		byKey.set(`${claim.generation}:${claim.token}`, claim);
		authorityEntries.push([`lock/${name}`, metadataBytes(claim)]);
	}
	if (claims.length > options.maxLockGenerations) throw new Error("Legacy consumer high-water claim bound is exhausted.");
	for (let index = 0; index < claims.length; index += 1) {
		if (claims[index].generation !== index + 1) throw new Error("Legacy consumer high-water claims are not contiguous.");
	}
	for (const [key, name] of heartbeatNames) {
		const claim = byKey.get(key);
		if (!claim || name !== legacyHeartbeatFileName(claim)) {
			throw new Error("Legacy consumer high-water lock contains an orphan heartbeat.");
		}
		const heartbeat = await readExactMetadata(
			join(lockDirectory, name),
			options.metadataMaxBytes,
			(value) => validateLegacyHeartbeat(value, claim),
			"Legacy consumer high-water heartbeat",
			options,
			budget,
		);
		authorityEntries.push([`lock/${name}`, metadataBytes(heartbeat)]);
	}
	const terminals = new Map();
	for (const [key, name] of terminalNames) {
		const claim = byKey.get(key);
		if (!claim || name !== legacyTerminalFileName(claim)) {
			throw new Error("Legacy consumer high-water lock contains an orphan terminal marker.");
		}
		const terminal = await readExactMetadata(
			join(lockDirectory, name),
			options.metadataMaxBytes,
			(value) => validateLegacyTerminal(value, claim, options.stateMaxBytes),
			"Legacy consumer high-water terminal marker",
			options,
			budget,
		);
		terminals.set(key, terminal);
		authorityEntries.push([`lock/${name}`, metadataBytes(terminal)]);
	}
	for (const claim of claims) {
		const key = `${claim.generation}:${claim.token}`;
		if (!heartbeatNames.has(key)) throw new Error("Legacy consumer high-water claim lacks its exact heartbeat.");
	}
	const appliedTerminals = new Set();
	for (const [key, name] of appliedNames) {
		const claim = byKey.get(key);
		const terminal = terminals.get(key);
		if (!claim || !terminal || name !== legacyAppliedFileName(claim)) {
			throw new Error("Legacy consumer high-water lock contains an orphan applied marker.");
		}
		const applied = await readExactMetadata(
			join(lockDirectory, name),
			options.metadataMaxBytes,
			(value) => validateLegacyApplied(value, claim, terminal),
			"Legacy consumer high-water applied marker",
			options,
			budget,
		);
		authorityEntries.push([`lock/${name}`, metadataBytes(applied)]);
		appliedTerminals.add(key);
	}
	const decidedTransactions = new Map();
	const decidedDigests = new Set([GENESIS_DIGEST]);
	let tipDigest = GENESIS_DIGEST;
	let tipBytes = null;
	let decidedLength = 0;
	for (const claim of claims) {
		const terminal = terminals.get(`${claim.generation}:${claim.token}`);
		if (!terminal || terminal.outcome !== "commit") continue;
		for (const transaction of terminal.transactions) {
			if (transaction.baseDigest !== tipDigest) {
				throw new Error("Legacy consumer high-water commit decisions do not form one exact authoritative chain.");
			}
			const prior = decidedTransactions.get(transaction.baseDigest);
			if (prior && !metadataBytes(prior).equals(metadataBytes(transaction))) {
				throw new Error("Legacy consumer high-water commit decisions equivocate at one base digest.");
			}
			decidedTransactions.set(transaction.baseDigest, transaction);
			const validated = validateTransaction(transaction, tipDigest, options.stateMaxBytes);
			tipDigest = transaction.candidateDigest;
			tipBytes = validated.candidateBytes;
			decidedDigests.add(tipDigest);
			decidedLength += 1;
			if (decidedLength > options.maxTransactionDepth) {
				throw new Error("Legacy consumer high-water decisions exceed their safe transaction bound.");
			}
		}
	}
	let actualDigest = GENESIS_DIGEST;
	let actualCount = 0;
	while (actualTransactions.has(actualDigest)) {
		const actual = actualTransactions.get(actualDigest);
		const decided = decidedTransactions.get(actualDigest);
		if (!decided || !metadataBytes(actual).equals(metadataBytes(decided))) {
			throw new Error("Legacy consumer high-water transition lacks its exact immutable commit decision.");
		}
		actualDigest = actual.candidateDigest;
		actualCount += 1;
		if (actualCount > options.maxTransactionDepth) {
			throw new Error("Legacy consumer high-water transition chain exceeds its safe bound.");
		}
	}
	if (actualCount !== actualTransactions.size) {
		throw new Error("Legacy consumer high-water transaction chain contains a corrupt, unreachable, or extra transition.");
	}
	for (const [baseDigest, actual] of actualTransactions) {
		const decided = decidedTransactions.get(baseDigest);
		if (!decided || !metadataBytes(actual).equals(metadataBytes(decided))) {
			throw new Error("Legacy consumer high-water transition differs from its exact commit decision.");
		}
	}
	for (const key of appliedTerminals) {
		const terminal = terminals.get(key);
		for (const transaction of terminal.transactions) {
			const actual = actualTransactions.get(transaction.baseDigest);
			if (!actual || !metadataBytes(actual).equals(metadataBytes(transaction))) {
				throw new Error("Legacy consumer high-water applied marker is missing its completed transition.");
			}
		}
	}
	const recoveries = [];
	for (const claim of claims) {
		const key = `${claim.generation}:${claim.token}`;
		const terminal = terminals.get(key);
		if (!terminal) {
			recoveries.push({ kind: "retire", claim });
			continue;
		}
		if (terminal.outcome === "commit" && !appliedTerminals.has(key)) {
			recoveries.push({
				kind: "commit",
				claim,
				terminal,
				missingTransactions: terminal.transactions.filter((transaction) => !actualTransactions.has(transaction.baseDigest)),
			});
		}
	}
	const projection = await readSecureFile(
		statePath,
		options.stateMaxBytes,
		"Legacy consumer high-water projection",
		options,
		0,
	);
	if (projection !== null && projection.length < 1) throw new Error("Legacy consumer high-water projection is malformed.");
	if (decidedLength === 0 && projection !== null) {
		tipBytes = projection;
		tipDigest = digest(projection);
		authorityEntries.push(["explicit-quiescent-projection", projection]);
	} else if (projection !== null && !decidedDigests.has(digest(projection))) {
		throw new Error("Legacy consumer high-water projection is not an authenticated prefix of its immutable authority.");
	}

	if (budget.bytes > options.maxJournalBytes) {
		throw new Error("Legacy consumer high-water authority exceeds its safe byte bound.");
	}
	const authoritySha256 = authorityDigest(authorityEntries, tipDigest, tipBytes);
	if (retirementMarker !== null) {
		const expectedMarker = legacyRetirementMarkerFor(statePath, { authoritySha256, tipDigest });
		if (!metadataBytes(retirementMarker).equals(metadataBytes(expectedMarker))) {
			throw new Error("Legacy consumer high-water retirement marker conflicts with the exact pre-marker authority or tip.");
		}
		if (recoveries.length !== 0) {
			throw new Error("Legacy consumer high-water retirement marker was published before its authority became quiescent.");
		}
	}
	return {
		tipDigest,
		tipBytes,
		length: decidedLength,
		authoritySha256,
		authorityEntries,
		recoveries,
		retirementMarker,
	};
}

function migrationCheckpoint(statePath, legacy) {
	const checkpoint = {
		schemaVersion: CHECKPOINT_SCHEMA_VERSION,
		epoch: 1,
		epochId: deterministicUuid(`pylon-consumer-v1-migration:${statePath}:${legacy.authoritySha256}:${legacy.tipDigest}`),
		previousCheckpointSha256: GENESIS_DIGEST,
		previousTipSha256: GENESIS_DIGEST,
		historySha256: digest(Buffer.from(
			`pylon-consumer-history:${digest(Buffer.from(statePath))}:v1:${legacy.authoritySha256}:${legacy.tipDigest}`,
		)),
		anchorDigest: legacy.tipDigest,
		anchorBase64: legacy.tipBytes === null ? null : legacy.tipBytes.toString("base64"),
		retiredEpochDirectory: null,
		sourceAuthoritySha256: legacy.authoritySha256,
		sourceAuthorityTipDigest: legacy.tipDigest,
		sourceAuthorityTipBase64: legacy.tipBytes === null ? null : legacy.tipBytes.toString("base64"),
	};
	validateCheckpoint(checkpoint, Number.MAX_SAFE_INTEGER);
	return checkpoint;
}

function sameLegacyAuthority(left, right) {
	return left.authoritySha256 === right.authoritySha256 && left.tipDigest === right.tipDigest &&
		(left.tipBytes === null ? right.tipBytes === null : right.tipBytes !== null && left.tipBytes.equals(right.tipBytes));
}

function legacyOwnerIsDefinitivelyDead(claim, options) {
	try {
		options.processKill(claim.ownerPid, 0);
		return false;
	} catch (error) {
		if (error?.code === "ESRCH") return true;
		return false;
	}
}

function requireRecoverableLegacyOwners(legacy, options) {
	for (const recovery of legacy.recoveries) {
		if (!legacyOwnerIsDefinitivelyDead(recovery.claim, options)) {
			throw new Error(
				"Legacy consumer high-water migration is blocked by a live or uncertain incomplete v1 commit owner.",
			);
		}
	}
}

function recoveredLegacyAuthorityEntries(legacy) {
	const entries = [];
	for (const recovery of legacy.recoveries) {
		if (recovery.kind === "retire") {
			const terminal = {
				schemaVersion: LEGACY_LOCK_SCHEMA_VERSION,
				generation: recovery.claim.generation,
				token: recovery.claim.token,
				outcome: "retired",
			};
			entries.push([`lock/${legacyTerminalFileName(recovery.claim)}`, metadataBytes(terminal)]);
			continue;
		}
		for (const transaction of recovery.missingTransactions) {
			entries.push([`transactions/${transaction.baseDigest}.json`, metadataBytes(transaction)]);
		}
		const applied = {
			schemaVersion: LEGACY_LOCK_SCHEMA_VERSION,
			generation: recovery.claim.generation,
			token: recovery.claim.token,
			terminalSha256: digest(metadataBytes(recovery.terminal)),
		};
		entries.push([`lock/${legacyAppliedFileName(recovery.claim)}`, metadataBytes(applied)]);
	}
	return entries;
}

function expectedRecoveredLegacyAuthoritySha256(legacy) {
	return authorityDigest(
		[...legacy.authorityEntries, ...recoveredLegacyAuthorityEntries(legacy)],
		legacy.tipDigest,
		legacy.tipBytes,
	);
}

function legacyAuthorityIsExactRecoveryProgress(previous, current) {
	if (
		previous.tipDigest !== current.tipDigest ||
		(previous.tipBytes === null
			? current.tipBytes !== null
			: current.tipBytes === null || !previous.tipBytes.equals(current.tipBytes))
	) return false;
	const required = new Map(previous.authorityEntries);
	const allowed = new Map(recoveredLegacyAuthorityEntries(previous));
	const actual = new Map(current.authorityEntries);
	if (required.size !== previous.authorityEntries.length || actual.size !== current.authorityEntries.length) return false;
	for (const [name, bytes] of required) {
		if (!actual.get(name)?.equals(bytes)) return false;
	}
	for (const [name, bytes] of actual) {
		if (required.has(name)) continue;
		if (!allowed.get(name)?.equals(bytes)) return false;
	}
	return true;
}
async function publishExactLegacyMetadata(path, value, validate, description, directory, context, writer, kind, options) {
	await publishImmutable({
		path,
		bytes: metadataBytes(value),
		directory,
		kind,
		context,
		writer,
		options,
		revalidate: false,
	});
	const actual = await readExactMetadata(path, options.metadataMaxBytes, validate, description, options);
	if (!metadataBytes(actual).equals(metadataBytes(value))) {
		throw new Error(`${description} lost its immutable exact-value publication.`);
	}
}

async function helpLegacyAuthority(retiredLockDirectory, transactionDirectory, legacy, context, options) {
	for (const recovery of legacy.recoveries) {
		await secureDirectory(retiredLockDirectory, "Legacy consumer high-water lock directory", options);
		if (await lstatOrNull(join(retiredLockDirectory, LEGACY_RETIREMENT_MARKER_NAME), options) !== null) {
			throw new Error("Legacy consumer authority recovery cannot cross its immutable retirement marker.");
		}
		if (recovery.kind === "retire") {
			const terminal = {
				schemaVersion: LEGACY_LOCK_SCHEMA_VERSION,
				generation: recovery.claim.generation,
				token: recovery.claim.token,
				outcome: "retired",
			};
			await publishExactLegacyMetadata(
				join(retiredLockDirectory, legacyTerminalFileName(recovery.claim)),
				terminal,
				(value) => validateLegacyTerminal(value, recovery.claim, options.stateMaxBytes),
				"Legacy consumer high-water recovered terminal marker",
				retiredLockDirectory,
				context,
				recovery.claim,
				"terminal-retired",
				options,
			);
			continue;
		}
		for (const transaction of recovery.missingTransactions) {
			await publishExactLegacyMetadata(
				join(transactionDirectory, `${transaction.baseDigest}.json`),
				transaction,
				(value) => validateTransaction(value, transaction.baseDigest, options.stateMaxBytes).value,
				"Legacy consumer high-water recovered transition",
				transactionDirectory,
				context,
				recovery.claim,
				"transition",
				options,
			);
		}
		const applied = {
			schemaVersion: LEGACY_LOCK_SCHEMA_VERSION,
			generation: recovery.claim.generation,
			token: recovery.claim.token,
			terminalSha256: digest(metadataBytes(recovery.terminal)),
		};
		await publishExactLegacyMetadata(
			join(retiredLockDirectory, legacyAppliedFileName(recovery.claim)),
			applied,
			(value) => validateLegacyApplied(value, recovery.claim, recovery.terminal),
			"Legacy consumer high-water recovered applied marker",
			retiredLockDirectory,
			context,
			recovery.claim,
			"applied",
			options,
		);
	}
	await options.syncDirectory(retiredLockDirectory);
	await options.syncDirectory(transactionDirectory);
}

async function legacyMigrationSource(statePath, options) {
	const guardPath = `${statePath}.lock`;
	const retiredLockDirectory = `${statePath}.lock.v1-retired`;
	const guardEntry = await lstatOrNull(guardPath, options);
	const retiredEntry = await lstatOrNull(retiredLockDirectory, options);
	if (retiredEntry && (!retiredEntry.isDirectory() || retiredEntry.isSymbolicLink?.())) {
		throw new Error("Prior retired v1 consumer lock authority must be one real directory and is never replaced.");
	}
	if (guardEntry?.isDirectory() && !guardEntry.isSymbolicLink?.() && retiredEntry) {
		throw new Error("Live and retired v1 consumer lock authority both exist; migration fails closed.");
	}
	if (retiredEntry) {
		if (guardEntry !== null && (!guardEntry.isFile() || guardEntry.isSymbolicLink?.())) {
			throw new Error("Prior retired v1 consumer authority has an unsafe or ambiguous live lock path.");
		}
		return { guardPath, sourceLockDirectory: retiredLockDirectory, layout: "prior-retired", guardEntry };
	}
	if (guardEntry?.isDirectory() && !guardEntry.isSymbolicLink?.()) {
		return { guardPath, sourceLockDirectory: guardPath, layout: "in-place", guardEntry };
	}
	throw new Error("Prior v1 consumer lock authority is absent, unsafe, or ambiguous.");
}

async function publishLegacyRetirementMarker(source, legacy, context, options) {
	if (legacy.retirementMarker !== null) return legacy;
	if (legacy.recoveries.length !== 0) {
		throw new Error("Legacy consumer high-water authority must be quiescent before retirement marker publication.");
	}
	const markerPath = join(source.sourceLockDirectory, LEGACY_RETIREMENT_MARKER_NAME);
	const marker = legacyRetirementMarkerFor(context.statePath, legacy);
	const authenticateBeforeMarkerLink = async () => {
		const currentSource = await legacyMigrationSource(context.statePath, options);
		if (currentSource.layout !== "in-place" || currentSource.sourceLockDirectory !== source.sourceLockDirectory) {
			throw new Error("Legacy consumer high-water source changed before retirement marker publication.");
		}
		const current = await readLegacyAuthority(
			context.statePath,
			source.sourceLockDirectory,
			`${context.statePath}.transactions`,
			options,
		);
		if (current.retirementMarker !== null) {
			if (sameLegacyAuthority(legacy, current)) {
				throw Object.assign(new Error("Concurrent migration already published the exact retirement marker."), {
					code: "PYLON_EXACT_RETIREMENT_JOIN",
				});
			}
			throw new Error("Legacy consumer high-water authority changed before retirement marker publication.");
		}
		if (!sameLegacyAuthority(legacy, current) || current.recoveries.length !== 0) {
			throw new Error("Legacy consumer high-water authority changed before retirement marker publication.");
		}
		requireRecoverableLegacyOwners(current, options);
	};
	try {
		await publishImmutable({
			path: markerPath,
			bytes: metadataBytes(marker),
			directory: source.sourceLockDirectory,
			kind: "legacy-retirement",
			context,
			writer: { generation: 0, token: context.checkpoint.epochId, type: "rotation" },
			options,
			revalidate: false,
			beforeLink: authenticateBeforeMarkerLink,
		});
	} catch (error) {
		if (error?.code !== "PYLON_EXACT_RETIREMENT_JOIN") throw error;
		const joined = await readLegacyAuthority(
			context.statePath,
			source.sourceLockDirectory,
			`${context.statePath}.transactions`,
			options,
		);
		if (joined.retirementMarker === null || !sameLegacyAuthority(legacy, joined)) throw error;
	}
	await options.syncDirectory(source.sourceLockDirectory);
	await options.syncDirectory(dirname(source.sourceLockDirectory));
	const guarded = await readLegacyAuthority(
		context.statePath,
		source.sourceLockDirectory,
		`${context.statePath}.transactions`,
		options,
	);
	if (guarded.retirementMarker === null || !sameLegacyAuthority(legacy, guarded)) {
		throw new Error("Legacy consumer high-water retirement marker does not authenticate its exact pre-marker authority.");
	}
	await options.hooks?.afterMigrationRetirementMarker?.({ markerPath, marker: structuredClone(marker) });
	await options.hooks?.afterMigrationGuard?.({ guardPath: source.guardPath, markerPath });
	return guarded;
}

async function publishPriorLayoutGuard(source, legacy, context, options) {
	if (source.guardEntry === null) {
		await publishImmutable({
			path: source.guardPath,
			bytes: metadataBytes(legacyGuardFor(context.statePath)),
			directory: dirname(source.guardPath),
			kind: "legacy-guard",
			context,
			writer: { generation: 0, token: context.checkpoint.epochId, type: "rotation" },
			options,
			revalidate: false,
			beforeLink: async () => {
				const current = await readLegacyAuthority(
					context.statePath,
					source.sourceLockDirectory,
					`${context.statePath}.transactions`,
					options,
				);
				if (!sameLegacyAuthority(legacy, current) || current.recoveries.length !== 0) {
					throw new Error("Prior retired v1 authority changed before downgrade guard publication.");
				}
			},
		});
	}
	if (await inspectLegacyGuard(context, options) !== "guard") {
		throw new Error("Prior retired v1 authority lacks its exact permanent downgrade guard.");
	}
	await options.syncDirectory(dirname(source.guardPath));
	await options.hooks?.afterMigrationGuard?.({ guardPath: source.guardPath });
}

async function validateMigratedAuthority(context, options) {
	if (context.checkpoint.epoch < 1 || context.checkpoint.sourceAuthoritySha256 === GENESIS_DIGEST) {
		throw new Error("Prior v1 consumer authority exists but the v2 journal lacks an authenticated migration checkpoint.");
	}
	const source = await legacyMigrationSource(context.statePath, options);
	const sourceTipBytes = context.checkpoint.sourceAuthorityTipBase64 === null
		? null
		: Buffer.from(context.checkpoint.sourceAuthorityTipBase64, "base64");
	const legacy = await readLegacyAuthority(
		context.statePath,
		source.sourceLockDirectory,
		`${context.statePath}.transactions`,
		options,
	);
	if (
		legacy.authoritySha256 !== context.checkpoint.sourceAuthoritySha256 ||
		legacy.tipDigest !== context.checkpoint.sourceAuthorityTipDigest || legacy.recoveries.length !== 0 ||
		(sourceTipBytes === null ? legacy.tipBytes !== null : !sourceTipBytes.equals(legacy.tipBytes))
	) throw new Error("The v2 migration checkpoint does not authenticate the complete prior v1 authority and tip.");
	const guardKind = await inspectLegacyGuard(context, options);
	if (
		(source.layout === "in-place" && (guardKind !== "retirement-marker" || legacy.retirementMarker === null)) ||
		(source.layout === "prior-retired" && guardKind !== "guard")
	) throw new Error("Prior v1 consumer authority is not fenced by its exact permanent downgrade guard.");
	return { source, legacy };
}

export async function migrateConsumerStateJournal(statePath, rawOptions = {}) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local state path is required for v1 journal migration.");
	const options = normalizeOptions(rawOptions);
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	await ensureDurableConsumerStateDirectory(directory, options.directoryOperations);
	await secureDirectory(directory, "Consumer high-water state directory", options);
	const transactionDirectory = `${absoluteStatePath}.transactions`;
	const transactionEntry = await lstatOrNull(transactionDirectory, options);
	if (!transactionEntry) throw new Error("No prior v1 consumer transaction authority exists to migrate.");
	if (!transactionEntry.isDirectory() || transactionEntry.isSymbolicLink?.()) {
		throw new Error("Prior v1 consumer transaction authority must be one real directory.");
	}
	let source = await legacyMigrationSource(absoluteStatePath, options);
	let legacy = await readLegacyAuthority(absoluteStatePath, source.sourceLockDirectory, transactionDirectory, options);
	const initialCheckpoint = migrationCheckpoint(absoluteStatePath, legacy);
	await options.hooks?.afterMigrationAuthorityRead?.({
		checkpoint: structuredClone(initialCheckpoint),
		legacy: structuredClone(legacy),
	});

	const journalDirectory = `${absoluteStatePath}.journal`;
	await ensureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	const temporaryDirectory = join(journalDirectory, TEMPORARY_DIRECTORY_NAME);
	await ensureDirectory(temporaryDirectory, "Consumer high-water temporary directory", options);

	source = await legacyMigrationSource(absoluteStatePath, options);
	const currentLegacy = await readLegacyAuthority(absoluteStatePath, source.sourceLockDirectory, transactionDirectory, options);
	if (!sameLegacyAuthority(legacy, currentLegacy) && !legacyAuthorityIsExactRecoveryProgress(legacy, currentLegacy)) {
		throw new Error("Concurrent v1 migration changed the exact authenticated legacy authority or tip.");
	}
	legacy = currentLegacy;
	if (source.layout === "in-place" && legacy.retirementMarker === null) {
		requireRecoverableLegacyOwners(legacy, options);
		const expectedRecoveredAuthoritySha256 = expectedRecoveredLegacyAuthoritySha256(legacy);
		const recoveryCheckpoint = migrationCheckpoint(absoluteStatePath, legacy);
		try {
			await helpLegacyAuthority(source.sourceLockDirectory, transactionDirectory, legacy, {
				statePath: absoluteStatePath,
				guardPath: source.guardPath,
				journalDirectory,
				checkpoint: recoveryCheckpoint,
				checkpointPath: join(journalDirectory, checkpointName(recoveryCheckpoint)),
				checkpointDigest: digest(metadataBytes(recoveryCheckpoint)),
				epochDirectory: join(journalDirectory, epochName(recoveryCheckpoint)),
				temporaryDirectory,
			}, options);
		} catch (error) {
			const joined = await readLegacyAuthority(absoluteStatePath, source.sourceLockDirectory, transactionDirectory, options);
			if (joined.retirementMarker === null) throw error;
			legacy = joined;
		}
		if (legacy.retirementMarker === null) {
			const recovered = await readLegacyAuthority(absoluteStatePath, source.sourceLockDirectory, transactionDirectory, options);
			if (
				recovered.recoveries.length !== 0 || recovered.authoritySha256 !== expectedRecoveredAuthoritySha256 ||
				recovered.tipDigest !== legacy.tipDigest ||
				(legacy.tipBytes === null ? recovered.tipBytes !== null : recovered.tipBytes === null || !legacy.tipBytes.equals(recovered.tipBytes))
			) throw new Error("V1 authority recovery did not produce only the exact authenticated dead-owner completion.");
			legacy = recovered;
		}
	}
	if (source.layout === "prior-retired" && legacy.recoveries.length !== 0) {
		throw new Error("Interrupted prior-layout v1 migration authority is supported read-only and still requires recovery.");
	}
	if (legacy.recoveries.length !== 0) {
		throw new Error("Legacy consumer high-water authority is not quiescent after recovery.");
	}

	let checkpoint = migrationCheckpoint(absoluteStatePath, legacy);
	let bootstrapContext = {
		statePath: absoluteStatePath,
		guardPath: source.guardPath,
		journalDirectory,
		checkpoint,
		checkpointPath: join(journalDirectory, checkpointName(checkpoint)),
		checkpointDigest: digest(metadataBytes(checkpoint)),
		epochDirectory: join(journalDirectory, epochName(checkpoint)),
		temporaryDirectory,
	};
	if (source.layout === "in-place") {
		legacy = await publishLegacyRetirementMarker(source, legacy, bootstrapContext, options);
	} else {
		await publishPriorLayoutGuard(source, legacy, bootstrapContext, options);
	}

	const guardedLegacy = await readLegacyAuthority(
		absoluteStatePath,
		source.sourceLockDirectory,
		transactionDirectory,
		options,
	);
	if (!sameLegacyAuthority(legacy, guardedLegacy) || guardedLegacy.recoveries.length !== 0) {
		throw new Error("V1 authority mutated across its exact durable retirement handoff.");
	}
	checkpoint = migrationCheckpoint(absoluteStatePath, guardedLegacy);
	bootstrapContext = {
		...bootstrapContext,
		checkpoint,
		checkpointPath: join(journalDirectory, checkpointName(checkpoint)),
		checkpointDigest: digest(metadataBytes(checkpoint)),
		epochDirectory: join(journalDirectory, epochName(checkpoint)),
	};
	const authenticateBeforeCheckpointLink = async () => {
		const currentSource = await legacyMigrationSource(absoluteStatePath, options);
		if (currentSource.layout !== source.layout || currentSource.sourceLockDirectory !== source.sourceLockDirectory) {
			throw new Error("V1 authority source changed immediately before migration checkpoint publication.");
		}
		const current = await readLegacyAuthority(
			absoluteStatePath,
			source.sourceLockDirectory,
			transactionDirectory,
			options,
		);
		if (!sameLegacyAuthority(guardedLegacy, current) || current.recoveries.length !== 0) {
			throw new Error("V1 authority mutated immediately before migration checkpoint publication.");
		}
	};
	const scan = await initializeJournal(
		absoluteStatePath,
		journalDirectory,
		options,
		checkpoint,
		authenticateBeforeCheckpointLink,
	);
	if (!scan.head || !metadataBytes(scan.head.checkpoint).equals(metadataBytes(checkpoint))) {
		throw new Error("V1 migration encountered a different existing v2 journal checkpoint.");
	}
	const context = contextFromHead(absoluteStatePath, source.guardPath, journalDirectory, scan.head);
	await validateMigratedAuthority(context, options);
	await repairProjection(context, await walkTransactions(context, options), options, {
		generation: 0,
		token: checkpoint.epochId,
		type: "rotation",
	});
	await validateMigratedAuthority(context, options);
	await options.hooks?.afterMigrationComplete?.({ checkpoint: structuredClone(checkpoint) });
	await validateMigratedAuthority(context, options);
	return { epoch: 1, tipSha256: checkpoint.anchorDigest, sourceAuthoritySha256: checkpoint.sourceAuthoritySha256 };
}

async function prepareContext(statePath, options) {
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	await ensureDurableConsumerStateDirectory(directory, options.directoryOperations);
	await secureDirectory(directory, "Consumer high-water state directory", options);
	const guardPath = `${absoluteStatePath}.lock`;
	const retiredLockDirectory = `${absoluteStatePath}.lock.v1-retired`;
	const journalDirectory = `${absoluteStatePath}.journal`;
	const legacyTransactionDirectory = `${absoluteStatePath}.transactions`;

	// Detect every old-authority signal before creating a guard or a genesis journal.
	const legacyEntry = await lstatOrNull(legacyTransactionDirectory, options);
	const guardEntry = await lstatOrNull(guardPath, options);
	const retiredEntry = await lstatOrNull(retiredLockDirectory, options);
	const journalEntry = await lstatOrNull(journalDirectory, options);
	if (legacyEntry && (!legacyEntry.isDirectory() || legacyEntry.isSymbolicLink?.())) {
		throw new Error("Prior v1 consumer transaction authority must be one real directory.");
	}
	if (retiredEntry && (!retiredEntry.isDirectory() || retiredEntry.isSymbolicLink?.())) {
		throw new Error("Prior retired v1 consumer lock authority must be one real directory and is never replaced.");
	}
	if (guardEntry && (
		guardEntry.isSymbolicLink?.() || (!guardEntry.isFile() && !guardEntry.isDirectory())
	)) throw new Error("Legacy consumer lock guard is not one exact regular non-symlink file.");

	let inPlaceMarkerEntry = null;
	if (guardEntry?.isDirectory() && !guardEntry.isSymbolicLink?.()) {
		const markerPath = join(guardPath, LEGACY_RETIREMENT_MARKER_NAME);
		inPlaceMarkerEntry = await lstatOrNull(markerPath, options);
		if (inPlaceMarkerEntry) {
			if (!inPlaceMarkerEntry.isFile() || inPlaceMarkerEntry.isSymbolicLink?.()) {
				throw new Error("Legacy consumer high-water retirement marker must be one real file.");
			}
			await readExactMetadata(
				markerPath,
				options.metadataMaxBytes,
				(value) => validateLegacyRetirementMarker(value, absoluteStatePath),
				"Legacy consumer high-water retirement marker",
				options,
			);
		}
	}

	let scan = null;
	if (journalEntry) {
		if (!journalEntry.isDirectory() || journalEntry.isSymbolicLink?.()) {
			throw new Error("Consumer high-water journal directory must be one real directory.");
		}
		await secureDirectory(journalDirectory, "Consumer high-water journal directory", options);
		try {
			scan = await scanJournalRoot(absoluteStatePath, journalDirectory, options);
		} catch (error) {
			if (error?.message !== "Consumer high-water journal lacks its exact temporary namespace.") throw error;
			const names = await options.readDirectory(journalDirectory);
			if (names.length === 1 && names[0] === TEMPORARY_DIRECTORY_NAME) {
				scan = await scanJournalRoot(absoluteStatePath, journalDirectory, options);
			} else if (names.length !== 0) {
				throw error;
			}
		}
	}
	const hasInPlaceLegacyDirectory = guardEntry?.isDirectory() && !guardEntry.isSymbolicLink?.();
	const hasMigratedV2Head = scan?.head?.checkpoint.sourceAuthoritySha256 !== undefined &&
		scan.head.checkpoint.sourceAuthoritySha256 !== GENESIS_DIGEST;
	const hasLegacySignal = legacyEntry !== null || retiredEntry !== null || hasInPlaceLegacyDirectory || hasMigratedV2Head;
	if (hasLegacySignal) {
		if (!legacyEntry) {
			if (hasInPlaceLegacyDirectory && !inPlaceMarkerEntry && !retiredEntry && !hasMigratedV2Head) {
				throw new Error(
					`Legacy consumer lock directory exists at ${guardPath}. Stop every legacy proper-lockfile client, ` +
					"confirm that no owner remains, remove that directory manually, and retry.",
				);
			}
			throw new Error("Prior v1 consumer authority is incomplete because its transaction namespace is missing.");
		}
		// This independently validates the selected live/in-place or prior-retired lock namespace.
		await legacyMigrationSource(absoluteStatePath, options);
		if (!journalEntry) {
			throw new Error(
				"Prior v1 consumer authority exists. Stop every old client and run the explicit quiescent consumer journal migration command.",
			);
		}
		if (!scan?.head || scan.missingHeadEpoch) {
			throw new Error("Prior v1 authority has no complete authenticated v2 migration checkpoint.");
		}
		const context = contextFromHead(absoluteStatePath, guardPath, journalDirectory, scan.head);
		await validateMigratedAuthority(context, options);
		return { context, scan };
	}

	if (scan?.head) {
		if (scan.missingHeadEpoch) scan = await initializeJournal(absoluteStatePath, journalDirectory, options);
		return { context: contextFromHead(absoluteStatePath, guardPath, journalDirectory, scan.head), scan };
	}
	await ensureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	await ensureDirectory(join(journalDirectory, TEMPORARY_DIRECTORY_NAME), "Consumer high-water temporary directory", options);
	scan = await initializeJournal(absoluteStatePath, journalDirectory, options);
	return { context: contextFromHead(absoluteStatePath, guardPath, journalDirectory, scan.head), scan };
}

async function runNormalLocked(statePath, action, rawOptions) {
	const options = normalizeOptions(rawOptions);
	for (;;) {
		const prepared = await prepareContext(statePath, options);
		const acquired = await acquireNormalOperation(prepared.context, options);
		if (acquired.rotated) continue;
		const { context } = prepared;
		const { claim, temporaries } = acquired;
		options.activeWriter = claim;
		let terminal = null;
		let heartbeatStopped = false;
		const stopHeartbeat = options.startHeartbeat({
			interval: options.update,
			beat: () => refreshHeartbeat(context, claim, options),
		});
		const stopHeartbeatOnce = async () => {
			if (heartbeatStopped) return;
			heartbeatStopped = true;
			await stopHeartbeat();
		};
		const release = async (cause) => {
			if (terminal !== null) return;
			const wanted = {
				schemaVersion: LOCK_SCHEMA_VERSION,
				generation: claim.generation,
				token: claim.token,
				outcome: "released",
			};
			terminal = await publishTerminal(context, claim, wanted, options);
			if (terminal.outcome !== "released") {
				throw new Error("Consumer high-water lock ownership was retired before release.", { cause });
			}
		};
		try {
			const { scan: rootScan, authority: rootAuthority } = await scanAuthenticatedContextRoot(context, options);
			if (rootAuthority.kind !== "current") {
				throw new Error("Consumer high-water operation lost its exact current checkpoint authority.");
			}
			await cleanupAuthority(context, claim, rootScan, temporaries, options, false);
			await ensureLegacyGuard(context, claim, options);
			let chain = await walkTransactions(context, options);
			let legacyBytes = null;
			if (chain.tipBytes === null) {
				const legacy = await readProjection(context, "legacy-state-read", options);
				if (legacy.malformed) throw new Error("Consumer high-water state is malformed.");
				legacyBytes = legacy.bytes;
			} else {
				chain = await repairProjection(context, chain, options);
			}
			const baseBytes = chain.tipBytes ?? legacyBytes;
			const baseDigest = baseBytes === null ? GENESIS_DIGEST : digest(baseBytes);
			let stagedCandidate = null;
			let candidateWasStaged = false;
			const commitTransactions = async (candidateBytes) => {
				const transactions = [];
				if (chain.tipBytes === null && legacyBytes !== null) {
					transactions.push(transactionFor(GENESIS_DIGEST, legacyBytes));
				}
				if (candidateBytes !== null && digest(candidateBytes) !== baseDigest) {
					transactions.push(transactionFor(baseDigest, candidateBytes));
				}
				if (transactions.length === 0) return false;
				if (chain.length + transactions.length > options.maxTransactionDepth) {
					throw new Error("Consumer high-water transaction epoch reached its safe bound; run the consumer journal rotation command.");
				}
				const wanted = {
					schemaVersion: LOCK_SCHEMA_VERSION,
					generation: claim.generation,
					token: claim.token,
					outcome: "commit",
					transactions,
				};
				await options.hooks?.beforeCommitDecision?.({ claim, transactions });
				terminal = await publishTerminal(context, claim, wanted, options);
				if (terminal.outcome !== "commit" || !metadataBytes(terminal).equals(metadataBytes(wanted))) {
					throw new Error("Consumer high-water transaction lost ownership before its commit decision.");
				}
				await options.hooks?.afterCommitDecision?.({ claim, terminal });
				await finishCommit(context, claim, terminal, options);
				return true;
			};
			const transaction = Object.freeze({
				readStateBytes: () => baseBytes === null ? null : Buffer.from(baseBytes),
				commitState: async (value) => {
					if (terminal !== null || candidateWasStaged) {
						throw new Error("Consumer high-water transaction already staged a candidate or has a terminal decision.");
					}
					const bytes = Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(value);
					if (bytes.length < 1 || bytes.length > options.stateMaxBytes) throw new Error("Consumer high-water state is malformed.");
					stagedCandidate = bytes;
					candidateWasStaged = true;
				},
			});
			let result;
			let actionError;
			try {
				result = await action(context.statePath, transaction);
			} catch (error) {
				actionError = error;
			}
			if (actionError === undefined && (candidateWasStaged || legacyBytes !== null)) {
				await commitTransactions(candidateWasStaged ? stagedCandidate : null);
			}
			await stopHeartbeatOnce();
			await release(actionError);
			if (actionError !== undefined) throw actionError;
			return result;
		} catch (error) {
			await stopHeartbeatOnce();
			await release(error);
			throw error;
		}
	}
}

function isExpectedRemovedClaimRead(error, context, options) {
	if (
		!(error instanceof BoundedFileUnlinkedDuringReadError) ||
		error.constructor !== BoundedFileUnlinkedDuringReadError || error.name !== "BoundedFileUnlinkedDuringReadError" ||
		error.description !== "Consumer high-water operation claim" || typeof error.path !== "string" ||
		!Buffer.isBuffer(error.bytes) || error.bytes.length < 1 || error.bytes.length > options.metadataMaxBytes ||
		!isExactUnlinkedDuringReadEvidence(error)
	) return false;
	const name = basename(error.path);
	const match = claimPattern.exec(name);
	if (
		!match || error.expectedSha256 !== match[2] || error.sha256 !== match[2] || digest(error.bytes) !== match[2] ||
		error.path !== join(context.epochDirectory, name) || dirname(error.path) !== context.epochDirectory
	) return false;
	let claim;
	try {
		claim = validateClaim(JSON.parse(error.bytes), context, options.stateMaxBytes);
	} catch {
		return false;
	}
	return claim.generation === Number(match[1]) && metadataBytes(claim).equals(error.bytes) &&
		error.path === claimPath(context, claim);
}

async function completedRotationResult(context, intent, options) {
	const { authority } = await scanAuthenticatedContextRoot(context, options);
	if (
		authority.kind !== "successor" ||
		!metadataBytes(authority.entry.checkpoint).equals(metadataBytes(intent.checkpoint))
	) return null;
	await scanRotationPublicationSet(context, intent.checkpoint, options, true);
	return { epoch: intent.checkpoint.epoch, tipSha256: intent.checkpoint.anchorDigest };
}

async function recoverCompletedCurrentRotation(context, scan, options) {
	if (context.checkpoint.epoch === 1 || scan.claims.length !== 0) return null;
	const tip = await effectiveTip(context, options);
	if (tip.length !== 0 || tip.tipDigest !== context.checkpoint.anchorDigest) return null;
	const writer = { generation: 0, token: context.checkpoint.epochId, type: "rotation" };
	await repairProjection(context, tip, options, writer);
	const { scan: rootScan, authority: rootAuthority } = await scanAuthenticatedContextRoot(context, options);
	if (rootAuthority.kind !== "current") {
		throw new Error("Consumer high-water recovery lost its exact current checkpoint authority.");
	}
	await cleanupAuthority(context, writer, rootScan, scan.temporaries, options, false, scan);
	return { epoch: context.checkpoint.epoch, tipSha256: context.checkpoint.anchorDigest };
}

async function runRotation(statePath, rawOptions) {
	const options = normalizeRotationOptions(rawOptions);
	for (;;) {
		let context;
		let expectedIntent;
		try {
			({ context } = await prepareContext(statePath, options));
			await ensureLegacyGuard(context, { generation: 0, token: context.checkpoint.epochId, type: "rotation" }, options);
			const initialScan = await scanEpoch(context, options);
			const latest = initialScan.claims.at(-1);
			const preparationOptions = latest?.type === "rotation"
				? { ...options, inProgressCheckpoint: validateRotationIntent(latest.intent, context, options.stateMaxBytes).checkpoint }
				: options;
			const completed = await recoverCompletedCurrentRotation(context, initialScan, preparationOptions);
			if (completed) return completed;
			expectedIntent = rotationIntentFor(context, await effectiveTip(context, preparationOptions));
		} catch (error) {
			if (error instanceof ConsumerEpochAdvancedError) continue;
			throw error;
		}
		let frontier;
		try {
			frontier = await resolveOperationFrontier(context, options);
		} catch (error) {
			if (!(error instanceof ConsumerEpochAdvancedError) && !isExpectedRemovedClaimRead(error, context, options)) throw error;
			const completedResult = await completedRotationResult(context, expectedIntent, options);
			if (completedResult) return completedResult;
			throw error;
		}
		if (frontier.rotated) continue;
		if (frontier.active) {
			throw new Error("Consumer high-water state is actively locked; rotation will retry after the claim quiesces.");
		}
		if (frontier.retry) continue;
		const nextGeneration = (frontier.scan.claims.at(-1)?.generation ?? 0) + 1;
		if (nextGeneration > MAX_OPERATION_GENERATIONS) {
			throw new Error("Consumer high-water operation epoch is exhausted and cannot publish its cap-exempt rotation slot.");
		}
		const tip = await effectiveTip(context, options);
		const wanted = rotationClaimFor(context, nextGeneration, tip);
		const confirmation = await scanEpoch(context, options);
		if (!sameOperationClaim(confirmation.claims.at(-1) ?? null, frontier.frontier)) continue;
		const confirmedTip = await effectiveTip(context, options);
		const confirmed = rotationClaimFor(context, nextGeneration, confirmedTip);
		if (!metadataBytes(confirmed).equals(metadataBytes(wanted))) continue;
		let claim;
		try {
			claim = await tryCreateRotationClaim(context, nextGeneration, confirmedTip, options);
		} catch (error) {
			const completedResult = await completedRotationResult(context, confirmed.intent, options).catch(() => null);
			if (completedResult) return completedResult;
			throw error;
		}
		if (!claim) continue;
		try {
			await helpRotationOperation(context, claim, options);
		} catch (error) {
			const completedResult = await completedRotationResult(context, claim.intent, options).catch(() => null);
			if (completedResult) return completedResult;
			throw error;
		}
		await scanRotationPublicationSet(context, claim.intent.checkpoint, options, true);
		return { epoch: claim.intent.checkpoint.epoch, tipSha256: claim.intent.checkpoint.anchorDigest };
	}
}

export async function withConsumerStateLock(statePath, action, rawOptions = {}) {
	if (typeof action !== "function") throw new Error("Consumer high-water lock action must be a function.");
	return runNormalLocked(statePath, action, rawOptions);
}

export async function rotateConsumerStateJournal(statePath, rawOptions = {}) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local state path is required for journal rotation.");
	return runRotation(statePath, rawOptions);
}

// V3 primitives remain separate from the public v2 preparation/rotation entrypoints.
const generationBuilders = new WeakMap();
const generationHeartbeatPattern = new RegExp(`^heartbeat-([0-9]{16})-(${uuidSource})-([0-9]{16})\\.json$`);
const generationReceiptPattern = /^receipt-([0-9a-f]{64})\.json$/;
const generationReceiptTemporaryPattern = new RegExp(`^\\.receipt-p([1-9][0-9]*)-w(${uuidSource})-t([0-9a-f]{64})\\.tmp$`);
function generationOptions(raw = {}) {
	const options = {
		stateMaxBytes: GENERATION_STATE_MAX_BYTES,
		maxJournalBytes: GENERATION_JOURNAL_MAX_BYTES,
		currentUid: process.getuid(), lstatEntry: lstat, openFile: open, readDirectory: readdir,
		makeDirectory: mkdir, linkFile: link, renameFile: rename, removeFile: rm, now: Date.now,
		processKill: process.kill.bind(process), stale: PYLON_CONSUMER_LOCK_STALE_MS, ...raw,
	};
	options.metadataMaxBytes = generationRecordMaxBytes(options.stateMaxBytes);
	if (!Number.isSafeInteger(options.maxJournalBytes) || options.maxJournalBytes < 1 || options.maxJournalBytes > GENERATION_JOURNAL_MAX_BYTES ||
		!Number.isSafeInteger(options.currentUid) || options.currentUid < 0) throw new Error("Generation byte bound or uid is invalid.");
	return options;
}
function generationCanonical(bytes, maxBytes) {
	if (!Buffer.isBuffer(bytes) || bytes.length < 1 || bytes.length > maxBytes) throw new Error("Generation metadata exceeds its byte bound.");
	const value = JSON.parse(bytes);
	if (!metadataBytes(value).equals(bytes)) throw new Error("Generation metadata is not canonical.");
	return value;
}
function validateGenerationTransaction(value, expectedBaseDigest, stateMaxBytes) {
	if (!exactKeys(value, ["schemaVersion", "baseDigest", "candidateDigest", "candidateBase64"]) || value.schemaVersion !== 1 ||
		value.baseDigest !== expectedBaseDigest || !/^[0-9a-f]{64}$/.test(value.candidateDigest ?? "") ||
		typeof value.candidateBase64 !== "string" || value.candidateBase64.length > 4 * Math.ceil(stateMaxBytes / 3)) throw new Error("Generation transaction is malformed or exceeds its byte bound.");
	const candidateBytes = Buffer.from(value.candidateBase64, "base64");
	if (candidateBytes.length < 1 || candidateBytes.length > stateMaxBytes || candidateBytes.toString("base64") !== value.candidateBase64 ||
		digest(candidateBytes) !== value.candidateDigest || value.candidateDigest === value.baseDigest) throw new Error("Generation transaction payload is malformed.");
	return { value, candidateBytes };
}
function generationEpochAuthority(snapshot, options) {
	const checkpoint = validateGenerationCheckpoint(snapshot.checkpoint, options.stateMaxBytes).checkpoint;
	if (!Buffer.isBuffer(snapshot.checkpointBytes) || !metadataBytes(checkpoint).equals(snapshot.checkpointBytes) || snapshot.name !== consumerGenerationName(checkpoint)) throw new Error("Generation predecessor checkpoint authority is not exact.");
	const records = snapshot.epochRecords;
	if (!(records instanceof Map) || records.size > GENERATION_EPOCH_MAX_ENTRIES) throw new Error("Generation epoch entry bound is invalid.");
	let totalBytes = snapshot.checkpointBytes.length * 2;
	for (const bytes of records.values()) {
		if (!Buffer.isBuffer(bytes) || bytes.length < 1 || bytes.length > options.metadataMaxBytes) throw new Error("Generation epoch metadata byte bound is invalid.");
		totalBytes += bytes.length * 2;
		if (totalBytes > options.maxJournalBytes) throw new Error("Generation epoch exceeds its byte bound.");
	}
	const contents = new Map();
	const indexes = new Map();
	const heartbeats = new Map();
	const heartbeatRecords = [];
	const terminals = new Map();
	const applied = new Map();
	const transitions = new Map();
	for (const [name, bytes] of records) {
		const value = generationCanonical(bytes, options.metadataMaxBytes);
		let match;
		if ((match = claimPattern.exec(name))) {
			if (value.generation !== Number(match[1]) || generationName(value.generation) !== match[1] || digest(bytes) !== match[2]) throw new Error("Generation claim name is not exact.");
			if (value.type === "rotation") {
				const successor = validateGenerationCheckpoint(value.intent?.checkpoint, options.stateMaxBytes);
				const expected = consumerGenerationRotationClaim(checkpoint, value.generation, {
					tipDigest: successor.checkpoint.anchorDigest, tipBytes: successor.anchorBytes, previousGenerationIdentity: successor.checkpoint.previousGenerationIdentity, retirementAuthoritySha256: successor.checkpoint.retirementAuthoritySha256,
				}, options.stateMaxBytes);
				if (!bytes.equals(metadataBytes(expected))) throw new Error("Generation rotation intent is not exact.");
			} else validateClaim(value, null, options.stateMaxBytes);
			contents.set(match[2], value);
		} else if ((match = claimIndexPattern.exec(name))) {
			validateClaimIndex(value, Number(match[1]));
			if (generationName(value.generation) !== match[1]) throw new Error("Generation claim CAS name is malformed.");
			indexes.set(value.generation, value);
		} else if ((match = heartbeatPattern.exec(name))) {
			heartbeatRecords.push([`${Number(match[1])}:${match[2]}`, value]);
			const key = `${Number(match[1])}:${match[2]}`;
			if (!heartbeats.has(key) || heartbeats.get(key).refreshedAtMs < value.refreshedAtMs) heartbeats.set(key, value);
		} else if ((match = generationHeartbeatPattern.exec(name))) {
			heartbeatRecords.push([`${Number(match[1])}:${match[2]}`, value]);
			if (value.refreshedAtMs !== Number(match[3]) || value.generation !== Number(match[1]) || value.token !== match[2]) throw new Error("Generation immutable heartbeat name is not exact.");
			const key = `${Number(match[1])}:${match[2]}`;
			if (!heartbeats.has(key) || heartbeats.get(key).refreshedAtMs < value.refreshedAtMs) heartbeats.set(key, value);
		}
		else if ((match = terminalPattern.exec(name))) terminals.set(`${Number(match[1])}:${match[2]}`, value);
		else if ((match = appliedPattern.exec(name))) applied.set(`${Number(match[1])}:${match[2]}`, value);
		else if ((match = transitionPattern.exec(name))) {
			validateGenerationTransaction(value, match[1], options.stateMaxBytes);
			transitions.set(match[1], value);
		} else throw new Error("Generation epoch contains an unexpected entry.");
	}
	const claims = [];
	const byKey = new Map();
	for (const [slot, index] of [...indexes].sort(([a], [b]) => a - b)) {
		const claim = contents.get(index.claimSha256);
		if (!claim || claim.generation !== slot || slot !== claims.length + 1) throw new Error("Generation rotation authority has a missing claim CAS or noncontiguous slot.");
		claims.push(claim);
		byKey.set(`${claim.generation}:${claim.token}`, claim);
	}
	for (const claim of contents.values()) {
		if (claim.generation > claims.length + 1) throw new Error("Generation epoch contains a future unindexed claim.");
	}
	for (const [key, value] of heartbeatRecords) {
		const claim = byKey.get(key);
		if (!claim) throw new Error("Generation epoch contains an orphan heartbeat.");
		validateHeartbeat(value, claim);
	}
	for (const [key, value] of terminals) {
		const claim = byKey.get(key);
		if (!claim) throw new Error("Generation epoch contains an orphan terminal.");
		validateTerminal(value, claim, options.stateMaxBytes, validateGenerationTransaction);
	}
	for (const [key, value] of applied) {
		const claim = byKey.get(key);
		if (!claim) throw new Error("Generation epoch contains an orphan applied marker.");
		validateApplied(value, claim, terminals.get(key));
	}
	let tipDigest = checkpoint.anchorDigest;
	let tipBytes = validateGenerationCheckpoint(checkpoint, options.stateMaxBytes).anchorBytes;
	const decided = new Map();
	let depth = 0;
	for (const claim of claims) {
		const key = `${claim.generation}:${claim.token}`;
		const terminal = terminals.get(key);
		if (claim !== claims.at(-1) && (claim.type === "rotation" || !terminal || (terminal.outcome === "commit" && !applied.has(key)))) throw new Error("Generation epoch crossed an unresolved earlier slot.");
		if (terminal?.outcome !== "commit") continue;
		for (const transaction of terminal.transactions) {
			if (transaction.baseDigest !== tipDigest || decided.has(tipDigest) || ++depth > MAX_TRANSACTION_DEPTH) throw new Error("Generation commit decisions do not form one bounded exact chain.");
			decided.set(tipDigest, transaction);
			tipDigest = transaction.candidateDigest;
		}
	}
	let actualDigest = checkpoint.anchorDigest;
	const visited = new Set();
	while (transitions.has(actualDigest)) {
		const actual = transitions.get(actualDigest);
		if (visited.has(actualDigest) || !decided.has(actualDigest) || !metadataBytes(actual).equals(metadataBytes(decided.get(actualDigest)))) throw new Error("Generation transition lacks its exact commit decision.");
		visited.add(actualDigest);
		tipBytes = validateGenerationTransaction(actual, actualDigest, options.stateMaxBytes).candidateBytes;
		actualDigest = actual.candidateDigest;
	}
	if (visited.size !== transitions.size) throw new Error("Generation epoch has an unreachable transition.");
	for (const key of applied.keys()) {
		for (const transaction of terminals.get(key).transactions) {
			if (!transitions.has(transaction.baseDigest) || !metadataBytes(transitions.get(transaction.baseDigest)).equals(metadataBytes(transaction))) throw new Error("Generation applied marker lacks its exact transition.");
		}
	}
	return { claims, contents, indexes, heartbeats, terminals, applied, transitions, tip: { tipDigest: actualDigest, tipBytes, length: visited.size }, decidedTipDigest: tipDigest };
}

// The caller must read/revalidate the predecessor from its pinned directory immediately
// before a handoff. Snapshots are evidence inputs, never a permission to reuse stale authority.
export function assertConsumerGenerationSuccessor(predecessor, name, bytes, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	const scan = generationEpochAuthority(predecessor, options);
	const latest = scan.claims.at(-1);
	if (latest?.type !== "rotation" || scan.decidedTipDigest !== scan.tip.tipDigest) throw new Error("Generation successor lacks exact latest rotation authority.");
	const expected = consumerGenerationRotationClaim(predecessor.checkpoint, latest.generation, { ...scan.tip, previousGenerationIdentity: latest.intent.checkpoint.previousGenerationIdentity, retirementAuthoritySha256: latest.intent.checkpoint.retirementAuthoritySha256 }, options.stateMaxBytes);
	if (expected.intent.checkpoint.retirementAuthoritySha256 !== GENESIS_DIGEST) validateGenerationRetirementCertificate(predecessor, expected.intent.checkpoint, options);
	if (!metadataBytes(latest).equals(metadataBytes(expected)) || name !== consumerGenerationName(expected.intent.checkpoint) || !Buffer.isBuffer(bytes) || !bytes.equals(metadataBytes(expected.intent.checkpoint))) throw new Error("Generation successor differs from its exact latest rotation authority and immutable tip.");
	return expected.intent.checkpoint;
}
function expectedConsumerGeneration(authority, options) {
	if (exactKeys(authority, ["genesis"])) return consumerGenerationGenesisCheckpoint(authority.genesis, options.stateMaxBytes);
	if (!exactKeys(authority, ["predecessor"])) throw new Error("Generation construction requires exact genesis or predecessor authority.");
	const scan = generationEpochAuthority(authority.predecessor, options);
	const candidate = scan.claims.at(-1)?.intent?.checkpoint;
	if (!candidate) throw new Error("Generation successor lacks latest rotation authority.");
	return assertConsumerGenerationSuccessor(authority.predecessor, consumerGenerationName(candidate), metadataBytes(candidate), options);
}
function generationSameInode(a, b) { return a.dev === b.dev && a.ino === b.ino; }
function generationEntryStat(stat, type, options) {
	if (stat.isSymbolicLink?.() || (type === "directory" ? !stat.isDirectory() : !stat.isFile()) || stat.uid !== options.currentUid || (stat.mode & 0o7777) !== (type === "directory" ? 0o700 : 0o600)) throw new Error("Generation entry has unsafe type, owner or exact permissions.");
	return stat;
}
async function generationDirectory(path, options, sync = false) {
	const before = generationEntryStat(await options.lstatEntry(path), "directory", options);
	await options.hooks?.afterInitialPathStat?.({ path, stat: before });
	const handle = await options.openFile(path, constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0));
	try {
		const opened = generationEntryStat(await handle.stat(), "directory", options);
		if (!generationSameInode(before, opened)) throw new Error("Generation directory identity changed.");
		if (sync) await handle.sync();
		const final = generationEntryStat(await options.lstatEntry(path), "directory", options);
		if (!generationSameInode(opened, final)) throw new Error("Generation directory identity changed.");
		return Object.freeze({ dev: opened.dev, ino: opened.ino });
	} finally { await handle.close(); }
}
async function generationBoundary(options, phase, operation, path) {
	await options.hooks?.generationBoundary?.({ phase, operation, path });
}
async function generationSync(path, options) {
	await generationBoundary(options, "before", "sync", path);
	await generationDirectory(path, options, true);
	await generationBoundary(options, "after", "sync", path);
}
async function generationNames(path, limit, options) {
	const names = await options.readDirectory(path);
	if (!Array.isArray(names) || names.length > limit || new Set(names).size !== names.length || names.some((name) => typeof name !== "string" || basename(name) !== name || [".", ".."].includes(name))) throw new Error("Generation directory exceeds its entry bound or closed namespace.");
	return names;
}
class GenerationDiscoveryLost extends Error {}
const generationDiscovery = Symbol("generation discovery");
async function generationRootPreflight(root, options) {
	const rootNames = await generationNames(root, GENERATION_ROOT_MAX_ENTRIES, options);
	const finalPattern = /^generation-[0-9]{16}-[0-9a-f]{64}$/;
	const hiddenPattern = new RegExp(`^\\.building-(?:p[1-9][0-9]*-)?${uuidSource}$`);
	const retiredPattern = /^\.((retired)|(deleting))-generation-[0-9]{16}-[0-9a-f]{64}$/;
	if (rootNames.filter((name) => finalPattern.test(name)).length > 2 || rootNames.some((name) => !finalPattern.test(name) && !hiddenPattern.test(name) && !retiredPattern.test(name))) throw new Error("Generation root contains an unexpected entry or competing finals.");
	let totalBytes = 0;
	const charge = async (path) => {
		const stat = generationEntryStat(await options.lstatEntry(path), "file", options);
		if (stat.size < (generationReceiptTemporaryPattern.test(basename(path)) ? 0 : 1) || stat.size > options.metadataMaxBytes) throw new Error("Generation root metadata exceeds its byte bound.");
		totalBytes += stat.size;
		if (totalBytes > options.maxJournalBytes) throw new Error("Generation root exceeds its aggregate byte bound.");
	};
	for (const name of rootNames) {
		const path = join(root, name);
		try {
			generationEntryStat(await options.lstatEntry(path), "directory", options);
		} catch (error) {
			if (options[generationDiscovery] && options.lstatEntry === lstat && error?.code === "ENOENT") throw new GenerationDiscoveryLost();
			throw error;
		}
		const entries = await generationNames(path, 4, options);
		if (entries.some((entry) => !["checkpoint.json", "retirement.json", "epoch", "receipts"].includes(entry))) throw new Error("Generation root contains an unexpected nested entry.");
		for (const entry of entries) {
			if (["checkpoint.json", "retirement.json"].includes(entry)) { await charge(join(path, entry)); continue; }
			const directory = join(path, entry);
			generationEntryStat(await options.lstatEntry(directory), "directory", options);
			const names = await generationNames(directory, entry === "epoch" ? GENERATION_EPOCH_MAX_ENTRIES : GENERATION_RECEIPT_MAX_ENTRIES, options);
			for (const child of names) await charge(join(directory, child));
		}
	}
	return totalBytes;
}
async function readGenerationSnapshot(path, checkpoint, options, expectedIdentity = null, requireEmpty = false) {
	const root = dirname(path);
	await generationRootPreflight(root, options);
	const names = await generationNames(path, 4, options);
	if (!["checkpoint.json,epoch,receipts", "checkpoint.json,epoch,receipts,retirement.json"].includes(names.slice().sort().join())) throw new Error("Generation has an incomplete or unexpected closed namespace.");
	const epochNames = await generationNames(join(path, "epoch"), GENERATION_EPOCH_MAX_ENTRIES, options);
	const receiptNames = await generationNames(join(path, "receipts"), GENERATION_RECEIPT_MAX_ENTRIES, options);
	if (requireEmpty && (epochNames.length !== 0 || receiptNames.length !== 1)) throw new Error("Generation publication requires an exactly empty epoch and checkpoint-only receipts.");
	// Stat and charge every name, including both hardlinks, before opening nested metadata.
	const canonical = new Map();
	const byInode = new Map();
	const receiptEntries = [];
	let totalBytes = 0;
	for (const name of ["checkpoint.json", ...(names.includes("retirement.json") ? ["retirement.json"] : []), ...epochNames.map((name) => `epoch/${name}`)]) {
		const stat = generationEntryStat(await options.lstatEntry(join(path, name)), "file", options);
		if (stat.size < 1 || stat.size > options.metadataMaxBytes) throw new Error("Generation metadata exceeds its byte bound.");
		totalBytes += stat.size;
		if (totalBytes > options.maxJournalBytes) throw new Error("Generation exceeds its aggregate byte bound.");
		const key = `${stat.dev}:${stat.ino}`;
		if (byInode.has(key)) throw new Error("Generation canonical entries alias the same inode.");
		const entry = { name, stat, receipts: [] };
		canonical.set(name, entry); byInode.set(key, entry);
	}
	for (const name of receiptNames) {
		const fixed = generationReceiptPattern.exec(name);
		const temporary = generationReceiptTemporaryPattern.exec(name);
		if (!fixed && !temporary) throw new Error("Generation receipt name is malformed.");
		const stat = generationEntryStat(await options.lstatEntry(join(path, "receipts", name)), "file", options);
		if (stat.size < (temporary ? 0 : 1) || stat.size > options.metadataMaxBytes) throw new Error("Generation receipt exceeds its byte bound.");
		totalBytes += stat.size;
		if (totalBytes > options.maxJournalBytes) throw new Error("Generation exceeds its aggregate byte bound.");
		const target = byInode.get(`${stat.dev}:${stat.ino}`);
		if (!target && temporary && stat.nlink === 1) {
			receiptEntries.push({ name, stat, target: null, temporary: true, pid: Number(temporary[1]) });
			continue;
		}
		if (!target || (fixed?.[1] ?? temporary?.[3]) !== digest(Buffer.from(target.name)) || stat.size !== target.stat.size) throw new Error("Generation receipt lacks its exact canonical inode and target.");
		target.receipts.push({ name, stat, temporary: !!temporary });
		receiptEntries.push({ name, stat, target: target.name, temporary: !!temporary, pid: temporary ? Number(temporary[1]) : null });
	}
	for (const entry of canonical.values()) {
		if (entry.receipts.length !== 1 || entry.stat.nlink !== 2 || entry.receipts[0].stat.nlink !== 2 || (requireEmpty && entry.receipts[0].temporary)) throw new Error("Generation canonical metadata lacks its exact durable receipt inode.");
	}
	await generationDirectory(root, options);
	const identity = await generationDirectory(path, options);
	if (expectedIdentity !== null && !generationSameInode(identity, expectedIdentity)) throw new Error("Generation directory inode differs from the observed builder identity.");
	const epochIdentity = await generationDirectory(join(path, "epoch"), options);
	const receiptsIdentity = await generationDirectory(join(path, "receipts"), options);
	const epochRecords = new Map();
	let checkpointBytes;
	let retirementCertificate = null;
	for (const entry of canonical.values()) {
		const bytes = await readBoundedRegularFile(join(path, entry.name), {
			maxBytes: options.metadataMaxBytes, openFile: options.openFile, lstatEntry: options.lstatEntry,
			hooks: { ...options.hooks?.metadataRead, afterInitialPathStat: async (observation) => {
				if (!sameRetiredLinkStat(observation.stat, entry.stat)) throw new Error("Generation metadata changed after allocation preflight.");
				await options.hooks?.metadataRead?.afterInitialPathStat?.(observation);
			} },
			validateHandle: async (_handle, stat) => generationEntryStat(stat, "file", options),
		});
		if (bytes === null) throw new Error("Generation required metadata disappeared.");
		if (entry.name === "checkpoint.json") checkpointBytes = bytes;
		else if (entry.name === "retirement.json") retirementCertificate = bytes;
		else epochRecords.set(entry.name.slice(6), bytes);
	}
	if (!checkpointBytes.equals(metadataBytes(checkpoint))) throw new Error("Generation checkpoint differs from exact expected authority.");
	const snapshot = { path, name: consumerGenerationName(checkpoint), checkpoint, checkpointBytes, retirementCertificate, epochRecords, identity, epochIdentity, receiptsIdentity, totalBytes, receiptEntries, canonicalEntries: [...canonical.values()].map(({ name, stat }) => ({ name, stat })) };
	generationEpochAuthority(snapshot, options);
	for (const entry of receiptEntries) {
		if (!sameRetiredLinkStat(entry.stat, await options.lstatEntry(join(path, "receipts", entry.name)))) throw new Error("Generation receipt inode or stat changed.");
	}
	for (const [directory, observed, expectedNames, limit] of [[path, identity, names, 4], [join(path, "epoch"), epochIdentity, epochNames, GENERATION_EPOCH_MAX_ENTRIES], [join(path, "receipts"), receiptsIdentity, receiptNames, GENERATION_RECEIPT_MAX_ENTRIES]]) {
		if (!generationSameInode(observed, await generationDirectory(directory, options)) || (await generationNames(directory, limit, options)).sort().join() !== expectedNames.slice().sort().join()) throw new Error("Generation namespace changed during validation.");
	}
	return snapshot;
}
export async function readConsumerGeneration(path, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	const checkpoint = expectedConsumerGeneration(authority, options);
	if (basename(path) !== consumerGenerationName(checkpoint)) throw new Error("Generation final name differs from its exact authority.");
	return readGenerationSnapshot(path, checkpoint, options);
}
async function publishGenerationCheckpoint(path, checkpoint, options) {
	const receipts = join(path, "receipts");
	const target = join(path, "checkpoint.json");
	const targetHash = digest(Buffer.from("checkpoint.json"));
	const temporary = join(receipts, `.receipt-p${process.pid}-w${randomUUID()}-t${targetHash}.tmp`);
	const fixed = join(receipts, `receipt-${targetHash}.json`);
	const handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0), 0o600);
	try {
		await handle.writeFile(metadataBytes(checkpoint));
		await generationBoundary(options, "before", "file-sync", temporary);
		await handle.sync();
		await generationBoundary(options, "after", "file-sync", temporary);
	} finally { await handle.close(); }
	await generationSync(receipts, options);
	await generationBoundary(options, "before", "link", target);
	await options.linkFile(temporary, target);
	await generationBoundary(options, "after", "link", target);
	await generationSync(path, options);
	await generationBoundary(options, "before", "rename", fixed);
	await options.renameFile(temporary, fixed);
	await generationBoundary(options, "after", "rename", fixed);
	await generationSync(receipts, options);
}
export async function buildConsumerGeneration(root, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	const checkpoint = expectedConsumerGeneration(authority, options);
	if (2 * metadataBytes(checkpoint).length > options.maxJournalBytes) throw new Error("Generation checkpoint and receipt exceed aggregate byte bound.");
	await generationNames(root, GENERATION_ROOT_MAX_ENTRIES - 1, options);
	const rootBytes = await generationRootPreflight(root, options);
	if (rootBytes + 2 * metadataBytes(checkpoint).length > options.maxJournalBytes) throw new Error("Generation build exceeds the root aggregate byte bound.");
	const rootIdentity = await generationDirectory(root, options);
	const path = join(root, `.building-p${process.pid}-${randomUUID()}`);
	await options.makeDirectory(path, { mode: 0o700 });
	const identity = await generationDirectory(path, options);
	await options.makeDirectory(join(path, "epoch"), { mode: 0o700 });
	await options.makeDirectory(join(path, "receipts"), { mode: 0o700 });
	await generationSync(join(path, "epoch"), options);
	await generationSync(join(path, "receipts"), options);
	await publishGenerationCheckpoint(path, checkpoint, options);
	await generationSync(join(path, "epoch"), options);
	await generationSync(path, options);
	await generationSync(root, options);
	await readGenerationSnapshot(path, checkpoint, options, identity, true);
	const builder = Object.freeze({ path, identity });
	generationBuilders.set(builder, { path, identity, rootIdentity, checkpoint, predecessor: authority.predecessor ?? null });
	return builder;
}
export async function publishConsumerGeneration(builder, rawOptions = {}) {
	const evidence = generationBuilders.get(builder);
	if (!evidence) throw new Error("Generation publication requires an observed builder identity.");
	const options = generationOptions(rawOptions);
	const { path: source, identity, checkpoint, rootIdentity } = evidence;
	const root = dirname(source);
	const destination = join(root, consumerGenerationName(checkpoint));
	await readGenerationSnapshot(source, checkpoint, options, identity, true);
	if (!generationSameInode(rootIdentity, await generationDirectory(root, options))) throw new Error("Generation root inode changed before publication.");
	const observation = { source, destination, identity };
	await options.hooks?.beforeGenerationRename?.(observation);
	await generationBoundary(options, "before", "rename", destination);
	if (evidence.predecessor !== null) assertConsumerGenerationSuccessor(await generationReadPinned(evidence.predecessor, options), basename(destination), metadataBytes(checkpoint), options);
	try {
		await options.renameFile(source, destination);
	} catch (error) {
		if (options.renameFile !== rename || error?.code !== "ENOENT") throw error;
		// Only native source loss can join this rename; a byte-identical winner is not ours.
		await readGenerationSnapshot(destination, checkpoint, options, identity, true);
	}
	await generationBoundary(options, "after", "rename", destination);
	await options.hooks?.afterGenerationRename?.(observation);
	const result = await readGenerationSnapshot(destination, checkpoint, options, identity, true);
	if (evidence.predecessor !== null) assertConsumerGenerationSuccessor(await generationReadPinned(evidence.predecessor, options), result.name, result.checkpointBytes, options);
	if (!generationSameInode(rootIdentity, await generationDirectory(root, options))) throw new Error("Generation root inode changed after publication.");
	await generationSync(root, options);
	return result;
}

function generationCertificateEntry(snapshot, entry) {
	const bytes = entry.name === "checkpoint.json" ? snapshot.checkpointBytes : snapshot.epochRecords.get(entry.name.slice(6));
	return { name: entry.name, dev: entry.stat.dev, ino: entry.stat.ino, size: bytes.length, sha256: digest(bytes) };
}
function generationRetirementCertificate(snapshot, slot) {
	return { schemaVersion: 1, predecessorGeneration: snapshot.name, predecessorIdentity: snapshot.identity,
		epochIdentity: snapshot.epochIdentity, receiptsIdentity: snapshot.receiptsIdentity, slot,
		entries: snapshot.canonicalEntries.filter((entry) => entry.name !== "retirement.json").map((entry) => generationCertificateEntry(snapshot, entry)).sort((a, b) => a.name.localeCompare(b.name)) };
}
function validateGenerationRetirementCertificate(snapshot, successor, options) {
	const bytes = snapshot.retirementCertificate;
	if (!Buffer.isBuffer(bytes) || digest(bytes) !== successor.retirementAuthoritySha256 || !generationSameInode(snapshot.identity, successor.previousGenerationIdentity)) throw new Error("Generation retirement certificate does not bind the exact predecessor inode.");
	const certificate = generationCanonical(bytes, options.metadataMaxBytes);
	if (!exactKeys(certificate, ["schemaVersion", "predecessorGeneration", "predecessorIdentity", "epochIdentity", "receiptsIdentity", "slot", "entries"]) || certificate.schemaVersion !== 1 || certificate.predecessorGeneration !== snapshot.name || !generationSameInode(certificate.predecessorIdentity, snapshot.identity) || !Number.isSafeInteger(certificate.slot) || certificate.slot < 1 || certificate.slot > MAX_OPERATION_GENERATIONS || !Array.isArray(certificate.entries) || certificate.entries.length > GENERATION_EPOCH_MAX_ENTRIES + 1) throw new Error("Generation retirement certificate is malformed.");
	const records = new Map(snapshot.epochRecords);
	const latest = generationEpochAuthority(snapshot, options).claims.at(-1);
	if (latest?.type !== "rotation" || latest.generation !== certificate.slot) throw new Error("Generation retirement certificate lacks its exact rotation slot.");
	records.delete(basename(claimPath({ epochDirectory: "" }, latest)));
	records.delete(`claim-index-${generationName(latest.generation)}.json`);
	const preRotation = { ...snapshot, epochRecords: records, canonicalEntries: snapshot.canonicalEntries.filter((entry) => entry.name === "checkpoint.json" || records.has(entry.name.slice(6))) };
	const expected = generationRetirementCertificate(preRotation, certificate.slot);
	if (!metadataBytes(expected).equals(bytes)) throw new Error("Generation retirement certificate differs from the full exact predecessor authority.");
	return certificate;
}
async function generationNativeStatOrNull(path, options) {
	try { return await options.lstatEntry(path); } catch (error) {
		if (options.lstatEntry === lstat && error?.code === "ENOENT") return null;
		throw error;
	}
}
async function generationReadPinned(snapshot, options) {
	return readGenerationSnapshot(snapshot.path, snapshot.checkpoint, options, snapshot.identity);
}
async function generationWriteReceipt(snapshot, targetName, bytes, options, beforeLink) {
	if (targetName !== "retirement.json" && !/^epoch\/[a-z0-9-]+\.json$/.test(targetName)) throw new Error("Generation publication target is invalid.");
	if (bytes.length < 1 || bytes.length > options.metadataMaxBytes) throw new Error("Generation publication exceeds its byte bound.");
	const rootBytes = await generationRootPreflight(dirname(snapshot.path), options);
	if (rootBytes + bytes.length * 2 > options.maxJournalBytes) throw new Error("Generation publication exceeds aggregate byte bound.");
	await generationReadPinned(snapshot, options);
	const target = join(snapshot.path, targetName);
	const receipts = join(snapshot.path, "receipts");
	const targetHash = digest(Buffer.from(targetName));
	const temporary = join(receipts, `.receipt-p${process.pid}-w${randomUUID()}-t${targetHash}.tmp`);
	const fixed = join(receipts, `receipt-${targetHash}.json`);
	const handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0), 0o600);
	try {
		await handle.writeFile(bytes);
		await generationBoundary(options, "before", "file-sync", temporary);
		await handle.sync();
		await generationBoundary(options, "after", "file-sync", temporary);
	} finally { await handle.close(); }
	await generationSync(receipts, options);
	await beforeLink?.(temporary);
	if (!generationSameInode(snapshot.identity, await generationDirectory(snapshot.path, options))) throw new Error("Generation publication was fenced by a different container inode.");
	await generationBoundary(options, "before", "link", target);
	let created = true;
	try { await options.linkFile(temporary, target); } catch (error) {
		if (options.linkFile !== link || error?.code !== "EEXIST") throw error;
		created = false;
	}
	await generationBoundary(options, "after", "link", target);
	await generationSync(dirname(target), options);
	if (!created) {
		const existing = await readSecureFile(target, options.metadataMaxBytes, "Generation existing metadata", options);
		await options.removeFile(temporary);
		await generationSync(receipts, options);
		return { created, bytes: existing };
	}
	await generationBoundary(options, "before", "rename", fixed);
	await options.renameFile(temporary, fixed);
	await generationBoundary(options, "after", "rename", fixed);
	await generationSync(receipts, options);
	return { created, bytes };
}

// Cold root discovery may discard only a direct native missing root entry, before
// pinning checkpoint bytes. All other operation and hook failures are terminal.
export async function discoverConsumerGenerations(root, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	const expectedGenesis = expectedConsumerGeneration(authority, options);
	if (expectedGenesis.epoch !== 1) throw new Error("Generation discovery requires exact genesis source authority.");
	const rootIdentity = await generationDirectory(root, options);
	for (let attempt = 0; attempt < PROJECTION_RETRY_LIMIT; attempt += 1) {
		try {
			await generationRootPreflight(root, { ...options, [generationDiscovery]: true });
		} catch (error) {
			if (!(error instanceof GenerationDiscoveryLost)) throw error;
			if (!generationSameInode(rootIdentity, await generationDirectory(root, options))) throw new Error("Generation discovery root inode changed.");
			continue;
		}
		const names = await generationNames(root, GENERATION_ROOT_MAX_ENTRIES, options);
		const finals = names.filter((name) => /^generation-[0-9]{16}-[0-9a-f]{64}$/.test(name)).sort();
		const snapshots = [];
		let lost = false;
		for (const name of finals) {
			const path = join(root, name);
			const bytes = await readSecureFile(join(path, "checkpoint.json"), options.metadataMaxBytes, "Generation discovery checkpoint", options, 1, options.hooks?.metadataRead);
			if (bytes === null) { lost = true; break; }
			const checkpoint = validateGenerationCheckpoint(generationCanonical(bytes, options.metadataMaxBytes), options.stateMaxBytes).checkpoint;
			if (name !== consumerGenerationName(checkpoint) || checkpoint.statePathSha256 !== expectedGenesis.statePathSha256) throw new Error("Generation discovery checkpoint has a conflicting source.");
			for (const field of ["sourceKind", "sourceAuthoritySha256", "sourceTipDigest", "sourceTipBase64", "migrationKind", "migrationAuthoritySha256", "migrationTipDigest", "migrationTipBase64"]) {
				if (checkpoint[field] !== expectedGenesis[field]) throw new Error("Generation discovery provenance changed.");
			}
			if (checkpoint.epoch === 1 && !bytes.equals(metadataBytes(expectedGenesis))) throw new Error("Generation genesis differs from exact supplied source state.");
			const identity = await generationDirectory(path, options);
			const snapshot = await readGenerationSnapshot(path, checkpoint, options, identity);
			if (snapshots.length !== 0) assertConsumerGenerationSuccessor(await generationReadPinned(snapshots[0], options), name, bytes, options);
			snapshots.push(snapshot);
		}
		if (lost) {
			if (snapshots.length !== 0) throw new Error("Generation required pinned successor disappeared.");
			continue;
		}
		if (!generationSameInode(rootIdentity, await generationDirectory(root, options))) throw new Error("Generation discovery root inode changed.");
		return { root, rootIdentity, names, generations: snapshots };
	}
	throw new Error("Generation discovery exceeded its bounded native-loss handoff limit.");
}

export async function recoverConsumerGenerationBuilder(path, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	if (!new RegExp(`^\\.building-(?:p[1-9][0-9]*-)?${uuidSource}$`).test(basename(path))) throw new Error("Generation recovery requires an exact builder name.");
	const checkpoint = expectedConsumerGeneration(authority, options);
	const identity = await generationDirectory(path, options);
	const rootIdentity = await generationDirectory(dirname(path), options);
	await generationRootPreflight(dirname(path), options);
	let names = await generationNames(path, 3, options);
	const ownerPid = /^\.building-p([1-9][0-9]*)-/.exec(basename(path))?.[1];
	if (ownerPid && !temporaryProcessIsAlive({ pid: Number(ownerPid) }, options)) {
		if (names.some((name) => !["epoch", "receipts", "checkpoint.json"].includes(name))) throw new Error("Generation dead builder contains an unexpected entry.");
		for (const name of ["epoch", "receipts"]) {
			if (!names.includes(name)) {
				try { await options.makeDirectory(join(path, name), { mode: 0o700 }); } catch (error) { if (options.makeDirectory !== mkdir || error?.code !== "EEXIST") throw error; }
				await generationSync(path, options);
			}
		}
		if (!(await generationNames(join(path, "receipts"), 1, options)).length && !names.includes("checkpoint.json")) {
			await publishGenerationCheckpoint(path, checkpoint, options);
		}
		names = await generationNames(path, 3, options);
	}
	if (names.some((name) => !["epoch", "receipts", "checkpoint.json"].includes(name)) || !names.includes("receipts") || !names.includes("epoch") || (await generationNames(join(path, "epoch"), 0, options)).length !== 0) throw new Error("Generation incomplete builder cannot prove its owned checkpoint writer.");
	const receipts = join(path, "receipts");
	const receiptNames = await generationNames(receipts, 1, options);
	if (receiptNames.length !== 1) throw new Error("Generation builder lacks its exact durable receipt proof.");
	const receiptName = receiptNames[0];
	const targetHash = digest(Buffer.from("checkpoint.json"));
	if (receiptName !== `receipt-${targetHash}.json` && generationReceiptTemporaryPattern.exec(receiptName)?.[3] !== targetHash) throw new Error("Generation builder receipt has a conflicting target.");
	const receiptPath = join(receipts, receiptName);
	const receiptStat = generationEntryStat(await options.lstatEntry(receiptPath), "file", options);
	const bytes = await readSecureFile(receiptPath, options.metadataMaxBytes, "Generation builder receipt", options);
	if (bytes === null || !bytes.equals(metadataBytes(checkpoint))) throw new Error("Generation builder receipt differs from exact construction authority.");
	const target = join(path, "checkpoint.json");
	const canonical = await generationNativeStatOrNull(target, options);
	if (canonical === null) {
		if (receiptStat.nlink !== 1 || receiptName.startsWith("receipt-")) throw new Error("Generation builder has malformed pre-link receipt evidence.");
		await generationSync(receipts, options);
		await generationBoundary(options, "before", "link", target);
		try { await options.linkFile(receiptPath, target); } catch (error) {
			if (options.linkFile !== link || error?.code !== "EEXIST") throw error;
		}
		await generationBoundary(options, "after", "link", target);
	}
	if (!generationSameInode(receiptStat, generationEntryStat(await options.lstatEntry(target), "file", options))) throw new Error("Generation builder checkpoint is a different inode from its receipt.");
	await generationSync(path, options);
	if (!receiptName.startsWith("receipt-")) {
		const fixed = join(receipts, `receipt-${targetHash}.json`);
		await generationBoundary(options, "before", "rename", fixed);
		await options.renameFile(receiptPath, fixed);
		await generationBoundary(options, "after", "rename", fixed);
	}
	await generationSync(receipts, options);
	await generationSync(path, options);
	await generationSync(dirname(path), options);
	await readGenerationSnapshot(path, checkpoint, options, identity, true);
	const builder = Object.freeze({ path, identity });
	generationBuilders.set(builder, { path, identity, rootIdentity, checkpoint, predecessor: authority.predecessor ?? null });
	return builder;
}

async function generationQuiesce(snapshot, options, ownTemporary = null) {
	const scan = generationEpochAuthority(snapshot, options);
	for (const receipt of snapshot.receiptEntries.filter((entry) => entry.temporary)) {
		const path = join(snapshot.path, "receipts", receipt.name);
		if (path === ownTemporary) continue;
		let decided = false;
		if (receipt.target === null) {
			const bytes = await readSecureFile(path, options.metadataMaxBytes, "Generation owned receipt temporary", options, 0);
			if (bytes === null) throw new Error("Generation receipt temporary disappeared after pinning.");
			let value;
			try { value = JSON.parse(bytes); } catch (error) { if (!(error instanceof SyntaxError)) throw error; }
			if (value && metadataBytes(value).equals(bytes)) {
				let claim = value.type === "normal" ? validateClaim(value, null, options.stateMaxBytes) : scan.claims.find((claim) => claim.generation === value.generation && claim.token === value.token);
				let target = null;
				if (value.type === "normal") target = `epoch/${basename(claimPath({ epochDirectory: "" }, value))}`;
				else if (value.claimSha256 && value.schemaVersion === 1) {
					claim = scan.contents.get(value.claimSha256);
					if (claim && value.generation === claim.generation) target = `epoch/claim-index-${generationName(value.generation)}.json`;
				} else if (claim && value.outcome) {
					validateTerminal(value, claim, options.stateMaxBytes, validateGenerationTransaction);
					target = `epoch/terminal-${generationName(claim.generation)}-${claim.token}.json`;
				} else if (claim && value.refreshedAtMs !== undefined) {
					validateHeartbeat(value, claim);
					target = `epoch/heartbeat-${generationName(claim.generation)}-${claim.token}-${generationName(value.refreshedAtMs)}.json`;
				} else if (claim && value.terminalSha256) {
					validateApplied(value, claim, scan.terminals.get(`${claim.generation}:${claim.token}`));
					target = `epoch/applied-${generationName(claim.generation)}-${claim.token}.json`;
				}
				if (target !== null && generationReceiptTemporaryPattern.exec(receipt.name)[3] !== digest(Buffer.from(target))) throw new Error("Generation owned receipt temporary has a conflicting target hash.");
				if (claim && target !== null) {
					const winner = scan.claims.find((candidate) => candidate.generation === claim.generation);
					decided = !!winner && (!metadataBytes(winner).equals(metadataBytes(claim)) || scan.terminals.has(`${claim.generation}:${claim.token}`));
				}
			}
		}
		if (receipt.target === null && !decided && temporaryProcessIsAlive({ pid: receipt.pid }, options)) throw new Error("Generation rotation is pending until its live unresolved receipt writer quiesces.");
		if (!sameRetiredLinkStat(receipt.stat, await options.lstatEntry(path))) throw new Error("Generation receipt temporary inode changed before recovery.");
		if (receipt.target === null) {
			await options.removeFile(path);
		} else {
			const fixed = join(snapshot.path, "receipts", `receipt-${digest(Buffer.from(receipt.target))}.json`);
			await generationBoundary(options, "before", "rename", fixed);
			await options.renameFile(path, fixed);
			await generationBoundary(options, "after", "rename", fixed);
		}
		await generationSync(join(snapshot.path, "receipts"), options);
	}
}

async function generationOwnsClaim(snapshot, claim, options, ownTemporary = null) {
	const current = await generationReadPinned(snapshot, options);
	const scan = generationEpochAuthority(current, options);
	const latest = scan.claims.at(-1);
	if (!latest || !metadataBytes(latest).equals(metadataBytes(claim)) || scan.terminals.has(`${claim.generation}:${claim.token}`)) throw new Error("Generation operation lost its exact latest claim ownership.");
	await generationQuiesce(current, options, ownTemporary);
	return { snapshot: current, scan };
}

async function generationPublishClaim(snapshot, claim, options) {
	const revalidate = async (temporary) => {
		const current = await generationReadPinned(snapshot, options);
		const scan = generationEpochAuthority(current, options);
		if (claim.type === "normal" && current.retirementCertificate !== null) throw new Error("Generation normal claim is fenced by retirement preparation.");
		const latest = scan.claims.at(-1);
		if (latest?.type === "rotation" && !metadataBytes(latest).equals(metadataBytes(claim))) throw new Error("Generation claim was fenced by its rotation CAS.");
		if (claim.generation !== (latest?.generation ?? 0) + 1 && claim.generation !== latest?.generation) throw new Error("Generation claim frontier changed before publication.");
		await generationQuiesce(current, options, temporary);
	};
	const claimName = `epoch/${basename(claimPath({ epochDirectory: "" }, claim))}`;
	const result = await generationWriteReceipt(snapshot, claimName, metadataBytes(claim), options, revalidate);
	if (!result.bytes?.equals(metadataBytes(claim))) throw new Error("Generation claim publication lost its digest-bound bytes.");
	const current = await generationReadPinned(snapshot, options);
	const cas = claimIndexFor(claim);
	const published = await generationWriteReceipt(current, `epoch/claim-index-${generationName(claim.generation)}.json`, metadataBytes(cas), options, revalidate);
	return published.bytes?.equals(metadataBytes(cas)) ?? false;
}

async function generationRepairProjection(root, authority, statePath, options) {
	for (let attempt = 0; attempt < PROJECTION_RETRY_LIMIT; attempt += 1) {
		const discovered = await discoverConsumerGenerations(root, authority, options);
		if (discovered.generations.length !== 1) throw new Error("Generation projection requires a converged unique final.");
		const snapshot = discovered.generations[0];
		const tip = generationEpochAuthority(snapshot, options).tip;
		const current = await readSecureFile(statePath, options.stateMaxBytes, "Generation projection", options, 0, options.hooks?.projectionRead);
		if (tip.tipBytes !== null && (current === null || !current.equals(tip.tipBytes))) {
			await options.hooks?.beforeProjectionWrite?.({ tipDigest: tip.tipDigest });
			const temporary = join(dirname(statePath), `.pylon-generation-projection-${randomUUID()}.tmp`);
			const handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0), 0o600);
			try { await handle.writeFile(tip.tipBytes); await handle.sync(); } finally { await handle.close(); }
			const identity = await options.lstatEntry(temporary);
			await options.hooks?.afterProjectionFileSync?.({ tipDigest: tip.tipDigest, temporary });
			await generationReadPinned(snapshot, options);
			await options.renameFile(temporary, statePath);
			if (!generationSameInode(identity, await options.lstatEntry(statePath))) throw new Error("Generation projection rename has a different destination inode.");
			await options.hooks?.afterProjectionRename?.({ tipDigest: tip.tipDigest });
			await generationSync(dirname(statePath), options);
			// A writer may have paused after its final authority read and overwritten
			// a newer projection. Only a fresh immutable-tip scan can permit success.
			const afterRename = await discoverConsumerGenerations(root, authority, options);
			if (afterRename.generations.length !== 1) continue;
			const newer = generationEpochAuthority(afterRename.generations[0], options).tip;
			if (newer.tipDigest !== tip.tipDigest) continue;
		}
		const readback = await readSecureFile(statePath, options.stateMaxBytes, "Generation projection", options, 0, options.hooks?.projectionRead);
		const afterRead = await discoverConsumerGenerations(root, authority, options);
		if (afterRead.generations.length !== 1) continue;
		const latest = generationEpochAuthority(afterRead.generations[0], options).tip;
		if (latest.tipDigest !== tip.tipDigest) continue;
		if (latest.tipBytes === null ? readback !== null : readback === null || !readback.equals(latest.tipBytes)) continue;
		return latest;
	}
	throw new Error("Generation projection could not catch up to its immutable tip.");
}

async function generationFinishCommit(snapshot, claim, terminal, root, authority, statePath, options) {
	for (const transaction of terminal.transactions) {
		const current = await generationReadPinned(snapshot, options);
		const result = await generationWriteReceipt(current, `epoch/transition-${transaction.baseDigest}.json`, metadataBytes(transaction), options);
		if (!result.bytes?.equals(metadataBytes(transaction))) throw new Error("Generation transition lost its immutable compare-and-set.");
	}
	await generationRepairProjection(root, authority, statePath, options);
	const applied = { schemaVersion: 2, generation: claim.generation, token: claim.token, terminalSha256: digest(metadataBytes(terminal)) };
	const current = await generationReadPinned(snapshot, options);
	const result = await generationWriteReceipt(current, `epoch/applied-${generationName(claim.generation)}-${claim.token}.json`, metadataBytes(applied), options);
	if (!result.bytes?.equals(metadataBytes(applied))) throw new Error("Generation applied marker differs from its exact terminal.");
	await generationRepairProjection(root, authority, statePath, options);
}

async function generationMove(source, destination, identity, options, validate) {
	await options.hooks?.beforeGenerationMove?.({ source, destination, identity });
	await validate(source);
	await generationBoundary(options, "before", "rename", destination);
	const remainingSource = await generationNativeStatOrNull(source, options);
	if (remainingSource === null) {
		if (!generationSameInode(identity, await generationDirectory(destination, options))) throw new Error("Generation rename join has a different destination inode.");
		await validate(destination);
	} else {
		if (!generationSameInode(identity, remainingSource)) throw new Error("Generation rename source was replaced.");
		await validate(source);
	}
	try { await options.renameFile(source, destination); } catch (error) {
		if (options.renameFile !== rename || error?.code !== "ENOENT") throw error;
		if (!generationSameInode(identity, await generationDirectory(destination, options))) throw error;
	}
	await generationBoundary(options, "after", "rename", destination);
	await options.hooks?.afterGenerationMove?.({ source, destination, identity });
	if (!generationSameInode(identity, await generationDirectory(destination, options))) throw new Error("Generation rename destination is a different inode.");
	await validate(destination);
	await generationSync(dirname(source), options);
}

async function generationDeleteRetired(path, successor, options) {
	const expectedIdentity = successor.checkpoint.previousGenerationIdentity;
	if (!expectedIdentity || !generationSameInode(expectedIdentity, await generationDirectory(path, options))) throw new Error("Generation deletion container is a different predecessor inode.");
	const certificatePath = join(path, "retirement.json");
	const receiptName = `receipt-${digest(Buffer.from("retirement.json"))}.json`;
	const certificateReceiptPath = join(path, "receipts", receiptName);
	let certificateBytes = await readSecureFile(certificatePath, options.metadataMaxBytes, "Generation deletion certificate", options);
	if (certificateBytes === null) certificateBytes = await readSecureFile(certificateReceiptPath, options.metadataMaxBytes, "Generation deletion certificate receipt", options);
	const names = await generationNames(path, 4, options);
	if (certificateBytes === null) {
		// The final proof links are deleted only after every authority file. The
		// successor still commits this exact container inode through the empty cut.
		if (names.some((name) => !["epoch", "receipts"].includes(name))) throw new Error("Generation deletion lost its certificate before authority cleanup.");
		for (const name of names) {
			await generationDirectory(join(path, name), options);
			await generationNames(join(path, name), 0, options);
		}
	} else {
		if (digest(certificateBytes) !== successor.checkpoint.retirementAuthoritySha256) throw new Error("Generation deletion certificate differs from successor commitment.");
		const certificate = generationCanonical(certificateBytes, options.metadataMaxBytes);
		if (certificate.predecessorGeneration !== successor.checkpoint.previousGeneration || !generationSameInode(certificate.predecessorIdentity, expectedIdentity) || !Array.isArray(certificate.entries) || certificate.entries.length > GENERATION_EPOCH_MAX_ENTRIES + 1) throw new Error("Generation deletion certificate is not bound to its successor.");
		const expected = new Map(certificate.entries.map((entry) => [entry.name, entry]));
		if (expected.size !== certificate.entries.length || expected.get("checkpoint.json")?.sha256 !== successor.checkpoint.previousCheckpointSha256) throw new Error("Generation deletion certificate lacks exact predecessor checkpoint.");
		const claim = { schemaVersion: 3, generation: certificate.slot, token: successor.checkpoint.epochId, type: "rotation", intent: {
			schemaVersion: 3, predecessorGeneration: successor.checkpoint.previousGeneration,
			checkpointSha256: successor.checkpoint.previousCheckpointSha256, tipSha256: successor.checkpoint.anchorDigest, checkpoint: successor.checkpoint,
		} };
		for (const [name, value] of [[`epoch/${basename(claimPath({ epochDirectory: "" }, claim))}`, claim], [`epoch/claim-index-${generationName(certificate.slot)}.json`, claimIndexFor(claim)]]) {
			const bytes = metadataBytes(value);
			expected.set(name, { name, size: bytes.length, sha256: digest(bytes), dev: null, ino: null });
		}
		expected.set("retirement.json", { name: "retirement.json", size: certificateBytes.length, sha256: digest(certificateBytes), dev: null, ino: null });
		const allowed = new Map();
		for (const [name, entry] of expected) {
			allowed.set(name, entry);
			allowed.set(`receipts/receipt-${digest(Buffer.from(name))}.json`, entry);
		}
		const actual = [];
		for (const name of names) {
			if (["epoch", "receipts"].includes(name)) {
				const directory = join(path, name);
				const identity = await generationDirectory(directory, options);
				if (!generationSameInode(identity, certificate[name === "epoch" ? "epochIdentity" : "receiptsIdentity"])) throw new Error("Generation deletion child directory changed inode.");
				for (const child of await generationNames(directory, name === "epoch" ? GENERATION_EPOCH_MAX_ENTRIES : GENERATION_RECEIPT_MAX_ENTRIES, options)) actual.push(`${name}/${child}`);
			} else actual.push(name);
		}
		const observations = new Map();
		for (const name of actual) {
			const descriptor = allowed.get(name);
			if (!descriptor) throw new Error("Generation deleting container contains an unauthorized entry.");
			const stat = generationEntryStat(await options.lstatEntry(join(path, name)), "file", options);
			if (stat.size !== descriptor.size || (descriptor.dev !== null && !generationSameInode(stat, descriptor)) || ![1, 2].includes(stat.nlink)) throw new Error("Generation deleting entry differs from its committed inode or size.");
			const bytes = await readSecureFile(join(path, name), options.metadataMaxBytes, "Generation deleting authority", options);
			if (bytes === null || digest(bytes) !== descriptor.sha256) throw new Error("Generation deleting authority differs from its committed bytes.");
			observations.set(name, stat);
		}
		for (const [name] of expected) {
			const canonical = observations.get(name);
			const receipt = observations.get(`receipts/receipt-${digest(Buffer.from(name))}.json`);
			if (canonical && receipt ? !generationSameInode(canonical, receipt) || canonical.nlink !== 2 || receipt.nlink !== 2 : (canonical ?? receipt)?.nlink !== undefined && (canonical ?? receipt).nlink !== 1) throw new Error("Generation deletion receipt is not its exact remaining canonical inode.");
		}
		const proofNames = new Set(["retirement.json", `receipts/${receiptName}`]);
		for (const name of [...actual.filter((name) => !proofNames.has(name)), ...actual.filter((name) => proofNames.has(name))]) {
			await generationReadPinned(successor, options);
			if (!generationSameInode(expectedIdentity, await generationDirectory(path, options))) throw new Error("Generation deletion container was replaced.");
			const entryPath = join(path, name);
			const current = generationEntryStat(await options.lstatEntry(entryPath), "file", options);
			if (!generationSameInode(current, observations.get(name))) throw new Error("Generation deleting entry was replaced before unlink.");
			await generationBoundary(options, "before", "unlink", entryPath);
			await options.removeFile(entryPath);
			await generationBoundary(options, "after", "unlink", entryPath);
			await generationSync(dirname(entryPath), options);
		}
	}
	for (const name of await generationNames(path, 2, options)) {
		if (!["epoch", "receipts"].includes(name)) throw new Error("Generation deletion is not empty.");
		await generationNames(join(path, name), 0, options);
		await generationBoundary(options, "before", "remove-directory", join(path, name));
		await options.removeFile(join(path, name), { recursive: true });
		await generationBoundary(options, "after", "remove-directory", join(path, name));
		await generationSync(path, options);
	}
	await generationReadPinned(successor, options);
	if (!generationSameInode(expectedIdentity, await generationDirectory(path, options))) throw new Error("Generation deleting container was replaced at final removal.");
	await generationNames(path, 0, options);
	await generationBoundary(options, "before", "remove-directory", path);
	await options.removeFile(path, { recursive: true });
	await generationBoundary(options, "after", "remove-directory", path);
	await generationSync(dirname(path), options);
}

async function generationConverge(root, authority, discovered, options) {
	let successor = discovered.generations.at(-1);
	if (!successor) return null;
	if (discovered.generations.length === 2) {
		const predecessor = discovered.generations[0];
		const validate = async (path) => {
			const pinned = await readGenerationSnapshot(path, predecessor.checkpoint, options, predecessor.identity);
			const currentSuccessor = await generationReadPinned(successor, options);
			assertConsumerGenerationSuccessor(pinned, currentSuccessor.name, currentSuccessor.checkpointBytes, options);
			await generationQuiesce(pinned, options);
		};
		await validate(predecessor.path);
		const retired = join(root, `.retired-${predecessor.name}`);
		await generationMove(predecessor.path, retired, predecessor.identity, options, validate);
	}
	successor = await generationReadPinned(successor, options);
	for (const name of await generationNames(root, GENERATION_ROOT_MAX_ENTRIES, options)) {
		if (!name.startsWith(".retired-") && !name.startsWith(".deleting-")) continue;
		const expected = successor.checkpoint.previousGeneration;
		if (![`.retired-${expected}`, `.deleting-${expected}`].includes(name)) throw new Error("Generation cleanup contains an orphan predecessor.");
		let path = join(root, name);
		if (name.startsWith(".retired-")) {
			const checkpointBytes = await readSecureFile(join(path, "checkpoint.json"), options.metadataMaxBytes, "Generation retired checkpoint", options);
			if (checkpointBytes === null) throw new Error("Generation retired checkpoint disappeared.");
			const checkpoint = validateGenerationCheckpoint(generationCanonical(checkpointBytes, options.metadataMaxBytes), options.stateMaxBytes).checkpoint;
			const validate = async (currentPath) => {
				const pinned = await readGenerationSnapshot(currentPath, checkpoint, options, successor.checkpoint.previousGenerationIdentity);
				const currentSuccessor = await generationReadPinned(successor, options);
				assertConsumerGenerationSuccessor(pinned, currentSuccessor.name, currentSuccessor.checkpointBytes, options);
				await generationQuiesce(pinned, options);
			};
			const deleting = join(root, `.deleting-${expected}`);
			await generationMove(path, deleting, successor.checkpoint.previousGenerationIdentity, options, validate);
			path = deleting;
		}
		await generationDeleteRetired(path, successor, options);
	}
	const final = await discoverConsumerGenerations(root, authority, options);
	if (final.generations.length !== 1 || final.names.some((name) => name.startsWith(".retired-") || name.startsWith(".deleting-"))) throw new Error("Generation preparation did not converge its predecessor cleanup.");
	return final.generations[0];
}

export async function prepareConsumerGeneration(root, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	let discovered = await discoverConsumerGenerations(root, authority, options);
	if (discovered.generations.length === 0) {
		const builders = discovered.names.filter((name) => name.startsWith(".building-"));
		const builder = builders.length === 0 ? await buildConsumerGeneration(root, authority, options) : await recoverConsumerGenerationBuilder(join(root, builders[0]), authority, options);
		await publishConsumerGeneration(builder, options);
		discovered = await discoverConsumerGenerations(root, authority, options);
	}
	return generationConverge(root, authority, discovered, options);
}

export async function rotateConsumerGeneration(root, authority, rawOptions = {}) {
	const options = generationOptions(rawOptions);
	let snapshot = await prepareConsumerGeneration(root, authority, options);
	await generationQuiesce(snapshot, options);
	snapshot = await generationReadPinned(snapshot, options);
	let scan = generationEpochAuthority(snapshot, options);
	let latest = scan.claims.at(-1);
	if (latest?.type === "normal" && (!scan.terminals.has(`${latest.generation}:${latest.token}`) || (scan.terminals.get(`${latest.generation}:${latest.token}`).outcome === "commit" && !scan.applied.has(`${latest.generation}:${latest.token}`)))) throw new Error("Generation rotation requires a resolved normal operation frontier.");
	if (latest?.type !== "rotation") {
		const slot = (latest?.generation ?? 0) + 1;
		const certificateBytes = metadataBytes(generationRetirementCertificate(snapshot, slot));
		const wanted = consumerGenerationRotationClaim(snapshot.checkpoint, slot, { ...scan.tip, previousGenerationIdentity: snapshot.identity, retirementAuthoritySha256: digest(certificateBytes) }, options.stateMaxBytes);
		const headroom = 2 * certificateBytes.length + 2 * metadataBytes(wanted).length + 2 * metadataBytes(claimIndexFor(wanted)).length + 2 * metadataBytes(wanted.intent.checkpoint).length;
		if (await generationRootPreflight(root, options) + headroom > options.maxJournalBytes) throw new Error("Generation lacks reserved rotation headroom.");
		await options.hooks?.beforeRotationDecision?.({ claim: wanted, intent: wanted.intent });
		const result = await generationWriteReceipt(snapshot, "retirement.json", certificateBytes, options);
		if (!result.bytes?.equals(certificateBytes)) throw new Error("Generation retirement certificate lost its immutable publication.");
		snapshot = await generationReadPinned(snapshot, options);
		if (!(await generationPublishClaim(snapshot, wanted, options))) throw new Error("Generation rotation lost its exact winning CAS.");
		latest = wanted;
		await options.hooks?.afterRotationIntent?.({ claim: latest, intent: latest.intent });
	}
	snapshot = await generationReadPinned(snapshot, options);
	assertConsumerGenerationSuccessor(snapshot, consumerGenerationName(latest.intent.checkpoint), metadataBytes(latest.intent.checkpoint), options);
	const builders = (await generationNames(root, GENERATION_ROOT_MAX_ENTRIES, options)).filter((name) => name.startsWith(".building-"));
	const builder = builders.length === 0 ? await buildConsumerGeneration(root, { predecessor: snapshot }, options) : await recoverConsumerGenerationBuilder(join(root, builders[0]), { predecessor: snapshot }, options);
	await publishConsumerGeneration(builder, { ...options, hooks: { ...options.hooks, beforeGenerationRename: async (observation) => {
		await options.hooks?.beforeGenerationRename?.(observation);
		const latestPredecessor = await generationReadPinned(snapshot, options);
		assertConsumerGenerationSuccessor(latestPredecessor, basename(observation.destination), metadataBytes(latest.intent.checkpoint), options);
	} } });
	const final = await prepareConsumerGeneration(root, authority, options);
	return { epoch: final.checkpoint.epoch, tipSha256: generationEpochAuthority(final, options).tip.tipDigest };
}

export async function withConsumerGenerationLock(root, authority, action, rawOptions = {}) {
	if (typeof action !== "function" || !authority?.genesis?.statePath) throw new Error("Generation operation requires an action and exact genesis authority.");
	const options = generationOptions(rawOptions);
	const statePath = resolve(authority.genesis.statePath);
	await generationDirectory(dirname(statePath), options);
	let snapshot;
	let claim;
	let acquired = false;
	for (let attempt = 0; attempt < PROJECTION_RETRY_LIMIT; attempt += 1) {
		snapshot = await prepareConsumerGeneration(root, authority, options);
		await generationQuiesce(snapshot, options);
		snapshot = await generationReadPinned(snapshot, options);
		let scan = generationEpochAuthority(snapshot, options);
		const latest = scan.claims.at(-1);
		if (latest?.type === "rotation" || snapshot.retirementCertificate !== null) { await rotateConsumerGeneration(root, authority, options); continue; }
		if (latest) {
			const key = `${latest.generation}:${latest.token}`;
			let terminal = scan.terminals.get(key);
			if (!terminal) {
				const heartbeat = scan.heartbeats.get(key)?.refreshedAtMs ?? latest.createdAtMs;
				if (options.now() - heartbeat < options.stale) throw new Error("Generation state is actively locked.");
				await options.hooks?.afterObserveStale?.({ claim: latest, heartbeat });
				const wanted = { schemaVersion: 2, generation: latest.generation, token: latest.token, outcome: "retired" };
				const result = await generationWriteReceipt(snapshot, `epoch/terminal-${generationName(latest.generation)}-${latest.token}.json`, metadataBytes(wanted), options);
				terminal = validateTerminal(generationCanonical(result.bytes, options.metadataMaxBytes), latest, options.stateMaxBytes, validateGenerationTransaction);
			}
			if (terminal.outcome === "commit" && !scan.applied.has(key)) await generationFinishCommit(snapshot, latest, terminal, root, authority, statePath, options);
		}
		snapshot = await generationReadPinned(snapshot, options);
		scan = generationEpochAuthority(snapshot, options);
		const slot = (scan.claims.at(-1)?.generation ?? 0) + 1;
		// Reserve the full next checkpoint/claim, duplicate receipt links, a
		// maximum staged transaction and its terminal, plus certificate growth.
		const certificateBytes = metadataBytes(generationRetirementCertificate(snapshot, slot)).length;
		const reserve = 4 * options.metadataMaxBytes + 4 * (4 * Math.ceil(options.stateMaxBytes / 3) + 1024) + 2 * (certificateBytes + 16_384);
		if (certificateBytes + 2048 > options.metadataMaxBytes || slot > (options.maxLockGenerations ?? PYLON_CONSUMER_ROTATE_CLAIM_TRIGGER) || scan.tip.length >= (options.maxTransactionDepth ?? PYLON_CONSUMER_ROTATE_TRANSITION_TRIGGER) || await generationRootPreflight(root, options) + reserve > options.maxJournalBytes) {
			if (scan.claims.length === 0) throw new Error("Generation byte budget cannot reserve one maximum operation and rotation.");
			await rotateConsumerGeneration(root, authority, options);
			continue;
		}
		claim = { schemaVersion: 2, generation: slot, token: randomUUID(), type: "normal", ownerPid: process.pid, createdAtMs: options.now() };
		if (!(await generationPublishClaim(snapshot, claim, options))) continue;
		await generationOwnsClaim(snapshot, claim, options);
		acquired = true;
		break;
	}
	if (!acquired) throw new Error("Generation operation could not acquire its bounded claim frontier.");
	let terminal = null;
	let active = true;
	let staged = null;
	let stagedOnce = false;
	let heartbeatFailure = null;
	const beat = async () => {
		if (!active) return false;
		try {
			const owned = await generationOwnsClaim(snapshot, claim, options);
			const refreshedAtMs = options.now();
			const value = { schemaVersion: 2, generation: claim.generation, token: claim.token, refreshedAtMs };
			await generationWriteReceipt(owned.snapshot, `epoch/heartbeat-${generationName(claim.generation)}-${claim.token}-${generationName(refreshedAtMs)}.json`, metadataBytes(value), options,
				async (temporary) => generationOwnsClaim(snapshot, claim, options, temporary));
			return true;
		} catch (error) { heartbeatFailure = error; throw error; }
	};
	await beat();
	const stopHeartbeat = (options.startHeartbeat ?? defaultHeartbeatScheduler)({ interval: options.update ?? PYLON_CONSUMER_LOCK_UPDATE_MS, beat });
	let stopped = false;
	const stop = async () => { if (!stopped) { stopped = true; await stopHeartbeat(); } };
	const publishDecision = async (wanted) => {
		const current = await generationReadPinned(snapshot, options);
		const result = await generationWriteReceipt(current, `epoch/terminal-${generationName(claim.generation)}-${claim.token}.json`, metadataBytes(wanted), options);
		terminal = validateTerminal(generationCanonical(result.bytes, options.metadataMaxBytes), claim, options.stateMaxBytes, validateGenerationTransaction);
		if (!result.bytes.equals(metadataBytes(wanted))) throw new Error("Generation operation lost ownership before its terminal decision.");
	};
	try {
		await options.hooks?.afterClaim?.({ claim });
		const base = await generationRepairProjection(root, authority, statePath, options);
		const transaction = Object.freeze({
			readStateBytes: () => base.tipBytes === null ? null : Buffer.from(base.tipBytes),
			commitState: async (value) => {
				if (!active || stagedOnce || terminal !== null) throw new Error("Generation transaction is no longer live or already staged.");
				const bytes = Buffer.from(value);
				if (bytes.length < 1 || bytes.length > options.stateMaxBytes) throw new Error("Generation state exceeds its byte bound.");
				staged = bytes; stagedOnce = true;
			},
		});
		let result;
		try { result = await action(statePath, transaction); } finally { active = false; await stop(); }
		if (heartbeatFailure !== null) throw heartbeatFailure;
		await generationOwnsClaim(snapshot, claim, options);
		if (staged !== null && digest(staged) !== base.tipDigest) {
			const wanted = { schemaVersion: 2, generation: claim.generation, token: claim.token, outcome: "commit", transactions: [transactionFor(base.tipDigest, staged)] };
			await options.hooks?.beforeCommitDecision?.({ claim, transactions: wanted.transactions });
			await publishDecision(wanted);
			await options.hooks?.afterCommitDecision?.({ claim, terminal });
			await generationFinishCommit(snapshot, claim, terminal, root, authority, statePath, options);
		} else await publishDecision({ schemaVersion: 2, generation: claim.generation, token: claim.token, outcome: "released" });
		await generationRepairProjection(root, authority, statePath, options);
		await prepareConsumerGeneration(root, authority, options);
		return result;
	} catch (error) {
		active = false;
		await stop();
		// Preserve the original action/I/O failure, including its object identity.
		// A failed commit decision remains helpable; callbacks are never replayed.
		if (terminal === null) {
			try { await publishDecision({ schemaVersion: 2, generation: claim.generation, token: claim.token, outcome: "released" }); } catch { /* Recovery uses the durable slot on the next entry. */ }
		}
		throw error;
	}
}
