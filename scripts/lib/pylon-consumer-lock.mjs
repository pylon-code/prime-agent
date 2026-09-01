import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, lstat, mkdir, open, readdir, rename, rm } from "node:fs/promises";
import { basename, dirname, join, parse, relative, resolve, sep } from "node:path";

import { readBoundedRegularFile } from "./pylon-bounded-file.mjs";

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
export const PYLON_CONSUMER_ROTATE_CLAIM_TRIGGER = 60_000;
export const PYLON_CONSUMER_ROTATE_TRANSITION_TRIGGER = 3_800;
const LOCK_SCHEMA_VERSION = 2;
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
const claimPattern = /^claim-([0-9]{16})\.json$/;
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

function claimPath(context, generation) {
	return join(context.epochDirectory, `claim-${generationName(generation)}.json`);
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

function validateTerminal(value, claim, stateMaxBytes) {
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
		validateTransaction(transaction, expectedBase, stateMaxBytes);
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

async function readSecureFile(path, maxBytes, description, options, minBytes = 1, hooks) {
	return readBoundedRegularFile(path, {
		maxBytes,
		minBytes,
		description,
		openFile: options.openFile,
		lstatEntry: options.lstatEntry,
		hooks,
		validateHandle: (handle, stat) => secureHandle(handle, stat, description, "file", options),
	});
}

async function readExactMetadata(path, maxBytes, validate, description, options, budget) {
	const bytes = await readSecureFile(path, maxBytes, description, options);
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
		"checkpoint", "projection", "transition", "claim", "initial-heartbeat", "heartbeat",
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

async function revalidateAuthority(context, operation, options) {
	await options.hooks?.beforePathOperation?.({
		operation,
		statePath: context.statePath,
		lockDirectory: context.journalDirectory,
		transactionDirectory: context.epochDirectory,
	});
	await ensureDurableConsumerStateDirectory(dirname(context.statePath), {
		...options.directoryOperations,
		create: false,
	});
	await secureDirectory(dirname(context.statePath), "Consumer high-water state directory", options);
	await secureDirectory(context.journalDirectory, "Consumer high-water journal directory", options);
	await secureDirectory(context.temporaryDirectory, "Consumer high-water temporary directory", options);
	await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
	const entries = await options.readDirectory(context.journalDirectory);
	if (entries.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe allocation bound.");
	}
	const checkpoints = entries.map((name) => ({ name, match: checkpointPattern.exec(name) })).filter((entry) => entry.match);
	if (checkpoints.length < 1 || checkpoints.length > 2) throw new Error("Consumer high-water journal checkpoint set is malformed.");
	checkpoints.sort((left, right) => Number(left.match[1]) - Number(right.match[1]));
	if (checkpoints.at(-1).name !== basename(context.checkpointPath)) {
		throw new Error("Consumer high-water journal epoch changed and fenced a paused writer.");
	}
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
}) {
	if (revalidate) await revalidateAuthority(context, kind, options);
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
		if (revalidate) await revalidateAuthority(context, `${kind}-link`, options);
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

async function publishMetadata(path, value, kind, context, writer, options) {
	const created = await publishImmutable({
		path,
		bytes: metadataBytes(value),
		directory: dirname(path),
		kind,
		context,
		writer,
		options,
	});
	if (created) return { value, created: true };
	await revalidateAuthority(context, `${kind}-existing`, options);
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

async function scanJournalRoot(statePath, journalDirectory, options, replacementRetries = PROJECTION_RETRY_LIMIT) {
	await secureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	await options.syncDirectory(journalDirectory);
	const names = await options.readDirectory(journalDirectory);
	if (names.length > MAX_JOURNAL_ROOT_ENTRIES + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe allocation bound.");
	}
	const checkpointEntries = [];
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
			const checkpoint = await readExactMetadata(
				path,
				options.metadataMaxBytes,
				(value) => validateCheckpoint(value, options.stateMaxBytes).value,
				"Consumer high-water journal checkpoint",
				options,
			);
			if (checkpoint === null) {
				const epoch = Number(checkpointMatch[1]);
				const hasLaterCheckpoint = names.some((candidate) => {
					const match = checkpointPattern.exec(candidate);
					return match && Number(match[1]) > epoch;
				});
				if (hasLaterCheckpoint && replacementRetries > 0) {
					return scanJournalRoot(statePath, journalDirectory, options, replacementRetries - 1);
				}
				throw new Error("Consumer high-water journal lost its current checkpoint during an authenticated scan.");
			}
			if (checkpointName(checkpoint) !== name || checkpoint.epoch !== Number(checkpointMatch[1])) {
				throw new Error("Consumer high-water journal checkpoint name is malformed.");
			}
			checkpointEntries.push({ name, path, checkpoint, digest: digest(metadataBytes(checkpoint)) });
			continue;
		}
		const epochMatch = epochPattern.exec(name);
		if (epochMatch) {
			let entry;
			try {
				entry = await options.lstatEntry(path);
			} catch (error) {
				const epoch = Number(epochMatch[1]);
				const hasLaterEpoch = names.some((candidate) => {
					const match = epochPattern.exec(candidate);
					return match && Number(match[1]) > epoch;
				});
				if (error?.code === "ENOENT" && hasLaterEpoch && replacementRetries > 0) {
					return scanJournalRoot(statePath, journalDirectory, options, replacementRetries - 1);
				}
				throw error;
			}
			if (!entry.isDirectory() || entry.isSymbolicLink?.()) throw new Error("Consumer high-water epoch entry must be one real directory.");
			try {
				await secureDirectory(path, "Consumer high-water epoch directory", options);
			} catch (error) {
				const epoch = Number(epochMatch[1]);
				const hasLaterEpoch = names.some((candidate) => {
					const match = epochPattern.exec(candidate);
					return match && Number(match[1]) > epoch;
				});
				if (error?.code === "ENOENT" && hasLaterEpoch && replacementRetries > 0) {
					return scanJournalRoot(statePath, journalDirectory, options, replacementRetries - 1);
				}
				throw error;
			}
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
	const authoritativeEntries = checkpointEntries.length + epochEntries.length + 1;
	if (authoritativeEntries > MAX_JOURNAL_ROOT_ENTRIES) {
		throw new Error("Consumer high-water journal root exceeds its safe entry bound.");
	}
	checkpointEntries.sort((left, right) => left.checkpoint.epoch - right.checkpoint.epoch);
	epochEntries.sort((left, right) => left.epoch - right.epoch);
	if (checkpointEntries.length > 2 || epochEntries.length > 2) {
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
	return { checkpointEntries, epochEntries, temporaries, head, missingHeadEpoch };
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
	return isProjectionReplacementTransient(error) || [
		"Consumer high-water journal epoch changed and fenced a paused writer.",
		"Consumer high-water journal checkpoint changed and fenced a paused writer.",
	].includes(error?.message);
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
	await revalidateAuthority(context, "scan-claims", options);
	await options.syncDirectory(context.epochDirectory);
	const names = await options.readDirectory(context.epochDirectory);
	if (names.length > options.maxJournalEntries + MAX_TEMPORARY_ENTRIES) {
		throw new Error("Consumer high-water epoch exceeds its safe allocation bound.");
	}
	const claimNames = new Map();
	const heartbeatNames = new Map();
	const terminalNames = new Map();
	const appliedNames = new Map();
	const temporaries = [];
	let authoritativeEntryCount = 0;
	for (const name of names) {
		let match;
		if ((match = claimPattern.exec(name))) {
			if (claimNames.has(Number(match[1]))) throw new Error("Consumer high-water lock contains a duplicate claim.");
			claimNames.set(Number(match[1]), name);
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
	for (const [generation, name] of [...claimNames].sort((left, right) => left[0] - right[0])) {
		const claim = await readExactMetadata(
			join(context.epochDirectory, name),
			options.metadataMaxBytes,
			(value) => validateClaim(value, context, options.stateMaxBytes),
			"Consumer high-water operation claim",
			options,
			budget,
		);
		if (claim.generation !== generation || name !== `claim-${generationName(generation)}.json`) {
			throw new Error("Consumer high-water lock claim name differs from its exact generation.");
		}
		claims.push(claim);
		byKey.set(`${generation}:${claim.token}`, claim);
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

async function finishCommitAtAuthenticatedDescendant(context, options) {
	const scan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
	const head = scan.head;
	if (
		!head || scan.missingHeadEpoch || head.checkpoint.epoch !== context.checkpoint.epoch + 1 ||
		head.checkpoint.previousCheckpointSha256 !== context.checkpointDigest ||
		head.checkpoint.retiredEpochDirectory !== basename(context.epochDirectory)
	) return false;
	const descendant = contextFromHead(context.statePath, context.guardPath, context.journalDirectory, head);
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
			await readApplied(context, claim, terminal, options);
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
	const scan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
	const nextCheckpointPath = join(context.journalDirectory, checkpointName(checkpoint));
	const nextEpochPath = join(context.journalDirectory, epochName(checkpoint));
	const allowedCheckpointPaths = new Set([context.checkpointPath, nextCheckpointPath]);
	const allowedEpochPaths = new Set([context.epochDirectory, nextEpochPath]);
	const currentCheckpoint = scan.checkpointEntries.find((entry) => entry.path === context.checkpointPath);
	const currentEpoch = scan.epochEntries.find((entry) => entry.path === context.epochDirectory);
	if (
		scan.checkpointEntries.some((entry) => !allowedCheckpointPaths.has(entry.path)) ||
		scan.epochEntries.some((entry) => !allowedEpochPaths.has(entry.path)) ||
		(currentCheckpoint && currentCheckpoint.digest !== context.checkpointDigest) ||
		!scan.epochEntries.some((entry) => entry.path === nextEpochPath) ||
		(!requirePublished && (!currentCheckpoint || !currentEpoch))
	) throw new Error("Consumer high-water rotation found a competing root or epoch publication.");
	const published = scan.checkpointEntries.find((entry) => entry.path === nextCheckpointPath);
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

async function tryCreateNormalClaim(context, generation, options) {
	const claim = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: randomUUID(),
		type: "normal",
		ownerPid: process.pid,
		createdAtMs: options.now(),
	};
	const result = await publishMetadata(claimPath(context, generation), claim, "claim", context, claim, options);
	if (!result.created) return null;
	validateClaim(result.value, context, options.stateMaxBytes);
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
	const result = await publishMetadata(claimPath(context, generation), claim, "claim", context, claim, options);
	if (!result.created) return null;
	validateClaim(result.value, context, options.stateMaxBytes);
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
				!claimPattern.test(name) && !heartbeatPattern.test(name) && !terminalPattern.test(name) &&
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
	const final = await scanJournalRoot(context.statePath, context.journalDirectory, options);
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
		final.checkpointEntries.length !== allowedCheckpoints || final.epochEntries.length !== expectedEpochs.size ||
		final.temporaries.some((temporary) => !temporaryProcessIsAlive(temporary, options)) ||
		final.head?.path !== context.checkpointPath ||
		final.epochEntries.some((entry) => !expectedEpochs.has(entry.path))
	) throw new Error("Consumer high-water journal did not converge to one bounded current epoch.");
}

async function helpRotationOperation(context, claim, options) {
	if (claim.type !== "rotation") throw new Error("Consumer high-water rotation helper requires one rotation operation slot.");
	const intent = validateRotationIntent(claim.intent, context, options.stateMaxBytes);
	const completedBeforeHelp = await completedRotationResult(context, intent, options).catch(() => null);
	if (completedBeforeHelp) return true;
	try {
		const scan = await scanEpoch(context, options);
		const latest = scan.claims.at(-1);
		if (operationIdentity(latest) !== operationIdentity(claim) || !metadataBytes(latest).equals(metadataBytes(claim))) {
			throw new Error("Consumer high-water rotation operation is not the unique latest slot.");
		}
		const tip = await effectiveTip(context, options);
		if (tip.tipDigest !== intent.tipSha256) {
			throw new Error("Consumer high-water rotation operation no longer matches its exact authoritative tip.");
		}
		const rootScan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
		const nextEpochName = epochName(intent.checkpoint);
		await cleanupAuthority(
			context,
			claim,
			rootScan,
			scan.temporaries,
			options,
			true,
			scan,
			nextEpochName,
		);
		await finishRotationCheckpoint(context, intent.checkpoint, claim, options);
		return true;
	} catch (error) {
		const completed = await completedRotationResult(context, intent, options).catch(() => null);
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
		maxJournalEntries: MAX_OPERATION_GENERATIONS * 4 + MAX_TRANSACTION_DEPTH + 32,
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
	options.maxJournalEntries = MAX_OPERATION_GENERATIONS * 4 + MAX_TRANSACTION_DEPTH + 32;
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
		if ((match = claimPattern.exec(name))) claimNames.set(Number(match[1]), name);
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
			const rootScan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
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

async function completedRotationResult(context, intent, options) {
	const scan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
	if (scan.head && metadataBytes(scan.head.checkpoint).equals(metadataBytes(intent.checkpoint))) {
		await scanRotationPublicationSet(context, intent.checkpoint, options, true);
		return { epoch: intent.checkpoint.epoch, tipSha256: intent.checkpoint.anchorDigest };
	}
	return null;
}

async function recoverCompletedCurrentRotation(context, scan, options) {
	if (context.checkpoint.epoch === 1 || scan.claims.length !== 0) return null;
	const tip = await effectiveTip(context, options);
	if (tip.length !== 0 || tip.tipDigest !== context.checkpoint.anchorDigest) return null;
	const writer = { generation: 0, token: context.checkpoint.epochId, type: "rotation" };
	await repairProjection(context, tip, options, writer);
	const rootScan = await scanJournalRoot(context.statePath, context.journalDirectory, options);
	await cleanupAuthority(context, writer, rootScan, scan.temporaries, options, false, scan);
	return { epoch: context.checkpoint.epoch, tipSha256: context.checkpoint.anchorDigest };
}

async function runRotation(statePath, rawOptions) {
	const options = normalizeRotationOptions(rawOptions);
	for (;;) {
		const { context } = await prepareContext(statePath, options);
		await ensureLegacyGuard(context, { generation: 0, token: context.checkpoint.epochId, type: "rotation" }, options);
		const initialScan = await scanEpoch(context, options);
		const completed = await recoverCompletedCurrentRotation(context, initialScan, options);
		if (completed) return completed;
		const frontier = await resolveOperationFrontier(context, options);
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
