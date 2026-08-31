import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
	link,
	lstat,
	mkdir,
	open,
	readdir,
	rename,
	rm,
} from "node:fs/promises";
import { basename, dirname, join, parse, relative, resolve, sep } from "node:path";

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
const LOCK_SCHEMA_VERSION = 1;
const TRANSACTION_SCHEMA_VERSION = 1;
const GENESIS_DIGEST = "0".repeat(64);
const DEFAULT_STATE_MAX_BYTES = 1024 * 1024;
const MAX_TRANSACTION_DEPTH = 4096;
const MAX_LOCK_GENERATIONS = 65_536;
const MAX_LOCK_ENTRIES = MAX_LOCK_GENERATIONS * 6;
const claimPattern = /^claim-([0-9]{16})\.json$/;
const transitionPattern = /^([0-9a-f]{64})\.json$/;
const lockEntryPattern = /^(?:claim-[0-9]{16}|(?:heartbeat|terminal|applied)-[0-9]{16}-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.json$/;

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
	if (!Number.isSafeInteger(generation) || generation < 1 || generation > 9_999_999_999_999_999) {
		throw new Error("Consumer high-water lock generation is exhausted or malformed.");
	}
	return String(generation).padStart(16, "0");
}

function claimPath(lockDirectory, generation) {
	return join(lockDirectory, `claim-${generationName(generation)}.json`);
}

function heartbeatPath(lockDirectory, claim) {
	return join(lockDirectory, `heartbeat-${generationName(claim.generation)}-${claim.token}.json`);
}

function terminalPath(lockDirectory, claim) {
	return join(lockDirectory, `terminal-${generationName(claim.generation)}-${claim.token}.json`);
}

function appliedPath(lockDirectory, claim) {
	return join(lockDirectory, `applied-${generationName(claim.generation)}-${claim.token}.json`);
}

function transitionPath(transactionDirectory, baseDigest) {
	return join(transactionDirectory, `${baseDigest}.json`);
}

function validateClaim(value) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "ownerPid", "createdAtMs"]) ||
		value.schemaVersion !== LOCK_SCHEMA_VERSION || !Number.isSafeInteger(value.generation) || value.generation < 1 ||
		!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.token ?? "") ||
		!Number.isSafeInteger(value.ownerPid) || value.ownerPid < 1 ||
		!Number.isSafeInteger(value.createdAtMs) || value.createdAtMs < 0
	) throw new Error("Consumer high-water lock claim is malformed.");
	return value;
}

function validateHeartbeat(value, claim) {
	if (
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
		!/^[0-9a-f]{64}$/.test(value.candidateDigest ?? "") ||
		typeof value.candidateBase64 !== "string" || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.candidateBase64)
	) throw new Error("Consumer high-water transaction is malformed.");
	const candidateBytes = Buffer.from(value.candidateBase64, "base64");
	if (
		candidateBytes.length < 1 || candidateBytes.length > stateMaxBytes ||
		candidateBytes.toString("base64") !== value.candidateBase64 || digest(candidateBytes) !== value.candidateDigest ||
		value.candidateDigest === value.baseDigest
	) throw new Error("Consumer high-water transaction payload is malformed.");
	return { value, candidateBytes };
}

function validateTerminal(value, claim, stateMaxBytes) {
	const common = ["schemaVersion", "generation", "token", "outcome"];
	if (
		!value || value.schemaVersion !== LOCK_SCHEMA_VERSION || value.generation !== claim.generation || value.token !== claim.token ||
		!["released", "retired", "commit"].includes(value.outcome)
	) throw new Error("Consumer high-water lock terminal marker is malformed.");
	if (value.outcome !== "commit") {
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
		value.terminalSha256 !== digest(metadataBytes(terminal))
	) throw new Error("Consumer high-water lock applied marker is malformed.");
	return value;
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
	{
		lstatEntry = lstat,
		makeDirectory = mkdir,
		syncDirectory = syncConsumerStateDirectory,
		create = true,
	} = {},
) {
	const absolute = resolve(directory);
	const root = parse(absolute).root;
	let parent = root;
	const rootEntry = await lstatEntry(root);
	if (!rootEntry.isDirectory()) {
		throw new Error("Consumer high-water state directory must be one canonical real directory.");
	}
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
		if (!entry.isDirectory()) {
			throw new Error("Consumer high-water state directory must be one canonical real directory.");
		}
		// This also flushes an entry observed after a concurrent creator made it.
		await syncDirectory(parent);
		parent = current;
	}
	return absolute;
}

async function revalidateBoundary(statePath, lockDirectory, transactionDirectory, operation, options) {
	await options.hooks?.beforePathOperation?.({ operation, statePath, lockDirectory, transactionDirectory });
	const directory = dirname(statePath);
	await ensureDurableConsumerStateDirectory(directory, { ...options.directoryOperations, create: false });
	for (const internalDirectory of [lockDirectory, transactionDirectory]) {
		let entry;
		try {
			entry = await options.lstatEntry(internalDirectory);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			continue;
		}
		if (!entry.isDirectory()) throw new Error("Consumer high-water metadata path must be one real directory.");
		await options.syncDirectory(directory);
	}
}

async function ensureInternalDirectory(statePath, lockDirectory, transactionDirectory, path, kind, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, kind, options);
	try {
		await options.makeDirectory(path, { mode: 0o700 });
	} catch (error) {
		if (error?.code !== "EEXIST") throw error;
	}
	const entry = await options.lstatEntry(path);
	if (!entry.isDirectory()) throw new Error("Consumer high-water metadata path must be one real directory.");
	await options.syncDirectory(dirname(statePath));
}

async function readPinnedFile(path, maxBytes, description, options) {
	let handle;
	try {
		handle = await options.openFile(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		if (["ELOOP", "EISDIR"].includes(error?.code)) throw new Error(`${description} is not one regular non-symlink file.`);
		throw error;
	}
	try {
		const stat = await handle.stat();
		if (!stat.isFile()) throw new Error(`${description} is not one regular non-symlink file.`);
		if (stat.size < 1 || stat.size > maxBytes) throw new Error(`${description} is malformed.`);
		const bytes = await handle.readFile();
		if (bytes.length !== stat.size) throw new Error(`${description} changed while it was read.`);
		return bytes;
	} finally {
		await handle.close();
	}
}

async function readExactMetadata(path, maxBytes, validate, description, options) {
	const bytes = await readPinnedFile(path, maxBytes, description, options);
	if (bytes === null) return null;
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

async function publishImmutable({
	path, bytes, directory, kind, statePath, lockDirectory, transactionDirectory, options,
}) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, kind, options);
	const temporary = join(directory, `.${basename(path)}.${process.pid}.${randomUUID()}.tmp`);
	let handle;
	let linked = false;
	try {
		handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
		await handle.writeFile(bytes);
		await handle.sync();
		await handle.close();
		handle = undefined;
		await options.hooks?.afterFileSync?.({ kind, path, temporary });
		await revalidateBoundary(statePath, lockDirectory, transactionDirectory, `${kind}-link`, options);
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
	}
}

async function publishMetadata(path, value, kind, statePath, lockDirectory, transactionDirectory, options) {
	const bytes = metadataBytes(value);
	const created = await publishImmutable({
		path, bytes, directory: dirname(path), kind, statePath, lockDirectory, transactionDirectory, options,
	});
	if (created) return { value, created: true };
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, `${kind}-existing`, options);
	const existing = await readExactMetadata(
		path, options.metadataMaxBytes, (candidate) => candidate, "Consumer high-water lock metadata", options,
	);
	return { value: existing, created: false };
}

async function readProjection(statePath, lockDirectory, transactionDirectory, operation, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, operation, options);
	let handle;
	try {
		handle = await options.openFile(statePath, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	} catch (error) {
		if (error?.code === "ENOENT") return { exists: false, bytes: null, sha256: null, malformed: false };
		if (["ELOOP", "EISDIR"].includes(error?.code)) {
			throw new Error("Consumer high-water state is not one regular non-symlink file.");
		}
		throw error;
	}
	try {
		const stat = await handle.stat();
		if (!stat.isFile()) throw new Error("Consumer high-water state is not one regular non-symlink file.");
		if (stat.size < 1 || stat.size > options.stateMaxBytes) {
			return { exists: true, bytes: null, sha256: null, malformed: true };
		}
		const bytes = await handle.readFile();
		if (bytes.length !== stat.size) throw new Error("Consumer high-water state changed while it was read.");
		return { exists: true, bytes, sha256: digest(bytes), malformed: false };
	} finally {
		await handle.close();
	}
}

async function walkTransactions(statePath, lockDirectory, transactionDirectory, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "walk-transactions", options);
	await options.syncDirectory(transactionDirectory);
	const entries = await options.readDirectory(transactionDirectory);
	if (entries.length > options.maxTransactionDepth * 2) {
		throw new Error("Consumer high-water transaction directory exceeds its safe entry bound.");
	}
	const named = new Map();
	for (const name of entries) {
		if (name.startsWith(".")) continue;
		const match = transitionPattern.exec(name);
		if (!match || named.has(match[1])) throw new Error("Consumer high-water transaction directory contains a malformed entry.");
		named.set(match[1], name);
	}
	const visited = new Set();
	let tipDigest = GENESIS_DIGEST;
	let tipBytes = null;
	for (let depth = 0; named.has(tipDigest); depth += 1) {
		if (depth >= options.maxTransactionDepth || visited.has(tipDigest)) {
			throw new Error("Consumer high-water transaction chain is cyclic or exceeds its safe bound.");
		}
		visited.add(tipDigest);
		const path = transitionPath(transactionDirectory, tipDigest);
		await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "read-transition", options);
		const value = await readExactMetadata(
			path, options.metadataMaxBytes,
			(candidate) => validateTransaction(candidate, tipDigest, options.stateMaxBytes).value,
			"Consumer high-water transaction", options,
		);
		const validated = validateTransaction(value, tipDigest, options.stateMaxBytes);
		tipDigest = value.candidateDigest;
		tipBytes = validated.candidateBytes;
	}
	if (visited.size !== named.size) throw new Error("Consumer high-water transaction chain contains an unreachable transition.");
	return { tipDigest, tipBytes, length: visited.size };
}

async function repairProjection(statePath, lockDirectory, transactionDirectory, initialTip, options) {
	let tip = initialTip;
	for (let attempt = 0; attempt < 8; attempt += 1) {
		if (tip.tipBytes === null) return tip;
		const projection = await readProjection(statePath, lockDirectory, transactionDirectory, "projection-read", options);
		if (projection.sha256 !== tip.tipDigest) {
			await options.hooks?.beforeProjectionWrite?.({ tipDigest: tip.tipDigest });
			await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "projection-write", options);
			const temporary = join(dirname(statePath), `.${basename(statePath)}.${process.pid}.${randomUUID()}.tmp`);
			let handle;
			try {
				handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
				await handle.writeFile(tip.tipBytes);
				await handle.sync();
				await handle.close();
				handle = undefined;
				await options.hooks?.afterProjectionFileSync?.({ tipDigest: tip.tipDigest, temporary });
				await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "projection-rename", options);
				await options.renameFile(temporary, statePath);
				await options.hooks?.afterProjectionRename?.({ tipDigest: tip.tipDigest });
				await options.syncDirectory(dirname(statePath));
				await options.hooks?.afterProjectionDirectorySync?.({ tipDigest: tip.tipDigest });
			} finally {
				if (handle !== undefined) await handle.close();
				await options.removeFile(temporary, { force: true });
			}
		}
		const latest = await walkTransactions(statePath, lockDirectory, transactionDirectory, options);
		if (latest.tipDigest === tip.tipDigest) return latest;
		tip = latest;
	}
	throw new Error("Consumer high-water projection could not catch up with its immutable transaction tip.");
}

async function publishTransition(statePath, lockDirectory, transactionDirectory, transaction, options) {
	validateTransaction(transaction, transaction.baseDigest, options.stateMaxBytes);
	const path = transitionPath(transactionDirectory, transaction.baseDigest);
	const result = await publishMetadata(
		path, transaction, "transition", statePath, lockDirectory, transactionDirectory, options,
	);
	const existing = validateTransaction(result.value, transaction.baseDigest, options.stateMaxBytes).value;
	if (!metadataBytes(existing).equals(metadataBytes(transaction))) {
		throw new Error("Consumer high-water transaction lost its immutable base-digest compare-and-set.");
	}
}

async function scanClaims(statePath, lockDirectory, transactionDirectory, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "scan-claims", options);
	await options.syncDirectory(lockDirectory);
	const entries = await options.readDirectory(lockDirectory);
	if (entries.length > MAX_LOCK_ENTRIES) throw new Error("Consumer high-water lock directory exceeds its safe entry bound.");
	const claims = [];
	for (const name of entries) {
		if (name.startsWith(".")) continue;
		if (!lockEntryPattern.test(name)) throw new Error("Consumer high-water lock directory contains a malformed entry.");
		const match = claimPattern.exec(name);
		if (!match) continue;
		const generation = Number(match[1]);
		await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "read-claim", options);
		const claim = await readExactMetadata(
			join(lockDirectory, name), options.metadataMaxBytes, validateClaim, "Consumer high-water lock claim", options,
		);
		if (claim.generation !== generation || name !== `claim-${generationName(generation)}.json`) {
			throw new Error("Consumer high-water lock claim name differs from its exact generation.");
		}
		claims.push(claim);
	}
	claims.sort((left, right) => left.generation - right.generation);
	if (claims.length > MAX_LOCK_GENERATIONS) throw new Error("Consumer high-water lock generation bound is exhausted.");
	for (let index = 0; index < claims.length; index += 1) {
		if (claims[index].generation !== index + 1) throw new Error("Consumer high-water lock generations are not contiguous.");
	}
	return claims;
}

async function readTerminal(statePath, lockDirectory, transactionDirectory, claim, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "read-terminal", options);
	return readExactMetadata(
		terminalPath(lockDirectory, claim), options.metadataMaxBytes,
		(value) => validateTerminal(value, claim, options.stateMaxBytes),
		"Consumer high-water lock terminal marker", options,
	);
}

async function readHeartbeat(statePath, lockDirectory, transactionDirectory, claim, options) {
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "read-heartbeat", options);
	const heartbeat = await readExactMetadata(
		heartbeatPath(lockDirectory, claim), options.metadataMaxBytes,
		(value) => validateHeartbeat(value, claim), "Consumer high-water lock heartbeat", options,
	);
	return heartbeat ?? { ...claim, refreshedAtMs: claim.createdAtMs };
}

async function publishTerminal(statePath, lockDirectory, transactionDirectory, claim, wanted, options) {
	const result = await publishMetadata(
		terminalPath(lockDirectory, claim), wanted, `terminal-${wanted.outcome}`,
		statePath, lockDirectory, transactionDirectory, options,
	);
	return validateTerminal(result.value, claim, options.stateMaxBytes);
}

async function refreshHeartbeat(statePath, lockDirectory, transactionDirectory, claim, options) {
	if (await readTerminal(statePath, lockDirectory, transactionDirectory, claim, options) !== null) return false;
	const value = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		refreshedAtMs: options.now(),
	};
	const path = heartbeatPath(lockDirectory, claim);
	await revalidateBoundary(statePath, lockDirectory, transactionDirectory, "heartbeat", options);
	const temporary = join(lockDirectory, `.${basename(path)}.${process.pid}.${randomUUID()}.tmp`);
	let handle;
	try {
		handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
		await handle.writeFile(metadataBytes(value));
		await handle.sync();
		await handle.close();
		handle = undefined;
		if (await readTerminal(statePath, lockDirectory, transactionDirectory, claim, options) !== null) return false;
		await options.renameFile(temporary, path);
		return true;
	} finally {
		if (handle !== undefined) await handle.close();
		await options.removeFile(temporary, { force: true });
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

async function publishApplied(statePath, lockDirectory, transactionDirectory, claim, terminal, options) {
	const value = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		terminalSha256: digest(metadataBytes(terminal)),
	};
	const result = await publishMetadata(
		appliedPath(lockDirectory, claim), value, "applied", statePath, lockDirectory, transactionDirectory, options,
	);
	validateApplied(result.value, claim, terminal);
	await options.hooks?.afterApplied?.({ claim, terminal });
}

async function finishCommit(statePath, lockDirectory, transactionDirectory, claim, terminal, options) {
	for (const transaction of terminal.transactions) {
		await publishTransition(statePath, lockDirectory, transactionDirectory, transaction, options);
	}
	const tip = await walkTransactions(statePath, lockDirectory, transactionDirectory, options);
	await repairProjection(statePath, lockDirectory, transactionDirectory, tip, options);
	await publishApplied(statePath, lockDirectory, transactionDirectory, claim, terminal, options);
}

async function resolveLatestClaim(statePath, lockDirectory, transactionDirectory, claim, options) {
	const terminal = await readTerminal(statePath, lockDirectory, transactionDirectory, claim, options);
	if (terminal?.outcome === "commit") {
		await finishCommit(statePath, lockDirectory, transactionDirectory, claim, terminal, options);
		return true;
	}
	if (terminal !== null) return true;
	const heartbeat = await readHeartbeat(statePath, lockDirectory, transactionDirectory, claim, options);
	if (options.now() - heartbeat.refreshedAtMs < options.stale) return false;
	await options.hooks?.afterObserveStale?.({ claim, heartbeat });
	const retired = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation: claim.generation,
		token: claim.token,
		outcome: "retired",
	};
	const decision = await publishTerminal(
		statePath, lockDirectory, transactionDirectory, claim, retired, options,
	);
	await options.hooks?.afterRetire?.({ claim, decision });
	if (decision.outcome === "commit") {
		await finishCommit(statePath, lockDirectory, transactionDirectory, claim, decision, options);
	}
	return true;
}

async function tryCreateClaim(statePath, lockDirectory, transactionDirectory, generation, options) {
	const claim = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: randomUUID(),
		ownerPid: process.pid,
		createdAtMs: options.now(),
	};
	const heartbeat = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: claim.token,
		refreshedAtMs: claim.createdAtMs,
	};
	await publishImmutable({
		path: heartbeatPath(lockDirectory, claim), bytes: metadataBytes(heartbeat), directory: lockDirectory,
		kind: "initial-heartbeat", statePath, lockDirectory, transactionDirectory, options,
	});
	const result = await publishMetadata(
		claimPath(lockDirectory, generation), claim, "claim", statePath, lockDirectory, transactionDirectory, options,
	);
	if (!result.created) return null;
	validateClaim(result.value);
	await options.hooks?.afterClaim?.({ claim });
	return claim;
}

async function acquireClaim(statePath, lockDirectory, transactionDirectory, options) {
	for (;;) {
		const claims = await scanClaims(statePath, lockDirectory, transactionDirectory, options);
		const latest = claims.at(-1);
		if (latest && !await resolveLatestClaim(statePath, lockDirectory, transactionDirectory, latest, options)) {
			throw new Error(`Consumer high-water state is actively locked: ${lockDirectory}`);
		}
		const nextGeneration = (latest?.generation ?? 0) + 1;
		if (nextGeneration > MAX_LOCK_GENERATIONS) throw new Error("Consumer high-water lock generation bound is exhausted.");
		const claim = await tryCreateClaim(
			statePath, lockDirectory, transactionDirectory, nextGeneration, options,
		);
		if (claim) return claim;
	}
}

function normalizeOptions({
	stale = PYLON_CONSUMER_LOCK_STALE_MS,
	update = PYLON_CONSUMER_LOCK_UPDATE_MS,
	stateMaxBytes = DEFAULT_STATE_MAX_BYTES,
	maxTransactionDepth = MAX_TRANSACTION_DEPTH,
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
} = {}) {
	if (
		!Number.isSafeInteger(stale) || !Number.isSafeInteger(update) || update < 1 || stale <= update ||
		!Number.isSafeInteger(stateMaxBytes) || stateMaxBytes < 1 || stateMaxBytes > 16 * 1024 * 1024 ||
		!Number.isSafeInteger(maxTransactionDepth) || maxTransactionDepth < 1
	) throw new Error("Consumer high-water lock timing, state-size, or transaction bound is invalid.");
	return {
		stale, update, stateMaxBytes, maxTransactionDepth, metadataMaxBytes: stateMaxBytes * 3 + 4096,
		now, startHeartbeat, hooks, directoryOperations, lstatEntry, makeDirectory, syncDirectory,
		openFile, linkFile, readDirectory, renameFile, removeFile,
	};
}

export async function withConsumerStateLock(statePath, action, rawOptions = {}) {
	if (typeof action !== "function") throw new Error("Consumer high-water lock action must be a function.");
	const options = normalizeOptions(rawOptions);
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	await ensureDurableConsumerStateDirectory(directory, options.directoryOperations);
	const lockDirectory = `${absoluteStatePath}.lock`;
	const transactionDirectory = `${absoluteStatePath}.transactions`;
	await ensureInternalDirectory(
		absoluteStatePath, lockDirectory, transactionDirectory, lockDirectory, "lock-directory", options,
	);
	await ensureInternalDirectory(
		absoluteStatePath, lockDirectory, transactionDirectory, transactionDirectory, "transaction-directory", options,
	);
	const claim = await acquireClaim(absoluteStatePath, lockDirectory, transactionDirectory, options);
	let terminal = null;
	let heartbeatStopped = false;
	const stopHeartbeat = options.startHeartbeat({
		interval: options.update,
		beat: () => refreshHeartbeat(absoluteStatePath, lockDirectory, transactionDirectory, claim, options),
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
		terminal = await publishTerminal(
			absoluteStatePath, lockDirectory, transactionDirectory, claim, wanted, options,
		);
		if (terminal.outcome !== "released") {
			throw new Error("Consumer high-water lock ownership was retired before release.", { cause });
		}
	};
	try {
		let chain = await walkTransactions(absoluteStatePath, lockDirectory, transactionDirectory, options);
		let legacyBytes = null;
		if (chain.tipBytes === null) {
			const legacy = await readProjection(
				absoluteStatePath, lockDirectory, transactionDirectory, "legacy-state-read", options,
			);
			if (legacy.malformed) throw new Error("Consumer high-water state is malformed.");
			legacyBytes = legacy.bytes;
		} else {
			chain = await repairProjection(absoluteStatePath, lockDirectory, transactionDirectory, chain, options);
		}
		const baseBytes = chain.tipBytes ?? legacyBytes;
		const baseDigest = baseBytes === null ? GENESIS_DIGEST : digest(baseBytes);
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
				throw new Error("Consumer high-water transaction chain exceeds its safe bound.");
			}
			const wanted = {
				schemaVersion: LOCK_SCHEMA_VERSION,
				generation: claim.generation,
				token: claim.token,
				outcome: "commit",
				transactions,
			};
			await options.hooks?.beforeCommitDecision?.({ claim, transactions });
			// This immutable decision is the only gate to transition publication.
			// A retirement winner is permanent; a complete commit winner is helpable.
			terminal = await publishTerminal(
				absoluteStatePath, lockDirectory, transactionDirectory, claim, wanted, options,
			);
			if (terminal.outcome !== "commit" || !metadataBytes(terminal).equals(metadataBytes(wanted))) {
				throw new Error("Consumer high-water transaction lost ownership before its commit decision.");
			}
			await options.hooks?.afterCommitDecision?.({ claim, terminal });
			await finishCommit(absoluteStatePath, lockDirectory, transactionDirectory, claim, terminal, options);
			return true;
		};
		const transaction = Object.freeze({
			readStateBytes: () => baseBytes === null ? null : Buffer.from(baseBytes),
			commitState: async (value) => {
				if (terminal !== null) throw new Error("Consumer high-water transaction already has a terminal decision.");
				const bytes = Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(value);
				if (bytes.length < 1 || bytes.length > options.stateMaxBytes) throw new Error("Consumer high-water state is malformed.");
				await commitTransactions(bytes);
			},
		});
		let result;
		let actionError;
		try {
			result = await action(absoluteStatePath, transaction);
		} catch (error) {
			actionError = error;
		}
		await stopHeartbeatOnce();
		if (terminal === null && actionError === undefined && legacyBytes !== null) {
			await commitTransactions(null);
		}
		await release(actionError);
		if (actionError !== undefined) throw actionError;
		return result;
	} catch (error) {
		await stopHeartbeatOnce();
		await release(error);
		throw error;
	}
}
