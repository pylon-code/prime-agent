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
const TRANSACTION_SCHEMA_VERSION = 1;
const CHECKPOINT_SCHEMA_VERSION = 1;
const LEGACY_GUARD_SCHEMA_VERSION = 1;
const GENESIS_DIGEST = "0".repeat(64);
const DEFAULT_STATE_MAX_BYTES = 1024 * 1024;
const DEFAULT_JOURNAL_MAX_BYTES = 64 * 1024 * 1024;
const MAX_TRANSACTION_DEPTH = 4096;
const MAX_LOCK_GENERATIONS = 65_536;
const MAX_JOURNAL_ROOT_ENTRIES = 16;
const uuidSource = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const uuidPattern = new RegExp(`^${uuidSource}$`);
const claimPattern = /^claim-([0-9]{16})\.json$/;
const transitionPattern = /^transition-([0-9a-f]{64})\.json$/;
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

function validateClaim(value) {
	if (
		!exactKeys(value, ["schemaVersion", "generation", "token", "ownerPid", "createdAtMs"]) ||
		value.schemaVersion !== LOCK_SCHEMA_VERSION || !Number.isSafeInteger(value.generation) || value.generation < 1 ||
		!uuidPattern.test(value.token ?? "") || !Number.isSafeInteger(value.ownerPid) || value.ownerPid < 1 ||
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
			"historySha256", "anchorDigest", "anchorBase64", "retiredEpochDirectory",
		]) || value.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !Number.isSafeInteger(value.epoch) || value.epoch < 1 ||
		!uuidPattern.test(value.epochId ?? "") || !/^[0-9a-f]{64}$/.test(value.previousCheckpointSha256 ?? "") ||
		!/^[0-9a-f]{64}$/.test(value.previousTipSha256 ?? "") || !/^[0-9a-f]{64}$/.test(value.historySha256 ?? "") ||
		!/^[0-9a-f]{64}$/.test(value.anchorDigest ?? "") ||
		!(value.retiredEpochDirectory === null || epochPattern.test(value.retiredEpochDirectory)) ||
		!(value.anchorBase64 === null || typeof value.anchorBase64 === "string")
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

function validateTerminal(value, claim, stateMaxBytes) {
	const common = ["schemaVersion", "generation", "token", "outcome"];
	if (
		!value || value.schemaVersion !== LOCK_SCHEMA_VERSION || value.generation !== claim.generation ||
		value.token !== claim.token || !["released", "retired", "commit", "rotate"].includes(value.outcome)
	) throw new Error("Consumer high-water lock terminal marker is malformed.");
	if (["released", "retired"].includes(value.outcome)) {
		if (!exactKeys(value, common)) throw new Error("Consumer high-water lock terminal marker is malformed.");
		return value;
	}
	if (value.outcome === "rotate") {
		if (!exactKeys(value, [...common, "checkpoint"])) throw new Error("Consumer high-water rotation marker is malformed.");
		validateCheckpoint(value.checkpoint, stateMaxBytes);
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

function legacyGuardFor(statePath) {
	return {
		schemaVersion: LEGACY_GUARD_SCHEMA_VERSION,
		kind: "pylon-consumer-legacy-lock-guard",
		statePathSha256: digest(Buffer.from(statePath)),
	};
}

async function secureHandle(handle, stat, description, type, options) {
	if ((type === "file" && !stat.isFile()) || (type === "directory" && !stat.isDirectory())) {
		throw new Error(`${description} must be one real ${type}.`);
	}
	if (options.currentUid === null) return stat;
	if (stat.uid !== options.currentUid) throw new Error(`${description} must be owned by the current uid.`);
	if ((stat.mode & 0o022) !== 0) {
		await handle.chmod(type === "directory" ? 0o700 : 0o600);
		stat = await handle.stat();
	}
	if (
		stat.uid !== options.currentUid || (stat.mode & 0o022) !== 0 ||
		(type === "file" && !stat.isFile()) || (type === "directory" && !stat.isDirectory())
	) throw new Error(`${description} has unsafe owner or write permissions.`);
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
		"terminal-released", "terminal-retired", "terminal-commit", "terminal-rotate", "applied", "legacy-guard",
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
	await secureDirectory(context.epochDirectory, "Consumer high-water epoch directory", options);
	const entries = await options.readDirectory(context.journalDirectory);
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

async function publishImmutable({ path, bytes, directory, kind, context, writer, options, revalidate = true }) {
	if (revalidate) await revalidateAuthority(context, kind, options);
	const temporary = join(directory, temporaryName(path, kind, writer, context));
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
	};
}

async function scanJournalRoot(statePath, journalDirectory, options) {
	await secureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	await options.syncDirectory(journalDirectory);
	const names = await options.readDirectory(journalDirectory);
	if (names.length > MAX_JOURNAL_ROOT_ENTRIES) throw new Error("Consumer high-water journal root exceeds its safe entry bound.");
	const checkpointEntries = [];
	const epochEntries = [];
	const temporaries = [];
	for (const name of names) {
		const path = join(journalDirectory, name);
		const checkpointMatch = checkpointPattern.exec(name);
		if (checkpointMatch) {
			const checkpoint = await readExactMetadata(
				path,
				options.metadataMaxBytes,
				(value) => validateCheckpoint(value, options.stateMaxBytes).value,
				"Consumer high-water journal checkpoint",
				options,
			);
			if (checkpointName(checkpoint) !== name || checkpoint.epoch !== Number(checkpointMatch[1])) {
				throw new Error("Consumer high-water journal checkpoint name is malformed.");
			}
			checkpointEntries.push({ name, path, checkpoint, digest: digest(metadataBytes(checkpoint)) });
			continue;
		}
		const epochMatch = epochPattern.exec(name);
		if (epochMatch) {
			const entry = await options.lstatEntry(path);
			if (!entry.isDirectory() || entry.isSymbolicLink?.()) throw new Error("Consumer high-water epoch entry must be one real directory.");
			await secureDirectory(path, "Consumer high-water epoch directory", options);
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
	checkpointEntries.sort((left, right) => left.checkpoint.epoch - right.checkpoint.epoch);
	if (checkpointEntries.length > 2 || epochEntries.length > 2) {
		throw new Error("Consumer high-water journal root contains unbounded checkpoint metadata.");
	}
	for (let index = 1; index < checkpointEntries.length; index += 1) {
		if (checkpointEntries[index - 1].checkpoint.epoch + 1 !== checkpointEntries[index].checkpoint.epoch) {
			throw new Error("Consumer high-water journal checkpoints are not contiguous.");
		}
	}
	const head = checkpointEntries.at(-1) ?? null;
	if (head) {
		const previous = checkpointEntries.at(-2);
		if (previous && (
			head.checkpoint.previousCheckpointSha256 !== previous.digest ||
			head.checkpoint.retiredEpochDirectory !== epochName(previous.checkpoint) ||
			head.checkpoint.historySha256 !== digest(Buffer.from(
				`${previous.checkpoint.historySha256}:${previous.digest}:${head.checkpoint.anchorDigest}`,
			))
		)) throw new Error("Consumer high-water journal checkpoint does not anchor its exact predecessor.");
	}
	const missingHeadEpoch = head ? !epochEntries.some((entry) => entry.name === epochName(head.checkpoint)) : false;
	return { checkpointEntries, epochEntries, temporaries, head, missingHeadEpoch };
}

async function initializeJournal(statePath, journalDirectory, options) {
	let scan = await scanJournalRoot(statePath, journalDirectory, options);
	if (scan.head) {
		if (!scan.missingHeadEpoch) return scan;
		const expectedGenesis = genesisCheckpoint(statePath);
		if (
			scan.head.checkpoint.epoch !== 1 || !metadataBytes(scan.head.checkpoint).equals(metadataBytes(expectedGenesis)) ||
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
	const checkpoint = genesisCheckpoint(statePath);
	const bootstrap = { generation: 0, token: randomUUID() };
	const bootstrapContext = {
		statePath,
		journalDirectory,
		checkpoint,
		checkpointPath: join(journalDirectory, checkpointName(checkpoint)),
		checkpointDigest: digest(metadataBytes(checkpoint)),
		epochDirectory: join(journalDirectory, epochName(checkpoint)),
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
	if (entries.length > options.maxJournalEntries) throw new Error("Consumer high-water epoch exceeds its safe entry bound.");
	const named = new Map();
	for (const name of entries) {
		const match = transitionPattern.exec(name);
		if (match) {
			if (named.has(match[1])) throw new Error("Consumer high-water journal contains a duplicate transition.");
			named.set(match[1], name);
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

async function repairProjection(context, initialTip, options, writer = options.activeWriter) {
	let tip = initialTip;
	for (let attempt = 0; attempt < 8; attempt += 1) {
		if (tip.tipBytes === null) return tip;
		const projection = await readProjection(context, "projection-read", options);
		if (projection.sha256 !== tip.tipDigest) {
			await options.hooks?.beforeProjectionWrite?.({ tipDigest: tip.tipDigest });
			await revalidateAuthority(context, "projection-write", options);
			const temporary = join(dirname(context.statePath), temporaryName(context.statePath, "projection", writer, context));
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
				await options.syncDirectory(dirname(context.statePath));
				await options.hooks?.afterProjectionDirectorySync?.({ tipDigest: tip.tipDigest });
			} finally {
				if (handle !== undefined) await handle.close();
				await options.removeFile(temporary, { force: true });
			}
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
	if (names.length > options.maxJournalEntries) throw new Error("Consumer high-water epoch exceeds its safe entry bound.");
	const claimNames = new Map();
	const heartbeatNames = new Map();
	const terminalNames = new Map();
	const appliedNames = new Map();
	const temporaries = [];
	for (const name of names) {
		let match;
		if ((match = claimPattern.exec(name))) {
			if (claimNames.has(Number(match[1]))) throw new Error("Consumer high-water lock contains a duplicate claim.");
			claimNames.set(Number(match[1]), name);
		} else if ((match = heartbeatPattern.exec(name))) {
			heartbeatNames.set(`${Number(match[1])}:${match[2]}`, name);
		} else if ((match = terminalPattern.exec(name))) {
			terminalNames.set(`${Number(match[1])}:${match[2]}`, name);
		} else if ((match = appliedPattern.exec(name))) {
			appliedNames.set(`${Number(match[1])}:${match[2]}`, name);
		} else if (transitionPattern.test(name)) {
			// Validated by the transaction walk before any state decision.
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
	const budget = { bytes: 0 };
	const claims = [];
	const byKey = new Map();
	for (const [generation, name] of [...claimNames].sort((left, right) => left[0] - right[0])) {
		const claim = await readExactMetadata(
			join(context.epochDirectory, name),
			options.metadataMaxBytes,
			validateClaim,
			"Consumer high-water lock claim",
			options,
			budget,
		);
		if (claim.generation !== generation || name !== `claim-${generationName(generation)}.json`) {
			throw new Error("Consumer high-water lock claim name differs from its exact generation.");
		}
		claims.push(claim);
		byKey.set(`${generation}:${claim.token}`, claim);
	}
	if (claims.length > options.maxLockGenerations) throw new Error("Consumer high-water lock generation bound is exhausted.");
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
	}
	return { claims, temporaries };
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
	const temporary = join(context.epochDirectory, temporaryName(path, "heartbeat", claim, context));
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
		await options.syncDirectory(context.epochDirectory);
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

async function finishCommit(context, claim, terminal, options) {
	for (const transaction of terminal.transactions) await publishTransition(context, transaction, claim, options);
	const tip = await walkTransactions(context, options);
	await repairProjection(context, tip, options, claim);
	await publishApplied(context, claim, terminal, options);
}

function rotationCheckpoint(context, tip) {
	const checkpoint = {
		schemaVersion: CHECKPOINT_SCHEMA_VERSION,
		epoch: context.checkpoint.epoch + 1,
		epochId: randomUUID(),
		previousCheckpointSha256: context.checkpointDigest,
		previousTipSha256: tip.tipDigest,
		historySha256: digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${tip.tipDigest}`,
		)),
		anchorDigest: tip.tipDigest,
		anchorBase64: tip.tipBytes === null ? null : tip.tipBytes.toString("base64"),
		retiredEpochDirectory: basename(context.epochDirectory),
	};
	validateCheckpoint(checkpoint, Number.MAX_SAFE_INTEGER);
	return checkpoint;
}

async function finishRotation(context, claim, terminal, options) {
	const checkpoint = validateCheckpoint(terminal.checkpoint, options.stateMaxBytes).value;
	if (
		checkpoint.epoch !== context.checkpoint.epoch + 1 ||
		checkpoint.previousCheckpointSha256 !== context.checkpointDigest ||
		checkpoint.retiredEpochDirectory !== basename(context.epochDirectory) ||
		checkpoint.historySha256 !== digest(Buffer.from(
			`${context.checkpoint.historySha256}:${context.checkpointDigest}:${checkpoint.anchorDigest}`,
		))
	) throw new Error("Consumer high-water rotation does not anchor the exact current epoch.");
	let tip = await walkTransactions(context, options);
	if (tip.tipBytes === null && checkpoint.anchorBase64 !== null) {
		const legacy = await readProjection(context, "rotation-legacy-state-read", options);
		if (legacy.malformed || legacy.bytes === null) {
			throw new Error("Consumer high-water rotation cannot authenticate its legacy projection anchor.");
		}
		tip = { tipDigest: digest(legacy.bytes), tipBytes: legacy.bytes, length: tip.length };
	}
	if (
		checkpoint.previousTipSha256 !== tip.tipDigest || checkpoint.anchorDigest !== tip.tipDigest ||
		(checkpoint.anchorBase64 === null ? tip.tipBytes !== null : !Buffer.from(checkpoint.anchorBase64, "base64").equals(tip.tipBytes))
	) throw new Error("Consumer high-water rotation does not anchor the exact immutable tip.");
	const nextEpoch = join(context.journalDirectory, epochName(checkpoint));
	await ensureDirectory(nextEpoch, "Consumer high-water epoch directory", options);
	await options.hooks?.afterRotationEpochSync?.({ checkpoint, nextEpoch });
	const nextPath = join(context.journalDirectory, checkpointName(checkpoint));
	await publishImmutable({
		path: nextPath,
		bytes: metadataBytes(checkpoint),
		directory: context.journalDirectory,
		kind: "checkpoint",
		context,
		writer: claim,
		options,
	});
	await options.hooks?.afterRotationCheckpoint?.({ checkpoint, nextPath });
}

async function resolveLatestClaim(context, claim, options) {
	const terminal = await readTerminal(context, claim, options);
	if (terminal?.outcome === "commit") {
		await finishCommit(context, claim, terminal, options);
		return "resolved";
	}
	if (terminal?.outcome === "rotate") {
		await finishRotation(context, claim, terminal, options);
		return "rotated";
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
	if (decision.outcome === "rotate") {
		await finishRotation(context, claim, decision, options);
		return "rotated";
	}
	return "resolved";
}

async function tryCreateClaim(context, generation, options) {
	const claim = {
		schemaVersion: LOCK_SCHEMA_VERSION,
		generation,
		token: randomUUID(),
		ownerPid: process.pid,
		createdAtMs: options.now(),
	};
	const result = await publishMetadata(claimPath(context, generation), claim, "claim", context, claim, options);
	if (!result.created) return null;
	validateClaim(result.value);
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

async function acquireClaim(context, options, forRotation) {
	for (;;) {
		const scan = await scanEpoch(context, options);
		const latest = scan.claims.at(-1);
		if (latest) {
			const resolved = await resolveLatestClaim(context, latest, options);
			if (resolved === "rotated") return { rotated: true };
			if (resolved === "active") throw new Error(`Consumer high-water state is actively locked: ${context.journalDirectory}`);
		}
		const nextGeneration = (latest?.generation ?? 0) + 1;
		if (nextGeneration > options.maxLockGenerations) {
			throw new Error("Consumer high-water claim epoch is exhausted; run the consumer journal rotation command.");
		}
		if (!forRotation && nextGeneration === options.maxLockGenerations) {
			throw new Error("Consumer high-water claim reserve was reached; run the consumer journal rotation command.");
		}
		const claim = await tryCreateClaim(context, nextGeneration, options);
		if (claim) return { claim, temporaries: scan.temporaries, rotated: false };
	}
}

function temporaryIsFenced(temporary, context, claim) {
	if (temporary.epochId !== context.checkpoint.epochId) return true;
	if (temporary.generation === 0 || temporary.generation < claim.generation) return true;
	return temporary.generation === claim.generation && temporary.token !== claim.token;
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

async function cleanupAuthority(context, claim, rootScan, epochTemporaries, options, requireQuiescent) {
	await revalidateAuthority(context, "cleanup", options);
	const candidates = [...rootScan.temporaries, ...epochTemporaries];
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
		candidates.push(temporary);
	}
	for (const temporary of candidates) {
		const fenced = temporaryIsFenced(temporary, context, claim);
		if (!fenced && temporary.token !== claim.token) {
			throw new Error("Consumer high-water journal contains a live or future owned temporary.");
		}
		if (!fenced) continue;
		if (temporaryProcessIsAlive(temporary, options)) {
			if (requireQuiescent) {
				throw new Error("Consumer high-water journal rotation requires every prior owned temporary writer to quiesce.");
			}
			continue;
		}
		await options.removeFile(temporary.path, { force: true });
		await options.syncDirectory(dirname(temporary.path));
	}
	let retiredEpochDeferred = false;
	for (const epoch of rootScan.epochEntries) {
		if (epoch.name === basename(context.epochDirectory)) continue;
		if (epoch.name !== context.checkpoint.retiredEpochDirectory) {
			throw new Error("Consumer high-water journal contains an orphan epoch directory.");
		}
		const retiredNames = await options.readDirectory(epoch.path);
		const retiredTemporaries = [];
		for (const name of retiredNames) {
			if (name.startsWith(".")) {
				const temporary = await inspectTemporary(join(epoch.path, name), options);
				if (temporary) retiredTemporaries.push(temporary);
			}
		}
		if (retiredTemporaries.some((temporary) => temporaryProcessIsAlive(temporary, options))) {
			if (requireQuiescent) {
				throw new Error("Consumer high-water journal rotation requires every retired temporary writer to quiesce.");
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
	const allowedEpochs = retiredEpochDeferred ? 2 : 1;
	if (
		final.checkpointEntries.length !== allowedCheckpoints || final.epochEntries.length !== allowedEpochs ||
		final.temporaries.some((temporary) => !temporaryProcessIsAlive(temporary, options)) ||
		final.head?.path !== context.checkpointPath ||
		!final.epochEntries.some((entry) => entry.path === context.epochDirectory)
	) throw new Error("Consumer high-water journal did not converge to one bounded current epoch.");
}

async function inspectLegacyGuard(context, options) {
	let entry;
	try {
		entry = await options.lstatEntry(context.guardPath);
	} catch (error) {
		if (error?.code === "ENOENT") return "absent";
		throw error;
	}
	if (entry.isDirectory()) {
		throw new Error(
			`Legacy consumer lock directory exists at ${context.guardPath}. Stop every legacy proper-lockfile client, ` +
			"confirm that no owner remains, remove that directory manually, and retry.",
		);
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
	if (await inspectLegacyGuard(context, options) === "guard") return;
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
		!Number.isSafeInteger(stateMaxBytes) || stateMaxBytes < 1 || stateMaxBytes > 16 * 1024 * 1024 ||
		!Number.isSafeInteger(maxTransactionDepth) || maxTransactionDepth < 1 || maxTransactionDepth > MAX_TRANSACTION_DEPTH ||
		!Number.isSafeInteger(maxLockGenerations) || maxLockGenerations < 2 || maxLockGenerations > MAX_LOCK_GENERATIONS ||
		!Number.isSafeInteger(maxJournalBytes) || maxJournalBytes < stateMaxBytes || maxJournalBytes > 256 * 1024 * 1024 ||
		!(currentUid === null || Number.isSafeInteger(currentUid))
	) throw new Error("Consumer high-water lock timing, state-size, journal, or transaction bound is invalid.");
	return {
		stale,
		update,
		stateMaxBytes,
		maxTransactionDepth,
		maxLockGenerations,
		maxJournalBytes,
		maxJournalEntries: maxLockGenerations * 4 + maxTransactionDepth + 32,
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

async function prepareContext(statePath, options) {
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	await ensureDurableConsumerStateDirectory(directory, options.directoryOperations);
	await secureDirectory(directory, "Consumer high-water state directory", options);
	const guardPath = `${absoluteStatePath}.lock`;
	const journalDirectory = `${absoluteStatePath}.journal`;
	await ensureDirectory(journalDirectory, "Consumer high-water journal directory", options);
	const scan = await initializeJournal(absoluteStatePath, journalDirectory, options);
	return { context: contextFromHead(absoluteStatePath, guardPath, journalDirectory, scan.head), scan };
}

async function runLocked(statePath, action, rawOptions, rotate) {
	const options = normalizeOptions(rawOptions);
	for (;;) {
		const prepared = await prepareContext(statePath, options);
		const acquired = await acquireClaim(prepared.context, options, rotate);
		if (acquired.rotated) continue;
		const { context, scan } = prepared;
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
			await cleanupAuthority(context, claim, scan, temporaries, options, rotate);
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
			if (rotate) {
				const tip = legacyBytes === null ? chain : {
					tipDigest: digest(legacyBytes),
					tipBytes: legacyBytes,
					length: chain.length,
				};
				const wanted = {
					schemaVersion: LOCK_SCHEMA_VERSION,
					generation: claim.generation,
					token: claim.token,
					outcome: "rotate",
					checkpoint: rotationCheckpoint(context, tip),
				};
				await options.hooks?.beforeRotationDecision?.({ claim, checkpoint: wanted.checkpoint });
				terminal = await publishTerminal(context, claim, wanted, options);
				if (terminal.outcome !== "rotate" || !metadataBytes(terminal).equals(metadataBytes(wanted))) {
					throw new Error("Consumer high-water journal rotation lost its immutable authority decision.");
				}
				await stopHeartbeatOnce();
				await finishRotation(context, claim, terminal, options);
				return {
					epoch: terminal.checkpoint.epoch,
					tipSha256: terminal.checkpoint.anchorDigest,
				};
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
					if (terminal !== null) throw new Error("Consumer high-water transaction already has a terminal decision.");
					const bytes = Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(value);
					if (bytes.length < 1 || bytes.length > options.stateMaxBytes) throw new Error("Consumer high-water state is malformed.");
					await commitTransactions(bytes);
				},
			});
			let result;
			let actionError;
			try {
				result = await action(context.statePath, transaction);
			} catch (error) {
				actionError = error;
			}
			await stopHeartbeatOnce();
			if (terminal === null && actionError === undefined && legacyBytes !== null) await commitTransactions(null);
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

export async function withConsumerStateLock(statePath, action, rawOptions = {}) {
	if (typeof action !== "function") throw new Error("Consumer high-water lock action must be a function.");
	return runLocked(statePath, action, rawOptions, false);
}

export async function rotateConsumerStateJournal(statePath, rawOptions = {}) {
	if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local state path is required for journal rotation.");
	return runLocked(statePath, null, rawOptions, true);
}
