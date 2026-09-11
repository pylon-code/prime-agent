import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, lstat, mkdir, open, readdir, rename, rm } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { readBoundedRegularFile } from "./pylon-bounded-file.mjs";
import { generationBytes as bytes, generationDigest as digest, GENERATION_ZERO as ZERO, generationRecordMaxBytes, GENERATION_STATE_MAX_BYTES } from "./pylon-generation-format.mjs";

const BLOCKER = "claim-9999999999999999.json";
const MARKER = ".pylon-consumer-v1-retired.json";
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const checkpointPattern = new RegExp(`^checkpoint-([0-9]{16})-(${UUID})\\.json$`);
const epochPattern = new RegExp(`^epoch-([0-9]{16})-(${UUID})$`);
const temporaryPattern = new RegExp(`^\\.pylon-consumer-tmp-v1-p([1-9][0-9]*)-e(${UUID})-g([0-9]{16})-w(${UUID})-n([0-9a-f]{12})-k([a-z0-9-]{1,40})-t([0-9a-f]{64})\\.tmp$`);
const MAX_ENTRIES = 65_537 * 5 + 4096 + 32;
const MAX_BYTES = 256 * 1024 * 1024;
const same = (a, b) => a !== null && b !== null && a.dev === b.dev && a.ino === b.ino;
const identity = ({ dev, ino }) => ({ dev, ino });
const slot = (value) => String(value).padStart(16, "0");
const key = (claim) => `${claim.generation}:${claim.token}`;
const sameBytes = (a, b) => a === null ? b === null : b !== null && a.equals(b);
const closed = (value, keys) => value !== null && typeof value === "object" && !Array.isArray(value) && Object.keys(value).sort().join() === keys.slice().sort().join();

function commitment(domain, entries) {
	const hash = createHash("sha256").update(`${domain}\0`);
	for (const [name, value] of entries) {
		for (const data of [Buffer.from(name), value]) {
			const size = Buffer.alloc(8); size.writeBigUInt64BE(BigInt(data.length)); hash.update(size).update(data);
		}
	}
	return hash.digest("hex");
}
function optionsFor(raw) {
	const options = { stateMaxBytes: GENERATION_STATE_MAX_BYTES, currentUid: process.getuid(), lstatEntry: lstat, openFile: open, readDirectory: readdir,
		makeDirectory: mkdir, linkFile: link, renameFile: rename, removeFile: rm, processKill: process.kill.bind(process), ...raw };
	options.metadataMaxBytes = generationRecordMaxBytes(options.stateMaxBytes);
	if (!Number.isSafeInteger(options.currentUid) || options.currentUid < 0) throw new Error("Migration uid is invalid.");
	return options;
}
async function boundary(options, phase, operation, path) { await options.hooks?.migrationBoundary?.({ phase, operation, path }); }
async function absent(path, options) {
	try { const stat = await options.lstatEntry(path); if (!stat) throw new Error("Migration stat operation returned invalid evidence."); return stat; }
	catch (error) { if (options.lstatEntry === lstat && error?.code === "ENOENT") return null; throw error; }
}
function safeStat(stat, type, options, frozen = false) {
	const modes = type === "file" ? [0o600] : frozen ? [0o700, 0o500] : [0o700];
	if (stat.isSymbolicLink() || !(type === "file" ? stat.isFile() : stat.isDirectory()) || stat.uid !== options.currentUid || !modes.includes(stat.mode & 0o7777)) throw new Error("Migration entry has unsafe type, owner or exact permissions.");
	return stat;
}
async function directory(path, options, frozen = false, synchronize = false) {
	const initial = safeStat(await options.lstatEntry(path), "directory", options, frozen);
	await options.hooks?.migrationDirectoryObserved?.({ path, identity: identity(initial) });
	const handle = await options.openFile(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
	try {
		const opened = safeStat(await handle.stat(), "directory", options, frozen);
		if (!same(initial, opened)) throw new Error("Migration directory changed inode while opening.");
		if (synchronize) { await boundary(options, "before", "directory-sync", path); await handle.sync(); await boundary(options, "after", "directory-sync", path); }
		if (!same(opened, safeStat(await options.lstatEntry(path), "directory", options, frozen))) throw new Error("Migration directory changed inode after opening.");
		return identity(opened);
	} finally { await handle.close(); }
}
async function names(path, max, options) {
	const result = await options.readDirectory(path);
	if (!Array.isArray(result) || result.length > max || new Set(result).size !== result.length || result.some((name) => typeof name !== "string" || basename(name) !== name || [".", ".."].includes(name))) throw new Error("Migration namespace exceeds its closed entry bound.");
	return result.sort();
}
async function file(path, options, max = options.metadataMaxBytes, required = true, minBytes = 1) {
	const data = await readBoundedRegularFile(path, { maxBytes: max, minBytes, openFile: options.openFile, lstatEntry: options.lstatEntry,
		hooks: options.hooks?.metadataRead, validateHandle: async (_handle, stat) => safeStat(stat, "file", options) });
	if (data === null && required) throw new Error("Migration required pinned file disappeared.");
	return data;
}
function canonical(data) {
	const value = JSON.parse(data);
	if (!bytes(value).equals(data)) throw new Error("Migration metadata is not canonical.");
	return value;
}
async function inventory(path, options, { frozen = false, excludeBlocker = false, maximum = MAX_ENTRIES, budget = { bytes: 0 } } = {}) {
	const id = await directory(path, options, frozen);
	const entries = await names(path, maximum, options);
	const records = new Map(); const stats = new Map();
	for (const name of entries) {
		const stat = safeStat(await options.lstatEntry(join(path, name)), "file", options);
		if (stat.size < (temporaryPattern.test(name) ? 0 : 1) || stat.size > options.metadataMaxBytes) throw new Error("Historical metadata exceeds its byte bound.");
		budget.bytes += stat.size;
		if (budget.bytes > MAX_BYTES) throw new Error("Historical source exceeds its aggregate byte bound.");
		stats.set(name, stat);
	}
	for (const name of entries) {
		const data = await file(join(path, name), options, options.metadataMaxBytes, true, temporaryPattern.test(name) ? 0 : 1);
		const stat = await options.lstatEntry(join(path, name));
		if (!same(stats.get(name), stat) || stats.get(name).size !== stat.size || stats.get(name).mtimeMs !== stat.mtimeMs || stats.get(name).ctimeMs !== stat.ctimeMs) throw new Error("Historical file changed after inventory.");
		if (!excludeBlocker || name !== BLOCKER) records.set(name, data);
	}
	if (!same(id, await directory(path, options, frozen)) || entries.join() !== (await names(path, maximum, options)).join()) throw new Error("Historical namespace changed during inventory.");
	return { path, identity: id, records, stats, names: entries };
}
function dead(pid, options) {
	try { options.processKill(pid, 0); return false; } catch (error) { if (error?.code === "ESRCH") return true; if (error?.code === "EPERM") return false; throw error; }
}

// The factory shares the protected closed value validators and generation engine;
// filesystem migration never calls an old operation/rotation helper.
export function createConsumerMigrationApi(format) {
	async function readV1(statePath, lockPath, projection, options, frozen = false) {
		const budget = { bytes: 0 };
		const lock = await inventory(lockPath, options, { frozen, maximum: 65_537 * 4 + 2, budget });
		const transactions = await inventory(`${statePath}.transactions`, options, { frozen, maximum: 4096, budget });
		const claims = []; const heartbeats = new Map(); const terminals = new Map(); const applied = new Map();
		let marker = null; const records = [];
		for (const [name, data] of lock.records) {
			const value = canonical(data); let match;
			if (name === MARKER) { marker = format.validateLegacyRetirementMarker(value, statePath); continue; }
			if ((match = /^claim-([0-9]{16})\.json$/.exec(name))) {
				format.validateLegacyClaim(value);
				if (value.generation !== Number(match[1]) || slot(value.generation) !== match[1]) throw new Error("Historical v1 claim name is malformed.");
				claims.push(value);
			} else if ((match = new RegExp(`^(heartbeat|terminal|applied)-([0-9]{16})-(${UUID})\\.json$`).exec(name))) {
				({ heartbeat: heartbeats, terminal: terminals, applied })[match[1]].set(`${Number(match[2])}:${match[3]}`, value);
			} else throw new Error("Historical v1 authority contains an unexpected entry.");
			records.push([`lock/${name}`, data]);
		}
		claims.sort((a, b) => a.generation - b.generation);
		if (claims.length > 65_536 || claims.some((claim, index) => claim.generation !== index + 1)) throw new Error("Historical v1 claims are not bounded and contiguous.");
		const byKey = new Map(claims.map((claim) => [key(claim), claim]));
		for (const [k, value] of heartbeats) { if (!byKey.has(k)) throw new Error("Historical v1 orphan heartbeat."); format.validateLegacyHeartbeat(value, byKey.get(k)); }
		for (const claim of claims) if (!heartbeats.has(key(claim))) throw new Error("Historical v1 claim lacks heartbeat.");
		for (const [k, value] of terminals) { if (!byKey.has(k)) throw new Error("Historical v1 orphan terminal."); format.validateLegacyTerminal(value, byKey.get(k), options.stateMaxBytes); }
		for (const [k, value] of applied) { if (!byKey.has(k)) throw new Error("Historical v1 orphan applied marker."); format.validateLegacyApplied(value, byKey.get(k), terminals.get(k)); }
		let tipDigest = ZERO; let tipBytes = null; const decided = new Map(); const prefixes = new Set([ZERO]);
		for (const claim of claims) {
			const terminal = terminals.get(key(claim));
			for (const transaction of terminal?.outcome === "commit" ? terminal.transactions : []) {
				if (transaction.baseDigest !== tipDigest || decided.has(tipDigest) || decided.size >= 4096) throw new Error("Historical v1 decisions are not one bounded exact chain.");
				decided.set(tipDigest, transaction); tipBytes = format.validateGenerationTransaction(transaction, tipDigest, options.stateMaxBytes).candidateBytes;
				tipDigest = transaction.candidateDigest; prefixes.add(tipDigest);
			}
		}
		const actual = new Map();
		for (const [name, data] of transactions.records) {
			const match = /^([0-9a-f]{64})\.json$/.exec(name); if (!match) throw new Error("Historical v1 unexpected transaction.");
			const value = canonical(data); format.validateGenerationTransaction(value, match[1], options.stateMaxBytes);
			if (!decided.has(match[1]) || !bytes(decided.get(match[1])).equals(data)) throw new Error("Historical v1 transition lacks exact commit decision.");
			actual.set(match[1], value); records.push([`transactions/${name}`, data]);
		}
		let reached = ZERO; const visited = new Set();
		while (actual.has(reached)) { if (visited.has(reached)) throw new Error("Historical v1 transaction cycle."); visited.add(reached); reached = actual.get(reached).candidateDigest; }
		if (visited.size !== actual.size) throw new Error("Historical v1 unreachable transaction.");
		for (const k of applied.keys()) for (const transaction of terminals.get(k).transactions) if (!actual.has(transaction.baseDigest)) throw new Error("Historical v1 applied transition is absent.");
		if (decided.size === 0 && projection !== null) { tipBytes = projection; tipDigest = digest(projection); records.push(["explicit-quiescent-projection", projection]); }
		else if (projection !== null && !prefixes.has(digest(projection))) throw new Error("Historical v1 projection is not an authenticated prefix.");
		const recoveries = [];
		for (const claim of claims) {
			const terminal = terminals.get(key(claim));
			if (!terminal) recoveries.push({ target: join(lockPath, `terminal-${slot(claim.generation)}-${claim.token}.json`), value: { schemaVersion: 1, generation: claim.generation, token: claim.token, outcome: "retired" }, owner: claim });
			else if (terminal.outcome === "commit" && !applied.has(key(claim))) {
				for (const transaction of terminal.transactions) if (!actual.has(transaction.baseDigest)) recoveries.push({ target: join(transactions.path, `${transaction.baseDigest}.json`), value: transaction, owner: claim });
				recoveries.push({ target: join(lockPath, `applied-${slot(claim.generation)}-${claim.token}.json`), value: { schemaVersion: 1, generation: claim.generation, token: claim.token, terminalSha256: digest(bytes(terminal)) }, owner: claim });
			}
		}
		const authoritySha256 = format.authorityDigest(records, tipDigest, tipBytes);
		if (marker !== null && (!bytes(marker).equals(bytes(format.legacyRetirementMarkerFor(statePath, { authoritySha256, tipDigest }))) || recoveries.length)) throw new Error("Historical v1 retirement marker differs from exact quiescent authority.");
		return { kind: "v1", lock, transactions, marker, records, tipDigest, tipBytes, authoritySha256, recoveries };
	}

	async function readV2(statePath, root, projection, options, legacy = null) {
		const rootIdentity = await directory(root, options);
		const rootNames = await names(root, 16 + 65_536, options);
		if (rootNames.filter((name) => checkpointPattern.test(name)).length > 2 || rootNames.filter((name) => epochPattern.test(name)).length > 2 || rootNames.filter((name) => !temporaryPattern.test(name)).length > 16) throw new Error("Historical v2 root exceeds its pre-allocation authority bound.");
		const checkpoints = []; const epochs = new Map(); const raw = []; const temporaries = []; const budget = { bytes: 0 };
		const stats = new Map();
		for (const name of rootNames) {
			const path = join(root, name); const stat = await options.lstatEntry(path); stats.set(name, stat);
			if (checkpointPattern.test(name) || temporaryPattern.test(name)) {
				safeStat(stat, "file", options); budget.bytes += stat.size;
				if (stat.size < 1 || stat.size > options.metadataMaxBytes || budget.bytes > MAX_BYTES) throw new Error("Historical v2 root exceeds byte bounds.");
			} else if (epochPattern.test(name) || name === ".owned-temporaries-v2") safeStat(stat, "directory", options);
			else throw new Error("Historical v2 root contains unexpected authority.");
		}
		for (const name of rootNames) {
			const path = join(root, name);
			if (checkpointPattern.test(name)) {
				const data = await file(path, options); const checkpoint = format.validateCheckpoint(canonical(data), options.stateMaxBytes).value;
				if (format.checkpointName(checkpoint) !== name) throw new Error("Historical v2 checkpoint name is not exact.");
				checkpoints.push({ checkpoint, data, name }); raw.push([name, data]);
			} else if (epochPattern.test(name) || name === ".owned-temporaries-v2") {
				const scanned = await inventory(path, options, { maximum: MAX_ENTRIES + 65_536, budget });
				if (name === ".owned-temporaries-v2") temporaries.push(...[...scanned.records].map(([child, data]) => ({ name: child, data, path: join(path, child) })));
				else epochs.set(name, scanned);
				for (const [child, data] of scanned.records) raw.push([`${name}/${child}`, data]);
			} else { const data = await file(path, options); temporaries.push({ name, data, path }); raw.push([name, data]); }
		}
		if (!rootNames.includes(".owned-temporaries-v2") || checkpoints.length < 1 || checkpoints.length > 2 || epochs.size > 2) throw new Error("Historical v2 root lacks bounded exact checkpoint authority.");
		checkpoints.sort((a, b) => a.checkpoint.epoch - b.checkpoint.epoch);
		const scans = new Map();
		for (const entry of checkpoints) {
			const checkpoint = entry.checkpoint;
			if (checkpoint.epoch === 1) {
				const expected = legacy ? format.migrationCheckpoint(statePath, legacy) : format.genesisCheckpoint(statePath);
				if (!bytes(expected).equals(entry.data)) throw new Error("Historical v2 genesis differs from exact state/source authority.");
			} else if (checkpoint.epochId !== format.deterministicUuid(`pylon-consumer-rotation-v2:${checkpoint.previousCheckpointSha256}:${checkpoint.anchorDigest}`)) throw new Error("Historical v2 rotated identity is not deterministic.");
			if (legacy ? checkpoint.sourceAuthoritySha256 !== legacy.authoritySha256 || checkpoint.sourceAuthorityTipDigest !== legacy.tipDigest || checkpoint.sourceAuthorityTipBase64 !== (legacy.tipBytes?.toString("base64") ?? null) : checkpoint.sourceAuthoritySha256 !== ZERO || checkpoint.sourceAuthorityTipDigest !== ZERO || checkpoint.sourceAuthorityTipBase64 !== null) throw new Error("Historical v2 underlying v1 provenance differs.");
			const epoch = epochs.get(format.epochName(checkpoint));
			if (!epoch) {
				if (entry !== checkpoints.at(-1) || checkpoint.epoch !== 1 || epochs.size !== 0) throw new Error("Historical v2 required epoch is missing.");
				continue;
			}
			const records = new Map();
			for (const [name, data] of epoch.records) { if (temporaryPattern.test(name)) temporaries.push({ name, data, path: join(epoch.path, name) }); else records.set(name, data); }
			const scan = format.generationEpochAuthority({ checkpoint, checkpointBytes: entry.data, name: format.epochName(checkpoint), epochRecords: records }, options, true);
			scans.set(entry.name, scan);
		}
		const head = checkpoints.at(-1); const previous = checkpoints.at(-2);
		if (previous) {
			const previousScan = scans.get(previous.name); const latest = previousScan?.claims.at(-1);
			const context = { checkpoint: previous.checkpoint, checkpointDigest: digest(previous.data), epochDirectory: format.epochName(previous.checkpoint) };
			if (latest?.type !== "rotation" || previousScan.decidedTipDigest !== previousScan.tip.tipDigest || !bytes(format.rotationClaimFor(context, latest.generation, previousScan.tip)).equals(bytes(latest)) || !bytes(latest.intent.checkpoint).equals(head.data)) throw new Error("Historical retained predecessor lacks exact latest rotation claim/CAS and tip.");
		}
		const headEpoch = format.epochName(head.checkpoint);
		const headScan = scans.get(head.name) ?? format.generationEpochAuthority({ checkpoint: head.checkpoint, checkpointBytes: head.data, name: headEpoch, epochRecords: new Map() }, options, true);
		const latest = headScan.claims.at(-1);
		for (const [name, epoch] of epochs) {
			if (name === headEpoch || previous && name === format.epochName(previous.checkpoint)) continue;
			if (latest?.type !== "rotation" || name !== format.epochName(latest.intent.checkpoint) || epoch.records.size !== 0) throw new Error("Historical v2 extra epoch lacks exact pending rotation intent.");
		}
		let tipDigest = head.checkpoint.anchorDigest; let tipBytes = format.validateCheckpoint(head.checkpoint, options.stateMaxBytes).anchorBytes;
		const prefixes = new Set([tipDigest]); const recoveries = [];
		for (const claim of headScan.claims) {
			const terminal = headScan.terminals.get(key(claim));
			if (claim.type === "normal" && !terminal) recoveries.push({ target: join(root, headEpoch, `terminal-${slot(claim.generation)}-${claim.token}.json`), value: { schemaVersion: 2, generation: claim.generation, token: claim.token, outcome: "retired" }, owner: claim });
			for (const transaction of terminal?.outcome === "commit" ? terminal.transactions : []) {
				tipBytes = format.validateGenerationTransaction(transaction, tipDigest, options.stateMaxBytes).candidateBytes; tipDigest = transaction.candidateDigest; prefixes.add(tipDigest);
				if (!headScan.transitions.has(transaction.baseDigest)) recoveries.push({ target: join(root, headEpoch, `transition-${transaction.baseDigest}.json`), value: transaction, owner: claim });
			}
			if (terminal?.outcome === "commit" && !headScan.applied.has(key(claim))) recoveries.push({ target: join(root, headEpoch, `applied-${slot(claim.generation)}-${claim.token}.json`), value: { schemaVersion: 2, generation: claim.generation, token: claim.token, terminalSha256: digest(bytes(terminal)) }, owner: claim });
		}
		if (tipBytes === null && projection !== null) { tipBytes = projection; tipDigest = digest(projection); }
		else if (projection !== null && !prefixes.has(digest(projection))) throw new Error("Historical v2 projection is not an authenticated prefix.");
		if (latest?.type === "rotation") {
			const context = { checkpoint: head.checkpoint, checkpointDigest: digest(head.data), epochDirectory: headEpoch };
			if (!bytes(format.rotationClaimFor(context, latest.generation, { tipBytes, tipDigest })).equals(bytes(latest))) throw new Error("Historical v2 latest rotation differs from exact immutable tip.");
		}
		for (const temporary of temporaries) {
			const match = temporaryPattern.exec(temporary.name);
			if (!match || !["checkpoint", "projection", "transition", "claim", "claim-index", "initial-heartbeat", "heartbeat", "terminal-released", "terminal-retired", "terminal-commit", "applied", "legacy-guard", "legacy-retirement"].includes(match[6])) throw new Error("Historical temporary grammar is invalid.");
			if (!dead(Number(match[1]), options)) throw new Error("Historical migration has a live unresolved temporary writer.");
		}
		if (!same(rootIdentity, await directory(root, options)) || rootNames.join() !== (await names(root, 16 + 65_536, options)).join()) throw new Error("Historical v2 root changed during read.");
		for (const [name, stat] of stats) if (!same(stat, await options.lstatEntry(join(root, name)))) throw new Error("Historical v2 root entry changed inode.");
		return { kind: "v2", root, identity: rootIdentity, checkpoints, epochs, head, headEpoch, records: raw, tipDigest, tipBytes, recoveries, temporaries };
	}


 async function inspect(statePath, rawOptions = {}) {
  if (typeof statePath !== "string" || !statePath) throw new Error("Migration state path is required.");
  const options = optionsFor(rawOptions);
  const state = resolve(statePath);
  await directory(dirname(state), options);
  const projection = await file(state, options, options.stateMaxBytes, false);
  const journal = await absent(`${state}.journal`, options);
  let legacyProjection = projection;
  if (journal) {
   await directory(`${state}.journal`, options);
   const checkpointNames = (await names(`${state}.journal`, 16 + 65_536, options)).filter((name) => checkpointPattern.test(name));
   if (checkpointNames.length > 2) throw new Error("Historical v2 checkpoint bound is exceeded.");
   if (checkpointNames.length) {
    const checkpoint = format.validateCheckpoint(canonical(await file(join(`${state}.journal`, checkpointNames.at(-1)), options)), options.stateMaxBytes).value;
    if (checkpoint.sourceAuthoritySha256 !== ZERO) legacyProjection = checkpoint.sourceAuthorityTipBase64 === null ? null : Buffer.from(checkpoint.sourceAuthorityTipBase64, "base64");
   }
  }
  const lock = await absent(`${state}.lock`, options);
  const retired = await absent(`${state}.lock.v1-retired`, options);
  const transactions = await absent(`${state}.transactions`, options);
  if (lock?.isDirectory() && retired) throw new Error("Historical authority has competing original and retired v1 locks.");
  let legacy = null;
  if (transactions || retired || lock?.isDirectory()) {
   if (!transactions || !(retired || lock?.isDirectory())) throw new Error("Historical v1 authority is incomplete.");
   legacy = await readV1(state, retired ? `${state}.lock.v1-retired` : `${state}.lock`, legacyProjection, options);
   if (retired && lock) {
    const expected = { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) };
    if (!bytes(expected).equals(await file(`${state}.lock`, options))) throw new Error("Historical prior-retired guard is not exact.");
   }
  }
  const source = journal ? await readV2(state, `${state}.journal`, projection, { ...options, maxJournalBytes: MAX_BYTES }, legacy) : legacy;
  if (!source) throw new Error("No historical authority exists.");
  return { source, legacy, projection };
 }
 return { inspect, readV1, readV2 };

}
