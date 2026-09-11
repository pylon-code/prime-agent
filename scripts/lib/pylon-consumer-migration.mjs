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
const MAX_CONSTRUCTION_ROOTS = 64;
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

async function sync(path, options, frozen = false) { return directory(path, options, frozen, true); }
async function make(path, options) {
 await boundary(options, "before", "mkdir", path);
 try { await options.makeDirectory(path, { mode: 0o700 }); }
 catch (error) { if (options.makeDirectory !== mkdir || error?.code !== "EEXIST") throw error; }
 await boundary(options, "after", "mkdir", path);
 const observed = await directory(path, options);
 await sync(path, options); await sync(dirname(path), options);
 return observed;
}
async function canonicalAncestors(state, options, create = false) {
 const components = dirname(resolve(state)).split("/").filter(Boolean);
 let path = "/";
 for (const component of components) {
  path = join(path, component);
  let stat = await absent(path, options);
  if (stat === null && create) {
   await boundary(options, "before", "mkdir", path);
   try { await options.makeDirectory(path, { mode: 0o700 }); }
   catch (error) { if (options.makeDirectory !== mkdir || error?.code !== "EEXIST") throw error; }
   await boundary(options, "after", "mkdir", path);
   await sync(path, options);
   const parentHandle = await options.openFile(dirname(path), constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
   try { await parentHandle.sync(); } finally { await parentHandle.close(); }
   stat = await options.lstatEntry(path);
  }
  if (!stat?.isDirectory() || stat.isSymbolicLink()) throw new Error("Migration ancestor must be a canonical real directory.");
 }
 await directory(dirname(state), options);
}
function receiptName(logical) { return `receipt-${digest(Buffer.from(logical))}.json`; }
const writingPattern = new RegExp(`^\\.writing-p([1-9][0-9]*)-(${UUID})-([0-9a-f]{64})\\.tmp$`);
async function receiptFor(meta, logical, target, expected, options, { publish = false, repair = true } = {}) {
 await directory(meta, options);
 const receipts = join(meta, "receipts");
 await directory(receipts, options);
 await directory(dirname(target), options, true);
 const fixed = join(receipts, receiptName(logical));
 const wanted = await absent(target, options);
 if (wanted !== null) {
  if (!sameBytes(await file(target, options), expected)) throw new Error("Migration immutable target has conflicting exact bytes.");
  let proof = await absent(fixed, options);
  if (proof === null) {
   if (!repair) throw new Error("Interrupted legacy migration receipt requires explicitly acknowledged migration.");
   for (const name of await names(receipts, MAX_ENTRIES, options)) {
    const match = writingPattern.exec(name);
    if (!match || match[3] !== digest(Buffer.from(logical))) continue;
    const candidate = await options.lstatEntry(join(receipts, name));
    if (!same(candidate, wanted)) continue;
    await boundary(options, "before", "receipt-rename", fixed);
    try { await options.renameFile(join(receipts, name), fixed); }
    catch (error) { if (options.renameFile !== rename || error?.code !== "ENOENT" || !same(candidate, await absent(fixed, options))) throw error; }
    await boundary(options, "after", "receipt-rename", fixed);
    await sync(receipts, options); proof = await options.lstatEntry(fixed); break;
   }
  }
  if (proof === null || !same(proof, wanted) || ![2, ...(logical === "guard.json" ? [3, 4] : [])].includes(wanted.nlink) || proof.nlink !== wanted.nlink) throw new Error("Migration immutable record lacks its exact durable receipt inode.");
  if (!sameBytes(await file(fixed, options), expected)) throw new Error("Migration receipt bytes changed.");
  return identity(wanted);
 }
 if (!publish) throw new Error("Migration required immutable record is absent.");
 await names(receipts, MAX_ENTRIES - 1, options);
 const temporary = join(receipts, `.writing-p${process.pid}-${randomUUID()}-${digest(Buffer.from(logical))}.tmp`);
 const handle = await options.openFile(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
 let observed;
 try {
  await boundary(options, "after", "receipt-create", temporary);
  await handle.writeFile(expected);
  observed = safeStat(await handle.stat(), "file", options);
  await boundary(options, "before", "file-sync", temporary); await handle.sync(); await boundary(options, "after", "file-sync", temporary);
 } finally { await handle.close(); }
 await sync(receipts, options);
 await boundary(options, "before", "immutable-link", target);
 try { await options.linkFile(temporary, target); }
 catch (error) {
  if (options.linkFile !== link || error?.code !== "EEXIST") throw error;
  // An independent winner is validated separately; it is not this attempt's link.
  await receiptFor(meta, logical, target, expected, options);
  if (!same(observed, await options.lstatEntry(temporary))) throw new Error("Migration losing receipt was replaced.");
  await options.removeFile(temporary); await sync(receipts, options);
  return identity(await options.lstatEntry(target));
 }
 await boundary(options, "after", "immutable-link", target);
 await sync(dirname(target), options);
 await boundary(options, "before", "receipt-rename", fixed);
 try { await options.renameFile(temporary, fixed); }
 catch (error) { if (options.renameFile !== rename || error?.code !== "ENOENT" || !same(observed, await absent(fixed, options))) throw error; }
 await boundary(options, "after", "receipt-rename", fixed);
 await sync(receipts, options);
 return receiptFor(meta, logical, target, expected, options);
}
async function immutable(meta, name, value, options) {
 const data = bytes(value);
 if (data.length > options.metadataMaxBytes) throw new Error("Migration metadata exceeds its exact byte bound.");
 return receiptFor(meta, name, join(meta, name), data, options, { publish: true });
}
function projectionFields(projection) { return { projectionSha256: projection === null ? ZERO : digest(projection), projectionBase64: projection?.toString("base64") ?? null }; }
function decodeProjection(value, options) {
 if (value.projectionBase64 === null) { if (value.projectionSha256 !== ZERO) throw new Error("Migration absent projection digest is invalid."); return null; }
 if (typeof value.projectionBase64 !== "string" || value.projectionBase64.length > 4 * Math.ceil(options.stateMaxBytes / 3)) throw new Error("Migration projection exceeds its encoded bound.");
 const data = Buffer.from(value.projectionBase64, "base64");
 if (data.length > options.stateMaxBytes || data.toString("base64") !== value.projectionBase64 || digest(data) !== value.projectionSha256) throw new Error("Migration projection does not match exact bytes and digest.");
 return data;
}

// The factory shares the protected closed value validators and generation engine;
// filesystem migration never calls an old operation/rotation helper.
export function createConsumerMigrationApi(format) {
	async function readV1(statePath, lockPath, projection, options, frozen = false) {
		const budget = { bytes: 0 };
		if (options.blockers?.has(lockPath)) {
   if (!sameBytes(await file(join(lockPath, BLOCKER), options), options.blockers.get(lockPath))) throw new Error("Historical v1 blocker differs from exact migration proof.");
  } else if (frozen) throw new Error("Frozen v1 authority requires its exact migration blocker proof.");
  const lock = await inventory(lockPath, options, { frozen, excludeBlocker: options.blockers?.has(lockPath) ?? false, maximum: 65_537 * 4 + 2, budget });
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
		if (decided.size === 0 && projection !== null && projection.length > 0) { tipBytes = projection; tipDigest = digest(projection); records.push(["explicit-quiescent-projection", projection]); }
		else if (projection !== null && projection.length > 0 && !prefixes.has(digest(projection))) throw new Error("Historical v1 projection is not an authenticated prefix.");
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
		const checkpoints = []; const epochs = new Map(); const directories = new Map(); const raw = []; const temporaries = []; const budget = { bytes: 0 };
		const stats = new Map();
		for (const name of rootNames) {
			const path = join(root, name); const stat = await options.lstatEntry(path); stats.set(name, stat);
			if (checkpointPattern.test(name) || temporaryPattern.test(name)) {
				safeStat(stat, "file", options); budget.bytes += stat.size;
				if (stat.size < (temporaryPattern.test(name) ? 0 : 1) || stat.size > options.metadataMaxBytes || budget.bytes > MAX_BYTES) throw new Error("Historical v2 root exceeds byte bounds.");
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
				if (options.blockers?.has(path) && !sameBytes(await file(join(path, BLOCKER), options), options.blockers.get(path))) throw new Error("Historical v2 blocker differs from exact migration proof.");
    const scanned = await inventory(path, options, { excludeBlocker: options.blockers?.has(path) ?? false, maximum: name === ".owned-temporaries-v2" ? 65_536 : MAX_ENTRIES + 65_536, budget });
				directories.set(name, scanned);
    if (name === ".owned-temporaries-v2") temporaries.push(...[...scanned.records].map(([child, data]) => ({ name: child, data, path: join(path, child) })));
				else epochs.set(name, scanned);
				for (const [child, data] of scanned.records) raw.push([`${name}/${child}`, data]);
			} else { const data = await file(path, options, options.metadataMaxBytes, true, 0); temporaries.push({ name, data, path }); raw.push([name, data]); }
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
		if (tipBytes === null && projection !== null && projection.length > 0) { tipBytes = projection; tipDigest = digest(projection); }
		else if (projection !== null && projection.length > 0 && !prefixes.has(digest(projection))) throw new Error("Historical v2 projection is not an authenticated prefix.");
		if (latest?.type === "rotation") {
			const context = { checkpoint: head.checkpoint, checkpointDigest: digest(head.data), epochDirectory: headEpoch };
			if (!bytes(format.rotationClaimFor(context, latest.generation, { tipBytes, tipDigest })).equals(bytes(latest))) throw new Error("Historical v2 latest rotation differs from exact immutable tip.");
		}
		if (temporaries.length > 65_536) throw new Error("Historical temporary namespace exceeds its exact aggregate entry bound.");
		for (const temporary of temporaries) {
			const match = temporaryPattern.exec(temporary.name);
   if (match && (!Number.isSafeInteger(Number(match[1])) || Number(match[1]) < 1 || !Number.isSafeInteger(Number(match[3])) || Number(match[3]) > 65_537)) throw new Error("Historical temporary writer or generation is outside its exact bound.");
			if (!match || !["checkpoint", "projection", "transition", "claim", "claim-index", "initial-heartbeat", "heartbeat", "terminal-released", "terminal-retired", "terminal-commit", "applied", "legacy-guard", "legacy-retirement"].includes(match[6])) throw new Error("Historical temporary grammar is invalid.");
			const epoch = checkpoints.find((entry) => entry.checkpoint.epochId === match[2]);
   const scan = epoch && scans.get(epoch.name);
   const winner = scan?.claims.find((claim) => claim.generation === Number(match[3]));
   const decided = winner && (winner.token !== match[4] || scan.terminals.has(key(winner)));
   if (!decided && !dead(Number(match[1]), options)) throw new Error("Historical migration has a live unresolved temporary writer.");
   if (dirname(temporary.path) === root && match[6] !== "checkpoint") throw new Error("Historical root temporary has a forbidden target kind.");
   if (epochPattern.test(basename(dirname(temporary.path))) && ["checkpoint", "projection", "legacy-guard"].includes(match[6])) throw new Error("Historical epoch temporary has a forbidden target kind.");
		}
		if (!same(rootIdentity, await directory(root, options)) || rootNames.join() !== (await names(root, 16 + 65_536, options)).join()) throw new Error("Historical v2 root changed during read.");
		for (const [name, stat] of stats) if (!same(stat, await options.lstatEntry(join(root, name)))) throw new Error("Historical v2 root entry changed inode.");
		return { kind: "v2", root, identity: rootIdentity, stats, directories, checkpoints, epochs, head, headEpoch, records: raw, tipDigest, tipBytes, recoveries, temporaries };
	}


 function sourceDescription(observed) {
  const { source, legacy, projection } = observed;
  const records = [["projection", projection ?? Buffer.alloc(0)]];
  const addInventory = (role, entry, selected = null) => {
   records.push([`${role}/identity`, bytes(entry.identity)]);
   for (const [name, data] of entry.records) {
    if (selected && !selected.has(name)) continue;
    records.push([`${role}/${name}/identity`, bytes(identity(entry.stats.get(name)))]);
    records.push([`${role}/${name}/bytes`, data]);
   }
  };
  if (legacy) {
   addInventory("v1-lock", legacy.lock); addInventory("v1-transactions", legacy.transactions);
  }
  if (source?.kind === "v2") {
   records.push(["v2-root/identity", bytes(source.identity)]);
   for (const checkpoint of source.checkpoints) {
    records.push([`v2-root/${checkpoint.name}/identity`, bytes(identity(source.stats.get(checkpoint.name)))]);
    records.push([`v2-root/${checkpoint.name}/bytes`, checkpoint.data]);
   }
   for (const [name, entry] of source.directories) addInventory(`v2/${name}`, entry);
   for (const [name, data] of source.records) if (!name.includes("/") && !checkpointPattern.test(name)) {
    records.push([`v2-root/${name}/identity`, bytes(identity(source.stats.get(name)))]); records.push([`v2-root/${name}/bytes`, data]);
   }
  }
  records.sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  return {
   kind: source?.kind ?? "fresh",
   legacyLayout: legacy === null ? null : legacy.lock.path.endsWith(".lock.v1-retired") ? "prior-retired" : "in-place",
   sourceIdentity: source?.kind === "v2" ? source.identity : null,
   legacyLockIdentity: legacy?.lock.identity ?? null,
   legacyTransactionsIdentity: legacy?.transactions.identity ?? null,
   authoritySha256: commitment("pylon-migration-source-v3", records),
   tipSha256: source?.tipDigest ?? (projection === null ? ZERO : digest(projection)),
   legacyAuthoritySha256: legacy?.authoritySha256 ?? ZERO,
   legacyMarkerSha256: legacy?.marker ? digest(bytes(legacy.marker)) : ZERO,
  };
 }
 function intentFor(state, observed) {
  return { schemaVersion: 3, kind: "pylon-consumer-migration-intent", statePathSha256: digest(Buffer.from(state)),
   source: sourceDescription(observed), ...projectionFields(observed.projection),
   legacyProjection: projectionFields(observed.legacy?.tipBytes ?? null) };
 }
 function validateIntent(value, state, options) {
  if (!closed(value, ["schemaVersion", "kind", "statePathSha256", "source", "projectionSha256", "projectionBase64", "legacyProjection"]) || value.schemaVersion !== 3 || value.kind !== "pylon-consumer-migration-intent" || value.statePathSha256 !== digest(Buffer.from(state))) throw new Error("Migration intent closed format or state path is invalid.");
  if (!closed(value.source, ["kind", "legacyLayout", "sourceIdentity", "legacyLockIdentity", "legacyTransactionsIdentity", "authoritySha256", "tipSha256", "legacyAuthoritySha256", "legacyMarkerSha256"]) || !["fresh", "v1", "v2"].includes(value.source.kind) || ![null, "prior-retired", "in-place"].includes(value.source.legacyLayout)) throw new Error("Migration intent source is malformed.");
  for (const field of ["sourceIdentity", "legacyLockIdentity", "legacyTransactionsIdentity"]) {
   const id = value.source[field];
   if (id !== null && (!closed(id, ["dev", "ino"]) || !Object.values(id).every((part) => Number.isSafeInteger(part) && part >= 0))) throw new Error("Migration source inode is malformed.");
  }
  for (const field of ["authoritySha256", "tipSha256", "legacyAuthoritySha256", "legacyMarkerSha256"]) if (!/^[0-9a-f]{64}$/.test(value.source[field] ?? "")) throw new Error("Migration source commitment is malformed.");
  if (!closed(value.legacyProjection, ["projectionSha256", "projectionBase64"])) throw new Error("Migration legacy projection is malformed.");
  decodeProjection(value, options); decodeProjection(value.legacyProjection, options);
  return value;
 }
 function blockerFor(intent) {
  return { schemaVersion: 3, kind: "pylon-consumer-impossible-generation-blocker", generation: "9999999999999999",
   statePathSha256: intent.statePathSha256, migrationIntentSha256: digest(bytes(intent)), source: intent.source };
 }
 function guardFor(intent) { return { schemaVersion: 3, kind: "pylon-consumer-v3-guard", statePathSha256: intent.statePathSha256, migrationIntentSha256: digest(bytes(intent)) }; }
 function lockPathFor(state, intent) { return intent.source.legacyLayout === "prior-retired" ? `${state}.lock.v1-retired` : `${state}.lock`; }
 async function readIntent(meta, state, options, allowHistoricalRepair = false) {
  await directory(meta, options);
  const data = await file(join(meta, "intent.json"), options, options.metadataMaxBytes, false);
  if (data === null) return null;
  const intent = validateIntent(canonical(data), state, options);
  await receiptFor(meta, "intent.json", join(meta, "intent.json"), data, options, { repair: allowHistoricalRepair || intent.source.kind === "fresh" });
  return intent;
 }
 async function historicalFromIntent(state, intent, options, { final = false, blocked = false } = {}) {
  const blockers = new Map();
  const blocker = bytes(blockerFor(intent));
  const expectedLegacyPath = lockPathFor(state, intent);
  let legacy = null;
  if (intent.source.legacyLayout !== null) {
   const found = await absent(join(expectedLegacyPath, BLOCKER), options);
   if (found) blockers.set(expectedLegacyPath, blocker);
   else if (blocked || final) throw new Error("Migration required v1 blocker disappeared.");
   const frozen = ((await options.lstatEntry(expectedLegacyPath)).mode & 0o7777) === 0o500 || ((await options.lstatEntry(`${state}.transactions`)).mode & 0o7777) === 0o500;
   legacy = await readV1(state, expectedLegacyPath, decodeProjection(intent.legacyProjection, options), { ...options, blockers }, frozen);
  }
  let source = legacy;
  if (intent.source.kind === "fresh") {
   for (const path of [`${state}.journal`, `${state}.journal.v2-retired`, `${state}.transactions`, `${state}.lock.v1-retired`]) if (await absent(path, options) !== null) throw new Error("Fresh v3 authority conflicts with a historical source; explicit migration is required.");
   const guard = await absent(`${state}.lock`, options);
   if (guard?.isDirectory()) throw new Error("Fresh v3 authority conflicts with a legacy lock directory.");
  }
  if (intent.source.kind === "v2") {
   const original = await absent(`${state}.journal`, options);
   const retired = await absent(`${state}.journal.v2-retired`, options);
   if (retired !== null && original !== null) throw new Error("Migration source retirement conflicts with a foreign historical journal inode.");
   const root = retired !== null ? `${state}.journal.v2-retired` : `${state}.journal`;
   if (final && retired === null) throw new Error("Migration final source is not path fenced.");
   if (!same(intent.source.sourceIdentity, retired ?? original)) throw new Error("Migration retained source has a different inode.");
   const rootNames = await names(root, 16 + 65_536, options);
   for (const name of rootNames.filter((entry) => epochPattern.test(entry))) {
    if (await absent(join(root, name, BLOCKER), options)) blockers.set(join(root, name), blocker);
    else if (blocked || final || retired !== null) throw new Error("Migration required v2 blocker disappeared.");
   }
   source = await readV2(state, root, decodeProjection(intent, options), { ...options, blockers, maxJournalBytes: MAX_BYTES }, legacy);
  }
  const observed = { source, legacy, projection: decodeProjection(intent, options) };
  if (!bytes(sourceDescription(observed)).equals(bytes(intent.source))) throw new Error("Migration historical source differs from its complete pre-block authority, inode, tip or projection commitment.");
  if (final && legacy && ([(await options.lstatEntry(legacy.lock.path)).mode, (await options.lstatEntry(legacy.transactions.path)).mode].some((mode) => (mode & 0o7777) !== 0o500))) throw new Error("Migration final v1 authority is not frozen at exact 0500.");
  return observed;
 }
 async function freeze(path, expected, revalidate, options) {
  await revalidate();
  const initial = safeStat(await options.lstatEntry(path), "directory", options, true);
  if (!same(expected, initial)) throw new Error("Migration freeze target differs from its pinned original inode.");
  const handle = await options.openFile(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
   const opened = safeStat(await handle.stat(), "directory", options, true);
   if (!same(initial, opened)) throw new Error("Migration freeze handle changed inode.");
   await boundary(options, "before", "freeze", path); await revalidate();
   if (!same(opened, await options.lstatEntry(path))) throw new Error("Migration freeze pathname was replaced.");
   await handle.chmod(0o500);
   await boundary(options, "after", "freeze", path);
   const after = safeStat(await handle.stat(), "directory", options, true);
   if ((after.mode & 0o7777) !== 0o500 || !same(opened, after) || !same(after, await options.lstatEntry(path))) throw new Error("Migration freeze did not preserve the exact original directory.");
   await boundary(options, "before", "directory-sync", path); await handle.sync(); await boundary(options, "after", "directory-sync", path);
  } finally { await handle.close(); }
  await sync(dirname(path), options); await revalidate();
 }
 async function installGuard(state, meta, intent, options) {
  if (intent.source.legacyLayout === "in-place") return;
  const guard = bytes(guardFor(intent));
  const target = `${state}.lock`;
  const current = await file(target, options, options.metadataMaxBytes, false);
  const proofPath = join(meta, "guard.json");
  await immutable(meta, "guard.json", guardFor(intent), options);
  const sourceIdentity = identity(await options.lstatEntry(proofPath));
  // guard.json and its receipt are immutable; the third link is the downgrade fence.
  if (sameBytes(current, guard)) {
   if (!same(sourceIdentity, await options.lstatEntry(target))) throw new Error("Migration v3 guard is a different inode from its proof.");
   return;
  }
  const oldGuard = { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: intent.statePathSha256 };
  if (current !== null && !current.equals(bytes(oldGuard))) throw new Error("Migration prior guard is not exact.");
  const previousIdentity = await absent(target, options);
  const temporary = join(meta, `guard-install-${digest(bytes(intent))}.tmp`);
  try { await options.linkFile(proofPath, temporary); }
  catch (error) { if (options.linkFile !== link || error?.code !== "EEXIST" || !same(sourceIdentity, await options.lstatEntry(temporary))) throw error; }
  await sync(meta, options);
  await boundary(options, "before", "guard-rename", target);
  const before = await absent(target, options);
  if (same(sourceIdentity, before)) {
   const pending = await absent(temporary, options);
   if (pending !== null) { if (!same(sourceIdentity, pending)) throw new Error("Migration guard link was replaced."); await options.removeFile(temporary); await sync(meta, options); }
   return;
  }
  if (previousIdentity === null ? before !== null : !same(previousIdentity, before)) throw new Error("Migration guard changed inode before replacement.");
  if (!same(sourceIdentity, await options.lstatEntry(temporary))) throw new Error("Migration guard temporary changed inode.");
  try { await options.renameFile(temporary, target); }
  catch (error) { if (options.renameFile !== rename || error?.code !== "ENOENT" || !same(sourceIdentity, await absent(target, options))) throw error; }
  await boundary(options, "after", "guard-rename", target);
  if (!same(sourceIdentity, await options.lstatEntry(target)) || !sameBytes(await file(target, options), guard)) throw new Error("Migration guard replacement differs from its exact receipt-backed inode.");
  await sync(meta, options); await sync(dirname(target), options);
 }
 async function finalAuthority(state, meta, intent, options) {
  const observed = await historicalFromIntent(state, intent, options, { final: true });
  if (intent.source.legacyLayout !== "in-place") {
   if (!sameBytes(await file(`${state}.lock`, options), bytes(guardFor(intent))) || !same(await options.lstatEntry(`${state}.lock`), await options.lstatEntry(join(meta, "guard.json")))) throw new Error("Migration final source lacks its exact v3 guard inode.");
  }
  const source = intent.source.kind === "fresh" ? null : { kind: intent.source.kind,
   authoritySha256: commitment("pylon-migration-final-source-v3", [["state", Buffer.from(state)], ["intent", bytes(intent)], ["final-path", Buffer.from(intent.source.kind === "v2" ? `${state}.journal.v2-retired` : lockPathFor(state, intent))], ["source", bytes(sourceDescription(observed))]]),
   tipBytes: observed.source.tipBytes };
  const migration = intent.source.kind === "v2" && observed.legacy ? { kind: "v1", authoritySha256: observed.legacy.authoritySha256, tipBytes: observed.legacy.tipBytes } : null;
  const authority = { genesis: { statePath: state, stateBytes: observed.source ? observed.source.tipBytes : decodeProjection(intent, options), source, migration } };
  const checkpoint = format.consumerGenerationGenesisCheckpoint(authority.genesis, options.stateMaxBytes);
  return { authority, checkpoint, observed };
 }
 async function validateMeta(state, meta, intent, observed, options) {
  const present = await names(meta, MAX_CONSTRUCTION_ROOTS + 7, options);
  const allowed = new Set(["receipts", "intent.json", "guard.json", "final.json", "root.json", "complete.json"]);
  if (intent) {
   allowed.add(`guard-install-${digest(bytes(intent))}.tmp`);
   const rootPattern = new RegExp(`^journal-${digest(bytes(intent))}-${UUID}$`);
   const roots = present.filter((name) => rootPattern.test(name));
   if (roots.length > MAX_CONSTRUCTION_ROOTS) throw new Error("Migration construction root bound is exhausted.");
   const selectedBytes = await file(join(meta, "root.json"), options, options.metadataMaxBytes, false);
   const selected = selectedBytes === null ? null : canonical(selectedBytes).goal;
   for (const name of roots) {
    allowed.add(name); await directory(join(meta, name), options);
    if (name !== selected && (await names(join(meta, name), 0, options)).length !== 0) throw new Error("Unselected migration construction root contains foreign authority.");
   }
  }
  if (!present.includes("receipts") || present.some((name) => !allowed.has(name))) throw new Error("Migration authority sidecar contains an unexpected closed entry.");
  const targets = new Map();
  for (const name of ["intent.json", "guard.json", "final.json", "root.json", "complete.json"]) targets.set(receiptName(name), join(meta, name));
  if (observed?.legacy) {
   targets.set(receiptName(`v1-lock/${MARKER}`), join(observed.legacy.lock.path, MARKER));
   for (const name of observed.legacy.lock.records.keys()) targets.set(receiptName(`v1-lock/${name}`), join(observed.legacy.lock.path, name));
   for (const name of observed.legacy.transactions.records.keys()) targets.set(receiptName(`v1-transactions/${name}`), join(observed.legacy.transactions.path, name));
   for (const recovery of observed.legacy.recoveries) {
    const logical = `v1-${recovery.target.startsWith(`${state}.transactions/`) ? "transactions" : "lock"}/${basename(recovery.target)}`;
    targets.set(receiptName(logical), recovery.target);
   }
   if (intent) targets.set(receiptName("blocker-v1"), join(observed.legacy.lock.path, BLOCKER));
  }
  if (intent && observed?.source?.kind === "v2") for (const epoch of observed.source.epochs.values()) targets.set(receiptName(`blocker-v2/${basename(epoch.path)}`), join(epoch.path, BLOCKER));
  const receipts = join(meta, "receipts"); await directory(receipts, options);
  const entries = await names(receipts, MAX_ENTRIES, options); let charged = 0;
  for (const path of [...present.filter((name) => name !== "receipts" && !name.startsWith("journal-")).map((name) => join(meta, name)), ...entries.map((name) => join(receipts, name))]) {
   const stat = safeStat(await options.lstatEntry(path), "file", options);
   if (stat.size < (writingPattern.test(basename(path)) ? 0 : 1) || stat.size > options.metadataMaxBytes || (charged += stat.size) > MAX_BYTES * 2) throw new Error("Migration authority sidecar exceeds its aggregate byte bound.");
  }
  for (const name of entries) {
   const path = join(receipts, name); const stat = await options.lstatEntry(path);
   const temporary = writingPattern.exec(name);
   if (temporary) {
    if (!targets.has(`receipt-${temporary[3]}.json`)) throw new Error("Migration temporary has an unknown immutable target.");
    if (![1, 2].includes(stat.nlink)) throw new Error("Migration receipt temporary has unexpected links.");
    if (stat.nlink === 2) {
     const target = targets.get(`receipt-${temporary[3]}.json`);
     if (!target || !same(stat, await options.lstatEntry(target))) throw new Error("Migration linked temporary lacks its exact canonical target inode.");
    }
    continue;
   }
   const target = targets.get(name);
   if (!target || !same(stat, await options.lstatEntry(target)) || ![2, ...(target === join(meta, "guard.json") ? [3, 4] : [])].includes(stat.nlink)) throw new Error("Migration fixed receipt lacks exact canonical authority.");
   if (!sameBytes(await file(path, options), await file(target, options))) throw new Error("Migration receipt differs from its canonical target bytes.");
  }
  if (intent && present.includes("guard.json")) {
   const guardStat = await options.lstatEntry(join(meta, "guard.json"));
   const target = await absent(`${state}.lock`, options);
   const temporary = await absent(join(meta, `guard-install-${digest(bytes(intent))}.tmp`), options);
   if (temporary && !same(guardStat, temporary)) throw new Error("Migration guard staging link changed inode.");
   const expectedLinks = 2 + (same(guardStat, target) ? 1 : 0) + (temporary ? 1 : 0);
   if (guardStat.nlink !== expectedLinks) throw new Error("Migration guard has an unaccounted authority link.");
  }
  return { charged, targets };
 }
 async function cleanupReceipts(state, meta, intent, observed, options) {
  const { targets } = await validateMeta(state, meta, intent, observed, options);
  const receipts = join(meta, "receipts"); const directoryIdentity = await directory(receipts, options);
  for (const name of await names(receipts, MAX_ENTRIES, options)) {
   const match = writingPattern.exec(name); if (!match) continue;
   const path = join(receipts, name); const stat = safeStat(await options.lstatEntry(path), "file", options);
   const fixed = join(receipts, `receipt-${match[3]}.json`); const target = targets.get(`receipt-${match[3]}.json`);
   const canonicalStat = await absent(target, options);
   await file(path, options, options.metadataMaxBytes, true, 0);
   if (stat.nlink === 2) {
    if (!same(stat, canonicalStat)) throw new Error("Migration linked temporary lost its canonical inode.");
    await boundary(options, "before", "receipt-rename", fixed);
    if (!same(stat, await options.lstatEntry(path)) || !same(directoryIdentity, await directory(receipts, options))) throw new Error("Migration receipt cleanup was fenced by inode replacement.");
    try { await options.renameFile(path, fixed); }
    catch (error) { if (options.renameFile !== rename || error?.code !== "ENOENT" || !same(stat, await absent(fixed, options))) throw error; }
    await boundary(options, "after", "receipt-rename", fixed); await sync(receipts, options);
   } else {
    const decided = canonicalStat !== null && same(canonicalStat, await absent(fixed, options));
    if (!decided && !dead(Number(match[1]), options)) throw new Error("Migration has a live unresolved receipt writer.");
    await boundary(options, "before", "receipt-unlink", path);
    if (!same(stat, await options.lstatEntry(path)) || !same(directoryIdentity, await directory(receipts, options))) throw new Error("Migration losing receipt changed inode before cleanup.");
    await options.removeFile(path); await boundary(options, "after", "receipt-unlink", path); await sync(receipts, options);
   }
  }
 }

 async function prepareInstalled(state, meta, intent, options) {
  const { authority, checkpoint } = await finalAuthority(state, meta, intent, options);
  const wanted = { schemaVersion: 3, kind: "pylon-consumer-final-source", intentSha256: digest(bytes(intent)), sourceAuthoritySha256: checkpoint.sourceAuthoritySha256, genesisSha256: digest(bytes(checkpoint)) };
  await immutable(meta, "final.json", wanted, options);
  let rootRecord = await file(join(meta, "root.json"), options, options.metadataMaxBytes, false);
  if (rootRecord === null) {
   await validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
   const roots = (await names(meta, MAX_CONSTRUCTION_ROOTS + 7, options)).filter((name) => name.startsWith("journal-"));
   if (roots.length >= MAX_CONSTRUCTION_ROOTS) throw new Error("Migration construction root bound is exhausted.");
   const goal = `journal-${digest(bytes(intent))}-${randomUUID()}`;
   const root = join(meta, goal);
   await boundary(options, "before", "mkdir", root);
   // A collision is never an ownership join. An unrecorded empty directory is
   // inert after a crash; resume creates another exclusive construction root.
   await options.makeDirectory(root, { mode: 0o700 });
   const created = await directory(root, options);
   await boundary(options, "after", "mkdir", root);
   if (!same(created, await directory(root, options))) throw new Error("Exclusive migration root changed inode after creation.");
   await sync(root, options); await sync(meta, options);
   const candidate = { schemaVersion: 3, kind: "pylon-consumer-root", intentSha256: digest(bytes(intent)), goal, identity: created };
   await immutable(meta, "root.json", candidate, options);
   rootRecord = await file(join(meta, "root.json"), options);
  }
  const expected = canonical(rootRecord);
  if (!closed(expected, ["schemaVersion", "kind", "intentSha256", "goal", "identity"]) || expected.schemaVersion !== 3 || expected.kind !== "pylon-consumer-root" || expected.intentSha256 !== digest(bytes(intent)) || !new RegExp(`^journal-${digest(bytes(intent))}-${UUID}$`).test(expected.goal ?? "") || !closed(expected.identity, ["dev", "ino"]) || !Object.values(expected.identity).every((value) => Number.isSafeInteger(value) && value >= 0)) throw new Error("Migration canonical root proof is malformed.");
  const root = join(meta, expected.goal);
  await receiptFor(meta, "root.json", join(meta, "root.json"), rootRecord, options);
  if (!same(expected.identity, await directory(root, options))) throw new Error("Migration canonical root is a different inode from its durable creation proof.");
  await cleanupReceipts(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
  await format.prepareConsumerGeneration(root, authority, options);
  if (!same(expected.identity, await directory(root, options))) throw new Error("Migration canonical root was replaced during preparation.");
  await validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
  return { root, authority, checkpoint };
 }
 async function recoverDirectV1(state, meta, observed, options) {
  if (observed.source.kind !== "v1") return observed;
  const initial = observed;
  const expectedLock = new Map(initial.legacy.lock.records);
  const expectedTransactions = new Map(initial.legacy.transactions.records);
  for (const recovery of initial.legacy.recoveries) {
   (recovery.target.startsWith(`${state}.transactions/`) ? expectedTransactions : expectedLock).set(basename(recovery.target), bytes(recovery.value));
  }
  const validateProgress = (current) => {
   if (!same(initial.legacy.lock.identity, current.legacy.lock.identity) || !same(initial.legacy.transactions.identity, current.legacy.transactions.identity) || current.legacy.tipDigest !== initial.legacy.tipDigest || !sameBytes(current.legacy.tipBytes, initial.legacy.tipBytes)) throw new Error("Migration v1 recovery changed original source identity or tip.");
   for (const [original, actual, expected] of [[initial.legacy.lock, current.legacy.lock, expectedLock], [initial.legacy.transactions, current.legacy.transactions, expectedTransactions]]) {
    if (actual.records.size !== expected.size) throw new Error("Migration v1 recovery contains unapproved authority changes.");
    for (const [name, data] of expected) if (!sameBytes(actual.records.get(name) ?? null, data) || original.stats.has(name) && !same(original.stats.get(name), actual.stats.get(name))) throw new Error("Migration v1 recovery differs from its exact allowed bytes and original inodes.");
   }
  };
  for (const recovery of observed.legacy.recoveries) {
   if (!dead(recovery.owner.ownerPid, options)) throw new Error("Migration has a live or uncertain unresolved v1 owner.");
   const logical = `v1-${recovery.target.startsWith(`${state}.transactions/`) ? "transactions" : "lock"}/${basename(recovery.target)}`;
   await receiptFor(meta, logical, recovery.target, bytes(recovery.value), options, { publish: true });
  }
  observed = await inspect(state, options);
  validateProgress(observed);
  if (observed.legacy.recoveries.length) throw new Error("Migration v1 recovery did not reach exact permitted decisions.");
  if (observed.legacy.marker === null) {
   const marker = format.legacyRetirementMarkerFor(state, observed.legacy);
   expectedLock.set(MARKER, bytes(marker));
   await receiptFor(meta, `v1-lock/${MARKER}`, join(observed.legacy.lock.path, MARKER), bytes(marker), options, { publish: true });
   observed = await inspect(state, options);
   validateProgress(observed);
  }
  return observed;
 }
 async function migrate(statePath, rawOptions = {}) {
  if (rawOptions.acknowledgeLegacyProcessesStopped !== true) throw new Error("Explicit acknowledgement that all legacy processes are stopped is required before any migration mutation.");
  if (typeof statePath !== "string" || !statePath) throw new Error("Migration state path is required.");
  const options = optionsFor(rawOptions); const state = resolve(statePath); const meta = `${state}.journal-v3`;
  await canonicalAncestors(state, options);
  let intent = await absent(meta, options) === null ? null : await readIntent(meta, state, options, true);
  if (intent === null) {
   let observed = await inspect(state, options);
   await make(meta, options); await make(join(meta, "receipts"), options);
   await validateMeta(state, meta, null, observed, options);
   observed = await recoverDirectV1(state, meta, observed, options);
   if (observed.source.kind === "v2" && !observed.source.epochs.has(observed.source.headEpoch)) {
    const original = observed;
    await make(join(original.source.root, original.source.headEpoch), options);
    observed = await inspect(state, options);
    if (!same(original.source.identity, observed.source.identity) || observed.source.epochs.size !== 1 || observed.source.epochs.get(original.source.headEpoch)?.records.size !== 0 || observed.source.tipDigest !== original.source.tipDigest || !sameBytes(observed.projection, original.projection)) throw new Error("Initial v2 epoch completion changed historical authority.");
    for (const [name, stat] of original.source.stats) if (!same(stat, observed.source.stats.get(name))) throw new Error("Initial v2 epoch completion replaced an original source inode.");
    if (!bytes(original.source.records).equals(bytes(observed.source.records))) throw new Error("Initial v2 epoch completion changed original source bytes.");
   }
   if (observed.source.kind === "v2") {
    for (const recovery of observed.source.recoveries) if (recovery.value.outcome === "retired" && !dead(recovery.owner.ownerPid, options)) throw new Error("Migration has a live unresolved v2 owner.");
   }
   intent = intentFor(state, observed);
   await immutable(meta, "intent.json", intent, options);
  }
  await historicalFromIntent(state, intent, options);
  const blocker = bytes(blockerFor(intent));
  let observed = await historicalFromIntent(state, intent, options);
  if (observed.legacy) await receiptFor(meta, "blocker-v1", join(observed.legacy.lock.path, BLOCKER), blocker, options, { publish: true });
  if (observed.source.kind === "v2") {
   // Every retained epoch is fenced: the protected reader can validate and help
   // a retained operation before inspecting the replacement regular guard.
   for (const epoch of observed.source.epochs.values()) await receiptFor(meta, `blocker-v2/${basename(epoch.path)}`, join(epoch.path, BLOCKER), blocker, options, { publish: true });
  }
  await options.hooks?.afterMigrationBlocker?.({ statePath: state, meta });
  observed = await historicalFromIntent(state, intent, options, { blocked: true });
  if (observed.legacy) {
   const revalidate = () => historicalFromIntent(state, intent, options, { blocked: true });
   await freeze(observed.legacy.lock.path, intent.source.legacyLockIdentity, revalidate, options);
   await freeze(observed.legacy.transactions.path, intent.source.legacyTransactionsIdentity, revalidate, options);
  }
  await historicalFromIntent(state, intent, options, { blocked: true });
  await installGuard(state, meta, intent, options);
  await options.hooks?.afterMigrationGuard?.({ statePath: state, meta });
  if (intent.source.kind === "v2" && await absent(`${state}.journal.v2-retired`, options) === null) {
   await historicalFromIntent(state, intent, options);
   const source = `${state}.journal`; const destination = `${state}.journal.v2-retired`;
   await boundary(options, "before", "source-rename", destination);
   const destinationEntry = await absent(destination, options);
   if (destinationEntry !== null) {
    if (!same(intent.source.sourceIdentity, destinationEntry) || await absent(source, options) !== null) throw new Error("Migration source retirement destination is a conflicting inode.");
   } else {
    if (!same(intent.source.sourceIdentity, await directory(source, options))) throw new Error("Migration source was replaced before retirement.");
    try { await options.renameFile(source, destination); }
    catch (error) { if (options.renameFile !== rename || error?.code !== "ENOENT" || !same(intent.source.sourceIdentity, await absent(destination, options))) throw error; }
   }
   await boundary(options, "after", "source-rename", destination);
   if (!same(intent.source.sourceIdentity, await directory(destination, options))) throw new Error("Migration source retirement has a different destination inode.");
   await sync(dirname(source), options);
  }
  const installed = await prepareInstalled(state, meta, intent, options);
  await options.hooks?.afterMigrationInstalled?.({ statePath: state, root: installed.root });
  let repaired = false;
  for (let attempt = 0; attempt < 32; attempt++) {
   try { await format.withConsumerGenerationLock(installed.root, installed.authority, async () => {}, options); repaired = true; break; }
   catch (error) { if (!format.isGenerationBusy(error)) throw error; }
  }
  if (!repaired) throw new Error("Concurrent migration projection repair exceeded its bounded active-owner joins.");
  await finalAuthority(state, meta, intent, options);
  const current = await format.prepareConsumerGeneration(installed.root, installed.authority, options);
  const complete = { schemaVersion: 3, kind: "pylon-consumer-migration-complete", intentSha256: digest(bytes(intent)), rootIdentity: await directory(installed.root, options), genesisSha256: digest(bytes(installed.checkpoint)) };
  await immutable(meta, "complete.json", complete, options);
  await options.hooks?.afterMigrationComplete?.({ statePath: state, checkpoint: current.checkpoint });
  await finalAuthority(state, meta, intent, options);
  return { epoch: current.checkpoint.epoch, tipSha256: format.generationEpochAuthority(current, options).tip.tipDigest, sourceAuthoritySha256: installed.checkpoint.sourceAuthoritySha256 };
 }

 async function prepareState(statePath, rawOptions = {}) {
  if (typeof statePath !== "string" || !statePath) throw new Error("A consumer-local state path is required.");
  const options = optionsFor(rawOptions); const state = resolve(statePath); const meta = `${state}.journal-v3`;
  await canonicalAncestors(state, options, true);
  let intent = await absent(meta, options) === null ? null : await readIntent(meta, state, options);
  if (intent === null) {
   for (const path of [`${state}.journal`, `${state}.journal.v2-retired`, `${state}.transactions`, `${state}.lock`, `${state}.lock.v1-retired`]) {
    if (await absent(path, options) !== null) throw new Error("Historical consumer authority requires explicit migration with acknowledgement that all legacy processes are stopped.");
   }
   const projection = await file(state, options, options.stateMaxBytes, false);
   const observed = { source: null, legacy: null, projection };
   intent = intentFor(state, observed);
   await make(meta, options); await make(join(meta, "receipts"), options);
   await immutable(meta, "intent.json", intent, options);
   await installGuard(state, meta, intent, options);
  } else if (intent.source.kind !== "fresh") {
   // Normal entry never advances an unfinished legacy migration.
   const completed = await file(join(meta, "complete.json"), options, options.metadataMaxBytes, false);
   if (completed === null) throw new Error("Interrupted legacy migration requires the explicitly acknowledged migration command.");
   const { checkpoint } = await finalAuthority(state, meta, intent, options);
   const selected = canonical(await file(join(meta, "root.json"), options));
   if (typeof selected.goal !== "string" || !new RegExp(`^journal-${digest(bytes(intent))}-${UUID}$`).test(selected.goal)) throw new Error("Migration completion has an invalid selected root.");
   const root = join(meta, selected.goal);
   const expected = { schemaVersion: 3, kind: "pylon-consumer-migration-complete", intentSha256: digest(bytes(intent)), rootIdentity: await directory(root, options), genesisSha256: digest(bytes(checkpoint)) };
   if (!completed.equals(bytes(expected))) throw new Error("Migration completion differs from exact final source and root authority.");
   await receiptFor(meta, "complete.json", join(meta, "complete.json"), completed, options, { repair: false });
  } else await installGuard(state, meta, intent, options);
  const installed = await prepareInstalled(state, meta, intent, options);
  if (intent.source.kind !== "fresh") {
   const complete = { schemaVersion: 3, kind: "pylon-consumer-migration-complete", intentSha256: digest(bytes(intent)), rootIdentity: await directory(installed.root, options), genesisSha256: digest(bytes(installed.checkpoint)) };
   await receiptFor(meta, "complete.json", join(meta, "complete.json"), bytes(complete), options);
  }
  return { ...installed, state, meta, intent, options };
 }
 async function withState(statePath, action, rawOptions = {}) {
  if (typeof action !== "function") throw new Error("Consumer high-water lock action must be a function.");
  const prepared = await prepareState(statePath, rawOptions);
  const { root, authority, state, meta, intent, options } = prepared;
  const validate = async () => validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
  const result = await format.withConsumerGenerationLock(root, authority, async (path, transaction) => {
   await validate();
   const result = await action(path, transaction);
   await validate();
   return result;
  }, options);
  await validate();
  return result;
 }
 async function rotate(statePath, rawOptions = {}) {
  const { root, authority, state, meta, intent, options } = await prepareState(statePath, rawOptions);
  const initial = await format.prepareConsumerGeneration(root, authority, options);
  const scan = format.generationEpochAuthority(initial, options);
  const latest = scan.claims.at(-1);
  const terminal = latest && scan.terminals.get(key(latest));
  if (latest?.type === "normal" && (!terminal || terminal.outcome === "commit" && !scan.applied.has(key(latest)))) {
   await format.withConsumerGenerationLock(root, authority, async () => {}, options);
   const recovered = await format.prepareConsumerGeneration(root, authority, options);
   if (recovered.checkpoint.epoch > initial.checkpoint.epoch) {
    await validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
    return { epoch: recovered.checkpoint.epoch, tipSha256: format.generationEpochAuthority(recovered, options).tip.tipDigest };
   }
  }
  const ready = await format.prepareConsumerGeneration(root, authority, options);
  const readyScan = format.generationEpochAuthority(ready, options);
  if (ready.checkpoint.epoch > 1 && readyScan.tip.length === 0 && readyScan.claims.at(-1)?.type !== "rotation" && ready.retirementCertificate === null) {
   const projection = await file(state, options, options.stateMaxBytes, false, 0);
   if (!sameBytes(projection, readyScan.tip.tipBytes)) await format.withConsumerGenerationLock(root, authority, async () => {}, options);
   await validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
   return { epoch: ready.checkpoint.epoch, tipSha256: readyScan.tip.tipDigest };
  }
  // The reserved rotation slot remains usable when the normal frontier is full.
  const result = await format.rotateConsumerGeneration(root, authority, options);
  await format.withConsumerGenerationLock(root, authority, async () => {}, options);
  await validateMeta(state, meta, intent, (await finalAuthority(state, meta, intent, options)).observed, options);
  return result;
 }

 async function inspect(statePath, rawOptions = {}) {
  if (typeof statePath !== "string" || !statePath) throw new Error("Migration state path is required.");
  const options = optionsFor(rawOptions);
  const state = resolve(statePath);
  await directory(dirname(state), options);
  const projection = await file(state, options, options.stateMaxBytes, false, 0);
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
  if (lock && !lock.isDirectory()) {
   const expected = { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) };
   if (!bytes(expected).equals(await file(`${state}.lock`, options))) throw new Error("Historical regular guard is not exact.");
  }
  if (await absent(`${state}.journal.v2-retired`, options) !== null) throw new Error("Historical retained source requires its existing migration proof.");
  const source = journal ? await readV2(state, `${state}.journal`, projection, { ...options, maxJournalBytes: MAX_BYTES }, legacy) : legacy;
  if (!source) throw new Error("No historical authority exists.");
  return { source, legacy, projection };
 }
 return { inspect, readV1, readV2, migrate, withState, rotate };

}
