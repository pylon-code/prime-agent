import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { chmod, cp, link, lstat, mkdtemp, open, readFile, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { test } from "node:test";
import * as journal from "./lib/pylon-consumer-lock.mjs";
import { generationCheckpointMaxBytes } from "./lib/pylon-generation-format.mjs";

const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const bytes = (value) => Buffer.from(`${JSON.stringify(value)}\n`);
const statePath = "/consumer/state.json";
const genesis = (stateBytes = Buffer.from("initial"), rest = {}) => ({ genesis: { statePath, stateBytes, ...rest } });
async function fixture(t) {
	const root = await mkdtemp(join(tmpdir(), "pylon-generation-"));
	await chmod(root, 0o700);
	t.after(() => rm(root, { recursive: true, force: true }));
	return root;
}
async function installed(t, authority = genesis()) {
	const root = await fixture(t);
	const builder = await journal.buildConsumerGeneration(root, authority);
	const result = await journal.publishConsumerGeneration(builder);
	return { root, authority, ...result };
}
async function record(path, name, value) {
	await writeFile(join(path, "epoch", name), bytes(value), { mode: 0o600 });
	await link(join(path, "epoch", name), join(path, "receipts", `receipt-${hash(Buffer.from(`epoch/${name}`))}.json`));
}
async function predecessor(t) {
	const result = await installed(t);
	const checkpoint = result.checkpoint;
	const claim = journal.consumerGenerationRotationClaim(checkpoint, 1, { tipBytes: Buffer.from("initial"), tipDigest: hash(Buffer.from("initial")) });
	await record(result.path, `claim-0000000000000001-${hash(bytes(claim))}.json`, claim);
	await record(result.path, "claim-index-0000000000000001.json", { schemaVersion: 1, generation: 1, claimSha256: hash(bytes(claim)) });
	const snapshot = await journal.readConsumerGeneration(result.path, result.authority);
	return { ...result, claim, snapshot };
}

test("v3 generation genesis reconstructs exact state, identity, history and provenance", async (t) => {
	const authority = genesis();
	const first = await installed(t, authority);
	const expected = journal.consumerGenerationGenesisCheckpoint(authority.genesis);
	assert.deepEqual(first.checkpoint, expected);
	assert.equal(expected.anchorBase64, Buffer.from("initial").toString("base64"));
	assert.notEqual(expected.epochId, journal.consumerGenerationGenesisCheckpoint(genesis(Buffer.from("other")).genesis).epochId);
	await assert.rejects(journal.readConsumerGeneration(first.path, genesis(Buffer.from("other"))), /exact|authority/);
	for (const stateBytes of [Buffer.alloc(0), Buffer.alloc(16 * 1024 * 1024 + 1)]) {
		assert.throws(() => journal.consumerGenerationGenesisCheckpoint(genesis(stateBytes).genesis), /bound|state/);
	}
});

test("v3 generation requires exact latest rotation claim CAS and byte-bound successor", async (t) => {
	const p = await predecessor(t);
	const candidate = bytes(p.claim.intent.checkpoint);
	const name = journal.consumerGenerationName(p.claim.intent.checkpoint);
	assert.doesNotThrow(() => journal.assertConsumerGenerationSuccessor(p.snapshot, name, candidate));
	for (const field of Object.keys(p.claim.intent.checkpoint)) {
		const malformed = structuredClone(p.claim.intent.checkpoint);
		malformed[field] = typeof malformed[field] === "number" ? malformed[field] + 1 : "wrong";
		assert.throws(() => journal.assertConsumerGenerationSuccessor(p.snapshot, name, bytes(malformed)), undefined, field);
	}
	assert.throws(() => journal.assertConsumerGenerationSuccessor({ ...p.snapshot, epochRecords: new Map() }, name, candidate), /rotation|authority/);
	const absentIndex = new Map(p.snapshot.epochRecords);
	absentIndex.delete("claim-index-0000000000000001.json");
	assert.throws(() => journal.assertConsumerGenerationSuccessor({ ...p.snapshot, epochRecords: absentIndex }, name, candidate), /rotation|authority/);
	const unknown = new Map(p.snapshot.epochRecords).set("unexpected.json", bytes({}));
	assert.throws(() => journal.assertConsumerGenerationSuccessor({ ...p.snapshot, epochRecords: unknown }, name, candidate), /epoch|entry/);
	const next = await journal.buildConsumerGeneration(p.root, { predecessor: p.snapshot });
	const published = await journal.publishConsumerGeneration(next);
	assert.equal(published.checkpoint.epoch, 2);
});

test("v3 generation receipt and publication durability order", async (t) => {
	const root = await fixture(t);
	const events = [];
	const hooks = { generationBoundary: ({ phase, operation, path }) => { events.push([phase, operation, basename(path)]); } };
	const builder = await journal.buildConsumerGeneration(root, genesis(), { hooks });
	assert.deepEqual((await readdir(root)), [basename(builder.path)]);
	assert.deepEqual(await readdir(join(builder.path, "epoch")), []);
	assert.equal((await readdir(join(builder.path, "receipts"))).length, 1);
	const published = await journal.publishConsumerGeneration(builder, { hooks });
	const operations = events.filter(([phase]) => phase === "after").map(([, operation, path]) => `${operation}:${path}`);
	const fileSync = operations.findIndex((value) => value.startsWith("file-sync:.receipt-"));
	const receiptSync = operations.indexOf("sync:receipts", fileSync + 1);
	const canonicalLink = operations.indexOf("link:checkpoint.json");
	const receiptRename = operations.findIndex((value) => value.startsWith("rename:receipt-"));
	const finalRename = operations.indexOf(`rename:${basename(published.path)}`);
	assert.ok(fileSync >= 0 && fileSync < receiptSync);
	assert.ok(receiptSync < canonicalLink && canonicalLink < receiptRename && receiptRename < finalRename);
	assert.ok(operations.slice(canonicalLink + 1, receiptRename).includes(`sync:${basename(builder.path)}`));
	assert.ok(operations.slice(receiptRename + 1, finalRename).includes("sync:receipts"));
	assert.ok(operations.slice(receiptRename + 1, finalRename).includes("sync:epoch"));
	assert.equal(operations.at(-1), `sync:${basename(root)}`);
});

test("v3 generation rejects incomplete epoch, orphan receipts and identical receipt replacements", async (t) => {
	for (const mutation of [
		async (p) => writeFile(join(p, "epoch", "unknown.json"), "{}", { mode: 0o600 }),
		async (p) => writeFile(join(p, "receipts", `receipt-${"0".repeat(64)}.json`), "{}", { mode: 0o600 }),
		async (p) => { const r = join(p, "receipts", (await readdir(join(p, "receipts")))[0]); const b = await readFile(r); await rm(r); await writeFile(r, b, { mode: 0o600 }); },
		async (p) => { await rm(join(p, "epoch"), { recursive: true }); },
		async (p) => chmod(join(p, "receipts"), 0o755),
		async (p) => { await rm(join(p, "epoch"), { recursive: true }); await symlink("receipts", join(p, "epoch")); },
	]) {
		const g = await installed(t);
		await mutation(g.path);
		await assert.rejects(journal.readConsumerGeneration(g.path, g.authority));
	}
});

test("v3 generation native rename loss joins only the observed builder inode", async (t) => {
	const root = await fixture(t);
	const builder = await journal.buildConsumerGeneration(root, genesis());
	let moved = false;
	const result = await journal.publishConsumerGeneration(builder, { hooks: { beforeGenerationRename: async ({ source, destination }) => {
		moved = true;
		await rename(source, destination);
	} } });
	assert.ok(moved);
	assert.equal((await lstat(result.path)).ino, builder.identity.ino);
	const otherRoot = await fixture(t);
	const other = await journal.buildConsumerGeneration(otherRoot, genesis());
	await assert.rejects(journal.publishConsumerGeneration(other, { hooks: { beforeGenerationRename: async ({ source, destination }) => {
		await cp(source, destination, { recursive: true });
		const receipt = join(destination, "receipts", (await readdir(join(destination, "receipts")))[0]);
		await rm(receipt); await link(join(destination, "checkpoint.json"), receipt);
		await rm(source, { recursive: true });
	} } }), /inode|identity/);
});

for (const code of ["ENOENT", "EIO", "EPERM"]) {
	test(`v3 generation preserves injected ${code} identity even with concurrent rename`, async (t) => {
		for (const stage of ["beforeGenerationRename", "afterGenerationRename", "renameFile"]) {
			const root = await fixture(t);
			const builder = await journal.buildConsumerGeneration(root, genesis());
			const error = Object.assign(new Error(`injected ${stage}`), { code });
			const options = stage === "renameFile" ? { renameFile: async (source, destination) => { await rename(source, destination); throw error; } } : { hooks: { [stage]: async ({ source, destination }) => {
				if (stage === "beforeGenerationRename") await rename(source, destination);
				throw error;
			} } };
			await assert.rejects(journal.publishConsumerGeneration(builder, options), (actual) => actual === error);
		}
	});
}

test("v3 generation bounds all three actual 16 MiB base64 fields and receipt duplication", async (t) => {
	const state = Buffer.alloc(16 * 1024 * 1024, 0x61);
	const provenance = { kind: "v2", authoritySha256: "1".repeat(64), tipBytes: state };
	const migration = { kind: "v1", authoritySha256: "2".repeat(64), tipBytes: state };
	const authority = genesis(state, { source: provenance, migration });
	const value = journal.consumerGenerationGenesisCheckpoint(authority.genesis);
	const encoded = bytes(value);
	assert.ok(encoded.length > 67_108_872);
	assert.ok(encoded.length <= generationCheckpointMaxBytes());
	const root = await fixture(t);
	const builder = await journal.buildConsumerGeneration(root, authority);
	const g = await journal.publishConsumerGeneration(builder);
	assert.equal(g.totalBytes, 2 * encoded.length);
	await assert.rejects(journal.readConsumerGeneration(g.path, authority, { maxJournalBytes: 2 * encoded.length - 1 }), /byte bound/);
	const claim = journal.consumerGenerationRotationClaim(g.checkpoint, 1, { tipDigest: hash(state), tipBytes: state });
	await record(g.path, `claim-0000000000000001-${hash(bytes(claim))}.json`, claim);
	await record(g.path, "claim-index-0000000000000001.json", { schemaVersion: 1, generation: 1, claimSha256: hash(bytes(claim)) });
	const snapshot = await journal.readConsumerGeneration(g.path, authority);
	const successor = await journal.buildConsumerGeneration(root, { predecessor: snapshot });
	const rotated = await journal.publishConsumerGeneration(successor);
	assert.equal(rotated.checkpoint.epoch, 2);
	assert.ok(snapshot.totalBytes + rotated.totalBytes > 384 * 1024 * 1024);
});

test("v3 generation enforces root, epoch and receipt counts before nested reads", async (t) => {
	const g = await installed(t);
	for (const target of [g.root, join(g.path, "epoch"), join(g.path, "receipts")]) {
		let nested = false;
		await assert.rejects(journal.readConsumerGeneration(g.path, g.authority, {
			readDirectory: async (path) => path === target ? Array.from({ length: 700_001 }, (_, i) => `entry-${i}`) : readdir(path),
			openFile: async () => { nested = true; throw new Error("nested open"); },
		}), /entry bound/);
		assert.equal(nested, false);
	}
});

test("v3 generation epoch authenticates commits, indexed losers, latest intent and immutable tip", async (t) => {
	const g = await installed(t);
	const token = randomUUID();
	const normal = { schemaVersion: 2, generation: 1, token, type: "normal", ownerPid: process.pid, createdAtMs: 1 };
	const tx = { schemaVersion: 1, baseDigest: g.checkpoint.anchorDigest, candidateDigest: hash(Buffer.from("updated")), candidateBase64: Buffer.from("updated").toString("base64") };
	const terminal = { schemaVersion: 2, generation: 1, token, outcome: "commit", transactions: [tx] };
	await record(g.path, `claim-0000000000000001-${hash(bytes(normal))}.json`, normal);
	await record(g.path, "claim-index-0000000000000001.json", { schemaVersion: 1, generation: 1, claimSha256: hash(bytes(normal)) });
	await record(g.path, `terminal-0000000000000001-${token}.json`, terminal);
	// A latest decided but unapplied commit is a valid helpable epoch, never rotation authority.
	const incomplete = await journal.readConsumerGeneration(g.path, g.authority);
	const intended = journal.consumerGenerationRotationClaim(g.checkpoint, 2, { tipDigest: tx.candidateDigest, tipBytes: Buffer.from("updated") });
	assert.throws(() => journal.assertConsumerGenerationSuccessor(incomplete, journal.consumerGenerationName(intended.intent.checkpoint), bytes(intended.intent.checkpoint)), /rotation authority/);
	await record(g.path, `transition-${tx.baseDigest}.json`, tx);
	await record(g.path, `applied-0000000000000001-${token}.json`, { schemaVersion: 2, generation: 1, token, terminalSha256: hash(bytes(terminal)) });
	await record(g.path, `claim-0000000000000002-${hash(bytes(intended))}.json`, intended);
	await record(g.path, "claim-index-0000000000000002.json", { schemaVersion: 1, generation: 2, claimSha256: hash(bytes(intended)) });
	const loser = { ...normal, generation: 2, token: randomUUID() };
	await record(g.path, `claim-0000000000000002-${hash(bytes(loser))}.json`, loser);
	const snapshot = await journal.readConsumerGeneration(g.path, g.authority);
	const name = journal.consumerGenerationName(intended.intent.checkpoint);
	assert.doesNotThrow(() => journal.assertConsumerGenerationSuccessor(snapshot, name, bytes(intended.intent.checkpoint)));
	for (const mutation of [
		(records) => records.set("claim-index-0000000000000002.json", bytes({ schemaVersion: 1, generation: 2, claimSha256: hash(bytes(loser)) })),
		(records) => records.delete(`transition-${tx.baseDigest}.json`),
		(records) => records.delete(`applied-0000000000000001-${token}.json`),
		(records) => { const future = { ...normal, generation: 3, token: randomUUID() }; records.set(`claim-0000000000000003-${hash(bytes(future))}.json`, bytes(future)); records.set("claim-index-0000000000000003.json", bytes({ schemaVersion: 1, generation: 3, claimSha256: hash(bytes(future)) })); },
	]) {
		const epochRecords = new Map(snapshot.epochRecords); mutation(epochRecords);
		assert.throws(() => journal.assertConsumerGenerationSuccessor({ ...snapshot, epochRecords }, name, bytes(intended.intent.checkpoint)));
	}
});

test("v3 generation rejects mutated hidden builder and malformed destination after native loss", async (t) => {
	for (const duringRename of [false, true]) {
		const root = await fixture(t);
		const builder = await journal.buildConsumerGeneration(root, genesis());
		const corrupt = (path) => writeFile(join(path, "epoch", "extra.json"), "{}", { mode: 0o600 });
		if (!duringRename) await corrupt(builder.path);
		await assert.rejects(journal.publishConsumerGeneration(builder, { hooks: { beforeGenerationRename: async ({ source, destination }) => {
			await rename(source, destination); await corrupt(destination);
		} } }));
	}
});

test("v3 generation preserves metadata read hook errors through concurrent ancestor rename", async (t) => {
	for (const code of ["ENOENT", "EIO", "EPERM"]) {
		const g = await installed(t);
		const error = Object.assign(new Error("metadata fault"), { code });
		await assert.rejects(journal.readConsumerGeneration(g.path, g.authority, { hooks: { metadataRead: { afterInitialPathStat: async () => {
			await rename(g.path, join(g.root, `.retired-${basename(g.path)}`)); throw error;
		} } } }), (actual) => actual === error);
	}
});

test("v3 generation charges the entire root including hidden builders before file reads", async (t) => {
	const g = await installed(t);
	await assert.rejects(journal.buildConsumerGeneration(g.root, genesis(), { maxJournalBytes: g.totalBytes }), /aggregate byte bound/);
	assert.equal((await readdir(g.root)).length, 1);
	await journal.buildConsumerGeneration(g.root, genesis());
	let fileOpened = false;
	await assert.rejects(journal.readConsumerGeneration(g.path, g.authority, { maxJournalBytes: g.totalBytes, hooks: { metadataRead: { afterInitialStat: () => { fileOpened = true; } } } }), /aggregate byte bound/);
	assert.equal(fileOpened, false);
	await writeFile(join(g.root, "unexpected"), "data", { mode: 0o600 });
	await assert.rejects(journal.readConsumerGeneration(g.path, g.authority), /unexpected entry/);
});

test("v3 generation injected filesystem failures remain terminal during a concurrent rename", async (t) => {
	for (const code of ["ENOENT", "EIO", "EPERM"]) {
		for (const operation of ["lstatEntry", "openFile", "readDirectory"]) {
			const g = await installed(t);
			const error = Object.assign(new Error(`${operation} fault`), { code });
			const original = { lstatEntry: lstat, openFile: open, readDirectory: readdir }[operation];
			let fired = false;
			await assert.rejects(journal.readConsumerGeneration(g.path, g.authority, { [operation]: async (path, ...args) => {
				if (!fired && path.startsWith(g.path)) {
					fired = true; await rename(g.path, join(g.root, `.retired-${basename(g.path)}`)); throw error;
				}
				return original(path, ...args);
			} }), (actual) => actual === error);
			assert.ok(fired);
		}
	}
});

test("v3 generation receipt crash cuts retain the durable temporary proof", async (t) => {
	for (const cut of ["file-sync", "link", "rename"]) {
		const root = await fixture(t);
		const error = new Error(`cut after receipt ${cut}`);
		await assert.rejects(journal.buildConsumerGeneration(root, genesis(), { hooks: { generationBoundary: ({ phase, operation }) => {
			if (phase === "after" && operation === cut) throw error;
		} } }), (actual) => actual === error);
		const [building] = await readdir(root);
		assert.ok(building.startsWith(".building-"));
		const path = join(root, building);
		const [receipt] = await readdir(join(path, "receipts"));
		assert.ok(receipt);
		const receiptStat = await lstat(join(path, "receipts", receipt));
		assert.equal(receiptStat.nlink, cut === "file-sync" ? 1 : 2);
		if (cut !== "file-sync") assert.equal((await lstat(join(path, "checkpoint.json"))).ino, receiptStat.ino);
		assert.equal(receipt.startsWith("receipt-"), cut === "rename");
	}
});

test("v3 generation validates an actual 16 MiB immutable transaction without recursive base64 matching", async (t) => {
	const g = await installed(t);
	const state = Buffer.alloc(16 * 1024 * 1024, 0x62);
	const token = randomUUID();
	const claim = { schemaVersion: 2, generation: 1, token, type: "normal", ownerPid: process.pid, createdAtMs: 1 };
	const transaction = { schemaVersion: 1, baseDigest: g.checkpoint.anchorDigest, candidateDigest: hash(state), candidateBase64: state.toString("base64") };
	const terminal = { schemaVersion: 2, generation: 1, token, outcome: "commit", transactions: [transaction] };
	await record(g.path, `claim-0000000000000001-${hash(bytes(claim))}.json`, claim);
	await record(g.path, "claim-index-0000000000000001.json", { schemaVersion: 1, generation: 1, claimSha256: hash(bytes(claim)) });
	await record(g.path, `terminal-0000000000000001-${token}.json`, terminal);
	await record(g.path, `transition-${transaction.baseDigest}.json`, transaction);
	const snapshot = await journal.readConsumerGeneration(g.path, g.authority);
	assert.ok(snapshot.epochRecords.get(`transition-${transaction.baseDigest}.json`).equals(bytes(transaction)));
});
