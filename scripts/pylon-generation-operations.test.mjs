import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { test } from "node:test";
import { buildConsumerGeneration, discoverConsumerGenerations, prepareConsumerGeneration, recoverConsumerGenerationBuilder, rotateConsumerGeneration, withConsumerGenerationLock } from "./lib/pylon-consumer-lock.mjs";

async function fixture(t) {
	const directory = await mkdtemp(join(tmpdir(), "pylon-generation-operations-"));
	await chmod(directory, 0o700);
	t.after(() => rm(directory, { recursive: true, force: true }));
	const root = join(directory, "journal");
	await mkdir(root, { mode: 0o700 });
	// mkdir is intentionally separate from journal preparation: migration owns installation.
	return { directory, root, authority: { genesis: { statePath: join(directory, "state.json"), stateBytes: null } } };
}
const options = { stateMaxBytes: 1024, startHeartbeat: () => async () => {} };

test("v3 operation stages callbacks and converges rotation before another callback", async (t) => {
	const f = await fixture(t);
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
		assert.equal(tx.readStateBytes(), null);
		await tx.commitState(Buffer.from("first"));
		assert.equal(await readFile(f.authority.genesis.statePath).catch(() => null), null);
	}, options);
	assert.equal((await readFile(f.authority.genesis.statePath)).toString(), "first");
	const result = await rotateConsumerGeneration(f.root, f.authority, options);
	assert.equal(result.epoch, 2);
	assert.equal((await readdir(f.root)).length, 1);
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => assert.equal(tx.readStateBytes().toString(), "first"), options);
});

test("v3 operation callback throw preserves identity and never commits staged bytes or replays", async (t) => {
	const f = await fixture(t);
	const error = Object.assign(new Error("callback"), { code: "ENOENT" });
	let calls = 0;
	let escaped;
	await assert.rejects(withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
		calls++; escaped = tx; await tx.commitState(Buffer.from("not committed")); throw error;
	}, options), (actual) => actual === error);
	assert.equal(calls, 1);
	await assert.rejects(escaped.commitState(Buffer.from("late")), /live|staged/);
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => assert.equal(tx.readStateBytes(), null), options);
});

test("v3 operation recovers durable two-final and every partial deletion cut before callback", async (t) => {
	for (const stage of ["two-finals", "retired", "deleting", "first-unlink", "last-proof", "empty-container"]) {
		const f = await fixture(t);
		await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from(stage)), options);
		const error = new Error(stage);
		let fired = false;
		await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, hooks: {
			afterGenerationRename: () => { if (stage === "two-finals") { fired = true; throw error; } },
			generationBoundary: ({ phase, operation, path }) => {
				if (fired || phase !== "after") return;
				const hit = stage === "retired" && operation === "rename" && basename(path).startsWith(".retired-") ||
					stage === "deleting" && operation === "rename" && basename(path).startsWith(".deleting-") ||
					stage === "first-unlink" && operation === "unlink" ||
					stage === "last-proof" && operation === "unlink" && basename(path) === `receipt-${createHash("sha256").update("retirement.json").digest("hex")}.json` ||
					stage === "empty-container" && operation === "remove-directory" && basename(path) === "receipts";
				if (hit) { fired = true; throw error; }
			},
		} }), (actual) => actual === error);
		assert.ok(fired, stage);
		let called = false;
		await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
			called = true;
			assert.equal((await readdir(f.root)).length, 1, stage);
			assert.equal(tx.readStateBytes().toString(), stage);
		}, options);
		assert.ok(called, stage);
	}
});

test("v3 operation cold root-readdir handoff restarts only direct native lost discovery", async (t) => {
	const f = await fixture(t);
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("initial")), options);
	let handedOff = false;
	let called = false;
	await withConsumerGenerationLock(f.root, f.authority, async () => { called = true; }, { ...options, readDirectory: async (path) => {
		const names = await readdir(path);
		if (path === f.root && !handedOff) {
			handedOff = true;
			await rotateConsumerGeneration(f.root, f.authority, options);
			await rotateConsumerGeneration(f.root, f.authority, options);
			const currentNames = await readdir(path);
			assert.ok(names.some((name) => name.startsWith("generation-") && !currentNames.includes(name)));
		}
		return names;
	} });
	assert.ok(called);
});

for (const code of ["ENOENT", "EIO", "EPERM"]) {
	test(`v3 operation preserves injected ${code} through final-retired and retired-deleting response loss`, async (t) => {
		for (const prefix of [".retired-", ".deleting-"]) {
			const f = await fixture(t);
			await prepareConsumerGeneration(f.root, f.authority, options);
			const error = Object.assign(new Error("response loss"), { code });
			let moved = false;
			await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, renameFile: async (source, destination) => {
				await rename(source, destination);
				if (basename(destination).startsWith(prefix)) { moved = true; throw error; }
			} }), (actual) => actual === error);
			assert.ok(moved);
			await prepareConsumerGeneration(f.root, f.authority, options);
			assert.equal((await readdir(f.root)).length, 1);
		}
	});
}

test("v3 operation rejects byte-identical retired container replacement", async (t) => {
	const f = await fixture(t);
	await prepareConsumerGeneration(f.root, f.authority, options);
	const cut = new Error("retired");
	let retired;
	await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, hooks: { afterGenerationMove: ({ destination }) => {
		if (basename(destination).startsWith(".retired-")) { retired = destination; throw cut; }
	} } }), (actual) => actual === cut);
	const replacement = join(f.directory, "replacement");
	await cp(retired, replacement, { recursive: true });
	const originalInode = (await lstat(retired)).ino;
	await rm(retired, { recursive: true });
	await rename(replacement, retired);
	assert.notEqual((await lstat(retired)).ino, originalInode);
	await assert.rejects(prepareConsumerGeneration(f.root, f.authority, options), /inode|receipt/);
});

test("v3 operation rescans after projection rename and readback and repairs a delayed writer forward", async (t) => {
	const f = await fixture(t);
	let advanced = false;
	let ownClock = 1;
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("first")), { ...options, now: () => ownClock, hooks: {
		afterProjectionFileSync: async () => {
			if (advanced) return;
			advanced = true;
			ownClock = 100_000;
			await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("second")), { ...options, now: () => ownClock });
		},
	} });
	assert.ok(advanced);
	assert.equal((await readFile(f.authority.genesis.statePath)).toString(), "second");
});

test("v3 operation revalidates latest rotation CAS after the final pre-rename barrier", async (t) => {
	const f = await fixture(t);
	const initial = await prepareConsumerGeneration(f.root, f.authority, options);
	let removed = false;
	await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, hooks: { generationBoundary: async ({ phase, operation, path }) => {
		if (!removed && phase === "before" && operation === "rename" && basename(path).startsWith("generation-") && path !== initial.path) {
			removed = true;
			const index = join(initial.path, "epoch", "claim-index-0000000000000001.json");
			await rm(index);
		}
	} } }));
	assert.ok(removed);
	assert.equal((await readdir(f.root)).filter((name) => name.startsWith("generation-")).length, 1);
});

test("v3 operation resumes an observed builder from a durable pre-link or linked receipt", async (t) => {
	for (const cut of ["file-sync", "link", "rename"]) {
		const f = await fixture(t);
		const error = new Error(cut);
		await assert.rejects(buildConsumerGeneration(f.root, f.authority, { ...options, hooks: { generationBoundary: ({ phase, operation }) => {
			if (phase === "after" && operation === cut) throw error;
		} } }), (actual) => actual === error);
		const path = join(f.root, (await readdir(f.root))[0]);
		const inode = (await lstat(path)).ino;
		const builder = await recoverConsumerGenerationBuilder(path, f.authority, options);
		assert.equal(builder.identity.ino, inode);
		await prepareConsumerGeneration(f.root, f.authority, options);
		assert.equal((await readdir(f.root)).length, 1);
	}
});

test("v3 operation cleans exact decided receipt losers despite PID reuse and blocks unresolved live writers", async (t) => {
	const f = await fixture(t);
	let winner;
	await withConsumerGenerationLock(f.root, f.authority, async () => {}, { ...options, hooks: { afterClaim: ({ claim }) => { winner = claim; } } });
	let snapshot = await prepareConsumerGeneration(f.root, f.authority, options);
	const loser = { ...winner, token: randomUUID() };
	const loserBytes = Buffer.from(`${JSON.stringify(loser)}\n`);
	const contentHash = createHash("sha256").update(loserBytes).digest("hex");
	const target = `epoch/claim-0000000000000001-${contentHash}.json`;
	const temporary = join(snapshot.path, "receipts", `.receipt-p${process.pid}-w${randomUUID()}-t${createHash("sha256").update(target).digest("hex")}.tmp`);
	await writeFile(temporary, loserBytes, { mode: 0o600 });
	await rotateConsumerGeneration(f.root, f.authority, options);
	snapshot = await prepareConsumerGeneration(f.root, f.authority, options);
	const live = { ...loser, token: randomUUID() };
	const liveBytes = Buffer.from(`${JSON.stringify(live)}\n`);
	const liveTarget = `epoch/claim-0000000000000001-${createHash("sha256").update(liveBytes).digest("hex")}.json`;
	await writeFile(join(snapshot.path, "receipts", `.receipt-p${process.pid}-w${randomUUID()}-t${createHash("sha256").update(liveTarget).digest("hex")}.tmp`), liveBytes, { mode: 0o600 });
	await assert.rejects(rotateConsumerGeneration(f.root, f.authority, options), /live unresolved receipt/);
});

test("v3 operation cold discovery preserves injected ENOENT even when a successor exists", async (t) => {
	const f = await fixture(t);
	const initial = await prepareConsumerGeneration(f.root, f.authority, options);
	const error = Object.assign(new Error("injected native-looking stat"), { code: "ENOENT" });
	let fired = false;
	await assert.rejects(discoverConsumerGenerations(f.root, f.authority, { ...options, lstatEntry: async (path) => {
		if (!fired && path === initial.path) {
			fired = true;
			await rotateConsumerGeneration(f.root, f.authority, options);
			throw error;
		}
		return lstat(path);
	} }), (actual) => actual === error);
	assert.ok(fired);
});
