import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { test } from "node:test";
import { buildConsumerGeneration, discoverConsumerGenerations, prepareConsumerGeneration, publishConsumerGeneration, readConsumerGeneration, recoverConsumerGenerationBuilder, rotateConsumerGeneration, withConsumerGenerationLock } from "./lib/pylon-consumer-lock.mjs";

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

function generationWorker(t, root, mode, cut = "", builder = "") {
	const child = fork(new URL("./fixtures/generation-operation/worker.mjs", import.meta.url), [root, mode, cut, builder], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
	const events = [];
	let stderr = "";
	child.stderr.on("data", (data) => { stderr += data.toString(); });
	let cutResolve;
	let cutReject;
	const atCut = new Promise((resolve, reject) => { cutResolve = resolve; cutReject = reject; });
	const exited = new Promise((resolve) => child.once("exit", (code, signal) => {
		if (cut && !events.some((event) => event.type === "cut")) cutReject(new Error(`Worker exited before cut: ${JSON.stringify(events)} ${stderr}`));
		resolve({ code, signal });
	}));
	child.on("message", (event) => { events.push(event); if (event.type === "cut") cutResolve(event); });
	child.on("error", cutReject);
	t.after(async () => {
		if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
		await exited;
	});
	return { child, atCut, exited, events };
}

test("v3 operation SIGKILL projection cuts recover owned staging without callback replay", async (t) => {
	for (const cut of ["projection-created", "projection-synced", "projection-before-rename", "projection-after-rename"]) {
		const f = await fixture(t);
		const worker = generationWorker(t, f.root, "commit", cut);
		const observation = await worker.atCut;
		assert.equal(observation.pid, worker.child.pid);
		worker.child.kill("SIGKILL");
		assert.equal((await worker.exited).signal, "SIGKILL");
		let callbacks = 0;
		await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => { callbacks++; assert.equal(tx.readStateBytes().toString(), "worker-state"); }, options);
		assert.equal(callbacks, 1);
		const current = await prepareConsumerGeneration(f.root, f.authority, options);
		assert.equal(current.receiptEntries.filter((entry) => entry.temporary).length, 0);
		assert.equal((await readFile(f.authority.genesis.statePath)).toString(), "worker-state");
		assert.deepEqual((await readdir(f.directory)).sort(), ["journal", "state.json"]);
	}
});

test("v3 operation projection rename joins only native loss of its own observed inode", async (t) => {
	for (const replacement of [false, true]) {
		const f = await fixture(t);
		const run = withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("candidate")), { ...options, hooks: {
			beforeProjectionRename: async ({ source, destination }) => {
				if (replacement) { await writeFile(destination, await readFile(source), { mode: 0o600 }); await rm(source); }
				else await rename(source, destination);
			},
		} });
		if (replacement) await assert.rejects(run, (error) => error.code === "ENOENT");
		else await run;
	}
});

for (const code of ["ENOENT", "EIO", "EPERM"]) {
	test(`v3 operation projection preserves injected ${code} after real rename`, async (t) => {
		for (const injection of ["renameFile", "beforeProjectionRename", "afterProjectionRename"]) {
			const f = await fixture(t);
			const error = Object.assign(new Error(injection), { code });
			const raw = injection === "renameFile" ? { renameFile: async (source, destination) => {
				await rename(source, destination); if (destination === f.authority.genesis.statePath) throw error;
			} } : { hooks: { [injection]: async (observation) => {
				if (observation.source) await rename(observation.source, observation.destination);
				throw error;
			} } };
			await assert.rejects(withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("candidate")), { ...options, ...raw }), (actual) => actual === error);
			await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => assert.equal(tx.readStateBytes().toString(), "candidate"), options);
		}
	});
}

test("v3 operation rotation cleans exact decided projection writers despite PID reuse and fences their delayed rename", async (t) => {
	const f = await fixture(t);
	let paused = false;
	await assert.rejects(withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("first")), { ...options, hooks: {
		afterProjectionFileSync: async () => {
			if (paused) return;
			paused = true;
			await withConsumerGenerationLock(f.root, f.authority, async () => {}, options);
			await rotateConsumerGeneration(f.root, f.authority, options);
		},
	} }));
	assert.ok(paused);
	const current = await prepareConsumerGeneration(f.root, f.authority, options);
	assert.equal(current.checkpoint.epoch, 2);
	assert.equal(current.receiptEntries.filter((entry) => entry.temporary).length, 0);
	assert.equal((await readFile(f.authority.genesis.statePath)).toString(), "first");
});

test("v3 operation heartbeat growth stops before consuming rotation certificate headroom", async (t) => {
	const f = await fixture(t);
	let beat;
	let now = 1;
	let beats = 0;
	await assert.rejects(withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
		await tx.commitState(Buffer.from("must remain staged"));
		for (let index = 0; index < 100; index++) {
			now++;
			try { await beat(); beats++; } catch (error) { assert.match(error.message, /headroom/); break; }
		}
	}, { ...options, now: () => now, startHeartbeat: ({ beat: callback }) => { beat = callback; return async () => {}; } }), /headroom/);
	assert.ok(beats > 0 && beats < 100);
	const rotated = await rotateConsumerGeneration(f.root, f.authority, options);
	assert.equal(rotated.epoch, 2);
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => assert.equal(tx.readStateBytes(), null), options);
});

test("v3 operation two processes join one observed builder inode after native rename loss", async (t) => {
	const f = await fixture(t);
	const builder = await buildConsumerGeneration(f.root, f.authority, options);
	const first = generationWorker(t, f.root, "builder", "builder-before-publish", builder.path);
	await first.atCut;
	const second = generationWorker(t, f.root, "builder", "builder-before-publish", builder.path);
	await second.atCut;
	first.child.send("continue");
	assert.equal((await first.exited).code, 0, JSON.stringify(first.events));
	second.child.send("continue");
	assert.equal((await second.exited).code, 0, JSON.stringify(second.events));
	const current = await prepareConsumerGeneration(f.root, f.authority, options);
	assert.equal(current.identity.ino, builder.identity.ino);
});

test("v3 operation two preparation helpers join committed retirement completion at rename cuts", async (t) => {
	for (const cut of ["retire-before-rename", "delete-before-rename"]) {
		const f = await fixture(t);
		await prepareConsumerGeneration(f.root, f.authority, options);
		const stopped = new Error("two finals");
		await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, hooks: { afterGenerationRename: () => { throw stopped; } } }), (error) => error === stopped);
		const first = generationWorker(t, f.root, "prepare", cut);
		await first.atCut;
		const second = generationWorker(t, f.root, "prepare", cut);
		await second.atCut;
		first.child.send("continue");
		assert.equal((await first.exited).code, 0, JSON.stringify(first.events));
		second.child.send("continue");
		assert.equal((await second.exited).code, 0, JSON.stringify(second.events));
		assert.equal((await readdir(f.root)).length, 1);
	}
});


test("v3 operation exact decided owners clean partial receipts of every operation kind despite PID reuse", async (t) => {
	const f = await fixture(t);
	let owner;
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("committed")), { ...options, hooks: { afterClaim: ({ claim }) => { owner = claim; } } });
	const snapshot = await prepareConsumerGeneration(f.root, f.authority, options);
	const sha = createHash("sha256").update(`${JSON.stringify(owner)}\n`).digest("hex");
	const targets = [`claim-0000000000000001-${sha}.json`, "claim-index-0000000000000001.json", `heartbeat-0000000000000001-${owner.token}-0000000000000001.json`, `terminal-0000000000000001-${owner.token}.json`, `applied-0000000000000001-${owner.token}.json`, `transition-${"0".repeat(64)}.json`];
	for (const name of targets) {
		const targetHash = createHash("sha256").update(`epoch/${name}`).digest("hex");
		await writeFile(join(snapshot.path, "receipts", `.receipt-p${process.pid}-w${randomUUID()}-t${targetHash}-g0000000000000001-c${sha}.tmp`), Buffer.alloc(0), { mode: 0o600 });
	}
	const result = await rotateConsumerGeneration(f.root, f.authority, options);
	assert.equal(result.epoch, 2);
	assert.equal((await readdir(f.root)).length, 1);
});

test("v3 operation live unresolved projection writers block rotation until their exact claim is decided", async (t) => {
	const f = await fixture(t);
	let owner;
	let temporary;
	await withConsumerGenerationLock(f.root, f.authority, async () => {
		const snapshot = await prepareConsumerGeneration(f.root, f.authority, options);
		const sha = createHash("sha256").update(`${JSON.stringify(owner)}\n`).digest("hex");
		temporary = join(snapshot.path, "receipts", `.projection-p${process.pid}-g0000000000000001-c${sha}-t${snapshot.checkpoint.statePathSha256}-a${randomUUID()}.tmp`);
		await writeFile(temporary, Buffer.alloc(0), { mode: 0o600 });
		await assert.rejects(rotateConsumerGeneration(f.root, f.authority, options), /live unresolved projection/);
		assert.equal((await lstat(temporary)).size, 0);
	}, { ...options, hooks: { afterClaim: ({ claim }) => { owner = claim; } } });
	await rotateConsumerGeneration(f.root, f.authority, options);
	await assert.rejects(lstat(temporary), (error) => error.code === "ENOENT");
});

test("v3 operation independently installed winner permits bounded losing-builder cleanup across its last proof", async (t) => {
	const f = await fixture(t);
	const first = await buildConsumerGeneration(f.root, f.authority, options);
	const loser = await buildConsumerGeneration(f.root, f.authority, options);
	await publishConsumerGeneration(first, options);
	const cut = new Error("loser last proof");
	await assert.rejects(prepareConsumerGeneration(f.root, f.authority, { ...options, hooks: { generationBoundary: ({ phase, operation, path }) => {
		if (phase === "after" && operation === "unlink" && path.startsWith(loser.path) && basename(path).startsWith("receipt-")) throw cut;
	} } }), (error) => error === cut);
	assert.ok((await readdir(f.root)).includes(basename(loser.path)));
	await prepareConsumerGeneration(f.root, f.authority, options);
	assert.equal((await readdir(f.root)).length, 1);
	await assert.rejects(publishConsumerGeneration(loser, options));
});

test("v3 operation every pinned canonical metadata read preserves hook errors across ancestor retirement", async (t) => {
	for (const kind of ["checkpoint.json", "retirement.json", "claim-", "claim-index-", "heartbeat-", "terminal-", "transition-", "applied-"]) {
		for (const code of ["ENOENT", "EIO", "EPERM", "native-loss"]) {
			const f = await fixture(t);
			await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("committed")), options);
			const stopped = new Error("two finals");
			await assert.rejects(rotateConsumerGeneration(f.root, f.authority, { ...options, hooks: { afterGenerationRename: () => { throw stopped; } } }), (error) => error === stopped);
			const names = (await readdir(f.root)).filter((name) => name.startsWith("generation-")).sort();
			const predecessorPath = join(f.root, names[0]);
			const injected = Object.assign(new Error(`${kind} ${code}`), { code });
			let fired = false;
			await assert.rejects(readConsumerGeneration(predecessorPath, f.authority, { ...options, hooks: { metadataRead: { afterInitialStat: async ({ path }) => {
				const name = basename(path);
				if (!fired && path.startsWith(predecessorPath) && (kind === "claim-" ? name.startsWith("claim-") && !name.startsWith("claim-index-") : name.startsWith(kind))) {
					fired = true;
					await rename(predecessorPath, join(f.root, `.retired-${names[0]}`));
					if (code !== "native-loss") throw injected;
				}
			} } } }), code === "native-loss" ? /changed|disappeared|ENOENT/ : (error) => error === injected);
			assert.ok(fired, `${kind} ${code}`);
		}
	}
});

test("v3 operation schedules callback heartbeats only after projection preparation", async (t) => {
	const f = await fixture(t);
	f.authority.genesis.stateBytes = Buffer.from("base");
	let prepared = false;
	let started = false;
	let stopped = false;
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
		assert.ok(started);
		assert.equal(tx.readStateBytes().toString(), "base");
	}, { ...options, hooks: { afterProjectionRename: () => { assert.equal(started, false); prepared = true; } }, startHeartbeat: () => {
		assert.ok(prepared);
		started = true;
		return async () => { stopped = true; };
	} });
	assert.ok(stopped);
});

test("v3 operation slow preparation loses ownership before callback without replay", async (t) => {
	const f = await fixture(t);
	f.authority.genesis.stateBytes = Buffer.from("base");
	let now = 1;
	let handedOff = false;
	let callbacks = 0;
	let schedules = 0;
	await assert.rejects(withConsumerGenerationLock(f.root, f.authority, async () => { callbacks++; }, {
		...options, now: () => now, stale: 100,
		startHeartbeat: () => { schedules++; return async () => {}; },
		hooks: { afterProjectionFileSync: async () => {
			if (handedOff) return;
			handedOff = true;
			now = 1000;
			await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => tx.commitState(Buffer.from("successor")), { ...options, now: () => now, stale: 100 });
		} },
	}), /ownership|claim|terminal|decided/);
	assert.ok(handedOff);
	assert.equal(callbacks, 0);
	assert.equal(schedules, 0);
	assert.equal((await readFile(f.authority.genesis.statePath)).toString(), "successor");
});
