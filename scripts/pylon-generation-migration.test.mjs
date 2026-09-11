import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { inspectConsumerMigrationSource, migrateConsumerGenerationJournal, withConsumerGenerationStateLock, rotateConsumerGenerationStateJournal } from "./lib/pylon-consumer-lock.mjs";
import { generationBytes as bytes, generationDigest as digest, GENERATION_ZERO as ZERO } from "./lib/pylon-generation-format.mjs";
import * as protectedV2 from "./fixtures/protected-publication-v2/pylon-consumer-lock.mjs";

const runtime = { startHeartbeat: () => async () => {}, stateMaxBytes: 1024 * 1024 };
async function fixture(t) {
 const directory = await realpath(await mkdtemp(join(tmpdir(), "pylon-migration-")));
 await chmod(directory, 0o700);
 t.after(() => rm(directory, { recursive: true, force: true }));
 return join(directory, "state.json");
}
async function put(path, value) { await writeFile(path, bytes(value), { mode: 0o600 }); }
async function v1(t, incomplete = false) {
 const state = await fixture(t);
 await mkdir(`${state}.lock`, { mode: 0o700 }); await mkdir(`${state}.transactions`, { mode: 0o700 });
 const claim = { schemaVersion: 1, generation: 1, token: randomUUID(), ownerPid: 2_000_000_000, createdAtMs: 0 };
 const value = Buffer.from("committed");
 const transaction = { schemaVersion: 1, baseDigest: ZERO, candidateDigest: digest(value), candidateBase64: value.toString("base64") };
 const terminal = { schemaVersion: 1, generation: 1, token: claim.token, outcome: "commit", transactions: [transaction] };
 await put(`${state}.lock/claim-0000000000000001.json`, claim);
 await put(`${state}.lock/heartbeat-0000000000000001-${claim.token}.json`, { schemaVersion: 1, generation: 1, token: claim.token, refreshedAtMs: 0 });
 await put(`${state}.lock/terminal-0000000000000001-${claim.token}.json`, terminal);
 if (!incomplete) {
  await put(`${state}.transactions/${ZERO}.json`, transaction);
  await put(`${state}.lock/applied-0000000000000001-${claim.token}.json`, { schemaVersion: 1, generation: 1, token: claim.token, terminalSha256: digest(bytes(terminal)) });
  await writeFile(state, value, { mode: 0o600 });
 }
 return { state, value, claim, transaction };
}

test("migration historical reader preserves exact direct and prior-retired v1 authority", async (t) => {
 for (const retired of [false, true]) {
  const { state, value } = await v1(t);
  if (retired) {
   await rename(`${state}.lock`, `${state}.lock.v1-retired`);
   await put(`${state}.lock`, { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) });
  }
  const observed = await inspectConsumerMigrationSource(state);
  assert.equal(observed.source.kind, "v1"); assert.deepEqual(observed.source.tipBytes, value);
  assert.equal(observed.source.records.length, 5); assert.equal(observed.source.recoveries.length, 0);
 }
});

test("migration historical reader identifies helpable v1 records without helping or repairing", async (t) => {
 const { state, value } = await v1(t, true);
 const original = await readdir(`${state}.lock`);
 const observed = await inspectConsumerMigrationSource(state);
 assert.deepEqual(observed.source.tipBytes, value); assert.equal(observed.source.recoveries.length, 2);
 assert.deepEqual(await readdir(`${state}.lock`), original); assert.deepEqual(await readdir(`${state}.transactions`), []);
 await assert.rejects(lstat(state), { code: "ENOENT" });
});

test("migration historical reader validates native v2 completed and committed incomplete decisions", async (t) => {
 for (const incomplete of [false, true]) {
  const state = await fixture(t); const failure = new Error("stop after durable decision");
  const operation = protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("new state"), { ...runtime, hooks: incomplete ? { afterCommitDecision: () => { throw failure; } } : {} });
  if (incomplete) await assert.rejects(operation, (error) => error === failure); else await operation;
  const observed = await inspectConsumerMigrationSource(state);
  assert.equal(observed.source.kind, "v2"); assert.equal(observed.source.tipBytes.toString(), "new state");
  assert.equal(observed.source.recoveries.length, incomplete ? 2 : 0);
  if (incomplete) await assert.rejects(lstat(state), { code: "ENOENT" });
 }
});

test("migration historical reader authenticates retained v2 latest rotation and every epoch", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("one"), runtime);
 await protectedV2.rotateConsumerStateJournal(state, runtime);
 const observed = await inspectConsumerMigrationSource(state);
 assert.equal(observed.source.checkpoints.length, 2);
 assert.equal(observed.source.epochs.size, 2);
 const predecessor = observed.source.checkpoints[0];
 const epoch = observed.source.epochs.get(predecessor.name.replace("checkpoint-", "epoch-").replace(".json", ""));
 const claimIndex = [...epoch.records].filter(([name]) => name.startsWith("claim-index-")).at(-1)[0];
 await rm(join(epoch.path, claimIndex));
 await assert.rejects(inspectConsumerMigrationSource(state), /rotation|unresolved|claim|CAS/);
});

test("migration historical reader rejects malformed namespace, nonprefix projection, and unproved frozen modes", async (t) => {
 const { state } = await v1(t);
 await put(`${state}.lock/extra.json`, {});
 await assert.rejects(inspectConsumerMigrationSource(state), /unexpected/);
 await rm(`${state}.lock/extra.json`);
 await writeFile(state, "forged", { mode: 0o600 });
 await assert.rejects(inspectConsumerMigrationSource(state), /prefix/);
 await writeFile(state, "committed", { mode: 0o600 });
 await chmod(`${state}.lock`, 0o500);
 await assert.rejects(inspectConsumerMigrationSource(state), /permissions/);
 await chmod(`${state}.lock`, 0o700);
});

test("migration historical reader preserves injected native-looking error identity", async (t) => {
 const { state } = await v1(t);
 for (const code of ["ENOENT", "EIO", "EPERM"]) {
  const failure = Object.assign(new Error(`injected ${code}`), { code });
  await assert.rejects(inspectConsumerMigrationSource(state, { lstatEntry: async (path) => {
   if (path === `${state}.journal`) throw failure;
   return lstat(path);
  } }), (error) => error === failure);
 }
 assert.equal((await readFile(state)).toString(), "committed");
});


test("migration historical reader keeps underlying v1 provenance after v2 advances projection", async (t) => {
 const { state, value } = await v1(t);
 await protectedV2.migrateConsumerStateJournal(state, runtime);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("v2 advanced"), runtime);
 const observed = await inspectConsumerMigrationSource(state);
 assert.deepEqual(observed.legacy.tipBytes, value);
 assert.equal(observed.source.tipBytes.toString(), "v2 advanced");
 assert.equal(observed.projection.toString(), "v2 advanced");
});

test("migration historical reader never silently excludes an unauthenticated blocker", async (t) => {
 const { state } = await v1(t);
 await put(`${state}.lock/claim-9999999999999999.json`, { schemaVersion: 3, kind: "forged" });
 await assert.rejects(inspectConsumerMigrationSource(state), /claim.*malformed/);
 const native = await fixture(t);
 await protectedV2.withConsumerStateLock(native, async (_path, tx) => tx.commitState("state"), runtime);
 const clean = await inspectConsumerMigrationSource(native);
 await put(join(clean.source.root, clean.source.headEpoch, "claim-9999999999999999.json"), { schemaVersion: 3, kind: "forged" });
 await assert.rejects(inspectConsumerMigrationSource(native), /claim.*malformed/);
});


test("v3 migration acknowledgement precedes every filesystem operation", async () => {
 let operations = 0;
 const forbidden = () => { operations++; throw new Error("must not execute"); };
 await assert.rejects(migrateConsumerGenerationJournal("/absent/test/state", { lstatEntry: forbidden, makeDirectory: forbidden, openFile: forbidden }), /acknowledgement/);
 assert.equal(operations, 0);
});

test("v3 migration preserves original v1 inodes and freezes both directories", async (t) => {
 const { state, value } = await v1(t, true);
 const before = await lstat(`${state}.lock`); const txBefore = await lstat(`${state}.transactions`);
 const result = await migrateConsumerGenerationJournal(state, { acknowledgeLegacyProcessesStopped: true, ...runtime });
 assert.equal(result.tipSha256, digest(value));
 for (const [path, original] of [[`${state}.lock`, before], [`${state}.transactions`, txBefore]]) {
  const actual = await lstat(path); assert.equal(actual.ino, original.ino); assert.equal(actual.dev, original.dev); assert.equal(actual.mode & 0o7777, 0o500);
 }
 assert.deepEqual(await readFile(state), value);
 const resumed = await migrateConsumerGenerationJournal(state, { acknowledgeLegacyProcessesStopped: true, ...runtime });
 assert.equal(resumed.tipSha256, digest(value));
 await chmod(`${state}.lock`, 0o700); await chmod(`${state}.transactions`, 0o700);
});

test("v3 migration retains v2 source inode and resumes its canonical root", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("native"), runtime);
 const original = await lstat(`${state}.journal`);
 const options = { acknowledgeLegacyProcessesStopped: true, ...runtime };
 const result = await migrateConsumerGenerationJournal(state, options);
 assert.equal(result.tipSha256, digest(Buffer.from("native")));
 assert.equal((await lstat(`${state}.journal.v2-retired`)).ino, original.ino);
 await assert.rejects(lstat(`${state}.journal`), { code: "ENOENT" });
 const resumed = await migrateConsumerGenerationJournal(state, options);
 assert.equal(resumed.tipSha256, result.tipSha256);
});

test("v3 migration blocker stops exact protected clients before help and projection repair", async (t) => {
 for (const family of ["v1", "prior-retired-v1", "v2", "v1-v2", "prior-retired-v1-v2"]) {
  let state;
  if (family === "v2") state = await fixture(t);
  else {
   ({ state } = await v1(t));
   if (family.startsWith("prior-retired")) {
    await rename(`${state}.lock`, `${state}.lock.v1-retired`);
    await put(`${state}.lock`, { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) });
   }
   if (family.endsWith("-v2")) await protectedV2.migrateConsumerStateJournal(state, runtime);
  }
  if (family.endsWith("v2")) {
   if (family === "v2") await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("before"), runtime);
   const stop = new Error("retained incomplete committed operation");
   await assert.rejects(protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("after"), {
    ...runtime, hooks: { afterCommitDecision: () => { throw stop; } },
   }), (error) => error === stop);
  }
  let checkpoints = 0;
  const proveStopped = async () => {
   checkpoints++;
   const before = await readFile(state); const original = await lstat(state);
   let callbacks = 0; let helperWrites = 0;
   const oldOptions = { ...runtime, hooks: { beforeProjectionWrite: () => { helperWrites++; }, afterMetadataLink: ({ kind }) => {
    if (["transition", "applied", "terminal-commit"].includes(kind)) helperWrites++;
   } } };
   await assert.rejects(protectedV2.withConsumerStateLock(state, async () => { callbacks++; }, oldOptions));
   await assert.rejects(protectedV2.rotateConsumerStateJournal(state, oldOptions));
   await assert.rejects(protectedV2.migrateConsumerStateJournal(state, oldOptions));
   assert.equal(callbacks, 0, family); assert.equal(helperWrites, 0, family);
   assert.deepEqual(await readFile(state), before, family); assert.equal((await lstat(state)).ino, original.ino, family);
  };
  await migrateConsumerGenerationJournal(state, { acknowledgeLegacyProcessesStopped: true, ...runtime, hooks: { afterMigrationBlocker: proveStopped, afterMigrationGuard: proveStopped } });
  assert.equal(checkpoints, 2, family);
  for (const path of [`${state}.lock`, `${state}.lock.v1-retired`, `${state}.transactions`]) {
   try { const stat = await lstat(path); if (stat.isDirectory()) await chmod(path, 0o700); } catch (error) { if (error.code !== "ENOENT") throw error; }
  }
 }
});

test("v3 migration rejects a foreign old bootstrap journal after source retirement", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("original"), runtime);
 const options = { acknowledgeLegacyProcessesStopped: true, ...runtime };
 const injected = new Error("cut after protected bootstrap refusal");
 let foreign;
 await assert.rejects(migrateConsumerGenerationJournal(state, { ...options, hooks: { migrationBoundary: async ({ phase, operation }) => {
  if (phase !== "after" || operation !== "source-rename") return;
  await assert.rejects(protectedV2.withConsumerStateLock(state, async () => assert.fail("old callback must never run"), runtime));
  foreign = await lstat(`${state}.journal`);
  throw injected;
 } } }), (error) => error === injected);
 await assert.rejects(migrateConsumerGenerationJournal(state, options), /foreign historical journal inode/);
 assert.equal((await lstat(`${state}.journal`)).ino, foreign.ino);
 assert.equal((await readFile(state)).toString(), "original");
});

test("v3 migration detects same-byte source replacement on canonical resume", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("original"), runtime);
 const options = { acknowledgeLegacyProcessesStopped: true, ...runtime };
 await migrateConsumerGenerationJournal(state, options);
 const source = `${state}.journal.v2-retired`;
 const name = (await readdir(source)).find((entry) => entry.startsWith("checkpoint-"));
 const data = await readFile(join(source, name));
 await rename(join(source, name), `${state}.old-checkpoint`);
 await writeFile(join(source, name), data, { mode: 0o600 });
 await assert.rejects(migrateConsumerGenerationJournal(state, options), /pre-block authority.*inode/);
});


test("v3 fresh state keeps exact genesis independent of later commits and rotations", async (t) => {
 const state = await fixture(t);
 await writeFile(state, "initial", { mode: 0o600 });
 await withConsumerGenerationStateLock(state, async (_path, transaction) => {
  assert.equal(transaction.readStateBytes().toString(), "initial"); await transaction.commitState("later");
 }, runtime);
 const intentPath = `${state}.journal-v3/intent.json`; const intent = await readFile(intentPath);
 await rotateConsumerGenerationStateJournal(state, runtime);
 await withConsumerGenerationStateLock(state, async (_path, transaction) => assert.equal(transaction.readStateBytes().toString(), "later"), runtime);
 assert.deepEqual(await readFile(intentPath), intent);
});

test("v3 normal entry requires explicit migration and preserves callback error staging", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("historical"), runtime);
 let called = 0;
 await assert.rejects(withConsumerGenerationStateLock(state, async () => { called++; }, runtime), /explicit migration/);
 assert.equal(called, 0);
 await migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true });
 const failure = new Error("callback failed after staging");
 await assert.rejects(withConsumerGenerationStateLock(state, async (_path, tx) => { await tx.commitState("must not commit"); throw failure; }, runtime), (error) => error === failure);
 await withConsumerGenerationStateLock(state, async (_path, tx) => assert.equal(tx.readStateBytes().toString(), "historical"), runtime);
});

test("v3 two migration helpers join one installed source and canonical root", async (t) => {
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("historical"), runtime);
 const options = { ...runtime, acknowledgeLegacyProcessesStopped: true };
 const attempts = await Promise.allSettled([migrateConsumerGenerationJournal(state, options), migrateConsumerGenerationJournal(state, options)]);
 const results = attempts.filter((attempt) => attempt.status === "fulfilled").map((attempt) => attempt.value);
 assert.ok(results.length >= 1, attempts.map((attempt) => attempt.reason?.stack).join("\n"));
 for (const attempt of attempts) if (attempt.status === "rejected") assert.match(attempt.reason.message, /changed|disappeared|ENOENT|inode|fenced|authority|actively locked|receipt|publication|conflicting exact bytes/);
 const resumed = await migrateConsumerGenerationJournal(state, options);
 for (const result of results) assert.deepEqual(result, resumed);
 const roots = (await readdir(`${state}.journal-v3`)).filter((name) => name.startsWith("journal-"));
 const selected = JSON.parse(await readFile(`${state}.journal-v3/root.json`)).goal;
 assert.ok(roots.includes(selected));
 for (const root of roots.filter((name) => name !== selected)) assert.deepEqual(await readdir(`${state}.journal-v3/${root}`), []);
});

async function historicalFamily(t, family) {
 let state;
 if (family === "v2") state = await fixture(t);
 else {
  ({ state } = await v1(t));
  if (family.startsWith("prior-retired")) {
   await rename(`${state}.lock`, `${state}.lock.v1-retired`);
   await put(`${state}.lock`, { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) });
  }
  if (family.endsWith("-v2")) await protectedV2.migrateConsumerStateJournal(state, runtime);
 }
 if (family.endsWith("v2")) {
  if (family === "v2") await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("before"), runtime);
  const failure = new Error("historical committed incomplete decision");
  await assert.rejects(protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("after"), {
   ...runtime, hooks: { afterCommitDecision: () => { throw failure; } },
  }), (error) => error === failure);
 }
 return state;
}
async function restoreFixtureModes(state) {
 for (const path of [`${state}.lock`, `${state}.lock.v1-retired`, `${state}.transactions`]) {
  try { const stat = await lstat(path); if (stat.isDirectory()) await chmod(path, 0o700); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
 }
}
async function protectedDenial(state) {
 const before = await readFile(state); const original = await lstat(state);
 let callbacks = 0; let writes = 0;
 const oldOptions = { ...runtime, hooks: { beforeProjectionWrite: () => { writes++; }, afterMetadataLink: ({ kind }) => {
  if (["transition", "applied", "terminal-commit"].includes(kind)) writes++;
 } } };
 await assert.rejects(protectedV2.withConsumerStateLock(state, async () => { callbacks++; }, oldOptions));
 await assert.rejects(protectedV2.migrateConsumerStateJournal(state, oldOptions));
 await assert.rejects(protectedV2.rotateConsumerStateJournal(state, oldOptions));
 assert.equal(callbacks, 0); assert.equal(writes, 0);
 assert.deepEqual(await readFile(state), before); assert.equal((await lstat(state)).ino, original.ino);
}

test("protected old clients cannot help at either freeze, source retirement, installation, or canonical completion", async (t) => {
 for (const family of ["v1", "prior-retired-v1", "v2", "v1-v2", "prior-retired-v1-v2"]) {
  const cuts = ["installed", "complete"];
  if (family !== "v2") cuts.push("lock-frozen", "transactions-frozen");
  if (family.endsWith("v2")) cuts.push("source-retired");
  for (const cut of cuts) {
   const state = await historicalFamily(t, family); const failure = new Error(`${family}/${cut}`); let reached = false;
   const stop = async () => { reached = true; await protectedDenial(state); throw failure; };
   await assert.rejects(migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true, hooks: {
    migrationBoundary: async ({ phase, operation, path }) => {
     if (phase !== "after") return;
     if (cut === "source-retired" && operation === "source-rename") await stop();
     if (operation === "freeze" && (cut === "transactions-frozen" ? path.endsWith(".transactions") : cut === "lock-frozen" && !path.endsWith(".transactions"))) await stop();
    },
    afterMigrationInstalled: cut === "installed" ? stop : undefined,
    afterMigrationComplete: cut === "complete" ? stop : undefined,
   } }), (error) => error === failure);
   assert.equal(reached, true, `${family}/${cut}`);
   await restoreFixtureModes(state);
  }
 }
});

test("migration source retirement joins the independently authenticated same-inode helper", async (t) => {
 const state = await historicalFamily(t, "v2"); let joined;
 const options = { ...runtime, acknowledgeLegacyProcessesStopped: true };
 const result = await migrateConsumerGenerationJournal(state, { ...options, hooks: { migrationBoundary: async ({ phase, operation }) => {
  if (phase === "before" && operation === "source-rename") joined = await migrateConsumerGenerationJournal(state, options);
 } } });
 assert.deepEqual(result, joined);
});

test("migration resumes every freeze and source-retirement hook error without classifying it as success", async (t) => {
 for (const [family, operation, phase] of [["v1", "freeze", "before"], ["v1", "freeze", "after"], ["v2", "source-rename", "before"], ["v2", "source-rename", "after"]]) {
  const state = await historicalFamily(t, family);
  const options = { ...runtime, acknowledgeLegacyProcessesStopped: true };
  const failure = Object.assign(new Error(`injected ${operation}/${phase}`), { code: "ENOENT" }); let fired = false;
  await assert.rejects(migrateConsumerGenerationJournal(state, { ...options, hooks: { migrationBoundary: (event) => {
   if (!fired && event.operation === operation && event.phase === phase) { fired = true; throw failure; }
  } } }), (error) => error === failure);
  assert.equal(fired, true);
  const completed = await migrateConsumerGenerationJournal(state, options);
  assert.equal(completed.tipSha256, digest(Buffer.from(family === "v1" ? "committed" : "after")));
  await restoreFixtureModes(state);
 }
});

test("migration rejects altered blocker commitments and a replaced canonical root", async (t) => {
 for (const field of ["authoritySha256", "tipSha256", "legacyMarkerSha256", "sourceIdentity"]) {
  const state = await historicalFamily(t, "v2"); const failure = new Error("blocker cut");
  const options = { ...runtime, acknowledgeLegacyProcessesStopped: true };
  await assert.rejects(migrateConsumerGenerationJournal(state, { ...options, hooks: { afterMigrationBlocker: () => { throw failure; } } }), (error) => error === failure);
  const epoch = (await readdir(`${state}.journal`)).find((name) => name.startsWith("epoch-"));
  const path = join(`${state}.journal`, epoch, "claim-9999999999999999.json");
  const blocker = JSON.parse(await readFile(path)); blocker.source[field] = field === "sourceIdentity" ? { dev: 0, ino: 0 } : "f".repeat(64);
  await put(path, blocker);
  await assert.rejects(migrateConsumerGenerationJournal(state, options), /blocker differs/);
 }
 const state = await fixture(t);
 await withConsumerGenerationStateLock(state, async (_path, tx) => tx.commitState("current"), runtime);
 const meta = `${state}.journal-v3`; const root = join(meta, (await readdir(meta)).find((name) => name.startsWith("journal-")));
 await rename(root, `${state}.saved-root`); await mkdir(root, { mode: 0o700 });
 await assert.rejects(withConsumerGenerationStateLock(state, async () => assert.fail("replacement must not run"), runtime), /different inode/);
});

test("fresh v3 entry creates private nested parents and rejects symlink ancestors", async (t) => {
 const state = await fixture(t); const nested = join(`${state}.private`, "nested", "state.json");
 await withConsumerGenerationStateLock(nested, async (_path, tx) => tx.commitState("nested"), runtime);
 assert.equal((await readFile(nested)).toString(), "nested");
 assert.equal((await lstat(`${state}.private`)).mode & 0o7777, 0o700);
 await symlink(`${state}.private`, `${state}.linked`);
 await assert.rejects(withConsumerGenerationStateLock(join(`${state}.linked`, "other.json"), async () => {}, runtime), /canonical real directory/);
});

test("migration receipt and root construction cuts resume only through exact durable authority", async (t) => {
 const cases = [
  ["receipt-create", "after", null], ["file-sync", "before", null], ["file-sync", "after", null],
  ["immutable-link", "before", "intent.json"], ["immutable-link", "after", "intent.json"],
  ["receipt-rename", "before", null], ["receipt-rename", "after", null],
  ["guard-rename", "before", null], ["guard-rename", "after", null],
  ["mkdir", "after", "journal-"], ["immutable-link", "after", "root.json"],
  ["immutable-link", "after", "complete.json"],
 ];
 for (const [operation, phase, suffix] of cases) {
  const state = await historicalFamily(t, "v2");
  const options = { ...runtime, acknowledgeLegacyProcessesStopped: true, processKill: () => { throw Object.assign(new Error("dead receipt fixture"), { code: "ESRCH" }); } };
  const failure = Object.assign(new Error(`cut ${operation}/${phase}/${suffix}`), { code: "EIO" }); let fired = false;
  await assert.rejects(migrateConsumerGenerationJournal(state, { ...options, hooks: { migrationBoundary: (event) => {
   if (!fired && event.operation === operation && event.phase === phase && (suffix === null || (suffix === "journal-" ? event.path.split("/").at(-1).startsWith(suffix) : event.path.endsWith(suffix)))) { fired = true; throw failure; }
  } } }), (error) => error === failure);
  assert.equal(fired, true);
  let callbacks = 0;
  await assert.rejects(withConsumerGenerationStateLock(state, async () => { callbacks++; }, runtime));
  assert.equal(callbacks, 0);
  const result = await migrateConsumerGenerationJournal(state, options);
  assert.equal(result.tipSha256, digest(Buffer.from("after")));
 }
});

test("migration preserves empty projection snapshots and full retained rotation authority", async (t) => {
 for (const family of ["v1", "v2", "v1-v2"]) {
  const state = await historicalFamily(t, family);
  await writeFile(state, Buffer.alloc(0), { mode: 0o600 });
  const result = await migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true });
  assert.equal(result.tipSha256, digest(Buffer.from(family === "v1" ? "committed" : "after")));
  assert.equal(JSON.parse(await readFile(`${state}.journal-v3/intent.json`)).projectionBase64, "");
  await restoreFixtureModes(state);
 }
 const state = await fixture(t);
 await protectedV2.withConsumerStateLock(state, async (_path, tx) => tx.commitState("rotated"), runtime);
 await protectedV2.rotateConsumerStateJournal(state, runtime);
 const result = await migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true });
 assert.equal(result.tipSha256, digest(Buffer.from("rotated")));
 await withConsumerGenerationStateLock(state, async (_path, tx) => tx.commitState("v3 later"), runtime);
 await rotateConsumerGenerationStateJournal(state, runtime);
 await withConsumerGenerationStateLock(state, async (_path, tx) => assert.equal(tx.readStateBytes().toString(), "v3 later"), runtime);
 assert.equal((await readdir(`${state}.journal.v2-retired`)).filter((name) => name.startsWith("checkpoint-")).length, 2);
});

test("historical malformed guard and live or uncertain unresolved owners fail before source mutation", async (t) => {
 const native = await historicalFamily(t, "v2");
 await put(`${native}.lock`, { schemaVersion: 1, kind: "forged" });
 await assert.rejects(migrateConsumerGenerationJournal(native, { ...runtime, acknowledgeLegacyProcessesStopped: true }), /guard is not exact/);
 await assert.rejects(lstat(`${native}.journal-v3`), { code: "ENOENT" });
 for (const alive of [true, false]) {
  const { state } = await v1(t, true); const original = await readdir(`${state}.lock`);
  const options = { ...runtime, acknowledgeLegacyProcessesStopped: true, processKill: () => {
   if (!alive) throw Object.assign(new Error("uncertain owner"), { code: "EPERM" });
  } };
  await assert.rejects(migrateConsumerGenerationJournal(state, options), /live or uncertain/);
  assert.deepEqual(await readdir(`${state}.lock`), original); assert.deepEqual(await readdir(`${state}.transactions`), []);
 }
});

test("direct v1 recovery rejects additional valid authority inserted during permitted completion", async (t) => {
 const { state } = await v1(t, true); let inserted = false;
 await assert.rejects(migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true, hooks: { migrationBoundary: async ({ phase, operation }) => {
  if (inserted || phase !== "after" || operation !== "immutable-link") return;
  inserted = true;
  const token = randomUUID();
  await put(`${state}.lock/claim-0000000000000002.json`, { schemaVersion: 1, generation: 2, token, ownerPid: 2_000_000_000, createdAtMs: 0 });
  await put(`${state}.lock/heartbeat-0000000000000002-${token}.json`, { schemaVersion: 1, generation: 2, token, refreshedAtMs: 0 });
  await put(`${state}.lock/terminal-0000000000000002-${token}.json`, { schemaVersion: 1, generation: 2, token, outcome: "released" });
 } } }), /unapproved authority changes/);
 assert.equal(inserted, true);
 await assert.rejects(lstat(`${state}.journal-v3/intent.json`), { code: "ENOENT" });
});

test("exclusive construction never adopts a foreign empty inode or an unrecorded crash directory", async (t) => {
 for (const phase of ["before", "after"]) {
  const state = await historicalFamily(t, "v2"); let original; let foreign; let target;
  await assert.rejects(migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true, hooks: { migrationBoundary: async (event) => {
   if (target || event.phase !== phase || event.operation !== "mkdir" || !event.path.split("/").at(-1).startsWith("journal-")) return;
   target = event.path;
   if (phase === "after") { original = `${state}.unrecorded-original`; await rename(target, original); }
   await mkdir(target, { mode: 0o700 }); foreign = await lstat(target);
  } } }), phase === "before" ? { code: "EEXIST" } : /changed inode/);
  assert.equal((await lstat(target)).ino, foreign.ino);
  const result = await migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true });
  assert.equal(result.tipSha256, digest(Buffer.from("after")));
  const selected = JSON.parse(await readFile(`${state}.journal-v3/root.json`));
  assert.notEqual(selected.identity.ino, foreign.ino);
  assert.deepEqual(await readdir(target), []);
  if (original) assert.deepEqual(await readdir(original), []);
 }
});

test("native v2 initial checkpoint cut gains an authenticated epoch blocker before old-client admission", async (t) => {
 const state = await fixture(t); const failure = new Error("initial checkpoint cut");
 await assert.rejects(protectedV2.withConsumerStateLock(state, async () => {}, { ...runtime, hooks: { afterMetadataDirectorySync: ({ kind }) => { if (kind === "checkpoint") throw failure; } } }), (error) => error === failure);
 assert.equal((await inspectConsumerMigrationSource(state)).source.epochs.size, 0);
 let callbacks = 0;
 await migrateConsumerGenerationJournal(state, { ...runtime, acknowledgeLegacyProcessesStopped: true, processKill: () => { throw Object.assign(new Error("dead bootstrap fixture"), { code: "ESRCH" }); }, hooks: { afterMigrationBlocker: async () => {
  await assert.rejects(protectedV2.withConsumerStateLock(state, async () => { callbacks++; }, runtime));
  await assert.rejects(protectedV2.rotateConsumerStateJournal(state, runtime));
 } } });
 assert.equal(callbacks, 0);
});
