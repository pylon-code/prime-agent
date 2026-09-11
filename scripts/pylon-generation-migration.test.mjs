import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { inspectConsumerMigrationSource } from "./lib/pylon-consumer-lock.mjs";
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
