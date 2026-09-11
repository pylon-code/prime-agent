import assert from "node:assert/strict";
import { open } from "node:fs/promises";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { migrateConsumerStateJournal, withConsumerStateLock } from "./lib/pylon-consumer-lock.mjs";
import { generationDigest as digest } from "./lib/pylon-generation-format.mjs";
import { cleanup, fixture } from "./fixtures/publication-crash/support.mjs";

const runtime = { stateMaxBytes: 1024, startHeartbeat: () => async () => {}, acknowledgeLegacyProcessesStopped: true };
const isSync = (event, path) => event.phase === "after" && event.operation === "directory-sync" && event.path === path;
test("nested fresh state brackets every actual file and directory fsync", async () => {
 const f = await fixture("fresh-nested"); const active = new Set(); const synchronized = [];
 const boundary = (event) => {
  if (!event.operation.includes("sync")) return;
  if (event.phase === "before") active.add(event.path); else active.delete(event.path);
 };
 try {
  await withConsumerStateLock(f.state, async (_path, tx) => tx.commitState("candidate"), { ...runtime, hooks: { generationBoundary: boundary, migrationBoundary: boundary }, async openFile(path, ...args) {
   const handle = await open(path, ...args);
   return new Proxy(handle, { get(target, property) {
    if (property === "sync") return async () => { assert.ok(active.has(path), `Actual fsync lacks its before boundary: ${path}`); synchronized.push(path); await target.sync(); };
    const value = Reflect.get(target, property); return typeof value === "function" ? value.bind(target) : value;
   } });
  } });
  assert.equal(active.size, 0); assert.ok(synchronized.includes(f.directory)); assert.ok(synchronized.includes(join(f.directory, "one")));
 } finally { await cleanup(f); }
});
for (const mode of ["interrupted", "concurrent-helper", "raced-installed-guard"]) test(`migration ${mode} synchronizes both guard parents before source retirement`, async () => {
 const f = await fixture("migrate-v2"); const stop = new Error("captured guard publication cut"); const finished = new Error("guard ordering verified");
 const events = [];
 const verify = async () => assert.rejects(migrateConsumerStateJournal(f.state, { ...runtime, hooks: { migrationBoundary(event) {
  events.push(event);
  if (event.phase === "before" && event.operation === "source-rename") {
   assert.ok(events.some((entry) => isSync(entry, `${f.state}.journal-v3`)), "Guard staging parent must be durable before retirement");
   assert.ok(events.some((entry) => isSync(entry, dirname(f.state))), "Guard canonical parent must be durable before retirement");
   throw finished;
  }
 } } }), (error) => error === finished);
 try {
  if (mode === "raced-installed-guard") {
   await assert.rejects(migrateConsumerStateJournal(f.state, { ...runtime, hooks: { async migrationBoundary(event) {
    if (event.phase === "before" && event.operation === "guard-rename") {
     await assert.rejects(migrateConsumerStateJournal(f.state, { ...runtime, hooks: { migrationBoundary(inner) {
      if (inner.phase === "after" && inner.operation === "guard-rename") throw stop;
     } } }), (error) => error === stop);
     events.length = 0;
    }
    events.push(event);
    if (event.phase === "before" && event.operation === "source-rename") {
     assert.ok(events.some((entry) => isSync(entry, `${f.state}.journal-v3`)), "Raced guard staging parent must be durable");
     assert.ok(events.some((entry) => isSync(entry, dirname(f.state))), "Raced guard canonical parent must be durable");
     throw finished;
    }
   } } }), (error) => error === finished);
  } else {
   await assert.rejects(migrateConsumerStateJournal(f.state, { ...runtime, hooks: { async migrationBoundary(event) {
    if (event.phase === "after" && event.operation === "guard-rename") {
     if (mode === "concurrent-helper") await verify();
     throw stop;
    }
   } } }), (error) => error === stop);
   if (mode === "interrupted") await verify();
  }
 } finally { await cleanup(f); }
});

for (const mode of ["linked-blocker", "fixed-blocker", "concurrent-blocker", "linked-cleanup"]) test(`migration ${mode} rejoins canonical and receipt durability in order`, async () => {
 const f = await fixture(mode === "linked-cleanup" ? "migrate-v1" : "migrate-v2"); const stop = new Error("captured receipt publication cut");
 const events = []; let target; let fixed; let repaired = false;
 const resume = () => migrateConsumerStateJournal(f.state, { ...runtime, hooks: { metadataRead: { afterInitialStat({ path }) {
  if (mode === "linked-cleanup" && path.includes("/.writing-") && path.endsWith(`${digest(Buffer.from("v1-lock/.pylon-consumer-v1-retired.json"))}.tmp`)) events.length = 0;
 } }, migrationBoundary(event) {
  events.push(event);
  if (event.phase === "before" && event.operation === "receipt-rename" && event.path === fixed) {
   assert.ok(events.some((entry) => isSync(entry, dirname(target))), "Canonical parent sync must precede fixed receipt rename");
   repaired = true;
  }
 }, afterMigrationBlocker() {
  if (mode !== "linked-cleanup") assert.ok(events.some((entry) => isSync(entry, `${f.state}.journal-v3/receipts`)), "A joining helper must complete the fixed receipt durability barrier");
 } } });
 try {
  await assert.rejects(migrateConsumerStateJournal(f.state, { ...runtime, hooks: { async migrationBoundary(event) {
   const matches = mode === "linked-cleanup" ? event.path.endsWith("/.pylon-consumer-v1-retired.json") : event.path.endsWith("/claim-9999999999999999.json");
   if (event.phase === "after" && event.operation === "immutable-link" && matches) {
    target = event.path;
    const logical = mode === "linked-cleanup" ? "v1-lock/.pylon-consumer-v1-retired.json" : `blocker-v2/${dirname(target).split("/").at(-1)}`;
    fixed = join(`${f.state}.journal-v3`, "receipts", `receipt-${digest(Buffer.from(logical))}.json`);
    if (mode === "fixed-blocker") return;
    if (mode === "concurrent-blocker") await resume();
    throw stop;
   }
   if (mode === "fixed-blocker" && fixed && event.phase === "after" && event.operation === "receipt-rename" && event.path === fixed) throw stop;
  } } }), (error) => error === stop);
  if (mode !== "concurrent-blocker") await resume();
  if (mode !== "fixed-blocker") assert.equal(repaired, true, "The actual linked receipt repair path must run");
  assert.ok(events.some((entry) => isSync(entry, dirname(target))));
  assert.ok(events.some((entry) => isSync(entry, `${f.state}.journal-v3/receipts`)));
 } finally { await cleanup(f); }
});
