import assert from "node:assert/strict";
import { chmod, cp, lstat, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { basename, join, relative } from "node:path";
import { test } from "node:test";
import { child, cleanup, fixture, families, projection } from "./fixtures/publication-crash/support.mjs";
import { generationDigest as digest } from "./lib/pylon-generation-format.mjs";

const rounds = 10;
const refusals = /actively locked|changed|disappeared|ENOENT|EEXIST|inode|receipt|publication|authority|conflicting|writer|ownership|claim|checkpoint|namespace|unfinished|incomplete|unsafe type, owner or exact permissions/;
const migrationScenarios = new Set(families.map((family) => `migrate-${family}`));
const staleMigrationTarget = "Migration temporary has an unknown immutable target.";
const lostBuilderCheckpoint = "Generation builder conflicts with the independently installed winner.";
function assertRefusal(scenario, message) {
 if (migrationScenarios.has(scenario) && [staleMigrationTarget, lostBuilderCheckpoint].includes(message)) return;
 assert.match(message, refusals);
}
async function competition(t, f, scenarios, round, historyProof) {
 const controlled = !scenarios[0].startsWith("migrate-") && round % 2 === 0;
 const workers = scenarios.map((scenario, index) => child(f, scenario, { pause: controlled && index === 0 ? "callback:after:stage:state.json" : undefined, append: true, stateMaxBytes: 8192, ready: true, marker: `round-${round}-${index}`, value: `value-${round}-${index}` }));
 try {
  await Promise.all(workers.map((worker) => worker.wait("ready")));
  if (controlled) {
   workers[0].process.send({ type: "release" }); const cut = await workers[0].wait("cut"); assert.equal(cut.pid, workers[0].process.pid);
   for (const worker of workers.slice(1)) worker.process.send({ type: "release" });
   await Promise.all(workers.slice(1).map((worker) => worker.exit));
   workers[0].process.send({ type: "release" });
  } else for (const worker of workers) worker.process.send({ type: "release" });
  const results = await Promise.all(workers.map(async (worker, index) => {
   const exit = await worker.exit; assert.equal(exit.signal, null);
   const result = worker.messages.find((message) => message.type === "done" || message.type === "error"); assert.ok(result, `Missing terminal IPC: ${JSON.stringify(exit)} ${worker.output}`);
   if (exit.code !== 0) { assert.equal(exit.code, 1); assert.equal(result.type, "error"); assertRefusal(scenarios[index], result.message); }
   return result;
  }));
  if (controlled) assert.equal(results[0].type, "done", "The staged owner must complete exactly once");
  for (let index = 0; index < workers.length; index++) {
   if (migrationScenarios.has(scenarios[index])) await assert.rejects(lstat(join(f.directory, `round-${round}-${index}.callbacks`)), { code: "ENOENT" });
   const markers = await readFile(join(f.directory, `round-${round}-${index}.callbacks`), "utf8").catch((error) => { if (error.code === "ENOENT") return ""; throw error; });
   assert.ok(["", "entered\n", "entered\nreturned\n"].includes(markers), "No callback invocation is replayed");
  }
  const recovery = child(f, scenarios[0].startsWith("migrate-") ? scenarios[0] : "commit", { stateMaxBytes: 8192, recover: true, marker: `recovery-${round}` });
  let resumed;
  try { resumed = await recovery.finish(); } finally { await recovery.stop(); }
  if (migrationScenarios.has(scenarios[0])) await assert.rejects(lstat(join(f.directory, `recovery-${round}.callbacks`)), { code: "ENOENT" });
  assert.equal(resumed.finals.length, 1);
  if (!scenarios[0].startsWith("migrate-")) {
   const value = await projection(f); const history = value === "base" ? [] : JSON.parse(value);
   assert.equal(new Set(history).size, history.length, "Each admitted value appears exactly once");
   assert.deepEqual(history.slice(0, historyProof.previous.length), historyProof.previous, "Every prior recovered value remains an exact prefix across later competition");
   for (const [index, result] of results.entries()) if (result.type === "done" && scenarios[index] === "commit") historyProof.acknowledged.add(`value-${round}-${index}`);
   for (const entry of historyProof.acknowledged) assert.ok(history.includes(entry), "Every acknowledged commit survives all later rounds");
   for (const entry of history) {
    const match = /^value-([0-9]+)-([0-9]+)$/.exec(entry); assert.ok(match);
    assert.equal(await readFile(join(f.directory, `round-${match[1]}-${match[2]}.callbacks`), "utf8"), "entered\nreturned\n");
   }
   historyProof.previous = history;
  }
  for (const result of results.filter((result) => result.type === "done")) assert.deepEqual(result.root, resumed.root);
  t.diagnostic(JSON.stringify({ round, scenarios, controlled, pids: workers.map((worker) => worker.process.pid), admitted: results.filter((result) => result.type === "done").length, refusals: results.filter((result) => result.type === "error").map((result) => result.message), root: resumed.root }));
 } finally { await Promise.all(workers.map((worker) => worker.stop())); }
}

test("publication repeated four-process normal and rotation competition", { timeout: 600000 }, async (t) => {
 const f = await fixture("commit");
 const historyProof = { previous: [], acknowledged: new Set() };
 try {
  for (let round = 0; round < rounds; round++) await competition(t, f, round % 2 ? ["commit", "commit", "rotate", "rotate"] : ["commit", "commit", "commit", "commit"], round, historyProof);
  assert.deepEqual(JSON.parse(await projection(f)), historyProof.previous);
 } finally { await cleanup(f); }
});
for (const family of families) test(`publication repeated four-process migration ${family}`, { timeout: 600000 }, async (t) => {
 for (let round = 0; round < rounds; round++) {
  const f = await fixture(`migrate-${family}`);
  try { await competition(t, f, Array(4).fill(`migrate-${family}`), round); assert.equal(await projection(f), "base"); }
  finally { await cleanup(f); }
 }
});

for (const family of families) test(`publication stale pre-intent migration refuses a peer blocker temporary ${family}`, { timeout: 60000 }, async (t) => {
 const scenario = `migrate-${family}`; const f = await fixture(scenario); const workers = [];
 try {
  const legacy = family.includes("v1"); const v2 = family.endsWith("v2");
  const lock = `${f.state}${family.startsWith("prior-retired") ? ".lock.v1-retired" : ".lock"}`;
  const sources = [
   ...(legacy ? [{ path: lock, frozen: true }, { path: `${f.state}.transactions`, frozen: true }] : []),
   ...(v2 ? [{ path: `${f.state}.journal`, frozen: false }] : []),
  ];
  const original = [];
  async function record(path, source) {
   const stat = await lstat(path);
   original.push({ path, source, stat, bytes: stat.isFile() ? await readFile(path) : null });
   if (stat.isDirectory()) for (const name of await readdir(path)) await record(join(path, name), source);
  }
  for (const source of sources) await record(source.path, source);
  const stale = child(f, scenario, { stateMaxBytes: 8192, pauseMigrationMetaRead: true, marker: "stale" }); workers.push(stale);
  const staleCut = await stale.wait("cut"); assert.equal(staleCut.pid, stale.process.pid);
  assert.equal(staleCut.phase, "pre-intent-meta-read"); assert.deepEqual(staleCut.names, ["receipts"]);
  const epochName = v2 ? (await readdir(`${f.state}.journal`)).find((name) => name.startsWith("epoch-")) : null;
  const blocker = join(legacy ? lock : join(`${f.state}.journal`, epochName), "claim-9999999999999999.json");
  const logical = legacy ? "blocker-v1" : `blocker-v2/${epochName}`;
  const blockerHash = digest(Buffer.from(logical));
  const normalizedBlocker = relative(f.directory, blocker).replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "UUID");
  const publisher = child(f, scenario, { stateMaxBytes: 8192, marker: "publisher", cutMatch: {
   hook: "migration", phase: "before", operation: "immutable-link", path: normalizedBlocker,
  } }); workers.push(publisher);
  const publisherCut = await publisher.wait("cut"); assert.equal(publisherCut.pid, publisher.process.pid);
  const meta = `${f.state}.journal-v3`; const receipts = join(meta, "receipts");
  const temporaries = (await readdir(receipts)).filter((name) => name.endsWith(`-${blockerHash}.tmp`));
  assert.equal(temporaries.length, 1); assert.ok(temporaries[0].startsWith(`.writing-p${publisher.process.pid}-`));
  const temporary = join(receipts, temporaries[0]); const pending = await lstat(temporary); const pendingBytes = await readFile(temporary);
  assert.equal(pending.nlink, 1); assert.equal(pending.mode & 0o7777, 0o600);
  await assert.rejects(lstat(blocker), { code: "ENOENT" });
  const intentBytes = await readFile(join(meta, "intent.json")); const intent = JSON.parse(intentBytes);
  assert.deepEqual(JSON.parse(pendingBytes), {
   schemaVersion: 3, kind: "pylon-consumer-impossible-generation-blocker", generation: "9999999999999999",
   statePathSha256: intent.statePathSha256, migrationIntentSha256: digest(intentBytes), source: intent.source,
  });
  stale.process.send({ type: "release" });
  assert.deepEqual(await stale.exit, { code: 1, signal: null });
  const refused = stale.messages.find((message) => message.type === "error"); assert.equal(refused.pid, stale.process.pid);
  assert.equal(refused.message, staleMigrationTarget); assertRefusal(scenario, refused.message);
  for (const denied of ["commit", "rotate", "migrate-unknown"]) assert.throws(() => assertRefusal(denied, refused.message), assert.AssertionError);
  await assert.rejects(lstat(join(meta, "complete.json")), { code: "ENOENT" });
  assert.equal(await projection(f), "base");
  publisher.process.send({ type: "release" }); const published = await publisher.finish();
  const recovery = child(f, scenario, { recover: true, stateMaxBytes: 8192, marker: "recovery" }); workers.push(recovery);
  const recovered = await recovery.finish();
  assert.deepEqual(recovered.root, published.root); assert.equal(recovered.finals.length, 1); assert.equal(await projection(f), "base");
  assert.deepEqual(await readFile(join(meta, "intent.json")), intentBytes);
  const finalBytes = await readFile(join(meta, "final.json")); const final = JSON.parse(finalBytes);
  const selected = JSON.parse(await readFile(join(meta, "root.json")));
  const complete = JSON.parse(await readFile(join(meta, "complete.json")));
  const checkpointBytes = await readFile(join(meta, selected.goal, recovered.finals[0], "checkpoint.json"));
  const checkpoint = JSON.parse(checkpointBytes);
  assert.deepEqual(selected.identity, recovered.root); assert.deepEqual(complete.rootIdentity, recovered.root);
  assert.equal(final.intentSha256, digest(intentBytes)); assert.equal(complete.intentSha256, digest(intentBytes));
  assert.equal(final.genesisSha256, digest(checkpointBytes)); assert.equal(complete.genesisSha256, digest(checkpointBytes));
  assert.equal(final.sourceAuthoritySha256, checkpoint.sourceAuthoritySha256);
  assert.equal(checkpoint.sourceKind, v2 ? "v2" : "v1"); assert.equal(checkpoint.sourceTipDigest, digest(Buffer.from("base")));
  assert.equal(checkpoint.migrationKind, v2 && legacy ? "v1" : null);
  const finalBlocker = legacy ? blocker : blocker.replace(`${f.state}.journal/`, `${f.state}.journal.v2-retired/`);
  const committed = await lstat(finalBlocker); const receipt = await lstat(join(receipts, `receipt-${blockerHash}.json`));
  assert.deepEqual([committed.dev, committed.ino, receipt.dev, receipt.ino], [pending.dev, pending.ino, pending.dev, pending.ino]);
  assert.deepEqual(await readFile(finalBlocker), pendingBytes);
  for (const entry of original) {
   const path = entry.source.frozen ? entry.path : entry.path.replace(`${f.state}.journal`, `${f.state}.journal.v2-retired`);
   const current = await lstat(path);
   assert.deepEqual([current.dev, current.ino], [entry.stat.dev, entry.stat.ino], path);
   assert.equal(current.mode & 0o7777, entry.stat.isDirectory() && entry.source.frozen ? 0o500 : entry.stat.mode & 0o7777, path);
   if (entry.bytes) assert.deepEqual(await readFile(path), entry.bytes, path);
  }
  if (legacy) {
   const initialLock = original.find((entry) => entry.path === lock).stat;
   assert.deepEqual(intent.source.legacyLockIdentity, { dev: initialLock.dev, ino: initialLock.ino });
  }
  if (v2) {
   const initialJournal = original.find((entry) => entry.path === `${f.state}.journal`).stat;
   assert.deepEqual(intent.source.sourceIdentity, { dev: initialJournal.dev, ino: initialJournal.ino });
  }
  for (const marker of ["stale", "publisher", "recovery"]) await assert.rejects(lstat(join(f.directory, `${marker}.callbacks`)), { code: "ENOENT" });
  t.diagnostic(JSON.stringify({ family, pids: workers.map((worker) => worker.process.pid), refusal: refused.message, blocker: basename(finalBlocker), root: recovered.root, sourceAuthoritySha256: checkpoint.sourceAuthoritySha256 }));
 } finally { await Promise.all(workers.map((worker) => worker.stop())); await cleanup(f); }
});

test("publication losing-builder reader refuses native open loss after peer cleanup", { timeout: 60000 }, async (t) => {
 const f = await fixture("loser-cleanup"); const workers = [];
 try {
  const names = await readdir(f.root);
  const winner = names.find((name) => name.startsWith("generation-"));
  const loser = names.find((name) => name.startsWith(".building-"));
  assert.ok(winner && loser);
  const winnerPath = join(f.root, winner, "checkpoint.json");
  const winnerBytes = await readFile(winnerPath); const winnerStat = await lstat(winnerPath);
  assert.deepEqual(await readFile(join(f.root, loser, "checkpoint.json")), winnerBytes);
  const reader = child(f, "loser-cleanup", { pauseLosingBuilderRead: true, marker: "stale-builder" }); workers.push(reader);
  const cut = await reader.wait("cut");
  assert.equal(cut.pid, reader.process.pid); assert.equal(cut.phase, "losing-builder-before-open");
  assert.equal(cut.path, join(f.root, loser, "checkpoint.json"));
  const pinned = await lstat(cut.path);
  assert.deepEqual(cut.identity, { dev: pinned.dev, ino: pinned.ino });
  const peer = child(f, "loser-cleanup", { marker: "cleanup-peer" }); workers.push(peer);
  const cleaned = await peer.finish(); assert.deepEqual(cleaned.entries, [winner]);
  await assert.rejects(lstat(cut.path), { code: "ENOENT" });
  reader.process.send({ type: "release" });
  assert.deepEqual(await reader.exit, { code: 1, signal: null });
  const refusal = reader.messages.find((message) => message.type === "error");
  assert.equal(refusal.pid, reader.process.pid);
  assert.equal(refusal.message, lostBuilderCheckpoint);
  const recovery = child(f, "loser-cleanup", { marker: "fresh-builder" }); workers.push(recovery);
  const recovered = await recovery.finish();
  assert.deepEqual(recovered.root, cleaned.root); assert.deepEqual(recovered.entries, [winner]);
  assert.equal(await projection(f), null);
  assert.deepEqual(await readFile(winnerPath), winnerBytes);
  const finalStat = await lstat(winnerPath);
  assert.deepEqual([finalStat.dev, finalStat.ino, finalStat.nlink], [winnerStat.dev, winnerStat.ino, winnerStat.nlink]);
  for (const marker of ["stale-builder", "cleanup-peer", "fresh-builder"]) await assert.rejects(lstat(join(f.directory, `${marker}.callbacks`)), { code: "ENOENT" });
  t.diagnostic(JSON.stringify({ pids: workers.map((worker) => worker.process.pid), refusal: refusal.message, winnerSha256: digest(winnerBytes), root: recovered.root, callbacks: 0 }));
  assertRefusal("migrate-v1-v2", refusal.message);
  for (const denied of ["commit", "rotate", "migrate-unknown"]) assert.throws(() => assertRefusal(denied, refusal.message), assert.AssertionError);
 } finally { await Promise.all(workers.map((worker) => worker.stop())); await cleanup(f); }
});

for (const kind of ["checkpoint.json", "claim-index-", "heartbeat-", "terminal-", "transition-", "applied-", "receipt-"]) test(`publication pinned ${kind} reader versus process rotation`, { timeout: 60000 }, async () => {
 const f = await fixture("commit"); const reader = child(f, "commit", { recover: true, pauseRead: kind, marker: "reader", stateMaxBytes: 8192 }); let rotator; let recovery;
 try {
  const pinned = await reader.wait("cut"); assert.equal(pinned.pid, reader.process.pid);
  rotator = child(f, "rotate", { stateMaxBytes: 8192 }); await rotator.finish();
  reader.process.send({ type: "release" });
  const exit = await reader.exit; assert.equal(exit.signal, null);
  const result = reader.messages.find((event) => event.type === "done" || event.type === "error"); assert.ok(result);
  if (result.type === "error") assert.match(result.message, /changed|disappeared|ENOENT|inode|retired/);
  recovery = child(f, "commit", { recover: true, marker: "recovery", stateMaxBytes: 8192 });
  const settled = await recovery.finish(); assert.equal(settled.finals.length, 1); assert.equal(await projection(f), "base");
 } finally { await reader.stop(); if (rotator) await rotator.stop(); if (recovery) await recovery.stop(); await cleanup(f); }
});

test("publication killed staged owner excludes peers and never replays its callback", { timeout: 60000 }, async () => {
 const f = await fixture("commit"); const owner = child(f, "commit", { pause: "callback:after:stage:state.json", marker: "held" }); const peers = [];
 try {
  const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid);
  for (const mode of ["commit", "rotate", "commit"]) peers.push(child(f, mode, { marker: `peer-${peers.length}` }));
  for (const peer of peers) {
   const exit = await peer.exit; assert.equal(exit.code, 1); assert.equal(exit.signal, null);
   assert.match(peer.messages.find((event) => event.type === "error").message, /actively locked/);
  }
  assert.equal(await projection(f), "base"); owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL");
  const recovery = child(f, "commit", { recover: true, marker: "recovery" }); peers.push(recovery); await recovery.finish();
  assert.equal(await projection(f), "base"); assert.equal(await readFile(join(f.directory, "held.callbacks"), "utf8"), "entered\n");
 } finally { await owner.stop(); await Promise.all(peers.map((peer) => peer.stop())); await cleanup(f); }
});

test("publication recovers the linked rotation claim before its index CAS", { timeout: 60000 }, async () => {
 const f = await fixture("rotate"); const owner = child(f, "rotate", { cutMatch: { hook: "generation", phase: "after", operation: "link", path: "state.json.journal-v3/journal-HASH-UUID/generation-0000000000000001-HASH/epoch/claim-0000000000000002-HASH.json" } }); let recovery;
 try {
  const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid);
  assert.equal(cut.event.operation, "link"); assert.equal(cut.event.phase, "after"); assert.match(cut.event.path, /epoch\/claim-0000000000000002-HASH.json$/);
  owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL");
  recovery = child(f, "rotate", { recover: true }); const result = await recovery.finish();
  assert.equal(result.finals.length, 1); assert.match(result.finals[0], /^generation-0000000000000002-/); assert.equal(await projection(f), "base");
 } finally { await owner.stop(); if (recovery) await recovery.stop(); await cleanup(f); }
});

for (const family of families) test(`publication helpers recover killed blocker owner ${family}`, { timeout: 60000 }, async (t) => {
 const f = await fixture(`migrate-${family}`); const owner = child(f, `migrate-${family}`, { migrationPause: true });
 try {
  const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid); assert.equal(cut.phase, "after-blocker");
  owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL");
  await competition(t, f, Array(4).fill(`migrate-${family}`), 0); assert.equal(await projection(f), "base");
 } finally { await owner.stop(); await cleanup(f); }
});

for (const replacement of ["same-byte-inode", "unknown-entry"]) test(`publication last-proof SIGKILL rejects ${replacement}`, { timeout: 60000 }, async () => {
 const reference = await fixture("rotate"); const recorder = child(reference, "rotate"); let index;
 try {
  const events = (await recorder.finish()).events;
  index = events.findLastIndex((event) => event.phase === "after" && event.operation === "unlink");
  assert.ok(index > 0);
 } finally { await recorder.stop(); await cleanup(reference); }
 const f = await fixture("rotate"); const owner = child(f, "rotate", { cut: index }); let recovery;
 try {
  const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid); assert.equal(cut.event.operation, "unlink");
  owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL");
  const record = JSON.parse(await readFile(`${f.state}.journal-v3/root.json`)); const root = join(`${f.state}.journal-v3`, record.goal);
  const deleting = (await readdir(root)).filter((name) => name.startsWith(".deleting-")); assert.equal(deleting.length, 1); const path = join(root, deleting[0]);
  if (replacement === "same-byte-inode") {
   const preserved = join(f.directory, "preserved-deleting"); await rename(path, preserved); await cp(preserved, path, { recursive: true });
   await chmod(path, 0o700); for (const name of await readdir(path)) await chmod(join(path, name), 0o700);
  } else await writeFile(join(path, "unknown"), "foreign", { mode: 0o600 });
  recovery = child(f, "rotate", { recover: true }); const exit = await recovery.exit;
  assert.equal(exit.code, 1); assert.equal(exit.signal, null);
  assert.match(recovery.messages.find((event) => event.type === "error").message, /inode|certificate|unexpected|closed|authority/);
  assert.equal(await projection(f), "base");
 } finally { await owner.stop(); if (recovery) await recovery.stop(); await cleanup(f); }
});

test("publication recovery rotates a claim that consumes heartbeat headroom before callback", { timeout: 60000 }, async () => {
 const f = await fixture("recover-projection"); const owner = child(f, "recover-projection", { cutMatch: { hook: "generation", phase: "after", operation: "link", path: "state.json.journal-v3/journal-HASH-UUID/generation-0000000000000001-HASH/epoch/claim-index-0000000000000003.json" } }); let recovery;
 try {
  const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid); assert.equal(cut.event.operation, "link");
  assert.equal(cut.event.phase, "after"); assert.match(cut.event.path, /claim-index-0000000000000003.json$/);
  owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL");
  recovery = child(f, "recover-projection", { recover: true, now: 1000000, marker: "capacity-recovery" });
  const result = await recovery.finish(); assert.equal(result.finals.length, 1); assert.match(result.finals[0], /^generation-0000000000000002-/);
  assert.equal(await projection(f), "candidate");
  assert.equal(await readFile(join(f.directory, "capacity-recovery.callbacks"), "utf8"), "entered\n");
 } finally { await owner.stop(); if (recovery) await recovery.stop(); await cleanup(f); }
});
