import assert from "node:assert/strict";
import { chmod, cp, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { child, cleanup, fixture, families, projection } from "./fixtures/publication-crash/support.mjs";

const rounds = 10;
const refusals = /actively locked|changed|disappeared|ENOENT|EEXIST|inode|receipt|publication|authority|conflicting|writer|ownership|claim|checkpoint|namespace|unfinished|incomplete|unsafe type, owner or exact permissions/;
async function competition(t, f, scenarios, round) {
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
  const results = await Promise.all(workers.map(async (worker) => {
   const exit = await worker.exit; assert.equal(exit.signal, null);
   const result = worker.messages.find((message) => message.type === "done" || message.type === "error"); assert.ok(result, `Missing terminal IPC: ${JSON.stringify(exit)} ${worker.output}`);
   if (exit.code !== 0) { assert.equal(exit.code, 1); assert.equal(result.type, "error"); assert.match(result.message, refusals); }
   return result;
  }));
  if (controlled) assert.equal(results[0].type, "done", "The staged owner must complete exactly once");
  for (let index = 0; index < workers.length; index++) {
   const markers = await readFile(join(f.directory, `round-${round}-${index}.callbacks`), "utf8").catch((error) => { if (error.code === "ENOENT") return ""; throw error; });
   assert.ok(["", "entered\n", "entered\nreturned\n"].includes(markers), "No callback invocation is replayed");
  }
  const recovery = child(f, scenarios[0].startsWith("migrate-") ? scenarios[0] : "commit", { stateMaxBytes: 8192, recover: true, marker: `recovery-${round}` });
  let resumed;
  try { resumed = await recovery.finish(); } finally { await recovery.stop(); }
  assert.equal(resumed.finals.length, 1);
  if (!scenarios[0].startsWith("migrate-")) {
   const value = await projection(f); const history = value === "base" ? [] : JSON.parse(value);
   assert.equal(new Set(history).size, history.length, "Each admitted value appears exactly once");
   for (const [index, result] of results.entries()) if (result.type === "done" && scenarios[index] === "commit") assert.ok(history.includes(`value-${round}-${index}`));
   for (const entry of history.filter((entry) => entry.startsWith(`value-${round}-`))) {
    const index = Number(entry.split("-").at(-1));
    assert.equal(await readFile(join(f.directory, `round-${round}-${index}.callbacks`), "utf8"), "entered\nreturned\n");
   }
  }
  for (const result of results.filter((result) => result.type === "done")) assert.deepEqual(result.root, resumed.root);
  t.diagnostic(JSON.stringify({ round, scenarios, controlled, pids: workers.map((worker) => worker.process.pid), admitted: results.filter((result) => result.type === "done").length, refusals: results.filter((result) => result.type === "error").map((result) => result.message), root: resumed.root }));
 } finally { await Promise.all(workers.map((worker) => worker.stop())); }
}

test("publication repeated four-process normal and rotation competition", { timeout: 600000 }, async (t) => {
 const f = await fixture("commit");
 try {
  for (let round = 0; round < rounds; round++) await competition(t, f, round % 2 ? ["commit", "commit", "rotate", "rotate"] : ["commit", "commit", "commit", "commit"], round);
  assert.ok((await projection(f)) === "base" || Array.isArray(JSON.parse(await projection(f))));
 } finally { await cleanup(f); }
});
for (const family of families) test(`publication repeated four-process migration ${family}`, { timeout: 600000 }, async (t) => {
 for (let round = 0; round < rounds; round++) {
  const f = await fixture(`migrate-${family}`);
  try { await competition(t, f, Array(4).fill(`migrate-${family}`), round); assert.equal(await projection(f), "base"); }
  finally { await cleanup(f); }
 }
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
 const f = await fixture("rotate"); const owner = child(f, "rotate", { cut: 17 }); let recovery;
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
