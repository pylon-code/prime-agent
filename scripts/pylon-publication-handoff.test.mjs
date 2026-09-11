import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { child, cleanup, fixture, projection } from "./fixtures/publication-crash/support.mjs";
import { rotateConsumerStateJournal, withConsumerStateLock } from "./lib/pylon-consumer-lock.mjs";

const options = { stateMaxBytes: 8192, startHeartbeat: () => async () => {} };
const tipSha256 = createHash("sha256").update("base").digest("hex");
const rotationSource = `
import { readdir } from "node:fs/promises";
import { rotateConsumerStateJournal } from ${JSON.stringify(new URL("./lib/pylon-consumer-lock.mjs", import.meta.url).href)};
const send = message => new Promise((resolve, reject) => process.send(message, error => error ? reject(error) : resolve()));
let paused = false;
try {
 const result = await rotateConsumerStateJournal(process.argv[1], {
  stateMaxBytes: 8192, startHeartbeat: () => async () => {},
  readDirectory: async path => {
   const names = await readdir(path);
   if (path === process.argv[2] && !paused) {
    paused = true;
    await new Promise((resolve, reject) => {
     process.once("message", message => message?.type === "release" ? resolve() : reject(new Error("Unexpected root barrier release")));
     process.send({ type: "ready", pid: process.pid, names }, error => { if (error) reject(error); });
    });
   }
   return names;
  },
 });
 await send({ type: "done", pid: process.pid, result });
} catch (error) {
 await send({ type: "failure", pid: process.pid, message: error.stack });
 process.exitCode = 1;
}
process.disconnect();
`;

function rootReader(state, root) {
 const process = spawn(globalThis.process.execPath, ["--input-type=module", "--eval", rotationSource, state, root], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
 const messages = []; let ended = false; let output = ""; const waiters = [];
 process.stderr.on("data", (data) => { output += data; });
 process.on("message", (message) => { messages.push(message); for (const wake of waiters.splice(0)) wake(); });
 const exit = new Promise((resolve, reject) => {
  process.once("error", reject);
  process.once("close", (code, signal) => { ended = true; resolve({ code, signal }); for (const wake of waiters.splice(0)) wake(); });
 });
 const watchdog = setTimeout(() => { if (!ended) process.kill("SIGKILL"); }, 60000);
 exit.finally(() => clearTimeout(watchdog));
 return { process, messages, exit,
  async ready() {
   while (!messages.some((message) => message.type === "ready")) {
    if (ended) throw new Error(`Root reader exited before its required barrier: ${JSON.stringify(messages)} ${output}`);
    await new Promise((resolve) => waiters.push(resolve));
   }
   const ready = messages.find((message) => message.type === "ready");
   assert.equal(ready.pid, process.pid);
   return ready;
  },
  async finish() {
   assert.deepEqual(await exit, { code: 0, signal: null }, `${JSON.stringify(messages)} ${output}`);
   assert.deepEqual(messages.map((message) => message.type), ["ready", "done"]);
   assert.ok(messages.every((message) => message.pid === process.pid));
   return messages[1].result;
  },
  async stop() { if (!ended) process.kill("SIGKILL"); await exit; },
 };
}

for (let round = 0; round < 3; round++) test(`current-public twelve-process stale root handoff round ${round + 1}`, { timeout: 90000 }, async () => {
 const f = await fixture("commit"); const readers = []; let publisher;
 try {
  const record = JSON.parse(await readFile(`${f.state}.journal-v3/root.json`));
  const root = join(`${f.state}.journal-v3`, record.goal);
  const initial = await readdir(root);
  assert.equal(initial.length, 1); assert.match(initial[0], /^generation-0000000000000001-/);
  for (let index = 0; index < 12; index++) readers.push(rootReader(f.state, root));
  for (const ready of await Promise.all(readers.map((reader) => reader.ready()))) assert.deepEqual(ready.names, initial);
  publisher = child(f, "rotate", { stateMaxBytes: 8192 });
  const published = await publisher.finish();
  assert.equal(published.result, 2); assert.equal(published.finals.length, 1);
  assert.match(published.finals[0], /^generation-0000000000000002-/);
  assert.equal((await readdir(root)).includes(initial[0]), false);
  for (const reader of readers) reader.process.send({ type: "release" });
  const results = await Promise.all(readers.map((reader) => reader.finish()));
  assert.deepEqual(results, Array.from({ length: 12 }, () => ({ epoch: 2, tipSha256 })));
  assert.deepEqual(await readdir(root), published.finals);
  assert.equal(await projection(f), "base");
 } finally {
  await Promise.all(readers.map((reader) => reader.stop()));
  if (publisher) await publisher.stop();
  await cleanup(f);
 }
});

test("current-public pinned checkpoint retirement fails closed before a fresh callback converges", async () => {
 const f = await fixture("commit"); let retired = false; let callbackCalls = 0;
 try {
  await assert.rejects(withConsumerStateLock(f.state, async () => { callbackCalls++; }, { ...options, hooks: {
   metadataRead: { afterInitialStat: async ({ path, handle, stat }) => {
    if (retired || !path.endsWith("/checkpoint.json") || !path.includes("/generation-")) return;
    retired = true;
    assert.equal(stat.nlink, 2);
    assert.deepEqual(await rotateConsumerStateJournal(f.state, options), { epoch: 2, tipSha256 });
    await assert.rejects(lstat(path), { code: "ENOENT" });
    const after = await handle.stat();
    assert.deepEqual([after.dev, after.ino, after.size, after.mtimeMs], [stat.dev, stat.ino, stat.size, stat.mtimeMs]);
    assert.equal(after.nlink, 0);
   } },
  } }), { message: "Generation discovery checkpoint changed while it was read." });
  assert.equal(retired, true); assert.equal(callbackCalls, 0); assert.equal(await projection(f), "base");
  await withConsumerStateLock(f.state, async (_path, tx) => {
   callbackCalls++;
   assert.equal(tx.readStateBytes().toString(), "base");
  }, options);
  assert.equal(callbackCalls, 1); assert.equal(await projection(f), "base");
  const record = JSON.parse(await readFile(`${f.state}.journal-v3/root.json`));
  const finals = await readdir(join(`${f.state}.journal-v3`, record.goal));
  assert.equal(finals.length, 1); assert.match(finals[0], /^generation-0000000000000002-/);
 } finally { await cleanup(f); }
});
