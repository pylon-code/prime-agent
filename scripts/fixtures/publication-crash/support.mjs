import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as old from "../protected-publication-v2/pylon-consumer-lock.mjs";
import { withConsumerStateLock, buildConsumerGeneration, publishConsumerGeneration } from "../../lib/pylon-consumer-lock.mjs";
import { generationBytes as bytes, generationDigest as digest, GENERATION_ZERO as ZERO } from "../../lib/pylon-generation-format.mjs";

export const options = { stateMaxBytes: 1024, now: () => 1000, stale: 100, startHeartbeat: () => async () => {} };
const oldOptions = { now: () => 1000, stateMaxBytes: 1024, startHeartbeat: () => async () => {} };
export const families = ["v1", "prior-retired-v1", "v2", "v1-v2", "prior-retired-v1-v2"];
export const recoveries = {
 "recover-builder-checkpoint": { scenario: "builder", match: { hook: "generation", phase: "after", operation: "file-sync" } },
 "recover-dead-receipt": { scenario: "commit", match: { hook: "generation", phase: "after", operation: "file-sync" } },
 "recover-commit-receipt": { scenario: "commit", match: { hook: "generation", phase: "before", operation: "rename" } },
 "recover-migration-intent": { scenario: "migrate-v2", match: { hook: "migration", phase: "after", operation: "immutable-link", path: "state.json.journal-v3/intent.json" } },
 "recover-projection": { scenario: "commit", match: { hook: "generation", phase: "after", operation: "create-projection" } },
};
export const scenarios = [...Object.keys(recoveries), "fresh", "commit", "rotate", "builder", "loser-cleanup", ...families.map((family) => `migrate-${family}`), "migrate-v2-incomplete", "migrate-v2-retained"];
export async function fixture(scenario) {
 if (recoveries[scenario]) {
  const setup = recoveries[scenario]; const f = await fixture(setup.scenario); const owner = child(f, setup.scenario, { cutMatch: setup.match, marker: "setup-owner" });
  try { const cut = await owner.wait("cut"); assert.equal(cut.pid, owner.process.pid); owner.process.kill("SIGKILL"); assert.equal((await owner.exit).signal, "SIGKILL"); }
  finally { await owner.stop(); }
  return f;
 }
 const directory = await realpath(await mkdtemp(join(tmpdir(), "pylon-publication-crash-")));
 await chmod(directory, 0o700);
 const state = join(directory, "state.json");
 const root = join(directory, "journal");
 const authority = { genesis: { statePath: state, stateBytes: null } };
 if (["builder", "loser-cleanup"].includes(scenario)) {
  await mkdir(root, { mode: 0o700 });
  if (scenario === "loser-cleanup") {
   const winner = await buildConsumerGeneration(root, authority, options);
   await buildConsumerGeneration(root, authority, options);
   await publishConsumerGeneration(winner, options);
  }
 } else if (["commit", "rotate"].includes(scenario)) {
  await withConsumerStateLock(state, async (_path, tx) => tx.commitState("base"), options);
 } else if (scenario.startsWith("migrate-")) {
  const family = scenario.slice(8);
  if (family.includes("v1")) {
   await mkdir(`${state}.lock`, { mode: 0o700 });
   await mkdir(`${state}.transactions`, { mode: 0o700 });
   const claim = { schemaVersion: 1, generation: 1, token: randomUUID(), ownerPid: 2_000_000_000, createdAtMs: 0 };
   const value = Buffer.from("base");
   const transaction = { schemaVersion: 1, baseDigest: ZERO, candidateDigest: digest(value), candidateBase64: value.toString("base64") };
   const terminal = { schemaVersion: 1, generation: 1, token: claim.token, outcome: "commit", transactions: [transaction] };
   const put = (path, value) => writeFile(path, bytes(value), { mode: 0o600 });
   await put(`${state}.lock/claim-0000000000000001.json`, claim);
   await put(`${state}.lock/heartbeat-0000000000000001-${claim.token}.json`, { schemaVersion: 1, generation: 1, token: claim.token, refreshedAtMs: 0 });
   await put(`${state}.lock/terminal-0000000000000001-${claim.token}.json`, terminal);
   await put(`${state}.transactions/${ZERO}.json`, transaction);
   await put(`${state}.lock/applied-0000000000000001-${claim.token}.json`, { schemaVersion: 1, generation: 1, token: claim.token, terminalSha256: digest(bytes(terminal)) });
   await writeFile(state, value, { mode: 0o600 });
   if (family.startsWith("prior-retired")) {
    await rename(`${state}.lock`, `${state}.lock.v1-retired`);
    await put(`${state}.lock`, { schemaVersion: 1, kind: "pylon-consumer-legacy-lock-guard", statePathSha256: digest(Buffer.from(state)) });
   }
   if (family.endsWith("-v2")) await old.migrateConsumerStateJournal(state, oldOptions);
  } else {
   await old.withConsumerStateLock(state, async (_path, tx) => tx.commitState("base"), oldOptions);
   if (family === "v2-retained") await old.rotateConsumerStateJournal(state, oldOptions);
   if (family === "v2-incomplete") {
    const stopped = new Error("fixture incomplete durable commit");
    await assert.rejects(old.withConsumerStateLock(state, async (_path, tx) => tx.commitState("base" + "-advanced"), { ...oldOptions, hooks: { afterCommitDecision: () => { throw stopped; } } }), (error) => error === stopped);
   }
  }
 }
 return { directory, state, root, authority };
}
export async function cleanup(f) {
 for (const path of [`${f.state}.lock`, `${f.state}.lock.v1-retired`, `${f.state}.transactions`]) {
  try { if ((await lstat(path)).isDirectory()) await chmod(path, 0o700); } catch (error) { if (error.code !== "ENOENT") throw error; }
 }
 await rm(f.directory, { recursive: true, force: true });
}
export function child(f, scenario, configuration = {}) {
 const process = fork(new URL("./worker.mjs", import.meta.url), [f.directory, scenario, JSON.stringify(configuration)], { stdio: ["ignore", "pipe", "pipe", "ipc"], execArgv: [] });
 const messages = []; const waiters = [];
 let output = ""; let ended = false;
 process.stdout.on("data", (data) => { output += data; }); process.stderr.on("data", (data) => { output += data; });
 process.on("message", (message) => { messages.push(message); for (const wake of waiters.splice(0)) wake(); });
 const exit = new Promise((resolve, reject) => {
  process.once("error", reject);
  process.once("close", (code, signal) => { ended = true; resolve({ code, signal }); for (const wake of waiters.splice(0)) wake(); });
 });
 const watchdog = setTimeout(() => { if (!ended) process.kill("SIGKILL"); }, 60000);
 exit.finally(() => clearTimeout(watchdog));
 return { process, messages, exit, get output() { return output; },
  async wait(type) {
   while (!messages.some((message) => message.type === type)) {
    if (ended) throw new Error(`Worker exited without required ${type}: ${JSON.stringify(messages.slice(-2))} ${output}`);
    await new Promise((resolve) => waiters.push(resolve));
   }
   return messages.find((message) => message.type === type);
  },
  async finish() {
   const result = await exit;
   assert.deepEqual(result, { code: 0, signal: null }, `${JSON.stringify(messages.slice(-2))} ${output}`);
   return messages.find((message) => message.type === "done");
  },
  async stop() { if (!ended) process.kill("SIGKILL"); await exit; },
 };
}
export async function inventory(f) {
 const paths = [];
 async function visit(path) {
  const stat = await lstat(path);
  paths.push({ path: path.slice(f.directory.length + 1), dev: stat.dev, ino: stat.ino, size: stat.size });
  if (stat.isDirectory()) for (const name of (await readdir(path)).sort()) await visit(join(path, name));
 }
 await visit(f.directory);
 return paths;
}
export async function projection(f) { return readFile(f.state, "utf8").catch((error) => { if (error.code === "ENOENT") return null; throw error; }); }
