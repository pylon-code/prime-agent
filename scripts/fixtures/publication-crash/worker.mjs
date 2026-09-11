import { appendFile, lstat, readFile, readdir } from "node:fs/promises";
import { basename, join } from "node:path";
import { withConsumerStateLock, migrateConsumerStateJournal, rotateConsumerStateJournal, prepareConsumerGeneration, discoverConsumerGenerations, withConsumerGenerationLock } from "../../lib/pylon-consumer-lock.mjs";

const [directory, requestedScenario, encoded] = process.argv.slice(2);
const config = JSON.parse(encoded);
const recoveryScenarios = { "recover-builder-checkpoint": "builder", "recover-dead-receipt": "commit", "recover-commit-receipt": "commit", "recover-migration-intent": "migrate-v2", "recover-projection": "commit" };
const scenario = recoveryScenarios[requestedScenario] ?? requestedScenario;
if (recoveryScenarios[requestedScenario]) config.recover = true;
const state = join(directory, "state.json");
const lowRoot = join(directory, "journal");
const authority = { genesis: { statePath: state, stateBytes: null } };
const send = (message) => new Promise((resolve, reject) => process.send(message, (error) => error ? reject(error) : resolve()));
const events = [];
const occurrences = new Map();
let readPaused = false;
async function pauseRead(path, kind) {
 if (readPaused || config.pauseRead !== kind || !path.includes("/generation-")) return;
 readPaused = true; process.send({ type: "cut", pid: process.pid, path, kind });
 await new Promise((resolve) => process.once("message", resolve));
}
const normalize = (path) => path.slice(directory.length + 1).replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "UUID").replace(/[0-9a-f]{32,}/g, "HASH").replace(/-p[0-9]+/g, "-pPID");
async function barrier(hook, event) {
 const key = `${hook}:${event.phase}:${event.operation}:${normalize(event.path)}`;
 const occurrence = (occurrences.get(key) ?? 0) + 1; occurrences.set(key, occurrence);
 const cut = { hook, phase: event.phase, operation: event.operation, path: normalize(event.path), occurrence };
 events.push(cut);
 if (config.cut === events.length - 1 || config.pause === key || config.cutMatch && Object.entries(config.cutMatch).every(([name, value]) => cut[name] === value)) {
  process.send({ type: "cut", pid: process.pid, index: events.length - 1, event: cut, events });
  await new Promise((resolve) => process.once("message", resolve));
 }
}
const options = { stateMaxBytes: config.stateMaxBytes ?? 1024, now: () => config.now ?? (config.recover ? 100000 : 1000), stale: 100, startHeartbeat: () => async () => {},
 hooks: {
  afterMigrationBlocker: config.migrationPause ? async () => {
   await send({ type: "cut", pid: process.pid, phase: "after-blocker" });
   await new Promise((resolve) => process.once("message", resolve));
  } : undefined,
  metadataRead: { afterInitialStat: async ({ path }) => {
   const name = basename(path);
   const kind = ["checkpoint.json", "claim-index-", "heartbeat-", "terminal-", "transition-", "applied-", "receipt-"].find((prefix) => name.startsWith(prefix));
   if (kind) await pauseRead(path, kind);
  } },
  generationBoundary: (event) => barrier("generation", event), migrationBoundary: (event) => barrier("migration", event),
  beforeCommitDecision: () => barrier("semantic", { phase: "before", operation: "commit-decision", path: state }),
  afterCommitDecision: () => barrier("semantic", { phase: "after", operation: "commit-decision", path: state }),
 } };
if (config.pauseRead === "receipt-") options.lstatEntry = async (path) => {
 const stat = await lstat(path);
 if (basename(path).startsWith("receipt-")) await pauseRead(path, "receipt-");
 return stat;
};
async function callback(_path, tx) {
 const marker = config.marker ?? "owner";
 await appendFile(join(directory, `${marker}.callbacks`), "entered\n", { mode: 0o600 });
 if (config.recover) return tx.readStateBytes()?.toString() ?? null;
 const current = tx.readStateBytes()?.toString();
 await tx.commitState(config.append ? JSON.stringify([...(current === "base" || current == null ? [] : JSON.parse(current)), config.value]) : config.value ?? "candidate");
 await barrier("callback", { phase: "after", operation: "stage", path: state });
 await appendFile(join(directory, `${marker}.callbacks`), "returned\n", { mode: 0o600 });
 return current;
}
try {
 if (config.ready) { process.send({ type: "ready", pid: process.pid }); await new Promise((resolve) => process.once("message", resolve)); }
 let result;
 if (config.pauseRead === "receipt-") {
  const record = JSON.parse(await readFile(`${state}.journal-v3/root.json`));
  result = await discoverConsumerGenerations(join(`${state}.journal-v3`, record.goal), authority, options);
 } else if (scenario.startsWith("migrate-")) result = await migrateConsumerStateJournal(state, { ...options, acknowledgeLegacyProcessesStopped: true });
 else if (["builder", "loser-cleanup"].includes(scenario)) {
  result = await prepareConsumerGeneration(lowRoot, authority, options);
  if (config.recover) await withConsumerGenerationLock(lowRoot, authority, async () => {}, options);
 } else if (scenario === "rotate" && !config.recover) result = await rotateConsumerStateJournal(state, options);
 else result = await withConsumerStateLock(state, callback, options);
 let root = lowRoot;
 if (!["builder", "loser-cleanup"].includes(scenario)) root = join(`${state}.journal-v3`, JSON.parse(await readFile(`${state}.journal-v3/root.json`)).goal);
 const stat = await lstat(root);
 const names = await readdir(root);
 await send({ type: "done", pid: process.pid, events, root: { dev: stat.dev, ino: stat.ino }, finals: names.filter((name) => name.startsWith("generation-")), entries: names, result: result?.epoch ?? null });
 process.disconnect();
} catch (error) { await send({ type: "error", pid: process.pid, message: error.message, code: error.code, events }); process.disconnect(); process.exitCode = 1; }
