import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { child, cleanup, fixture, inventory, projection, scenarios } from "./fixtures/publication-crash/support.mjs";

const manifest = JSON.parse(await readFile(new URL("./fixtures/publication-crash/boundaries.json", import.meta.url)));
const traceDigest = (events) => createHash("sha256").update(JSON.stringify(events)).digest("hex");

for (const scenario of scenarios) test(`publication crash boundary inventory ${scenario}`, { timeout: 900000 }, async (t) => {
  const original = await fixture(scenario); const recorder = child(original, scenario);
  let events;
  try {
   events = (await recorder.finish()).events;
   assert.equal(events.length, manifest[scenario].count, "Changed boundary count requires explicit inventory review");
   assert.equal(traceDigest(events), manifest[scenario].sha256, "Changed boundary ordering/path/occurrence requires explicit inventory review");
  } finally { await recorder.stop(); await cleanup(original); }
  for (const [index, expected] of events.entries()) {
   const start = performance.now(); const f = await fixture(scenario); const owner = child(f, scenario, { cut: index }); let recovery;
   try {
    const observed = await owner.wait("cut");
    assert.equal(observed.pid, owner.process.pid);
    assert.equal(observed.index, index);
    assert.deepEqual(observed.events, events.slice(0, index + 1), `Trace changed before ${scenario} cut ${index}`);
    assert.equal(owner.process.kill("SIGKILL"), true);
    assert.deepEqual(await owner.exit, { code: null, signal: "SIGKILL" });
    const beforeRecovery = await inventory(f);
    recovery = child(f, scenario, { now: 1000000, recover: true, marker: "recovery" });
    const result = await recovery.finish();
    assert.notEqual(result.pid, observed.pid);
    assert.equal(result.finals.length, 1);
    assert.equal(result.entries.some((name) => name.startsWith(".retired-") || name.startsWith(".deleting-")), false);
    const value = await projection(f);
    if (scenario.startsWith("recover-")) assert.equal(value, scenario === "recover-projection" ? "candidate" : scenario === "recover-builder-checkpoint" ? null : "base");
    else if (scenario.startsWith("migrate-")) assert.equal(value, scenario.endsWith("incomplete") ? "base-advanced" : "base");
    else if (scenario === "rotate") assert.equal(value, "base");
    else if (["fresh", "commit"].includes(scenario)) {
     const markers = await readFile(join(f.directory, "owner.callbacks"), "utf8").catch((error) => { if (error.code === "ENOENT") return ""; throw error; });
     assert.ok(["", "entered\n", "entered\nreturned\n"].includes(markers), "Original callback never replayed");
     const returned = markers.endsWith("returned\n");
     const committed = observed.events.some((event) => event.hook === "generation" && event.operation === "link" && event.phase === "after" && event.path.includes("/terminal-"));
     assert.equal(value, returned && committed ? "candidate" : scenario === "fresh" ? null : "base");
    }
    t.diagnostic(JSON.stringify({ scenario, index, ...expected, pid: observed.pid, signal: "SIGKILL", recoveryPid: result.pid, root: result.root, preservedEntries: beforeRecovery.length, projection: value, elapsedMs: Math.round(performance.now() - start) }));
   } finally { await owner.stop(); if (recovery) await recovery.stop(); await cleanup(f); }
  }
});
