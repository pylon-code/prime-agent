import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, lstatSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "node:test";
import { recordPreviewHighWater } from "./verify-pylon-preview-history.mjs";
import { verifyStableHistoryWithState } from "./verify-pylon-stable-history.mjs";
import { withConsumerStateLock, migrateConsumerStateJournal, rotateConsumerStateJournal } from "./lib/pylon-consumer-lock.mjs";
import { createReleaseManifest, PYLON_RELEASE_NODE_VERSION, PYLON_RELEASE_NPM_VERSION } from "./lib/pylon-release.mjs";
import { canonicalJson, createPreviewManifest, createStableManifest, sha256Bytes } from "./lib/pylon-publication.mjs";

const source = {
	repository: "https://github.com/pylon-code/prime-agent",
	commit: "0123456789abcdef0123456789abcdef01234567",
	tree: "89abcdef0123456789abcdef0123456789abcdef",
};
const version = "0.8.1";
const invocation = {
	sequenceEpoch: 1,
	sequence: 17,
	workflowRunId: "33428882721",
	publicationPolicyRevision: 1,
};

function fakeReleaseManifest() {
	return createReleaseManifest({
		source,
		version,
		toolchain: { node: PYLON_RELEASE_NODE_VERSION, npm: PYLON_RELEASE_NPM_VERSION },
		lockfileSha256: "a".repeat(64),
		artifacts: [
			["prime-agent", "pylon-prime-agent-0.8.1.tgz", "d"],
			["@earendil-works/pi-ai", "pylon-prime-agent-ai-0.8.1.tgz", "a"],
			["@earendil-works/pi-agent-core", "pylon-prime-agent-core-0.8.1.tgz", "b"],
			["@earendil-works/pi-tui", "pylon-prime-agent-tui-0.8.1.tgz", "c"],
		].map(([packageName, file, byte]) => {
			const bytes = Buffer.from(byte);
			return {
				package: packageName,
				file,
				size: bytes.byteLength,
				sha256: byte.repeat(64),
				sha512: byte.repeat(128),
			};
		}),
	});
}

function manifests() {
	const release = fakeReleaseManifest();
	const releaseBytes = Buffer.from(`${JSON.stringify(release, null, 2)}\n`);
	const preview = createPreviewManifest(release, releaseBytes, invocation);
	const previewBytes = Buffer.from(canonicalJson(preview));
	return { release, releaseBytes, preview, previewBytes };
}

function firstStable() {
	const { preview, previewBytes } = manifests();
	return createStableManifest({
		previewManifest: preview,
		previewManifestBytes: previewBytes,
		sequence: 1,
		previous: null,
		promotion: { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 },
	});
}

function secondStable(previous = firstStable(), options = {}) {
	const { preview, previewBytes } = manifests();
	const sequence = 2;
	const revocation = {
		stableTag: previous.tag,
		buildTag: previous.build.previewTag,
		reason: "security-withdrawal",
		revokedBySequence: sequence,
	};
	return createStableManifest({
		previewManifest: preview,
		previewManifestBytes: previewBytes,
		sequence,
		previous: { tag: previous.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(previous))) },
		revocations: options.withdraw ? [revocation] : [],
		promotion: options.withdraw ? { kind: "withdraw", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1, revocation } : { kind: "promote", policyCommit: source.commit, policyTree: source.tree, publicationPolicyRevision: 1 },
	});
}


test("current public v3 consumer preview high-water allows gaps but rejects rollback and same-sequence equivocation", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-preview-state-")));
	try {
		const { preview, previewBytes } = manifests();
		const statePath = join(fixture, "consumer", "nested", "preview.json");
		await assert.rejects(() => recordPreviewHighWater(preview, previewBytes, { statePath }), /--initialize/);
		assert.equal((await recordPreviewHighWater(preview, previewBytes, { statePath, initialize: true })).advanced, true);
		assert.equal((await recordPreviewHighWater(preview, previewBytes, { statePath })).advanced, false);
		const later = structuredClone(preview);
		later.sequence += 3;
		later.workflowRunId = String(Number(later.workflowRunId) + 3);
		assert.equal((await recordPreviewHighWater(later, Buffer.from(canonicalJson(later)), { statePath })).state.highWater.sequence, later.sequence);
		await assert.rejects(() => recordPreviewHighWater(preview, previewBytes, { statePath }), /older/);
		const equivocation = structuredClone(later);
		equivocation.build.releaseManifest.sha256 = "f".repeat(64);
		await assert.rejects(
			() => recordPreviewHighWater(equivocation, Buffer.from(canonicalJson(equivocation)), { statePath }),
			/equivocates/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("current public v3 consumer stable high-water requires explicit initialization, is idempotent, and advances atomically", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-state-")));
	try {
		const first = firstStable();
		const second = secondStable(first);
		const firstPath = join(fixture, "first.json");
		const secondPath = join(fixture, "second.json");
		const statePath = join(fixture, "consumer", "nested", "stable.json");
		writeFileSync(firstPath, canonicalJson(first));
		writeFileSync(secondPath, canonicalJson(second));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /--initialize/);
		const initialized = await verifyStableHistoryWithState([firstPath], { statePath, initialize: true });
		assert.equal(initialized.advanced, true);
		assert.equal(initialized.state.highWater.sequence, 1);
		const witnessedBytes = readFileSync(statePath, "utf8");
		const repeated = await verifyStableHistoryWithState([firstPath], { statePath });
		assert.equal(repeated.advanced, false);
		assert.equal(readFileSync(statePath, "utf8"), witnessedBytes);
		const advanced = await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(advanced.advanced, true);
		assert.equal(advanced.state.highWater.sequence, 2);
		writeFileSync(statePath, witnessedBytes);
		const repairedRollback = await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(repairedRollback.state.highWater.sequence, 2, "the immutable transaction tip outranks a rolled-back projection");
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2);
		rmSync(statePath);
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2, "a deleted projection is repaired from the journal");
		writeFileSync(statePath, "");
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath });
		assert.equal(JSON.parse(readFileSync(statePath, "utf8")).highWater.sequence, 2, "an empty projection is repaired from the journal");
		const legacyPath = join(fixture, "legacy.json");
		writeFileSync(legacyPath, canonicalJson(initialized.state), { mode: 0o600 });
		const migrated = await verifyStableHistoryWithState([firstPath], { statePath: legacyPath });
		assert.equal(migrated.advanced, false);
		assert.equal(readdirSync(`${legacyPath}.journal-v3`).some((name) => name.startsWith("journal-")), true);
		await assert.rejects(() => verifyStableHistoryWithState([firstPath, secondPath], { statePath, initialize: true }), /cannot reset/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("current public v3 consumer stable high-water rejects rollback and a rewritten witnessed sequence", async () => {
	const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-stable-state-")));
	try {
		const first = firstStable();
		const second = secondStable(first);
		const firstPath = join(fixture, "first.json");
		const secondPath = join(fixture, "second.json");
		const statePath = join(fixture, "stable.json");
		writeFileSync(firstPath, canonicalJson(first));
		writeFileSync(secondPath, canonicalJson(second));
		await verifyStableHistoryWithState([firstPath, secondPath], { statePath, initialize: true });
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /older than/);
		const rewrittenFirst = structuredClone(first);
		rewrittenFirst.promotion.policyTree = "f".repeat(40);
		writeFileSync(firstPath, canonicalJson(rewrittenFirst));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath], { statePath }), /older than|rewrites/);
		const rewrittenSecond = createStableManifest({
			previewManifest: manifests().preview,
			previewManifestBytes: manifests().previewBytes,
			sequence: 2,
			previous: { tag: rewrittenFirst.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(rewrittenFirst))) },
			promotion: { kind: "promote", policyCommit: source.commit, policyTree: "e".repeat(40), publicationPolicyRevision: 1 },
		});
		writeFileSync(secondPath, canonicalJson(rewrittenSecond));
		await assert.rejects(() => verifyStableHistoryWithState([firstPath, secondPath], { statePath }), /rewrites/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("current public API and CLI require acknowledgement without filesystem mutation", async () => {
 const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-public-ack-")));
 try {
  const state = join(fixture, "missing", "state.json");
  await assert.rejects(migrateConsumerStateJournal(state), /acknowledgement/);
  for (const args of [["--state", state], ["--state", state, "--acknowledge-legacy-processes-stopped", "--unknown"]]) {
   const result = spawnSync(process.execPath, [resolve("scripts/migrate-pylon-consumer-journal.mjs"), ...args], { encoding: "utf8" });
   assert.equal(result.status, 1); assert.match(result.stderr, /Usage:/);
   assert.deepEqual(readdirSync(fixture), []);
  }
 } finally { rmSync(fixture, { recursive: true, force: true }); }
});

test("current public operations stage atomically, reject active owners, preserve failures, and rotate exact genesis", async () => {
 const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-public-operations-")));
 const state = join(fixture, "state.json");
 const options = { stateMaxBytes: 1024, startHeartbeat: () => async () => {} };
 try {
  let release; let ready;
  const held = new Promise((resolve) => { release = resolve; });
  const staged = new Promise((resolve) => { ready = resolve; });
  const owner = withConsumerStateLock(state, async (_path, tx) => {
   await tx.commitState("committed"); ready(); await held;
  }, options);
  await staged;
  const rejection = await Promise.allSettled([
   withConsumerStateLock(state, async () => assert.fail("competing callback"), options),
   rotateConsumerStateJournal(state, options),
  ]);
  release(); await owner;
  for (const result of rejection) { assert.equal(result.status, "rejected"); assert.match(result.reason.message, /actively locked/); }
  const failure = new Error("exact callback failure");
  await assert.rejects(withConsumerStateLock(state, async (_path, tx) => { await tx.commitState("discarded"); throw failure; }, options), (error) => error === failure);
  const initial = readFileSync(`${state}.journal-v3/intent.json`);
  const rotated = await rotateConsumerStateJournal(state, options); assert.equal(rotated.epoch, 2);
  await withConsumerStateLock(state, async (_path, tx) => assert.equal(tx.readStateBytes().toString(), "committed"), options);
  assert.deepEqual(readFileSync(`${state}.journal-v3/intent.json`), initial);
  chmodSync(`${state}.journal-v3`, 0o777);
  let callbacks = 0;
  await assert.rejects(withConsumerStateLock(state, async () => { callbacks++; }, options), /permissions/);
  assert.equal(callbacks, 0); chmodSync(`${state}.journal-v3`, 0o700);
  assert.equal(lstatSync(`${state}.lock`).mode & 0o7777, 0o600);
 } finally { rmSync(fixture, { recursive: true, force: true }); }
});

test("protected and retained v2 regression fixtures match their pinned provenance bytes", () => {
 for (const family of ["protected-publication-v2", "retained-publication-v2"]) {
  const directory = resolve(import.meta.dirname, "fixtures", family);
  const provenance = JSON.parse(readFileSync(join(directory, "provenance.json")));
  assert.match(provenance.commit, /^[0-9a-f]{40}$/);
  for (const entry of provenance.files) assert.equal(sha256Bytes(readFileSync(join(directory, entry.path))), entry.sha256, `${family}/${entry.path}`);
 }
});

test("current public rotation uses reserved capacity without an extra predecessor normal claim", async () => {
 const fixture = realpathSync(mkdtempSync(join(tmpdir(), "pylon-public-capacity-")));
 try {
  const state = join(fixture, "state.json");
  const options = { stateMaxBytes: 1024, maxLockGenerations: 2, startHeartbeat: () => async () => {} };
  for (const value of ["one", "two"]) await withConsumerStateLock(state, async (_path, tx) => tx.commitState(value), options);
  const rotated = await rotateConsumerStateJournal(state, options);
  assert.equal(rotated.epoch, 2); assert.equal(rotated.tipSha256, sha256Bytes(Buffer.from("two")));
  assert.equal(readFileSync(state).toString(), "two");
  assert.deepEqual(await rotateConsumerStateJournal(state, options), rotated);
  rmSync(state);
  assert.deepEqual(await rotateConsumerStateJournal(state, options), rotated);
  assert.equal(readFileSync(state).toString(), "two");
 } finally { rmSync(fixture, { recursive: true, force: true }); }
});
