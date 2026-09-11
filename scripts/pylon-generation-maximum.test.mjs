import assert from "node:assert/strict";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { rotateConsumerGeneration, withConsumerGenerationLock } from "./lib/pylon-consumer-lock.mjs";

// Required publication gate, separated from smoke because full authority revalidation
// with three actual 16 MiB checkpoint fields takes several minutes.
async function fixture(t) {
	const directory = await mkdtemp(join(tmpdir(), "pylon-generation-maximum-"));
	await chmod(directory, 0o700);
	t.after(() => rm(directory, { recursive: true, force: true }));
	const root = join(directory, "journal");
	await mkdir(root, { mode: 0o700 });
	return { root, authority: { genesis: { statePath: join(directory, "state.json"), stateBytes: null } } };
}

test("v3 operation integrated actual 16 MiB provenance and transaction retain rotation headroom", async (t) => {
	const f = await fixture(t);
	const initial = Buffer.alloc(16 * 1024 * 1024, 0x61);
	const candidate = Buffer.alloc(16 * 1024 * 1024, 0x62);
	f.authority.genesis.stateBytes = initial;
	f.authority.genesis.source = { kind: "v2", authoritySha256: "1".repeat(64), tipBytes: initial };
	f.authority.genesis.migration = { kind: "v1", authoritySha256: "2".repeat(64), tipBytes: initial };
	const maximum = { startHeartbeat: () => async () => {} };
	let projectionCount = 0;
	await withConsumerGenerationLock(f.root, f.authority, async (_path, tx) => {
		assert.ok(tx.readStateBytes().equals(initial));
		await tx.commitState(candidate);
	}, { ...maximum, hooks: { afterProjectionFileSync: async ({ temporary }) => {
		projectionCount++;
		assert.equal((await lstat(temporary)).size, candidate.length);
		assert.ok(temporary.includes("/receipts/.projection-"));
	} } });
	assert.equal(projectionCount, 2);
	let peak = 0;
	const result = await rotateConsumerGeneration(f.root, f.authority, { ...maximum, hooks: { beforeGenerationRename: async () => {
		for (const name of await readdir(f.root)) {
			const path = join(f.root, name);
			for (const entry of await readdir(path)) {
				if (["epoch", "receipts"].includes(entry)) {
					for (const file of await readdir(join(path, entry))) peak += (await lstat(join(path, entry, file))).size;
				} else peak += (await lstat(join(path, entry))).size;
			}
		}
	} } });
	assert.equal(result.epoch, 2);
	assert.ok(peak > 400 * 1024 * 1024 && peak < 512 * 1024 * 1024, String(peak));
	assert.equal((await readdir(f.root)).length, 1);
	assert.ok((await readFile(f.authority.genesis.statePath)).equals(candidate));
	t.diagnostic(`Actual integrated rotation root bytes, charging receipt duplicates: ${peak}`);
});

