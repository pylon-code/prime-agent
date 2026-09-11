import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { closeSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { lstat, open } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { readBoundedRegularFile, readBoundedRegularFileSync } from "./lib/pylon-bounded-file.mjs";

const readers = [
	{ name: "async", read: readBoundedRegularFile, lstat },
	{ name: "sync", read: readBoundedRegularFileSync, lstat: lstatSync },
];

for (const reader of readers) {
	for (const code of ["ENOENT", "EIO", "EPERM", "ELOOP", "EISDIR"]) {
		for (const stage of ["ELOOP", "EISDIR"].includes(code) ? ["open"] : ["initial lstat", "open", "final lstat", "afterInitialPathStat", "afterInitialStat", "beforeFinalStat", "afterFinalStat", "stat", "read", "close"]) {
			test(`bounded ${reader.name} retains injected ${code} identity at ${stage}`, async () => {
				const fixture = mkdtempSync(join(tmpdir(), "pylon-bounded-identity-"));
				const path = join(fixture, "input");
				const bytes = Buffer.from("exact pinned input");
				const failure = Object.assign(new Error(`injected ${stage}`), { code });
				writeFileSync(path, bytes);
				const fail = () => { throw failure; };
				let calls = 0;
				const options = { maxBytes: 1024, expectedSha256: createHash("sha256").update(bytes).digest("hex") };
				if (stage === "initial lstat") options.lstatEntry = fail;
				else if (stage === "open") options.openFile = fail;
				else if (stage === "final lstat") {
					options.lstatEntry = (target) => {
						if (++calls === 2) {
							// A real namespace handoff cannot convert an injected error into native evidence.
							renameSync(target, `${target}.retired`);
							throw failure;
						}
						return reader.lstat(target);
					};
				} else if (["stat", "read", "close"].includes(stage)) {
					if (reader.name === "sync") {
						if (stage === "stat") options.statFile = fail;
						if (stage === "read") options.readFile = fail;
						if (stage === "close") options.closeFile = (descriptor) => { closeSync(descriptor); throw failure; };
					} else {
						options.openFile = async (target, flags) => {
							const handle = await open(target, flags);
							return {
								stat: stage === "stat" ? fail : handle.stat.bind(handle),
								read: stage === "read" ? fail : handle.read.bind(handle),
								close: async () => { await handle.close(); if (stage === "close") throw failure; },
							};
						};
					}
				} else options.hooks = { [stage]: () => { renameSync(path, `${path}.retired`); throw failure; } };
				try {
					await assert.rejects(async () => reader.read(path, options), (error) => error === failure);
				} finally {
					rmSync(fixture, { recursive: true, force: true });
				}
			});
		}
	}
	for (const replacement of ["symlink", "directory"]) test(`bounded ${reader.name} rejects native ${replacement} replacement before open`, async () => {
		const fixture = mkdtempSync(join(tmpdir(), "pylon-bounded-unsafe-")); const path = join(fixture, "input");
		try {
			writeFileSync(path, "input");
			await assert.rejects(async () => reader.read(path, { maxBytes: 1024, hooks: { afterInitialPathStat() {
				renameSync(path, `${path}.original`);
				if (replacement === "symlink") symlinkSync(`${path}.original`, path); else mkdirSync(path);
			} } }), /not one regular non-symlink file/);
		} finally { rmSync(fixture, { recursive: true, force: true }); }
	});
	test(`bounded ${reader.name} classifies only native initial and open absence`, async () => {
		const fixture = mkdtempSync(join(tmpdir(), "pylon-bounded-native-"));
		const path = join(fixture, "input");
		try {
			assert.equal(await reader.read(path, { maxBytes: 1024 }), null);
			writeFileSync(path, "input");
			assert.equal(await reader.read(path, {
				maxBytes: 1024,
				lstatEntry: reader.name === "async" ? async (target) => {
					const stat = await lstat(target);
					rmSync(target);
					return stat;
				} : (target) => {
					const stat = lstatSync(target);
					rmSync(target);
					return stat;
				},
			}), null);
		} finally {
			rmSync(fixture, { recursive: true, force: true });
		}
	});
}

test("protected publication reader and consumer fixtures retain exact commit provenance", () => {
	const root = new URL("./fixtures/protected-publication-v2/", import.meta.url);
	const provenance = JSON.parse(readFileSync(new URL("provenance.json", root), "utf8"));
	assert.equal(provenance.commit, "68603ed89bb597cd715fd6a77bc1c39d7e110298");
	assert.equal(provenance.repository, "pylon-code/prime-agent");
	assert.deepEqual(provenance.files.map((file) => file.path), ["pylon-consumer-lock.mjs", "pylon-bounded-file.mjs"]);
	for (const file of provenance.files) {
		assert.equal(file.sourcePath, `scripts/lib/${file.path}`);
		assert.equal(createHash("sha256").update(readFileSync(new URL(file.path, root))).digest("hex"), file.sha256);
	}
});
