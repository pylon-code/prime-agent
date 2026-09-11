import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { test } from "node:test";
import { extractRuntimeArchive, runtimeDependencyClosure, verifyRuntimeArchive } from "./lib/pylon-runtime-dependencies.mjs";

function registry(name, fields = {}) {
	return { version: "1.0.0", resolved: `https://registry.npmjs.org/${name}/-/package.tgz`, integrity: `sha512-${Buffer.alloc(64, 1).toString("base64")}`, ...fields };
}

function fixture() {
	return { packages: {
		"packages/coding-agent": { dependencies: { client: "1", "@earendil-works/pi-ai": "1" }, devDependencies: { testOnly: "1" } },
		"node_modules/@earendil-works/pi-ai": { link: true, resolved: "packages/ai" },
		"packages/ai": { dependencies: { shared: "1" } },
		"node_modules/client": registry("client", { dependencies: { shared: "2" }, peerDependencies: { peer: "1", absent: "1" }, peerDependenciesMeta: { absent: { optional: true } }, optionalDependencies: { linux: "1", macos: "1" } }),
		"node_modules/shared": registry("shared"),
		"node_modules/client/node_modules/shared": registry("shared", { version: "2.0.0" }),
		"node_modules/peer": registry("peer"),
		"node_modules/linux": registry("linux", { os: ["linux"] }),
		"node_modules/macos": registry("macos", { os: ["darwin"] }),
		"node_modules/testOnly": registry("testOnly"),
	} };
}

test("runtime closure retains exact nested, workspace, peer and cross-platform optional dependencies", () => {
	const closure = runtimeDependencyClosure(fixture());
	assert.deepEqual([...closure.keys()], [
		"node_modules/client", "node_modules/client/node_modules/shared", "node_modules/linux", "node_modules/macos",
		"node_modules/peer", "node_modules/shared", "packages/ai", "packages/coding-agent",
	]);
	assert.equal(closure.get("node_modules/client/node_modules/shared").version, "2.0.0");
});

test("runtime closure rejects missing required and peer dependencies or incomplete integrity", () => {
	for (const missing of ["node_modules/client", "node_modules/peer"]) {
		const lock = fixture();
		delete lock.packages[missing];
		assert.throws(() => runtimeDependencyClosure(lock), /Missing locked runtime dependency/);
	}
	const lock = fixture();
	delete lock.packages["node_modules/peer"].integrity;
	assert.throws(() => runtimeDependencyClosure(lock), /lacks locked registry integrity/);
	lock.packages["node_modules/client"] = { link: true, resolved: "../../foreign" };
	assert.throws(() => runtimeDependencyClosure(lock), /Unsupported runtime lock location/);
});

test("runtime inputs accept only bytes authenticated by the frozen lock", () => {
	const bytes = Buffer.from("locked archive");
	const entry = registry("fixture", { integrity: `sha512-${createHash("sha512").update(bytes).digest("base64")}` });
	assert.doesNotThrow(() => verifyRuntimeArchive(bytes, entry));
	assert.throws(() => verifyRuntimeArchive(Buffer.from("changed archive"), entry), /size\/integrity check/);
	assert.throws(() => verifyRuntimeArchive(Buffer.alloc(0), entry), /size\/integrity check/);
});

test("current runtime closure includes every locked clipboard platform and required schema peer", () => {
	const lock = JSON.parse(readFileSync(resolve(import.meta.dirname, "../package-lock.json"), "utf8"));
	const closure = runtimeDependencyClosure(lock);
	const clipboard = lock.packages["node_modules/@mariozechner/clipboard"];
	for (const name of Object.keys(clipboard.optionalDependencies)) assert.ok(closure.has(`node_modules/${name}`), name);
	assert.ok(closure.has("node_modules/zod"));
	assert.ok(closure.has("node_modules/undici"));
	assert.ok(closure.has("node_modules/koffi"));
	assert.ok(closure.has("node_modules/cli-highlight/node_modules/chalk"));
	assert.ok(!closure.has("node_modules/@biomejs/biome"));
});

function tarball(entries) {
	const blocks = [];
	for (const { name, value = "x", type = "0", mode = 0o644 } of entries) {
		const data = Buffer.from(type === "1" || type === "2" ? "" : value);
		const header = Buffer.alloc(512);
		header.write(name);
		for (const [offset, size, number] of [[100, 8, mode], [108, 8, 0], [116, 8, 0], [124, 12, data.length], [136, 12, 0]]) {
			header.write(`${number.toString(8).padStart(size - 1, "0")}\0`, offset);
		}
		header.fill(0x20, 148, 156);
		header.write(type, 156);
		if (type === "1" || type === "2") header.write("outside", 157);
		header.write("ustar\0", 257);
		header.write("00", 263);
		const checksum = header.reduce((sum, byte) => sum + byte, 0);
		header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148);
		blocks.push(header, data, Buffer.alloc((512 - data.length % 512) % 512));
	}
	return gzipSync(Buffer.concat([...blocks, Buffer.alloc(1024)]));
}

test("runtime extraction folds byte-identical normalized entries without running package scripts", async () => {
	const root = mkdtempSync(join(tmpdir(), "pylon-runtime-extract-"));
	try {
		const archive = join(root, "input.tgz");
		writeFileSync(archive, tarball([
			{ name: "legacy-root/package.json", value: JSON.stringify({ scripts: { postinstall: "exit 91" } }) },
			{ name: "legacy-root/./file.js", value: "export {};" },
			{ name: "legacy-root/file.js", value: "export {};" },
		]));
		await extractRuntimeArchive(archive, join(root, "output"));
		assert.equal(readFileSync(join(root, "output/file.js"), "utf8"), "export {};");
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});

test("runtime extraction rejects traversal, links, privileges and conflicting normalized duplicates", async () => {
	const root = mkdtempSync(join(tmpdir(), "pylon-runtime-reject-"));
	try {
		const archive = join(root, "input.tgz");
		for (const entries of [
			[{ name: "package/../escape" }], [{ name: "/package/escape" }],
			[{ name: "package/link", type: "2" }], [{ name: "package/link", type: "1" }],
			[{ name: "package/file", mode: 0o4755 }],
			[{ name: "package/file", value: "first" }, { name: "package/./file", value: "second" }],
			[{ name: "package/file" }, { name: "foreign/file" }],
		]) {
			writeFileSync(archive, tarball(entries));
			await assert.rejects(extractRuntimeArchive(archive, join(root, "output")), /Unsafe runtime|Conflicting duplicate/);
		}
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});
