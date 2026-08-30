import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync } from "node:fs";
import { appendFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import { afterEach, describe, expect, it } from "vitest";
import {
	createSnapshotCacheProcessRoot,
	prepareSnapshotTranscriptCache,
	prepareSnapshotTranscriptPayload,
	SNAPSHOT_MEMORY_CACHE_BYTES,
	SnapshotTranscriptCache,
	type SnapshotTranscriptCacheIo,
	sweepAbandonedSnapshotCacheRoots,
} from "../src/modes/daemon/snapshot-transcript-cache.js";

const tempDirs: string[] = [];

afterEach(() => {
	for (const directory of tempDirs.splice(0)) {
		rmSync(directory, { recursive: true, force: true });
	}
});

function tempDir(): string {
	const directory = mkdtempSync(join(tmpdir(), "snapshot-cache-test-"));
	tempDirs.push(directory);
	return directory;
}

function messages(count: number, chars: number): AgentMessage[] {
	return Array.from({ length: count }, (_, index) => ({
		role: "user" as const,
		content: `${index}:${"x".repeat(chars)}`,
		timestamp: index,
	}));
}

describe("snapshot transcript cache", () => {
	it("encodes transcript chunks as directly forwardable JSONL", () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-a",
			snapshotId: "snapshot-a",
			messages: messages(5, 80),
			cacheRoot: tempDir(),
			targetChunkBytes: 200,
		});

		expect(cache.chunkCount).toBeGreaterThan(1);
		const decoded = Array.from({ length: cache.chunkCount }, (_, index) =>
			JSON.parse(cache.readChunk(index).toString("utf8")),
		);
		expect(decoded.flatMap((chunk) => chunk.messages)).toEqual(messages(5, 80));
		expect(decoded.map((chunk) => chunk.index)).toEqual(decoded.map((_, index) => index));
		cache.dispose();
	});

	it("moves caches above the memory threshold to files", () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-b",
			snapshotId: "snapshot-b",
			messages: messages(6, 100),
			cacheRoot: tempDir(),
			targetChunkBytes: 180,
			memoryCacheBytes: 300,
		});

		expect(cache.fileBacked).toBe(true);
		expect(cache.readChunk(0).toString("utf8")).toContain('"session_snapshot_chunk"');
		cache.dispose();
	});

	it("streams opaque worker chunks to waiting attachments", async () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-c",
			snapshotId: "snapshot-c",
			cacheRoot: tempDir(),
		});
		const firstChunk = cache.waitForChunk(0);
		const encoded = Buffer.from(
			'{"type":"session_snapshot_chunk","activeSessionId":"active-c","snapshotId":"snapshot-c","index":0,"messages":[]}\n',
		);
		cache.appendEncodedChunk(encoded);
		await expect(firstChunk).resolves.toEqual(encoded);

		const end = cache.waitForChunk(1);
		cache.markComplete();
		await expect(end).resolves.toBeUndefined();
		cache.dispose();
	});

	it("defers disposal until active snapshot readers finish", () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-d",
			snapshotId: "snapshot-d",
			messages: messages(3, 80),
			cacheRoot: tempDir(),
			targetChunkBytes: 150,
		});
		const release = cache.retain();
		const firstChunk = cache.readChunk(0);

		cache.dispose();

		expect(cache.readChunk(0)).toEqual(firstChunk);
		release();
		expect(() => cache.readChunk(0)).toThrow("Unknown snapshot transcript chunk");
	});

	it("fails pending readers before deferred disposal", async () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-e",
			snapshotId: "snapshot-e",
			cacheRoot: tempDir(),
		});
		const release = cache.retain();
		const pending = cache.waitForChunk(0);
		const failure = new Error("snapshot source failed");

		cache.markFailed(failure);
		cache.dispose();

		await expect(pending).rejects.toBe(failure);
		release();
		expect(cache.complete).toBe(false);
	});

	it("cleans a partial file-backed cache when message encoding throws", () => {
		const cacheRoot = tempDir();
		const circular: { role: "user"; content: unknown; timestamp: number } = {
			role: "user",
			content: undefined,
			timestamp: 2,
		};
		circular.content = circular;
		const invalidMessages = [
			{ role: "user", content: "x".repeat(5 * 1024 * 1024), timestamp: 1 },
			circular,
		] as unknown as AgentMessage[];

		expect(
			() =>
				new SnapshotTranscriptCache({
					activeSessionId: "active-partial",
					snapshotId: "snapshot-partial",
					messages: invalidMessages,
					cacheRoot,
					memoryCacheBytes: 1,
				}),
		).toThrow();
		expect(readdirSync(cacheRoot)).toEqual([]);
	});

	it("sweeps only abandoned process-owned cache roots", () => {
		const parent = tempDir();
		const liveRoot = createSnapshotCacheProcessRoot(parent);
		const liveName = liveRoot.slice(parent.length + 1);
		const staleName = liveName.replace(/^owner-\d+-/, "owner-987654321-");
		const staleRoot = join(parent, staleName);
		mkdirSync(staleRoot, { mode: 0o700 });

		sweepAbandonedSnapshotCacheRoots(parent);

		expect(existsSync(liveRoot)).toBe(true);
		expect(existsSync(staleRoot)).toBe(false);
	});

	it("does not allocate storage for an already-aborted preparation", async () => {
		const cacheRoot = tempDir();
		const controller = new AbortController();
		controller.abort();

		await expect(
			prepareSnapshotTranscriptCache({
				activeSessionId: "active-aborted",
				snapshotId: "snapshot-aborted",
				messages: messages(2, 5 * 1024 * 1024),
				cacheRoot,
				memoryCacheBytes: 1,
				signal: controller.signal,
			}),
		).rejects.toThrow();
		expect(readdirSync(cacheRoot)).toEqual([]);
	});

	it("isolates concurrent file-backed preparations that use the same transfer id", async () => {
		const cacheRoot = tempDir();
		const prepared = await Promise.all([
			prepareSnapshotTranscriptCache({
				activeSessionId: "active-concurrent",
				snapshotId: "snapshot-concurrent",
				messages: messages(3, 2 * 1024 * 1024),
				cacheRoot,
				memoryCacheBytes: 1,
			}),
			prepareSnapshotTranscriptCache({
				activeSessionId: "active-concurrent",
				snapshotId: "snapshot-concurrent",
				messages: messages(3, 2 * 1024 * 1024),
				cacheRoot,
				memoryCacheBytes: 1,
			}),
		]);

		expect(readdirSync(cacheRoot)).toHaveLength(2);
		expect(prepared.every((cache) => cache.fileBacked && cache.complete)).toBe(true);
		for (const cache of prepared) cache.dispose();
		expect(readdirSync(cacheRoot)).toEqual([]);
	});

	it("completes repeated endings idempotently", () => {
		const cache = new SnapshotTranscriptCache({
			activeSessionId: "active-f",
			snapshotId: "snapshot-f",
			cacheRoot: tempDir(),
		});
		cache.appendEncodedChunk(Buffer.from("chunk"));

		cache.markComplete();
		cache.markComplete();

		expect(cache.complete).toBe(true);
		expect(() => cache.appendEncodedChunk(Buffer.from("stale"))).toThrow("is not writable");
		cache.dispose();
	});

	it.each(["append", "nested mutation"] as const)(
		"rejects a live transcript %s across preparation yields",
		async (mutation) => {
			const cacheRoot = tempDir();
			const source = messages(1, 3 * 1024 * 1024);
			const preparing = prepareSnapshotTranscriptPayload({ messages: source, cacheRoot });
			setImmediate(() => {
				if (mutation === "append") source.push({ role: "user", content: "late", timestamp: 2 });
				else (source[0] as { content: unknown }).content = "changed in place";
			});

			await expect(preparing).rejects.toThrow("generation changed");
			expect(readdirSync(cacheRoot)).toEqual([]);
		},
	);

	it("cleans an asynchronously spilled partial payload after a later circular message", async () => {
		const cacheRoot = tempDir();
		const circular: Record<string, unknown> = { role: "user", timestamp: 3 };
		circular.content = circular;
		const source = [
			{ role: "user", content: "a".repeat(5 * 1024 * 1024), timestamp: 1 },
			{ role: "user", content: "b".repeat(5 * 1024 * 1024), timestamp: 2 },
			circular,
		] as unknown as AgentMessage[];

		await expect(
			prepareSnapshotTranscriptPayload({ messages: source, cacheRoot, memoryCacheBytes: 1 }),
		).rejects.toThrow("circular");
		expect(readdirSync(cacheRoot)).toEqual([]);
	});

	it("preserves async write failures when cleanup also fails and clears state after remove failures", async () => {
		const cacheRoot = tempDir();
		const writeFailure = new Error("injected snapshot write failure");
		const removeFailure = new Error("injected snapshot remove failure");
		let writeCount = 0;
		const failingIo: SnapshotTranscriptCacheIo = {
			mkdir,
			async writeFile(path, data, options) {
				if (++writeCount === 2) throw writeFailure;
				await writeFile(path, data, options);
			},
			appendFile,
			readFile,
			async rm() {
				throw removeFailure;
			},
		};
		await expect(
			prepareSnapshotTranscriptPayload({
				messages: messages(3, 2 * 1024 * 1024),
				cacheRoot,
				memoryCacheBytes: 1,
				io: failingIo,
			}),
		).rejects.toBe(writeFailure);

		const removable = await prepareSnapshotTranscriptPayload({
			messages: messages(2, 3 * 1024 * 1024),
			cacheRoot,
			memoryCacheBytes: 1,
			io: { ...failingIo, writeFile, rm: async () => Promise.reject(removeFailure) },
		});
		await expect(removable.disposeAsync()).rejects.toBe(removeFailure);
		expect(removable.fileBacked).toBe(false);
		expect(removable.retainedPayloadBytes).toBe(0);
	});

	it("surfaces injected async read failures without losing payload ownership", async () => {
		const cacheRoot = tempDir();
		const readFailure = new Error("injected snapshot read failure");
		const payload = await prepareSnapshotTranscriptPayload({
			messages: messages(2, 3 * 1024 * 1024),
			cacheRoot,
			memoryCacheBytes: 1,
			io: { mkdir, writeFile, appendFile, readFile: async () => Promise.reject(readFailure), rm },
		});
		const transfer = payload.createTransfer("active-read", "snapshot-read") as AsyncIterable<unknown>;
		await expect(transfer[Symbol.asyncIterator]().next()).rejects.toBe(readFailure);
		payload.dispose();
	});

	it("uses a private crash-recoverable process root and repairs parent permissions", () => {
		const parent = tempDir();
		chmodSync(parent, 0o777);
		const modulePath = join(process.cwd(), "src/modes/daemon/snapshot-transcript-cache.ts");
		const child = spawnSync(
			"npx",
			[
				"tsx",
				"--eval",
				`import { createSnapshotCacheProcessRoot } from ${JSON.stringify(modulePath)}; console.log(createSnapshotCacheProcessRoot(${JSON.stringify(parent)}));`,
			],
			{ cwd: process.cwd(), encoding: "utf8" },
		);
		expect(child.status, child.stderr).toBe(0);
		const crashedRoot = child.stdout.trim().split("\n").at(-1)!;
		expect(existsSync(crashedRoot)).toBe(true);
		sweepAbandonedSnapshotCacheRoots(parent);
		expect(existsSync(crashedRoot)).toBe(false);
		expect(lstatSync(parent).mode & 0o077).toBe(0);

		const symlinkParent = join(tempDir(), "cache-parent-link");
		symlinkSync(parent, symlinkParent);
		expect(() => createSnapshotCacheProcessRoot(symlinkParent)).toThrow("private directory");
	});

	it("bounds 36 MiB single and multi-message preparation and shares one payload across three purposes", async () => {
		const cacheRoot = tempDir();
		const run = async (source: AgentMessage[]) => {
			let maxEventLoopDelayMs = 0;
			let previousTick = performance.now();
			const monitor = setInterval(() => {
				const current = performance.now();
				maxEventLoopDelayMs = Math.max(maxEventLoopDelayMs, current - previousTick - 5);
				previousTick = current;
			}, 5);
			const rssBefore = process.memoryUsage().rss;
			const started = performance.now();
			const payload = await prepareSnapshotTranscriptPayload({ messages: source, cacheRoot });
			await new Promise<void>((resolve) => setTimeout(resolve, 10));
			clearInterval(monitor);
			return {
				payload,
				elapsedMs: performance.now() - started,
				maxEventLoopDelayMs,
				rssDeltaBytes: process.memoryUsage().rss - rssBefore,
			};
		};

		const single = await run(messages(1, 36 * 1024 * 1024));
		expect(single.maxEventLoopDelayMs).toBeLessThan(50);
		expect(single.rssDeltaBytes).toBeLessThan(160 * 1024 * 1024);
		expect(single.payload.fileBacked).toBe(true);
		const transfers = ["attach", "replacement", "resync"].map((purpose) =>
			single.payload.createTransfer("active-benchmark", `snapshot-${purpose}`),
		);
		expect(single.payload.activeReaders).toBe(3);
		const iterators = transfers.map((transfer) =>
			(transfer as AsyncIterable<readonly Buffer[]>)[Symbol.asyncIterator](),
		);
		const firstChunks = await Promise.all(iterators.map((iterator) => iterator.next()));
		const payloadBuffers = firstChunks.map((entry) => entry.value![1]);
		expect(payloadBuffers[1]).toBe(payloadBuffers[0]);
		expect(payloadBuffers[2]).toBe(payloadBuffers[0]);
		const consume = async (iterator: (typeof iterators)[number]) => {
			while (!(await iterator.next()).done) {
				// Deliberately drain one reader while its siblings remain at the first chunk.
			}
		};
		await consume(iterators[0]!);
		expect(single.payload.activeReaders).toBe(2);
		expect(single.payload.retainedPayloadBytes).toBeLessThanOrEqual(SNAPSHOT_MEMORY_CACHE_BYTES);
		await consume(iterators[1]!);
		expect(single.payload.activeReaders).toBe(1);
		expect(single.payload.retainedPayloadBytes).toBeLessThanOrEqual(SNAPSHOT_MEMORY_CACHE_BYTES);
		await consume(iterators[2]!);
		expect(single.payload.activeReaders).toBe(0);
		expect(single.payload.retainedPayloadBytes).toBeLessThanOrEqual(SNAPSHOT_MEMORY_CACHE_BYTES);
		single.payload.dispose();

		const multiple = await run(messages(36, 1024 * 1024));
		expect(multiple.maxEventLoopDelayMs).toBeLessThan(50);
		expect(multiple.rssDeltaBytes).toBeLessThan(192 * 1024 * 1024);
		expect(multiple.payload.bytes).toBeGreaterThanOrEqual(36 * 1024 * 1024);
		const skewed = ["fast", "middle", "slow"].map((purpose) =>
			multiple.payload.createTransfer("active-skewed", `snapshot-${purpose}`),
		);
		const skewedIterators = skewed.map((transfer) =>
			(transfer as AsyncIterable<readonly Buffer[]>)[Symbol.asyncIterator](),
		);
		await Promise.all(skewedIterators.map((iterator) => iterator.next()));
		for (const iterator of skewedIterators) {
			while (!(await iterator.next()).done) {
				// Each earlier reader fully drains while every later reader stays pinned at chunk zero.
			}
			expect(multiple.payload.retainedPayloadBytes).toBeLessThanOrEqual(SNAPSHOT_MEMORY_CACHE_BYTES);
		}
		expect(multiple.payload.activeReaders).toBe(0);
		multiple.payload.dispose();
	}, 30_000);
});
