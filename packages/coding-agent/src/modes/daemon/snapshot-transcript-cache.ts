import { createHash, randomUUID } from "node:crypto";
import {
	chmodSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { appendFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import { getProcessStartId } from "../../core/session-lease.js";
import { isProcessAlive } from "../../utils/child-process.js";

export const SNAPSHOT_TARGET_CHUNK_BYTES = 512 * 1024;
export const SNAPSHOT_MEMORY_CACHE_BYTES = 4 * 1024 * 1024;
const SNAPSHOT_CACHE_UID = typeof process.getuid === "function" ? process.getuid() : "user";
const SNAPSHOT_TRANSFER_CACHE_PARENT = join(tmpdir(), `prime-agent-snapshot-transfers-${SNAPSHOT_CACHE_UID}`);
const SNAPSHOT_CACHE_OWNER_PREFIX = "owner-";
const SNAPSHOT_PREPARATION_YIELD_BYTES = 256 * 1024;
const SNAPSHOT_STRING_SLICE_CHARS = 64 * 1024;
let defaultSnapshotTransferCacheRoot: string | undefined;
let defaultSnapshotTransferCacheCleanupRegistered = false;

export interface SnapshotTranscriptCacheIo {
	mkdir(path: string, options: { recursive: boolean; mode: number }): Promise<unknown>;
	writeFile(path: string, data: Uint8Array, options: { mode: number; flag: "wx" }): Promise<unknown>;
	appendFile(path: string, data: Uint8Array): Promise<unknown>;
	readFile(path: string): Promise<Buffer>;
	rm(path: string, options: { recursive: boolean; force: boolean }): Promise<unknown>;
}

const defaultSnapshotTranscriptCacheIo: SnapshotTranscriptCacheIo = {
	mkdir,
	writeFile,
	appendFile,
	readFile,
	rm,
};

function processStartHash(processStartId: string | undefined): string {
	return createHash("sha256")
		.update(processStartId ?? "unknown")
		.digest("hex")
		.slice(0, 16);
}

function assertSafeSnapshotCacheParent(parentRoot: string): void {
	let metadata: ReturnType<typeof lstatSync>;
	try {
		metadata = lstatSync(parentRoot);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		try {
			mkdirSync(parentRoot, { recursive: false, mode: 0o700 });
		} catch (mkdirError) {
			if ((mkdirError as NodeJS.ErrnoException).code !== "EEXIST") throw mkdirError;
		}
		metadata = lstatSync(parentRoot);
	}
	if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
		throw new Error(`Snapshot cache parent is not a private directory: ${parentRoot}`);
	}
	if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
		throw new Error(`Snapshot cache parent is not owned by the current user: ${parentRoot}`);
	}
	if ((metadata.mode & 0o077) !== 0) {
		chmodSync(parentRoot, 0o700);
		const secured = lstatSync(parentRoot);
		if (!secured.isDirectory() || secured.isSymbolicLink() || (secured.mode & 0o077) !== 0) {
			throw new Error(`Snapshot cache parent permissions are unsafe: ${parentRoot}`);
		}
	}
}

/** Remove only process roots whose recorded pid/start identity can no longer own them. */
export function sweepAbandonedSnapshotCacheRoots(parentRoot: string): void {
	assertSafeSnapshotCacheParent(parentRoot);
	for (const entry of readdirSync(parentRoot, { withFileTypes: true })) {
		if (!entry.isDirectory() || !entry.name.startsWith(SNAPSHOT_CACHE_OWNER_PREFIX)) continue;
		const match = /^owner-(\d+)-([a-f0-9]{16})-[a-zA-Z0-9_-]+$/.exec(entry.name);
		if (!match) continue;
		const ownerPid = Number(match[1]);
		if (isProcessAlive(ownerPid)) {
			const currentStartId = getProcessStartId(ownerPid);
			if (currentStartId === undefined || processStartHash(currentStartId) === match[2]) continue;
		}
		try {
			rmSync(join(parentRoot, entry.name), { recursive: true, force: true });
		} catch {
			// One unreadable abandoned root must not block independent owners.
		}
	}
}

export function createSnapshotCacheProcessRoot(parentRoot: string, ownerToken = randomUUID()): string {
	sweepAbandonedSnapshotCacheRoots(parentRoot);
	const prefix = join(
		parentRoot,
		`${SNAPSHOT_CACHE_OWNER_PREFIX}${process.pid}-${processStartHash(getProcessStartId(process.pid))}-${ownerToken}-`,
	);
	const root = mkdtempSync(prefix);
	const metadata = lstatSync(root);
	if (
		!metadata.isDirectory() ||
		metadata.isSymbolicLink() ||
		(typeof process.getuid === "function" && metadata.uid !== process.getuid())
	) {
		try {
			rmSync(root, { recursive: true, force: true });
		} catch {
			// Preserve the ownership failure.
		}
		throw new Error(`Snapshot cache process root is unsafe: ${root}`);
	}
	if ((metadata.mode & 0o077) !== 0) chmodSync(root, 0o700);
	return root;
}

function getDefaultSnapshotTransferCacheRoot(): string {
	if (!defaultSnapshotTransferCacheRoot) {
		defaultSnapshotTransferCacheRoot = createSnapshotCacheProcessRoot(SNAPSHOT_TRANSFER_CACHE_PARENT);
	}
	if (!defaultSnapshotTransferCacheCleanupRegistered) {
		defaultSnapshotTransferCacheCleanupRegistered = true;
		process.once("exit", () => {
			const root = defaultSnapshotTransferCacheRoot;
			defaultSnapshotTransferCacheRoot = undefined;
			if (!root) return;
			try {
				rmSync(root, { recursive: true, force: true });
			} catch {
				// Startup sweep owns crash and permission cleanup on the next process.
			}
		});
	}
	return defaultSnapshotTransferCacheRoot;
}

interface SnapshotTranscriptChunk {
	buffer?: Buffer;
	path?: string;
}

interface SnapshotPayloadChunk extends SnapshotTranscriptChunk {
	messageCount: number;
	readPromise?: Promise<Buffer>;
}

export interface SnapshotTranscriptCacheOptions {
	activeSessionId: string;
	snapshotId: string;
	messages?: readonly AgentMessage[];
	cacheRoot: string;
	targetChunkBytes?: number;
	memoryCacheBytes?: number;
	io?: SnapshotTranscriptCacheIo;
}

interface SnapshotTranscriptChunkLifecycle {
	markFailed?(error: Error): void;
	dispose?(): void;
}

export type SnapshotTranscriptWireChunk = Buffer | (readonly Buffer[] & { readonly snapshotMessageCount?: number });
export type SnapshotTranscriptChunkSource = (
	| Iterable<SnapshotTranscriptWireChunk>
	| AsyncIterable<SnapshotTranscriptWireChunk>
) &
	SnapshotTranscriptChunkLifecycle;

type SnapshotTranscriptChunkIterable = Iterable<Buffer> & SnapshotTranscriptChunkLifecycle;

function cloneSnapshotValue<T>(value: T, seen = new Map<object, unknown>()): T {
	if (value === null || typeof value !== "object") return value;
	const objectValue = value as object;
	const existing = seen.get(objectValue);
	if (existing !== undefined) return existing as T;
	if (Buffer.isBuffer(value)) return Buffer.from(value) as T;
	if (ArrayBuffer.isView(value)) return structuredClone(value);
	if (value instanceof Date) return new Date(value.getTime()) as T;
	if (Array.isArray(value)) {
		const cloned: unknown[] = [];
		seen.set(objectValue, cloned);
		for (const item of value) cloned.push(cloneSnapshotValue(item, seen));
		return cloned as T;
	}
	const cloned: Record<string, unknown> = {};
	seen.set(objectValue, cloned);
	for (const key of Object.keys(value)) {
		cloned[key] = cloneSnapshotValue((value as Record<string, unknown>)[key], seen);
	}
	return cloned as T;
}

/** Select a coherent transcript view without copying immutable string payload bytes. */
export function createImmutableSnapshotMessages(messages: readonly AgentMessage[]): AgentMessage[] {
	return cloneSnapshotValue([...messages]);
}

function assertSnapshotSourceUnchanged(source: readonly AgentMessage[], selected: readonly AgentMessage[]): void {
	if (!isDeepStrictEqual(source, selected)) {
		throw new Error("Session snapshot generation changed during transcript preparation");
	}
}

interface JsonParts {
	parts: Buffer[];
	bytes: number;
}

interface IncrementalJsonYieldState {
	bytesSinceYield: number;
}

class IncrementalJsonEncoder {
	private readonly parts: Buffer[] = [];
	private bytes = 0;
	private readonly ancestors = new Set<object>();

	constructor(
		private readonly signal: AbortSignal | undefined,
		private readonly validateGeneration: () => void,
		private readonly yieldState: IncrementalJsonYieldState,
	) {}

	async encode(value: unknown): Promise<JsonParts> {
		const encoded = await this.encodeValue(value, "", false);
		if (!encoded) throw new Error("Snapshot message is not JSON serializable");
		await this.yieldIfNeeded();
		return { parts: this.parts, bytes: this.bytes };
	}

	private emit(value: string | Buffer): void {
		const buffer = typeof value === "string" ? Buffer.from(value) : value;
		if (buffer.length === 0) return;
		this.parts.push(buffer);
		this.bytes += buffer.length;
		this.yieldState.bytesSinceYield += buffer.length;
	}

	private async yieldIfNeeded(): Promise<void> {
		if (this.yieldState.bytesSinceYield < SNAPSHOT_PREPARATION_YIELD_BYTES) return;
		this.yieldState.bytesSinceYield = 0;
		await new Promise<void>((resolveYield) => setImmediate(resolveYield));
		this.signal?.throwIfAborted();
		this.validateGeneration();
	}

	private resolveJsonValue(value: unknown, key: string): unknown {
		if (value && typeof value === "object" && "toJSON" in value) {
			const toJSON = (value as { toJSON?: unknown }).toJSON;
			if (typeof toJSON === "function") return toJSON.call(value, key);
		}
		return value;
	}

	private async encodeString(value: string): Promise<void> {
		this.emit('"');
		for (let start = 0; start < value.length; ) {
			let end = Math.min(value.length, start + SNAPSHOT_STRING_SLICE_CHARS);
			if (
				end < value.length &&
				value.charCodeAt(end - 1) >= 0xd800 &&
				value.charCodeAt(end - 1) <= 0xdbff &&
				value.charCodeAt(end) >= 0xdc00 &&
				value.charCodeAt(end) <= 0xdfff
			) {
				end++;
			}
			const quoted = JSON.stringify(value.slice(start, end));
			this.emit(quoted.slice(1, -1));
			start = end;
			await this.yieldIfNeeded();
		}
		this.emit('"');
	}

	private async encodeValue(rawValue: unknown, key: string, arrayElement: boolean): Promise<boolean> {
		const value = this.resolveJsonValue(rawValue, key);
		if (value === null) {
			this.emit("null");
			return true;
		}
		switch (typeof value) {
			case "string":
				await this.encodeString(value);
				return true;
			case "number":
				this.emit(Number.isFinite(value) ? String(value === 0 ? 0 : value) : "null");
				return true;
			case "boolean":
				this.emit(value ? "true" : "false");
				return true;
			case "bigint":
				throw new TypeError("Do not know how to serialize a BigInt");
			case "undefined":
			case "function":
			case "symbol":
				if (arrayElement) {
					this.emit("null");
					return true;
				}
				return false;
			case "object":
				break;
		}
		const objectValue = value as object;
		if (this.ancestors.has(objectValue)) {
			throw new TypeError("Converting circular structure to JSON");
		}
		this.ancestors.add(objectValue);
		try {
			if (Array.isArray(value)) {
				this.emit("[");
				for (let index = 0; index < value.length; index++) {
					if (index > 0) this.emit(",");
					await this.encodeValue(value[index], String(index), true);
				}
				this.emit("]");
				return true;
			}
			this.emit("{");
			let emitted = 0;
			for (const property of Object.keys(value)) {
				const propertyValue = this.resolveJsonValue((value as Record<string, unknown>)[property], property);
				if (["undefined", "function", "symbol"].includes(typeof propertyValue)) continue;
				if (emitted++ > 0) this.emit(",");
				await this.encodeString(property);
				this.emit(":");
				await this.encodeValue(propertyValue, property, false);
			}
			this.emit("}");
			return true;
		} finally {
			this.ancestors.delete(objectValue);
		}
	}
}

export interface SnapshotTranscriptPayloadOptions {
	messages: readonly AgentMessage[];
	cacheRoot?: string;
	targetChunkBytes?: number;
	memoryCacheBytes?: number;
	signal?: AbortSignal;
	validateGeneration?: () => void;
	io?: SnapshotTranscriptCacheIo;
}

export class SnapshotTranscriptPayloadCache {
	private readonly chunks: SnapshotPayloadChunk[] = [];
	private cacheDirectory?: string;
	private totalBytes = 0;
	private messageTotal = 0;
	private readers = 0;
	private disposeRequested = false;
	private disposed = false;
	private cleanupPromise?: Promise<void>;
	readonly targetChunkBytes: number;

	constructor(
		private readonly options: {
			cacheRoot: string;
			targetChunkBytes?: number;
			memoryCacheBytes?: number;
			io?: SnapshotTranscriptCacheIo;
		},
	) {
		this.targetChunkBytes = options.targetChunkBytes ?? SNAPSHOT_TARGET_CHUNK_BYTES;
	}

	get chunkCount(): number {
		return this.chunks.length;
	}

	get messageCount(): number {
		return this.messageTotal;
	}

	get bytes(): number {
		return this.totalBytes;
	}

	get fileBacked(): boolean {
		return this.cacheDirectory !== undefined;
	}

	get activeReaders(): number {
		return this.readers;
	}

	get retainedPayloadBytes(): number {
		return this.chunks.reduce((total, chunk) => total + (chunk.buffer?.length ?? 0), 0);
	}

	private get io(): SnapshotTranscriptCacheIo {
		return this.options.io ?? defaultSnapshotTranscriptCacheIo;
	}

	async prepare(
		sourceMessages: readonly AgentMessage[],
		selectedMessages: readonly AgentMessage[],
		signal?: AbortSignal,
		validateGeneration?: () => void,
	): Promise<void> {
		const validatePreparation = () => {
			signal?.throwIfAborted();
			validateGeneration?.();
		};
		validatePreparation();
		const yieldState: IncrementalJsonYieldState = { bytesSinceYield: 0 };
		let currentParts: Buffer[] = [];
		let currentBytes = 0;
		let currentMessages = 0;
		const flush = async () => {
			if (currentMessages === 0) return;
			await this.storePayloadChunk(currentParts, currentBytes, currentMessages);
			currentParts = [];
			currentBytes = 0;
			currentMessages = 0;
		};
		for (const message of selectedMessages) {
			validatePreparation();
			const encoded = await new IncrementalJsonEncoder(signal, validatePreparation, yieldState).encode(message);
			const separatorBytes = currentMessages > 0 ? 1 : 0;
			if (currentMessages > 0 && currentBytes + separatorBytes + encoded.bytes > this.targetChunkBytes) {
				await flush();
			}
			if (currentMessages > 0) {
				currentParts.push(Buffer.from(","));
				currentBytes++;
			}
			currentParts.push(...encoded.parts);
			currentBytes += encoded.bytes;
			currentMessages++;
			this.messageTotal++;
			validatePreparation();
		}
		await flush();
		validatePreparation();
		assertSnapshotSourceUnchanged(sourceMessages, selectedMessages);
	}

	async readPayloadChunk(index: number): Promise<Buffer> {
		if (this.disposed) throw new Error(`Snapshot payload was disposed`);
		const chunk = this.chunks[index];
		if (!chunk) throw new Error(`Unknown snapshot transcript chunk: ${index}`);
		if (chunk.buffer) return chunk.buffer;
		if (!chunk.path) throw new Error(`Snapshot transcript chunk ${index} has no backing storage`);
		chunk.readPromise ??= this.io.readFile(chunk.path);
		const reading = chunk.readPromise;
		try {
			return await reading;
		} finally {
			if (chunk.readPromise === reading) chunk.readPromise = undefined;
		}
	}

	readPayloadChunkSync(index: number): Buffer {
		if (this.disposed) throw new Error(`Snapshot payload was disposed`);
		const chunk = this.chunks[index];
		if (!chunk) throw new Error(`Unknown snapshot transcript chunk: ${index}`);
		if (chunk.buffer) return chunk.buffer;
		if (!chunk.path) throw new Error(`Snapshot transcript chunk ${index} has no backing storage`);
		return readFileSync(chunk.path);
	}

	chunkMessageCount(index: number): number {
		const chunk = this.chunks[index];
		if (!chunk) throw new Error(`Unknown snapshot transcript chunk: ${index}`);
		return chunk.messageCount;
	}

	async transferChunk(activeSessionId: string, snapshotId: string, index: number): Promise<Buffer> {
		return this.envelopeChunk(activeSessionId, snapshotId, index, await this.readPayloadChunk(index));
	}

	transferChunkSync(activeSessionId: string, snapshotId: string, index: number): Buffer {
		return this.envelopeChunk(activeSessionId, snapshotId, index, this.readPayloadChunkSync(index));
	}

	createTransfer(activeSessionId: string, snapshotId: string): SnapshotTranscriptChunkSource {
		const release = this.retain();
		return {
			[Symbol.asyncIterator]: async function* (
				this: SnapshotTranscriptPayloadCache,
			): AsyncGenerator<SnapshotTranscriptWireChunk> {
				try {
					for (let index = 0; index < this.chunkCount; index++) {
						const payload = await this.readPayloadChunk(index);
						const prefix =
							`{"type":"session_snapshot_chunk","activeSessionId":${JSON.stringify(activeSessionId)},` +
							`"snapshotId":${JSON.stringify(snapshotId)},"index":${index},"messages":[`;
						const parts = [Buffer.from(prefix), payload, Buffer.from("]}\n")] as Buffer[] & {
							snapshotMessageCount: number;
						};
						parts.snapshotMessageCount = this.chunkMessageCount(index);
						yield parts;
					}
				} finally {
					release();
				}
			}.bind(this),
			dispose: release,
		};
	}

	retain(): () => void {
		if (this.disposed || this.disposeRequested) throw new Error("Snapshot payload was disposed");
		this.readers++;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.readers--;
			if (this.readers === 0) {
				if (this.disposeRequested) this.dispose();
				else this.releaseLoadedFileBuffers();
			}
		};
	}

	dispose(): void {
		this.disposeRequested = true;
		if (this.readers !== 0) return;
		if (!this.options.io) {
			const directory = this.cacheDirectory;
			try {
				if (directory) rmSync(directory, { recursive: true, force: true });
			} catch {
				// Disposal is best effort.
			} finally {
				this.cacheDirectory = undefined;
				this.chunks.length = 0;
				this.disposed = true;
			}
			return;
		}
		void this.cleanup().catch(() => undefined);
	}

	async disposeAsync(): Promise<void> {
		this.disposeRequested = true;
		if (this.readers === 0) await this.cleanup();
	}

	private envelopeChunk(activeSessionId: string, snapshotId: string, index: number, payload: Buffer): Buffer {
		const prefix =
			`{"type":"session_snapshot_chunk","activeSessionId":${JSON.stringify(activeSessionId)},` +
			`"snapshotId":${JSON.stringify(snapshotId)},"index":${index},"messages":[`;
		return Buffer.concat([Buffer.from(prefix), payload, Buffer.from("]}\n")]);
	}

	private async storePayloadChunk(parts: Buffer[], bytes: number, messageCount: number): Promise<void> {
		if (this.disposed || this.disposeRequested) throw new Error("Snapshot payload is not writable");
		this.totalBytes += bytes;
		const memoryLimit = this.options.memoryCacheBytes ?? SNAPSHOT_MEMORY_CACHE_BYTES;
		if (!this.cacheDirectory && this.totalBytes > memoryLimit) {
			this.cacheDirectory = join(this.options.cacheRoot, `payload-${randomUUID()}`);
			await this.io.mkdir(this.cacheDirectory, { recursive: false, mode: 0o700 });
			for (let index = 0; index < this.chunks.length; index++) {
				const existing = this.chunks[index]!;
				if (!existing.buffer) continue;
				const path = join(this.cacheDirectory, `${index}.json`);
				await this.io.writeFile(path, existing.buffer, { mode: 0o600, flag: "wx" });
				this.chunks[index] = { path, messageCount: existing.messageCount };
			}
		}
		if (this.cacheDirectory) {
			const path = join(this.cacheDirectory, `${this.chunks.length}.json`);
			const [first = Buffer.alloc(0), ...rest] = parts;
			await this.io.writeFile(path, first, { mode: 0o600, flag: "wx" });
			for (const part of rest) await this.io.appendFile(path, part);
			this.chunks.push({ path, messageCount });
		} else {
			this.chunks.push({ buffer: Buffer.concat(parts, bytes), messageCount });
		}
	}

	private releaseLoadedFileBuffers(): void {
		for (const chunk of this.chunks) {
			if (!chunk.path) continue;
			chunk.buffer = undefined;
			chunk.readPromise = undefined;
		}
	}

	private cleanup(): Promise<void> {
		if (this.cleanupPromise) return this.cleanupPromise;
		this.cleanupPromise = (async () => {
			const directory = this.cacheDirectory;
			try {
				if (directory) await this.io.rm(directory, { recursive: true, force: true });
			} finally {
				this.cacheDirectory = undefined;
				this.chunks.length = 0;
				this.disposed = true;
			}
		})();
		return this.cleanupPromise;
	}
}

export async function prepareSnapshotTranscriptPayload(
	options: SnapshotTranscriptPayloadOptions,
): Promise<SnapshotTranscriptPayloadCache> {
	options.signal?.throwIfAborted();
	const selected = createImmutableSnapshotMessages(options.messages);
	const payload = new SnapshotTranscriptPayloadCache({
		cacheRoot: options.cacheRoot ?? getDefaultSnapshotTransferCacheRoot(),
		...(options.targetChunkBytes !== undefined ? { targetChunkBytes: options.targetChunkBytes } : {}),
		...(options.memoryCacheBytes !== undefined ? { memoryCacheBytes: options.memoryCacheBytes } : {}),
		...(options.io ? { io: options.io } : {}),
	});
	try {
		await payload.prepare(options.messages, selected, options.signal, options.validateGeneration);
		return payload;
	} catch (error) {
		try {
			await payload.disposeAsync();
		} catch {
			// Preserve the encoding or write failure that caused preparation to fail.
		}
		throw error;
	}
}

export function createSnapshotTranscriptChunks(options: {
	activeSessionId: string;
	snapshotId: string;
	messages: readonly AgentMessage[];
	cacheRoot?: string;
	targetChunkBytes?: number;
	memoryCacheBytes?: number;
	signal?: AbortSignal;
}): SnapshotTranscriptChunkIterable {
	options.signal?.throwIfAborted();
	const cache = new SnapshotTranscriptCache({
		activeSessionId: options.activeSessionId,
		snapshotId: options.snapshotId,
		messages: options.messages,
		cacheRoot: options.cacheRoot ?? getDefaultSnapshotTransferCacheRoot(),
		...(options.targetChunkBytes !== undefined ? { targetChunkBytes: options.targetChunkBytes } : {}),
		...(options.memoryCacheBytes !== undefined ? { memoryCacheBytes: options.memoryCacheBytes } : {}),
	});
	return {
		*[Symbol.iterator](): Iterator<Buffer> {
			options.signal?.throwIfAborted();
			for (const chunk of cache) {
				options.signal?.throwIfAborted();
				yield chunk;
			}
			options.signal?.throwIfAborted();
		},
		markFailed(error) {
			cache.markFailed(error);
		},
		dispose() {
			cache.dispose();
		},
	};
}

export async function prepareSnapshotTranscriptCache(options: {
	activeSessionId: string;
	snapshotId: string;
	messages: readonly AgentMessage[];
	cacheRoot?: string;
	targetChunkBytes?: number;
	memoryCacheBytes?: number;
	signal?: AbortSignal;
	validateGeneration?: () => void;
	io?: SnapshotTranscriptCacheIo;
}): Promise<SnapshotTranscriptCache> {
	const payload = await prepareSnapshotTranscriptPayload(options);
	return SnapshotTranscriptCache.fromPreparedPayload(options.activeSessionId, options.snapshotId, payload);
}

export class SnapshotTranscriptCache {
	private readonly chunks: SnapshotTranscriptChunk[] = [];
	private cacheDirectory?: string;
	private totalBytes = 0;
	private completed = false;
	private readers = 0;
	private disposeRequested = false;
	private disposed = false;
	private failure?: Error;
	private preparedPayload?: SnapshotTranscriptPayloadCache;
	private readonly chunkWaiters = new Map<
		number,
		Array<{ resolve: (buffer: Buffer | undefined) => void; reject: (error: Error) => void }>
	>();
	readonly targetChunkBytes: number;
	readonly snapshotId: string;
	readonly activeSessionId: string;

	static fromPreparedPayload(
		activeSessionId: string,
		snapshotId: string,
		payload: SnapshotTranscriptPayloadCache,
	): SnapshotTranscriptCache {
		const cache = new SnapshotTranscriptCache({
			activeSessionId,
			snapshotId,
			cacheRoot: "",
			targetChunkBytes: payload.targetChunkBytes,
		});
		cache.preparedPayload = payload;
		cache.totalBytes = payload.bytes;
		cache.completed = true;
		return cache;
	}

	constructor(private readonly options: SnapshotTranscriptCacheOptions) {
		this.targetChunkBytes = options.targetChunkBytes ?? SNAPSHOT_TARGET_CHUNK_BYTES;
		this.snapshotId = options.snapshotId;
		this.activeSessionId = options.activeSessionId;
		if (options.messages) {
			try {
				this.encodeMessages(options.messages);
				this.completed = true;
			} catch (error) {
				this.markFailed(error instanceof Error ? error : new Error(String(error)));
				this.disposeNow();
				throw error;
			}
		}
	}

	get chunkCount(): number {
		return this.preparedPayload?.chunkCount ?? this.chunks.length;
	}

	get complete(): boolean {
		return this.completed && !this.failure && !this.disposed;
	}

	get bytes(): number {
		return this.totalBytes;
	}

	get fileBacked(): boolean {
		return this.preparedPayload?.fileBacked ?? this.cacheDirectory !== undefined;
	}

	readChunk(index: number): Buffer {
		if (this.preparedPayload) {
			return this.preparedPayload.transferChunkSync(this.activeSessionId, this.snapshotId, index);
		}
		const chunk = this.chunks[index];
		if (!chunk) throw new Error(`Unknown snapshot transcript chunk: ${index}`);
		if (chunk.buffer) return chunk.buffer;
		if (!chunk.path) throw new Error(`Snapshot transcript chunk ${index} has no backing storage`);
		return readFileSync(chunk.path);
	}

	*[Symbol.iterator](): Iterator<Buffer> {
		for (let index = 0; index < this.chunkCount; index++) yield this.readChunk(index);
	}

	appendEncodedChunk(buffer: Buffer): void {
		if (this.completed || this.failure || this.disposed || this.preparedPayload) {
			throw new Error(`Snapshot transcript ${this.snapshotId} is not writable`);
		}
		this.storeChunk(buffer);
	}

	markComplete(): void {
		if (this.completed) return;
		if (this.failure || this.disposed) {
			throw new Error(`Snapshot transcript ${this.snapshotId} cannot be completed`);
		}
		this.completed = true;
		for (const [index, waiters] of this.chunkWaiters) {
			if (index < this.chunks.length) continue;
			for (const waiter of waiters) waiter.resolve(undefined);
			this.chunkWaiters.delete(index);
		}
	}

	markFailed(error: Error): void {
		if (this.failure) return;
		this.failure = error;
		for (const waiters of this.chunkWaiters.values()) {
			for (const waiter of waiters) waiter.reject(error);
		}
		this.chunkWaiters.clear();
	}

	waitForChunk(index: number): Promise<Buffer | undefined> {
		if (this.failure) return Promise.reject(this.failure);
		if (this.preparedPayload) {
			return index < this.preparedPayload.chunkCount
				? this.preparedPayload.transferChunk(this.activeSessionId, this.snapshotId, index)
				: Promise.resolve(undefined);
		}
		if (index < this.chunks.length) return Promise.resolve(this.readChunk(index));
		if (this.completed) return Promise.resolve(undefined);
		return new Promise((resolve, reject) => {
			const waiters = this.chunkWaiters.get(index) ?? [];
			waiters.push({ resolve, reject });
			this.chunkWaiters.set(index, waiters);
		});
	}

	async waitForTransferChunk(snapshotId: string, index: number): Promise<SnapshotTranscriptWireChunk | undefined> {
		const chunk = await this.waitForChunk(index);
		if (!chunk || snapshotId === this.snapshotId) return chunk;
		const messagesMarker = Buffer.from('"messages":[');
		const markerIndex = chunk.indexOf(messagesMarker);
		const payloadStart = markerIndex + messagesMarker.length;
		const hasNewline = chunk.at(-1) === 0x0a;
		const payloadEnd = chunk.length - (hasNewline ? 3 : 2);
		if (
			markerIndex < 0 ||
			payloadEnd < payloadStart ||
			chunk.subarray(payloadEnd, payloadEnd + 2).toString() !== "]}"
		) {
			throw new Error(`Snapshot transcript ${this.snapshotId} has an invalid chunk envelope`);
		}
		const prefix =
			`{"type":"session_snapshot_chunk","activeSessionId":${JSON.stringify(this.activeSessionId)},` +
			`"snapshotId":${JSON.stringify(snapshotId)},"index":${index},"messages":[`;
		return [Buffer.from(prefix), chunk.subarray(payloadStart, payloadEnd), Buffer.from("]}\n")];
	}

	retain(): () => void {
		if (this.disposed) throw new Error(`Snapshot transcript ${this.snapshotId} was disposed`);
		const releasePayload = this.preparedPayload?.retain();
		this.readers++;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			releasePayload?.();
			this.readers--;
			if (this.readers === 0 && this.disposeRequested) this.disposeNow();
		};
	}

	dispose(): void {
		if (this.disposed || this.disposeRequested) return;
		this.disposeRequested = true;
		this.preparedPayload?.dispose();
		if (this.readers === 0) this.disposeNow();
	}

	private disposeNow(): void {
		if (this.disposed) return;
		this.disposed = true;
		this.markFailed(new Error(`Snapshot transcript ${this.snapshotId} was disposed`));
		const directory = this.cacheDirectory;
		try {
			if (directory) rmSync(directory, { recursive: true, force: true });
		} catch {
			// Disposal is best effort; state is always cleared below.
		} finally {
			this.cacheDirectory = undefined;
			this.chunks.length = 0;
		}
	}

	private encodeMessages(messages: readonly AgentMessage[]): void {
		let serializedMessages: string[] = [];
		let serializedBytes = 0;
		const flush = () => {
			if (serializedMessages.length === 0) return;
			const index = this.chunks.length;
			const prefix =
				`{"type":"session_snapshot_chunk","activeSessionId":${JSON.stringify(this.options.activeSessionId)},` +
				`"snapshotId":${JSON.stringify(this.options.snapshotId)},"index":${index},"messages":[`;
			const line = Buffer.from(`${prefix}${serializedMessages.join(",")}]}\n`);
			this.storeChunk(line);
			serializedMessages = [];
			serializedBytes = 0;
		};
		for (const message of messages) {
			const serialized = JSON.stringify(message);
			const bytes = Buffer.byteLength(serialized) + (serializedMessages.length > 0 ? 1 : 0);
			if (serializedMessages.length > 0 && serializedBytes + bytes > this.targetChunkBytes) flush();
			serializedMessages.push(serialized);
			serializedBytes += bytes;
		}
		flush();
	}

	private storeChunk(buffer: Buffer): void {
		this.totalBytes += buffer.length;
		const memoryLimit = this.options.memoryCacheBytes ?? SNAPSHOT_MEMORY_CACHE_BYTES;
		if (!this.cacheDirectory && this.totalBytes > memoryLimit) {
			this.cacheDirectory = join(
				this.options.cacheRoot,
				`${this.options.snapshotId.replaceAll(/[^a-zA-Z0-9_-]/g, "_")}-${randomUUID()}`,
			);
			mkdirSync(this.cacheDirectory, { recursive: false, mode: 0o700 });
			for (let index = 0; index < this.chunks.length; index++) {
				const existing = this.chunks[index]!;
				if (!existing.buffer) continue;
				const path = join(this.cacheDirectory, `${index}.jsonl`);
				writeFileSync(path, existing.buffer, { mode: 0o600, flag: "wx" });
				this.chunks[index] = { path };
			}
		}
		if (this.cacheDirectory) {
			const path = join(this.cacheDirectory, `${this.chunks.length}.jsonl`);
			writeFileSync(path, buffer, { mode: 0o600, flag: "wx" });
			this.chunks.push({ path });
		} else {
			this.chunks.push({ buffer });
		}
		const index = this.chunks.length - 1;
		const waiters = this.chunkWaiters.get(index);
		if (waiters) {
			for (const waiter of waiters) waiter.resolve(buffer);
			this.chunkWaiters.delete(index);
		}
	}
}
