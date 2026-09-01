import type { Socket } from "node:net";
import { PassThrough } from "node:stream";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import { describe, expect, it, vi } from "vitest";
import type { ActiveSessionState, DaemonSocketClient } from "../../../src/modes/daemon/active-session-state.js";
import { AgentDaemon } from "../../../src/modes/daemon/daemon-mode.js";
import {
	DAEMON_PROTOCOL_INFO,
	type DaemonAttachResult,
	type DaemonCommand,
	type DaemonOutbound,
} from "../../../src/modes/daemon/daemon-protocol.js";
import type { SessionSummary } from "../../../src/modes/daemon/daemon-session-list.js";
import { DaemonSupervisor } from "../../../src/modes/daemon/daemon-supervisor.js";
import {
	type DaemonWorkerFrameHeader,
	isDaemonWorkerFrameHeader,
} from "../../../src/modes/daemon/daemon-worker-protocol.js";
import {
	SnapshotTranscriptCache,
	type SnapshotTranscriptChunkSource,
} from "../../../src/modes/daemon/snapshot-transcript-cache.js";
import { type PrivateFrame, PrivateFrameDecoder } from "../../../src/modes/session-worker/private-framing.js";

const activeSessionId = "active-4602";
const snapshotId = "snapshot-4602";

interface WorkerHarness {
	descriptor: { workerId: string; rootActiveSessionId: string; lifecycle: "ready" | "recovering"; pid: number };
	authorizedActiveSessionIds: Set<string>;
	client?: {
		close: ReturnType<typeof vi.fn>;
		request: ReturnType<typeof vi.fn>;
		matchesAuthenticatedIncarnation: (incarnation: string) => boolean;
	};
	workerIncarnation?: string;
	summaries: Map<string, SessionSummary>;
	snapshotCache: Map<string, DaemonAttachResult>;
	transcriptCaches: Map<string, SnapshotTranscriptCache>;
	snapshotGenerations: Map<
		string,
		Map<
			string,
			{
				transcript: SnapshotTranscriptCache;
				result: DaemonAttachResult;
				incoming: boolean;
				retired: boolean;
				validation?: { promise: Promise<void>; resolve: () => void; reject: (error: Error) => void };
			}
		>
	>;
	snapshotLoads: Map<string, Promise<DaemonAttachResult>>;
	intentionalStop: boolean;
	stopRevision: number;
}

function summary(): SessionSummary {
	return {
		id: activeSessionId,
		activeSessionId,
		lifecycle: "live",
		activity: "idle",
		isSessionActive: false,
		sessionId: "session-4602",
		cwd: "/tmp",
		isStreaming: false,
		isCompacting: false,
		attachedClients: 0,
		messageCount: 1,
		sessionActions: { queuedCount: 0, steering: [], followUps: [] },
	};
}

function streamedResult(messages: AgentMessage[], resultSnapshotId = snapshotId): DaemonAttachResult {
	return {
		protocol: DAEMON_PROTOCOL_INFO,
		activeSessionId,
		snapshot: {
			activeSessionId,
			summary: summary(),
			state: { activeSessionId, sessionId: "session-4602" } as DaemonAttachResult["snapshot"]["state"],
			messages,
			lastEventSequence: 1,
			lastEventCursor: { generation: "generation-4602", sequence: 1 },
		},
		replay: {
			status: "complete",
			toSequence: 1,
			toCursor: { generation: "generation-4602", sequence: 1 },
		},
		lastEventSequence: 1,
		lastEventCursor: { generation: "generation-4602", sequence: 1 },
		snapshotStream: { id: resultSnapshotId, messageCount: messages.length, targetChunkBytes: 512 * 1024 },
		client: { id: "worker", capabilities: ["chunked_snapshot"] },
	};
}

function frame(
	message: DaemonOutbound,
	snapshotPurpose?: "attach" | "replacement" | "catchup",
): PrivateFrame<DaemonWorkerFrameHeader> {
	return {
		header: {
			kind: "outbound",
			outboundType: message.type,
			...("activeSessionId" in message ? { activeSessionId: message.activeSessionId } : {}),
			...("snapshotId" in message && typeof message.snapshotId === "string"
				? { snapshotId: message.snapshotId }
				: {}),
			payloadEncoding: "jsonl",
			...(snapshotPurpose ? { snapshotPurpose } : {}),
		},
		payload: Buffer.from(JSON.stringify(message)),
	};
}

function workerHarness() {
	const close = vi.fn();
	const request = vi.fn(async (command: { type: string }) => {
		if (command.type === "shutdown") {
			return { success: true };
		}
		throw new Error("unexpected snapshot reload");
	});
	const worker: WorkerHarness = {
		descriptor: {
			workerId: "worker-4602",
			rootActiveSessionId: activeSessionId,
			lifecycle: "ready",
			pid: 987_654_321,
		},
		authorizedActiveSessionIds: new Set([activeSessionId]),
		client: { close, request, matchesAuthenticatedIncarnation: (incarnation) => incarnation === "incarnation-4602" },
		workerIncarnation: "incarnation-4602",
		summaries: new Map([[activeSessionId, summary()]]),
		snapshotCache: new Map(),
		transcriptCaches: new Map<string, SnapshotTranscriptCache>(),
		snapshotGenerations: new Map(),
		snapshotLoads: new Map(),
		intentionalStop: false,
		stopRevision: 0,
	};
	return { close, request, worker };
}

function registerWorker(supervisor: DaemonSupervisor, worker: WorkerHarness): void {
	(supervisor as unknown as { workers: Map<string, WorkerHarness> }).workers.set(worker.descriptor.workerId, worker);
}

function socketClient(id: string, socket: PassThrough): DaemonSocketClient {
	return {
		id,
		socket: socket as unknown as Socket,
		attachedActiveSessionIds: new Set([activeSessionId]),
		catchupActiveSessionIds: new Set<string>(),
		detachInput: () => {},
		supportsExtensionUi: false,
		capabilities: new Set(["chunked_snapshot"]),
	} as DaemonSocketClient;
}

function snapshotFrames(messages: AgentMessage[]) {
	const result = streamedResult([]);
	const { messages: _messages, ...snapshot } = result.snapshot;
	return {
		begin: {
			type: "session_snapshot_begin",
			activeSessionId,
			snapshotId,
			snapshot,
			messageCount: messages.length,
			targetChunkBytes: 512 * 1024,
		} satisfies DaemonOutbound,
		chunk: {
			type: "session_snapshot_chunk",
			activeSessionId,
			snapshotId,
			index: 0,
			messages,
		} satisfies DaemonOutbound,
		end: {
			type: "session_snapshot_end",
			activeSessionId,
			snapshotId,
			chunkCount: 1,
			lastEventSequence: 1,
			lastEventCursor: { generation: "generation-4602", sequence: 1 },
		} satisfies DaemonOutbound,
	};
}

describe("ENG-4602 snapshot transfer containment", () => {
	it("observes the deferred attach snapshot promise", async () => {
		const daemon = new AgentDaemon("/tmp/eng-4602-worker.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			createRuntime: async () => {
				throw new Error("unexpected runtime creation");
			},
		});
		const state = {
			activeSessionId,
			clients: new Set<DaemonSocketClient>(),
			eventGeneration: "generation-4602",
			lastEventSequence: 1,
			runtime: { metadata: { kind: "top-level", createdAt: 1 }, session: { sessionId: "session-4602" } },
		} as unknown as ActiveSessionState;
		const socket = new PassThrough();
		const client = {
			id: "supervisor",
			socket: socket as unknown as Socket,
			transport: "private-framed",
			attachedActiveSessionIds: new Set<string>(),
			detachInput: () => {},
			supportsExtensionUi: false,
			capabilities: new Set<string>(),
		} as DaemonSocketClient;
		const streamError = new Error("encoder failed after begin");
		const log = vi.fn();
		const streamWorkerSnapshot = vi.fn(async () => {
			throw streamError;
		});
		const internals = daemon as unknown as {
			sessions: Map<string, ActiveSessionState>;
			createAttachResult(): DaemonAttachResult;
			streamWorkerSnapshot: typeof streamWorkerSnapshot;
			log: typeof log;
			handleCommand(client: DaemonSocketClient, command: DaemonCommand): Promise<unknown>;
		};
		internals.sessions.set(activeSessionId, state);
		internals.createAttachResult = () => streamedResult([]);
		internals.streamWorkerSnapshot = streamWorkerSnapshot;
		internals.log = log;
		const unhandled = vi.fn();
		process.on("unhandledRejection", unhandled);
		try {
			await internals.handleCommand(client, {
				type: "attach",
				activeSessionId,
				capabilities: ["attach_snapshot", "event_sequence", "slim_attach", "chunked_snapshot"],
			});
			await new Promise<void>((resolve) => setImmediate(resolve));
			await new Promise<void>((resolve) => setImmediate(resolve));
		} finally {
			process.off("unhandledRejection", unhandled);
			socket.destroy();
		}

		expect(streamWorkerSnapshot).toHaveBeenCalledOnce();
		expect(log).toHaveBeenCalledWith(`could not stream attach snapshot: ${String(streamError)}`);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it("uses a fresh transfer identity when transcript bytes change at the same event cursor", async () => {
		const daemon = new AgentDaemon("/tmp/eng-4602-transfer-identity.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			createRuntime: async () => {
				throw new Error("unexpected runtime creation");
			},
		});
		const state = {
			activeSessionId,
			clients: new Set<DaemonSocketClient>(),
			pendingAttaches: 0,
			eventGeneration: "generation-4602",
			lastEventSequence: 1,
			runtime: { metadata: { kind: "top-level", createdAt: 1 }, session: { sessionId: "session-4602" } },
		} as unknown as ActiveSessionState;
		const socket = new PassThrough();
		const client = {
			id: "supervisor",
			socket: socket as unknown as Socket,
			transport: "private-framed",
			attachedActiveSessionIds: new Set<string>(),
			detachInput: () => {},
			supportsExtensionUi: false,
			capabilities: new Set<string>(),
		} as DaemonSocketClient;
		const firstMessage = { role: "user", content: "before", timestamp: 1 } as const;
		const secondMessage = { role: "user", content: "after", timestamp: 1 } as const;
		const createAttachResult = vi
			.fn<() => DaemonAttachResult>()
			.mockReturnValueOnce(streamedResult([firstMessage]))
			.mockReturnValueOnce(streamedResult([secondMessage]));
		const streamWorkerSnapshot = vi.fn(
			async (
				_client: DaemonSocketClient,
				_result: DaemonAttachResult,
				_transcript: SnapshotTranscriptChunkSource,
			) => {},
		);
		const internals = daemon as unknown as {
			sessions: Map<string, ActiveSessionState>;
			createAttachResult: typeof createAttachResult;
			streamWorkerSnapshot: typeof streamWorkerSnapshot;
			handleCommand(client: DaemonSocketClient, command: DaemonCommand): Promise<unknown>;
		};
		internals.sessions.set(activeSessionId, state);
		internals.createAttachResult = createAttachResult;
		internals.streamWorkerSnapshot = streamWorkerSnapshot;
		const attach = {
			type: "attach",
			activeSessionId,
			capabilities: ["attach_snapshot", "event_sequence", "slim_attach", "chunked_snapshot"],
		} as const;

		const firstResponse = (await internals.handleCommand(client, attach)) as {
			success: true;
			data: DaemonAttachResult;
		};
		const secondResponse = (await internals.handleCommand(client, attach)) as {
			success: true;
			data: DaemonAttachResult;
		};
		await new Promise<void>((resolve) => setImmediate(resolve));

		expect(firstResponse.data.lastEventCursor).toEqual(secondResponse.data.lastEventCursor);
		expect(firstResponse.data.snapshotStream?.id).not.toBe(secondResponse.data.snapshotStream?.id);
		expect(streamWorkerSnapshot).toHaveBeenCalledTimes(2);
		const transcriptContents = await Promise.all(
			streamWorkerSnapshot.mock.calls.map(async ([, , transcript]) => {
				const contents: unknown[] = [];
				for await (const chunk of transcript) {
					const wireChunk = Buffer.isBuffer(chunk) ? chunk : Buffer.concat(chunk);
					const decoded = JSON.parse(wireChunk.toString("utf8")) as Extract<
						DaemonOutbound,
						{ type: "session_snapshot_chunk" }
					>;
					contents.push(
						...decoded.messages.map((message) => (message.role === "user" ? message.content : undefined)),
					);
				}
				return contents;
			}),
		);
		expect(transcriptContents).toEqual([["before"], ["after"]]);
		socket.destroy();
	});

	it("fails one worker snapshot without dropping another session on the supervisor channel", async () => {
		const daemon = new AgentDaemon("/tmp/eng-4602-stream.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			createRuntime: async () => {
				throw new Error("unexpected runtime creation");
			},
		});
		const transcript = new SnapshotTranscriptCache({
			activeSessionId,
			snapshotId,
			messages: [{ role: "user", content: "message", timestamp: 1 }],
			cacheRoot: "/tmp",
		});
		const streamError = new Error("chunk read failed");
		transcript.readChunk = vi.fn(() => {
			throw streamError;
		});
		const markFailed = vi.spyOn(transcript, "markFailed");
		const dispose = vi.spyOn(transcript, "dispose");
		const socket = new PassThrough();
		socket.on("error", () => {});
		const written: Buffer[] = [];
		socket.on("data", (chunk: Buffer) => written.push(Buffer.from(chunk)));
		const client = {
			id: "supervisor",
			socket: socket as unknown as Socket,
			transport: "private-framed",
			attachedActiveSessionIds: new Set([activeSessionId, "active-4602-sibling"]),
			catchupActiveSessionIds: new Set<string>(),
			detachInput: () => {},
			supportsExtensionUi: false,
			capabilities: new Set(["chunked_snapshot"]),
		} as DaemonSocketClient;
		const internals = daemon as unknown as {
			streamWorkerSnapshot(
				client: DaemonSocketClient,
				result: DaemonAttachResult,
				transcript: SnapshotTranscriptCache,
			): Promise<void>;
		};

		await expect(internals.streamWorkerSnapshot(client, streamedResult([]), transcript)).rejects.toBe(streamError);
		const decoder = new PrivateFrameDecoder(isDaemonWorkerFrameHeader);
		const frames = decoder.push(Buffer.concat(written));
		expect(frames.map((entry) => (entry.header.kind === "outbound" ? entry.header.outboundType : undefined))).toEqual(
			["session_snapshot_begin", "session_snapshot_failed"],
		);
		expect(JSON.parse(frames[1]!.payload.toString("utf8"))).toMatchObject({
			type: "session_snapshot_failed",
			activeSessionId,
			snapshotId,
			error: streamError.message,
		});
		expect(socket.destroyed).toBe(false);
		expect(client.attachedActiveSessionIds).toContain("active-4602-sibling");
		expect(transcript.complete).toBe(false);
		expect(client.snapshotStreaming).toBe(false);
		expect(markFailed.mock.invocationCallOrder[0]).toBeLessThan(dispose.mock.invocationCallOrder[0]!);
		socket.destroy();
	});

	it.each([
		{
			name: "non-monotonic first chunk",
			corrupt: (frames: ReturnType<typeof snapshotFrames>) => ({ ...frames.chunk, index: 1 }),
		},
		{
			name: "wrong fresh chunk snapshot id",
			corrupt: (frames: ReturnType<typeof snapshotFrames>) => ({
				...frames.chunk,
				snapshotId: "payload-mismatch",
			}),
		},
	] as const)("rejects a $name without closing the worker channel", ({ corrupt }) => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-fresh-chunk.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-fresh-chunk-state",
		});
		const { close, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const internals = supervisor as unknown as {
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);
		internals.handleWorkerFrame(worker, frame(frames.begin));
		const corruptFrame = frame(corrupt(frames) as DaemonOutbound);
		if (corruptFrame.header.kind !== "outbound") throw new Error("expected outbound frame");
		corruptFrame.header.snapshotId = snapshotId;

		internals.handleWorkerFrame(worker, corruptFrame);

		expect(close).not.toHaveBeenCalled();
		expect(worker.client).toBeDefined();
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
	});

	it.each([
		{
			name: "chunk count",
			corrupt: (end: ReturnType<typeof snapshotFrames>["end"]) => ({ ...end, chunkCount: 2 }),
		},
		{
			name: "event sequence",
			corrupt: (end: ReturnType<typeof snapshotFrames>["end"]) => ({ ...end, lastEventSequence: 2 }),
		},
		{
			name: "event cursor",
			corrupt: (end: ReturnType<typeof snapshotFrames>["end"]) => ({
				...end,
				lastEventCursor: { generation: "other", sequence: 1 },
			}),
		},
	] as const)("validates a fresh generation's $name before publishing completion", ({ corrupt }) => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-fresh-end.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-fresh-end-state",
		});
		const { close, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const internals = supervisor as unknown as {
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);
		internals.handleWorkerFrame(worker, frame(frames.begin));
		internals.handleWorkerFrame(worker, frame(frames.chunk));
		const transcript = worker.transcriptCaches.get(activeSessionId);
		if (!transcript) throw new Error("missing staged transcript");

		internals.handleWorkerFrame(worker, frame(corrupt(frames.end) as DaemonOutbound));

		expect(transcript.complete).toBe(false);
		expect(close).not.toHaveBeenCalled();
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
	});

	it.each([
		{
			name: "session identity",
			corrupt: (begin: ReturnType<typeof snapshotFrames>["begin"]) => ({
				...begin,
				snapshot: {
					...begin.snapshot,
					state: { ...begin.snapshot.state, sessionId: "wrong-session" },
				},
			}),
		},
		{
			name: "event progress",
			corrupt: (begin: ReturnType<typeof snapshotFrames>["begin"]) => ({
				...begin,
				snapshot: {
					...begin.snapshot,
					lastEventCursor: { generation: "generation-4602", sequence: 2 },
				},
			}),
		},
	] as const)("rejects invalid snapshot begin $name and ignores its late frames", ({ corrupt }) => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-invalid-begin.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-invalid-begin-state",
		});
		const { close, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const internals = supervisor as unknown as {
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);

		internals.handleWorkerFrame(worker, frame(corrupt(frames.begin) as DaemonOutbound));
		internals.handleWorkerFrame(worker, frame(frames.chunk));
		internals.handleWorkerFrame(worker, frame(frames.end));

		expect(close).not.toHaveBeenCalled();
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(worker.snapshotGenerations.has(activeSessionId)).toBe(false);
	});

	it("uses routed chunk metadata without reparsing a large canonical payload", () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-routed-chunk.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
		});
		const { worker } = workerHarness();
		registerWorker(supervisor, worker);
		const messages: AgentMessage[] = [{ role: "user", content: "x".repeat(1024 * 1024), timestamp: 1 }];
		const frames = snapshotFrames(messages);
		const internals = supervisor as unknown as {
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		internals.handleWorkerFrame(worker, frame(frames.begin));
		const payload = Buffer.from(`${JSON.stringify(frames.chunk)}\n`);
		const parse = vi.spyOn(JSON, "parse");
		try {
			internals.handleWorkerFrame(worker, {
				header: {
					kind: "outbound",
					outboundType: "session_snapshot_chunk",
					activeSessionId,
					snapshotId,
					payloadEncoding: "jsonl",
					snapshotChunkIndex: 0,
					snapshotChunkMessageCount: 1,
				},
				payload,
			});
			expect(parse).not.toHaveBeenCalled();
		} finally {
			parse.mockRestore();
		}
		expect(worker.transcriptCaches.get(activeSessionId)?.chunkCount).toBe(1);
		internals.handleWorkerFrame(worker, frame(frames.end));
		worker.transcriptCaches.get(activeSessionId)?.dispose();
	});

	it("keeps a multi-session worker connected after a scoped snapshot failure frame", () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-failure.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-failure-state",
		});
		const { close, worker } = workerHarness();
		registerWorker(supervisor, worker);
		worker.summaries.set("active-4602-sibling", {
			...summary(),
			id: "active-4602-sibling",
			activeSessionId: "active-4602-sibling",
			sessionId: "session-4602-sibling",
		});
		const transcript = new SnapshotTranscriptCache({
			activeSessionId,
			snapshotId,
			cacheRoot: "/tmp",
		});
		worker.snapshotCache.set(activeSessionId, streamedResult([]));
		worker.transcriptCaches.set(activeSessionId, transcript);
		const internals = supervisor as unknown as {
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};

		internals.handleWorkerFrame(
			worker,
			frame({
				type: "session_snapshot_failed",
				activeSessionId,
				snapshotId,
				error: "snapshot encoder failed",
			}),
		);

		expect(close).not.toHaveBeenCalled();
		expect(worker.descriptor.lifecycle).toBe("ready");
		expect(worker.summaries.has("active-4602-sibling")).toBe(true);
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(worker.snapshotGenerations.has(activeSessionId)).toBe(false);
	});

	it("invalidates only the snapshot load associated with a stale generation", () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-overlap.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-overlap-state",
		});
		const staleKey = `${activeSessionId}:chunked`;
		const freshKey = `${activeSessionId}:full`;
		const worker = {
			...workerHarness().worker,
			snapshotLoads: new Map([
				[staleKey, Promise.resolve(streamedResult([], "snapshot-stale"))],
				[freshKey, Promise.resolve(streamedResult([], "snapshot-fresh"))],
			]),
			snapshotLoadSnapshotIds: new Map([
				[staleKey, "snapshot-stale"],
				[freshKey, "snapshot-fresh"],
			]),
		};
		const internals = supervisor as unknown as {
			failWorkerSnapshotCache(
				worker: object,
				activeSessionId: string,
				error: Error,
				closeWorkerChannel: boolean,
				expectedSnapshotId: string,
			): void;
		};

		internals.failWorkerSnapshotCache(worker, activeSessionId, new Error("stale"), false, "snapshot-stale");

		expect(worker.snapshotLoads.has(staleKey)).toBe(false);
		expect(worker.snapshotLoads.has(freshKey)).toBe(true);
		expect(worker.snapshotLoadSnapshotIds.has(staleKey)).toBe(false);
		expect(worker.snapshotLoadSnapshotIds.get(freshKey)).toBe("snapshot-fresh");
	});

	it("quarantines a completed duplicate until transcript validation", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-state",
		});
		const { close, request, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const client = socketClient("public", new PassThrough());
		const streamSnapshot = vi.fn(async () => {});
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			workers: Map<string, WorkerHarness>;
			syncWorkerExtensionUi: ReturnType<typeof vi.fn>;
			streamSnapshot: typeof streamSnapshot;
			forwardToWorker(worker: WorkerHarness, command: DaemonCommand): Promise<unknown>;
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		internals.clients.add(client);
		internals.workers.set(worker.descriptor.workerId, worker);
		internals.syncWorkerExtensionUi = vi.fn(async () => {});
		internals.streamSnapshot = streamSnapshot;
		const messages: AgentMessage[] = [{ role: "user", content: "stable", timestamp: 1 }];
		const frames = snapshotFrames(messages);

		for (const message of [frames.begin, frames.chunk, frames.end]) {
			internals.handleWorkerFrame(worker, frame(message));
		}

		expect(worker.transcriptCaches.get(activeSessionId)?.complete).toBe(true);
		expect(worker.transcriptCaches.get(activeSessionId)?.chunkCount).toBe(1);
		expect(close).not.toHaveBeenCalled();

		internals.handleWorkerFrame(worker, frame(frames.begin, "replacement"));
		internals.handleWorkerFrame(worker, frame(frames.chunk, "replacement"));
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(streamSnapshot).not.toHaveBeenCalled();

		internals.handleWorkerFrame(worker, frame(frames.end, "replacement"));
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(worker.snapshotCache.has(activeSessionId)).toBe(true);
		expect(streamSnapshot).toHaveBeenCalledOnce();
		expect(close).not.toHaveBeenCalled();
		streamSnapshot.mockClear();

		const refreshedBegin = {
			...frames.begin,
			snapshot: {
				...frames.begin.snapshot,
				summary: {
					...frames.begin.snapshot.summary,
					activity: "working" as const,
					isSessionActive: true,
					attachedClients: 2,
				},
			},
		};
		internals.handleWorkerFrame(worker, frame(refreshedBegin, "replacement"));
		internals.handleWorkerFrame(worker, frame(frames.chunk, "replacement"));
		internals.handleWorkerFrame(worker, frame(frames.end, "replacement"));
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(worker.snapshotCache.get(activeSessionId)?.snapshot.summary).toMatchObject({
			activity: "working",
			isSessionActive: true,
		});
		expect(streamSnapshot).toHaveBeenCalledOnce();
		expect(close).not.toHaveBeenCalled();
		streamSnapshot.mockClear();
		const replacementSnapshotId = "snapshot-4602-recovered";
		request.mockImplementation(async (command: { type: string }) => {
			if (command.type === "attach") {
				return {
					type: "response",
					command: "attach",
					success: true,
					data: streamedResult([], replacementSnapshotId),
				};
			}
			if (command.type === "prompt") {
				return { type: "response", command: "prompt", success: true };
			}
			throw new Error(`unexpected worker request: ${command.type}`);
		});

		internals.handleWorkerFrame(worker, frame(frames.begin, "replacement"));
		internals.handleWorkerFrame(
			worker,
			frame(
				{
					...frames.chunk,
					messages: [{ role: "user", content: "stale", timestamp: 2 }],
				},
				"replacement",
			),
		);
		await new Promise<void>((resolve) => setImmediate(resolve));

		expect(close).not.toHaveBeenCalled();
		expect(worker.client).toBeDefined();
		expect(worker.descriptor.lifecycle).toBe("ready");
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(request.mock.calls.some(([command]) => command.type === "attach")).toBe(false);
		expect(streamSnapshot).not.toHaveBeenCalled();
		await expect(
			internals.forwardToWorker(worker, {
				id: "prompt-after-snapshot-mismatch",
				type: "prompt",
				activeSessionId,
				message: "still alive",
			}),
		).resolves.toMatchObject({ id: "prompt-after-snapshot-mismatch", success: true });
	});

	it("holds catch-up behind duplicate validation and rejects it on mismatch", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-gate.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-gate-state",
		});
		const { close, request, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const client = socketClient("catchup", new PassThrough());
		const streamSnapshot = vi.fn(async () => {});
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			workers: Map<string, WorkerHarness>;
			streamSnapshot: typeof streamSnapshot;
			catchUpClient(client: DaemonSocketClient): Promise<void>;
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		internals.clients.add(client);
		internals.workers.set(worker.descriptor.workerId, worker);
		internals.streamSnapshot = streamSnapshot;
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);
		for (const message of [frames.begin, frames.chunk, frames.end]) {
			internals.handleWorkerFrame(worker, frame(message));
		}

		internals.handleWorkerFrame(worker, frame(frames.begin));
		client.catchupActiveSessionIds?.add(activeSessionId);
		const catchup = internals.catchUpClient(client);
		await Promise.resolve();
		await Promise.resolve();

		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(request).not.toHaveBeenCalled();
		expect(streamSnapshot).not.toHaveBeenCalled();

		internals.handleWorkerFrame(worker, frame(frames.chunk));
		internals.handleWorkerFrame(worker, frame(frames.end));
		await catchup;

		expect(worker.snapshotCache.has(activeSessionId)).toBe(true);
		expect(request).not.toHaveBeenCalled();
		expect(streamSnapshot).toHaveBeenCalledOnce();
		streamSnapshot.mockClear();

		internals.handleWorkerFrame(worker, frame(frames.begin));
		client.catchupActiveSessionIds?.add(activeSessionId);
		const failedCatchup = internals.catchUpClient(client);
		await Promise.resolve();
		await Promise.resolve();
		internals.handleWorkerFrame(
			worker,
			frame({
				...frames.chunk,
				messages: [{ role: "user", content: "mismatch", timestamp: 2 }],
			}),
		);
		await failedCatchup;

		expect(request.mock.calls.some(([command]) => command.type === "attach")).toBe(false);
		expect(streamSnapshot).not.toHaveBeenCalled();
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(worker.client).toBeDefined();
		expect(worker.descriptor.lifecycle).toBe("ready");
		expect(close).not.toHaveBeenCalled();
	});

	it("rejects a quarantined catch-up before intentional worker stop", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-stop.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-stop-state",
		});
		const { close, request, worker } = workerHarness();
		registerWorker(supervisor, worker);
		const client = socketClient("catchup", new PassThrough());
		const streamSnapshot = vi.fn(async () => {});
		const persistWorker = vi.fn();
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			workers: Map<string, WorkerHarness>;
			shuttingDown: boolean;
			streamSnapshot: typeof streamSnapshot;
			persistWorker: typeof persistWorker;
			catchUpClient(client: DaemonSocketClient): Promise<void>;
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
			stopWorker(worker: WorkerHarness, removeDescriptor: boolean): Promise<void>;
		};
		internals.clients.add(client);
		internals.workers.set(worker.descriptor.workerId, worker);
		internals.shuttingDown = true;
		internals.streamSnapshot = streamSnapshot;
		internals.persistWorker = persistWorker;
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);
		for (const message of [frames.begin, frames.chunk, frames.end, frames.begin]) {
			internals.handleWorkerFrame(worker, frame(message));
		}
		const validation = worker.snapshotGenerations.get(activeSessionId)?.get(snapshotId)?.validation?.promise;
		if (!validation) {
			throw new Error("duplicate validation was not created");
		}
		client.catchupActiveSessionIds?.add(activeSessionId);
		const catchup = internals.catchUpClient(client);
		await Promise.resolve();
		await Promise.resolve();
		expect(request).not.toHaveBeenCalled();
		expect(streamSnapshot).not.toHaveBeenCalled();

		const unhandled = vi.fn();
		process.on("unhandledRejection", unhandled);
		try {
			const validationFailure = expect(validation).rejects.toThrow("stopped during snapshot transfer");
			await internals.stopWorker(worker, false);
			await validationFailure;
			await catchup;
			await new Promise<void>((resolve) => setImmediate(resolve));
		} finally {
			process.off("unhandledRejection", unhandled);
		}

		expect(request.mock.calls.some(([command]) => command.type === "attach")).toBe(false);
		expect(request.mock.calls.some(([command]) => command.type === "shutdown")).toBe(true);
		expect(streamSnapshot).not.toHaveBeenCalled();
		expect(close).toHaveBeenCalledOnce();
		expect(worker.snapshotCache.size).toBe(0);
		expect(worker.transcriptCaches.size).toBe(0);
		expect(worker.snapshotGenerations.size).toBe(0);
		expect(unhandled).not.toHaveBeenCalled();
	});

	it("lets a retained completed snapshot reader finish during intentional worker stop", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-reader-stop.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-reader-stop-state",
		});
		const { worker } = workerHarness();
		registerWorker(supervisor, worker);
		const persistWorker = vi.fn();
		const internals = supervisor as unknown as {
			persistWorker: typeof persistWorker;
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
			stopWorker(worker: WorkerHarness, removeDescriptor: boolean): Promise<void>;
		};
		internals.persistWorker = persistWorker;
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);
		for (const message of [frames.begin, frames.chunk, frames.end]) {
			internals.handleWorkerFrame(worker, frame(message));
		}
		const transcript = worker.transcriptCaches.get(activeSessionId);
		if (!transcript) {
			throw new Error("completed transcript was not cached");
		}
		const release = transcript.retain();

		await internals.stopWorker(worker, false);

		await expect(transcript.waitForChunk(0)).resolves.toEqual(frame(frames.chunk).payload);
		await expect(transcript.waitForChunk(1)).resolves.toBeUndefined();
		release();
		expect(worker.transcriptCaches.size).toBe(0);
		expect(worker.snapshotCache.size).toBe(0);
		expect(worker.snapshotGenerations.size).toBe(0);
	});

	it("retires same-ID generation faults without recovering the worker", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-invalid.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-invalid-state",
		});
		const recoverWorker = vi.fn(async () => {});
		const persistWorker = vi.fn();
		const assertRecoveryAllowed = vi.fn(async () => {});
		const internals = supervisor as unknown as {
			workers: Map<string, WorkerHarness>;
			recoverWorker: typeof recoverWorker;
			persistWorker: typeof persistWorker;
			assertRecoveryAllowed: typeof assertRecoveryAllowed;
			handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
		};
		internals.recoverWorker = recoverWorker;
		internals.persistWorker = persistWorker;
		internals.assertRecoveryAllowed = assertRecoveryAllowed;
		const frames = snapshotFrames([{ role: "user", content: "stable", timestamp: 1 }]);

		const reentrant = workerHarness();
		internals.workers.set(reentrant.worker.descriptor.workerId, reentrant.worker);
		internals.handleWorkerFrame(reentrant.worker, frame(frames.begin));
		internals.handleWorkerFrame(reentrant.worker, frame(frames.begin));
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(reentrant.close).not.toHaveBeenCalled();
		expect(reentrant.worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(reentrant.worker.client).toBeDefined();
		expect(reentrant.worker.descriptor.lifecycle).toBe("ready");
		expect(recoverWorker).not.toHaveBeenCalled();

		const completed = workerHarness();
		registerWorker(supervisor, completed.worker);
		for (const message of [frames.begin, frames.chunk, frames.end]) {
			internals.handleWorkerFrame(completed.worker, frame(message));
		}
		internals.handleWorkerFrame(completed.worker, frame({ ...frames.begin, messageCount: 2 }));
		expect(completed.close).not.toHaveBeenCalled();
		expect(completed.worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(completed.worker.client).toBeDefined();

		const mismatchedEnd = workerHarness();
		registerWorker(supervisor, mismatchedEnd.worker);
		for (const message of [frames.begin, frames.chunk, frames.end, frames.begin, frames.chunk]) {
			internals.handleWorkerFrame(mismatchedEnd.worker, frame(message));
		}
		internals.handleWorkerFrame(mismatchedEnd.worker, frame({ ...frames.end, lastEventSequence: 2 }));
		expect(mismatchedEnd.close).not.toHaveBeenCalled();
		expect(mismatchedEnd.worker.transcriptCaches.has(activeSessionId)).toBe(false);
		expect(mismatchedEnd.worker.client).toBeDefined();

		const replaced = workerHarness();
		registerWorker(supervisor, replaced.worker);
		internals.handleWorkerFrame(replaced.worker, frame(frames.begin));
		internals.handleWorkerFrame(replaced.worker, frame({ ...frames.begin, snapshotId: "snapshot-4602-new" }));
		expect(replaced.close).not.toHaveBeenCalled();
		expect(replaced.worker.transcriptCaches.get(activeSessionId)?.snapshotId).toBe("snapshot-4602-new");
	});

	it("emits one supervisor pre-begin catch-up failure without a self-requeue", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-pre-begin.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-pre-begin-state",
		});
		const socket = new PassThrough();
		socket.resume();
		const client = socketClient("pre-begin", socket);
		const records: DaemonOutbound[] = [];
		const preparationError = new Error("supervisor snapshot preparation failed");
		const attachClient = vi.fn(async () => {
			throw preparationError;
		});
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			attachClient: typeof attachClient;
			writeSnapshotRecord(client: DaemonSocketClient, message: DaemonOutbound): Promise<boolean>;
			queueCatchup(client: DaemonSocketClient, activeSessionId: string, purpose: "replacement" | "resync"): void;
			catchUpClient(client: DaemonSocketClient): Promise<void>;
		};
		internals.clients.add(client);
		internals.attachClient = attachClient;
		internals.writeSnapshotRecord = async (_client, message) => {
			records.push(message);
			return true;
		};

		internals.queueCatchup(client, activeSessionId, "resync");
		await internals.catchUpClient(client);

		expect(attachClient).toHaveBeenCalledOnce();
		expect(records).toEqual([
			expect.objectContaining({
				type: "session_snapshot_failed",
				activeSessionId,
				error: preparationError.message,
				purpose: "resync",
			}),
		]);
		expect(client.catchupActiveSessionIds).toEqual(new Set());
		socket.destroy();
	});

	it("emits one terminal failure without a supervisor-side retry loop", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-bounded-retry.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-bounded-retry-state",
		});
		const firstSnapshotId = "snapshot-4602-failed-first";
		const retrySnapshotId = "snapshot-4602-failed-retry";
		const firstError = new Error("first generation failed");
		const retryError = new Error("retry generation failed");
		const transcripts = [
			new SnapshotTranscriptCache({ activeSessionId, snapshotId: firstSnapshotId, cacheRoot: "/tmp" }),
			new SnapshotTranscriptCache({ activeSessionId, snapshotId: retrySnapshotId, cacheRoot: "/tmp" }),
		];
		transcripts[0]!.markFailed(firstError);
		transcripts[1]!.markFailed(retryError);
		const results = [streamedResult([], firstSnapshotId), streamedResult([], retrySnapshotId)];
		const socket = new PassThrough();
		socket.resume();
		const client = socketClient("bounded-retry", socket);
		const { worker } = workerHarness();
		registerWorker(supervisor, worker);
		const records: DaemonOutbound[] = [];
		let attachIndex = 0;
		const attachClient = vi.fn(async () => {
			const index = attachIndex++;
			const transcript = transcripts[index];
			const result = results[index];
			if (!transcript || !result) throw new Error("snapshot retry was not bounded");
			return { result, worker, transcript, releaseTranscript: transcript.retain() };
		});
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			attachClient: typeof attachClient;
			writeSnapshotRecord(client: DaemonSocketClient, message: DaemonOutbound): Promise<boolean>;
			queueCatchup(client: DaemonSocketClient, activeSessionId: string, purpose: "replacement" | "resync"): void;
			catchUpClient(client: DaemonSocketClient): Promise<void>;
		};
		internals.clients.add(client);
		internals.attachClient = attachClient;
		internals.writeSnapshotRecord = async (_client, message) => {
			records.push(message);
			return true;
		};

		internals.queueCatchup(client, activeSessionId, "replacement");
		await internals.catchUpClient(client);

		expect(attachClient).toHaveBeenCalledOnce();
		expect(records.map((record) => record.type)).toEqual(["session_snapshot_begin", "session_snapshot_failed"]);
		expect(records[0]).toMatchObject({ type: "session_snapshot_begin", purpose: "replacement" });
		expect(records[1]).toMatchObject({
			type: "session_snapshot_failed",
			activeSessionId,
			error: firstError.message,
			purpose: "replacement",
		});
		expect("snapshotId" in records[0]! && "snapshotId" in records[1]! && records[1].snapshotId).toBe(
			(records[0] as Extract<DaemonOutbound, { type: "session_snapshot_begin" }>).snapshotId,
		);
		expect((records[0] as Extract<DaemonOutbound, { type: "session_snapshot_begin" }>).snapshotId).not.toBe(
			firstSnapshotId,
		);
		socket.destroy();
		for (const transcript of transcripts) transcript.dispose();
	});

	it("expires an incomplete worker generation and settles cache and public waiters", async () => {
		vi.useFakeTimers();
		try {
			const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-deadline.sock", {
				defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
				descriptorDir: "/tmp/eng-4602-supervisor-deadline-state",
			});
			const { worker } = workerHarness();
			registerWorker(supervisor, worker);
			const client = socketClient("deadline", new PassThrough());
			const records: DaemonOutbound[] = [];
			const internals = supervisor as unknown as {
				clients: Set<DaemonSocketClient>;
				writeSnapshotRecord(client: DaemonSocketClient, message: DaemonOutbound): Promise<boolean>;
				handleWorkerFrame(worker: WorkerHarness, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
			};
			internals.clients.add(client);
			internals.writeSnapshotRecord = async (_client, message) => {
				records.push(message);
				return true;
			};
			const frames = snapshotFrames([{ role: "user", content: "never completes", timestamp: 1 }]);
			internals.handleWorkerFrame(worker, frame(frames.begin, "replacement"));
			const transcript = worker.transcriptCaches.get(activeSessionId);
			if (!transcript) throw new Error("snapshot deadline transcript was not created");
			const waiter = expect(transcript.waitForChunk(0)).rejects.toThrow("timed out before completion");

			await vi.advanceTimersByTimeAsync(30_000);
			await waiter;
			await Promise.resolve();

			expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
			expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
			expect(worker.snapshotGenerations.has(activeSessionId)).toBe(false);
			expect(records.filter((record) => record.type === "session_snapshot_failed")).toHaveLength(1);
			client.socket.destroy();
		} finally {
			vi.useRealTimers();
		}
	});

	it("settles a detached stream before a same-session reattach can revive zombie work", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-epoch.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-epoch-state",
		});
		const { worker } = workerHarness();
		registerWorker(supervisor, worker);
		const client = socketClient("epoch", new PassThrough());
		const transcript = new SnapshotTranscriptCache({ activeSessionId, snapshotId, cacheRoot: "/tmp" });
		const records: DaemonOutbound[] = [];
		const internals = supervisor as unknown as {
			advanceAttachmentEpoch(client: DaemonSocketClient, activeSessionId: string): number;
			detachClient(client: DaemonSocketClient, activeSessionId: string): void;
			writeSnapshotRecord(client: DaemonSocketClient, message: DaemonOutbound): Promise<boolean>;
			writeSnapshotBuffer(client: DaemonSocketClient, buffer: Uint8Array): Promise<boolean>;
			streamSnapshot(
				client: DaemonSocketClient,
				worker: WorkerHarness,
				result: DaemonAttachResult,
				transcript: SnapshotTranscriptCache,
			): Promise<void>;
		};
		internals.writeSnapshotRecord = async (_client, message) => {
			records.push(message);
			return true;
		};
		internals.writeSnapshotBuffer = async () => true;
		internals.advanceAttachmentEpoch(client, activeSessionId);
		const streaming = internals.streamSnapshot(client, worker, streamedResult([]), transcript);
		await Promise.resolve();
		expect(records.map((record) => record.type)).toEqual(["session_snapshot_begin"]);

		internals.detachClient(client, activeSessionId);
		client.attachedActiveSessionIds.add(activeSessionId);
		internals.advanceAttachmentEpoch(client, activeSessionId);
		await streaming;

		expect(records.map((record) => record.type)).toEqual(["session_snapshot_begin"]);
		transcript.dispose();
		client.socket.destroy();
	});

	it("retries one public cache-read failure without dropping another session on the shared client", async () => {
		const supervisor = new DaemonSupervisor("/tmp/eng-4602-supervisor-public.sock", {
			defaultSessionConfig: { agentDir: "/tmp", cwd: "/tmp" },
			descriptorDir: "/tmp/eng-4602-supervisor-public-state",
		});
		const result = streamedResult([]);
		const transcript = new SnapshotTranscriptCache({
			activeSessionId,
			snapshotId,
			messages: [{ role: "user", content: "message", timestamp: 1 }],
			cacheRoot: "/tmp",
		});
		const streamError = new Error("cached chunk read failed");
		transcript.readChunk = vi.fn(() => {
			throw streamError;
		});
		const failedSocket = new PassThrough();
		failedSocket.on("error", () => {});
		const failedWrites: Buffer[] = [];
		failedSocket.on("data", (chunk: Buffer) => failedWrites.push(Buffer.from(chunk)));
		const siblingSocket = new PassThrough();
		const failedClient = socketClient("failed", failedSocket);
		failedClient.attachedActiveSessionIds.add("active-4602-sibling");
		const siblingClient = socketClient("sibling", siblingSocket);
		const { close, worker } = workerHarness();
		registerWorker(supervisor, worker);
		worker.snapshotCache.set(activeSessionId, result);
		worker.transcriptCaches.set(activeSessionId, transcript);
		const internals = supervisor as unknown as {
			clients: Set<DaemonSocketClient>;
			streamSnapshot(
				client: DaemonSocketClient,
				worker: WorkerHarness,
				result: DaemonAttachResult,
				transcript: SnapshotTranscriptCache,
			): Promise<void>;
		};
		internals.clients.add(failedClient);
		internals.clients.add(siblingClient);

		await expect(internals.streamSnapshot(failedClient, worker, result, transcript)).rejects.toBe(streamError);

		const records = Buffer.concat(failedWrites)
			.toString("utf8")
			.trim()
			.split("\n")
			.map((line) => JSON.parse(line) as DaemonOutbound);
		expect(records.map((record) => record.type)).toEqual(["session_snapshot_begin", "session_snapshot_failed"]);
		expect(records[1]).toMatchObject({
			type: "session_snapshot_failed",
			activeSessionId,
			snapshotId,
			error: streamError.message,
		});
		expect(failedSocket.destroyed).toBe(false);
		expect(failedClient.attachedActiveSessionIds).toContain("active-4602-sibling");
		expect(siblingSocket.destroyed).toBe(false);
		expect(close).not.toHaveBeenCalled();
		expect(worker.snapshotCache.has(activeSessionId)).toBe(false);
		expect(worker.transcriptCaches.has(activeSessionId)).toBe(false);
		failedSocket.destroy();
		siblingSocket.destroy();
	});
});
