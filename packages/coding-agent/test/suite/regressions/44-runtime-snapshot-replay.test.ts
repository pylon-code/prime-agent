import { mkdtempSync, rmSync } from "node:fs";
import type { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ENV_AGENT_DIR } from "../../../src/config.js";
import { DaemonAgentConnection } from "../../../src/modes/agent-connection/daemon-agent-connection.js";
import type {
	AgentConnectionEvent,
	AgentConnectionSnapshot,
	AgentConnectionState,
} from "../../../src/modes/agent-connection/types.js";
import type { ActiveSessionState, DaemonSocketClient } from "../../../src/modes/daemon/active-session-state.js";
import type { DaemonHello, DaemonTransportClient } from "../../../src/modes/daemon/daemon-client.js";
import { AgentDaemon } from "../../../src/modes/daemon/daemon-mode.js";
import {
	DAEMON_PROTOCOL_INFO,
	type DaemonAttachResult,
	type DaemonCommand,
	type DaemonOutbound,
	type DaemonReplayInfo,
	type DaemonResumeCursor,
} from "../../../src/modes/daemon/daemon-protocol.js";
import { DaemonSupervisor } from "../../../src/modes/daemon/daemon-supervisor.js";
import type { DaemonWorkerFrameHeader } from "../../../src/modes/daemon/daemon-worker-protocol.js";
import { SnapshotTranscriptCache } from "../../../src/modes/daemon/snapshot-transcript-cache.js";
import type { PrivateFrame } from "../../../src/modes/session-worker/private-framing.js";
import { createHarness, type Harness } from "../harness.js";

const activeSessionId = "replay-active";
const generation = "replay-generation";
const harnesses: Harness[] = [];
const homes: string[] = [];
afterEach(() => {
	for (const harness of harnesses.splice(0)) harness.cleanup();
	for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true });
	vi.unstubAllEnvs();
});

async function fixture(schemaRevision = 33, eventSequence = true) {
	const home = mkdtempSync(join(tmpdir(), "prime-replay-home-"));
	homes.push(home);
	vi.stubEnv(ENV_AGENT_DIR, home);
	const harness = await createHarness();
	harnesses.push(harness);
	harness.setResponses([fauxAssistantMessage("finished")]);
	await harness.session.prompt("read the fixture");
	const messages = harness.session.messages;
	const state: AgentConnectionState = {
		activeSessionId,
		cwd: harness.tempDir,
		model: undefined,
		thinkingLevel: "medium",
		serviceTier: "default",
		availableThinkingLevels: ["medium"],
		isStreaming: false,
		isCompacting: false,
		isBashRunning: false,
		retryAttempt: 0,
		steeringMode: "all",
		followUpMode: "one-at-a-time",
		sessionFile: undefined,
		sessionId: harness.session.sessionId,
		sessionName: undefined,
		sessionDir: harness.tempDir,
		leafId: null,
		autoCompactionEnabled: true,
		messageCount: messages.length,
		sessionActions: { queuedCount: 0, steering: [], followUps: [] },
		compactionCount: 0,
		goal: { active: false, status: "idle", tokensUsed: 0, timeUsedSeconds: 0, continuationsUsed: 0 },
		scopedModels: [],
		activeToolNames: [],
		contextUsage: undefined,
	};
	const snapshot: DaemonAttachResult["snapshot"] = {
		activeSessionId,
		state,
		messages,
		lastEventSequence: 10,
		lastEventCursor: { generation, sequence: 10 },
		summary: {
			id: activeSessionId,
			activeSessionId,
			lifecycle: "live",
			activity: "idle",
			isSessionActive: false,
			sessionId: state.sessionId,
			cwd: state.cwd,
			isStreaming: false,
			isCompacting: false,
			attachedClients: 1,
			messageCount: messages.length,
			sessionActions: state.sessionActions,
		},
	};
	const socket = new PassThrough();
	const client: DaemonSocketClient = {
		id: "replay-client",
		socket: socket as unknown as Socket,
		attachedActiveSessionIds: new Set([activeSessionId]),
		detachInput: () => {},
		supportsExtensionUi: false,
		capabilities: new Set(eventSequence ? ["event_sequence"] : []),
	};
	// Keep the real worker's replay calculation; only its unrelated snapshot loader is replaced.
	const daemon = Object.assign(Object.create(AgentDaemon.prototype), {
		createSessionSnapshot: async () => snapshot,
	}) as {
		createAttachResult(
			client: DaemonSocketClient,
			state: ActiveSessionState,
			command: Extract<DaemonCommand, { type: "attach" }>,
		): Promise<DaemonAttachResult>;
	};
	const workerState = { activeSessionId, eventGeneration: generation } as ActiveSessionState;
	const workerResult = (resumeCursor?: DaemonResumeCursor) =>
		daemon.createAttachResult(client, workerState, { type: "attach", activeSessionId, resumeCursor });
	const hello: DaemonHello = {
		type: "daemon_hello",
		socketPath: join(harness.tempDir, "daemon.sock"),
		protocol: DAEMON_PROTOCOL_INFO,
		schemaRevision,
		clientId: client.id,
		serverCapabilities: eventSequence ? ["event_sequence"] : [],
	};
	let attached = false;
	const transport: DaemonTransportClient = {
		hello,
		isClosed: false,
		isConnected: true,
		getTransportGeneration: () => 1,
		supportsServerCapability: (capability) => hello.serverCapabilities.includes(capability),
		waitForHello: async () => hello,
		connect: async () => {},
		reconnect: async () => {},
		disconnectForReconnect: () => {},
		resetTransportForReconnect: () => {},
		onMessage: () => () => {},
		onClose: () => () => {},
		enableRequestRecovery: () => {},
		close: () => {},
		request: async (command) => {
			if (command.type === "detach") return { type: "response", command: command.type, success: true };
			if (command.type !== "attach" || attached) throw new Error("Unexpected snapshot retry");
			attached = true;
			const result = await workerResult();
			return {
				type: "response",
				command: command.type,
				success: true,
				data: {
					...result,
					client: { ...result.client, id: command.clientId! },
					lastEventSequence: 0,
					lastEventCursor: { generation, sequence: 0 },
					replay: { status: "complete", toSequence: 0, toCursor: { generation, sequence: 0 } },
					snapshot: {
						...snapshot,
						messages: [],
						lastEventSequence: 0,
						lastEventCursor: { generation, sequence: 0 },
						summary: { ...snapshot.summary, messageCount: 0 },
					},
				},
			};
		},
	};
	const connection = new DaemonAgentConnection(transport, activeSessionId);
	await connection.attach({ recoverable: false });
	const events: AgentConnectionEvent[] = [];
	connection.subscribe((event) => {
		events.push(event);
	});
	const sdk = connection as unknown as {
		handleDaemonMessage(message: DaemonOutbound, epoch: number): Promise<void>;
		attachmentEpoch: number;
		latestSnapshot: AgentConnectionSnapshot;
	};
	return { harness, snapshot, client, workerResult, connection, events, sdk };
}

async function catchup(
	h: Awaited<ReturnType<typeof fixture>>,
	result: DaemonAttachResult,
	streamed: boolean,
	consume = true,
) {
	if (streamed) h.client.capabilities.add("chunked_snapshot");
	const transcript = new SnapshotTranscriptCache({
		activeSessionId,
		snapshotId: "worker-snapshot",
		messages: result.snapshot.messages,
		cacheRoot: h.harness.tempDir,
	});
	const frames: DaemonOutbound[] = [];
	const worker = { descriptor: { workerId: "worker" } };
	const supervisor = Object.assign(Object.create(DaemonSupervisor.prototype), {
		workers: new Map([["worker", worker]]),
		attachmentEpochs: new WeakMap(),
		attachmentAbortControllers: new WeakMap(),
		attachClient: async () => ({ worker, result, transcript }),
		write: (_client: DaemonSocketClient, frame: DaemonOutbound) => {
			frames.push(frame);
			return true;
		},
		writeSnapshotRecord: async (_client: DaemonSocketClient, frame: DaemonOutbound) => {
			frames.push(frame);
			return true;
		},
		writeSnapshotBuffer: async (_client: DaemonSocketClient, payload: Buffer | readonly Buffer[]) => {
			frames.push(JSON.parse((Buffer.isBuffer(payload) ? payload : Buffer.concat(payload)).toString()));
			return true;
		},
		log: vi.fn(),
	}) as {
		queueCatchup(client: DaemonSocketClient, id: string): void;
		drainClientCatchups(client: DaemonSocketClient): Promise<void>;
	};
	try {
		supervisor.queueCatchup(h.client, activeSessionId);
		await supervisor.drainClientCatchups(h.client);
		if (consume) for (const frame of frames) await h.sdk.handleDaemonMessage(frame, h.sdk.attachmentEpoch);
		return frames;
	} finally {
		transcript.dispose();
	}
}

async function workerCatchup(h: Awaited<ReturnType<typeof fixture>>, streamed: boolean) {
	const frames: DaemonOutbound[] = [];
	const state = { activeSessionId, eventGeneration: generation, lastEventSequence: 10, clients: new Set([h.client]) };
	if (streamed) {
		h.client.transport = "private-framed";
		h.client.capabilities.add("chunked_snapshot");
	}
	const daemon = Object.assign(Object.create(AgentDaemon.prototype), {
		sessions: new Map([[activeSessionId, state]]),
		createAttachResult: () => h.workerResult(),
		prepareWorkerSnapshotTranscript: async (options: { snapshotId: string }) =>
			new SnapshotTranscriptCache({
				activeSessionId,
				snapshotId: options.snapshotId,
				messages: h.snapshot.messages,
				cacheRoot: h.harness.tempDir,
			}),
		write: (_client: DaemonSocketClient, frame: DaemonOutbound) => {
			frames.push(frame);
			return true;
		},
		writeWorkerSnapshotBuffer: async (_client: DaemonSocketClient, payload: Buffer | readonly Buffer[]) => {
			frames.push(JSON.parse((Buffer.isBuffer(payload) ? payload : Buffer.concat(payload)).toString()));
			return true;
		},
		log: vi.fn(),
	}) as { drainBackpressuredClientCatchups(client: DaemonSocketClient): Promise<void> };
	h.client.catchupActiveSessionIds = new Set([activeSessionId]);
	await daemon.drainBackpressuredClientCatchups(h.client);
	return frames;
}

describe("issue #44 runtime snapshot replay", () => {
	it.each([false, true])("preserves worker-issued replay inline/streamed=%s", async (streamed) => {
		const h = await fixture();
		try {
			const result = await h.workerResult();
			const frames = await catchup(h, result, streamed);
			expect((frames[0] as { replay?: DaemonReplayInfo }).replay).toEqual(result.replay);
			expect(h.events).toEqual([
				expect.objectContaining({
					type: "session_resynced",
					snapshot: expect.objectContaining({ replay: result.replay, messages: result.snapshot.messages }),
				}),
			]);
		} finally {
			await h.connection.dispose();
			h.client.socket.destroy();
		}
	});

	it.each([false, true])("preserves unavailable and partial replay inline/streamed=%s", async (streamed) => {
		for (const status of ["unavailable", "partial"] as const) {
			const h = await fixture();
			try {
				const result = await h.workerResult({ generation, sequence: 1 });
				if (status === "partial") result.replay = { ...result.replay, status };
				await catchup(h, result, streamed);
				expect(h.sdk.latestSnapshot.replay).toEqual(result.replay);
				expect(h.sdk.latestSnapshot.replay?.status).toBe(status);
			} finally {
				await h.connection.dispose();
				h.client.socket.destroy();
			}
		}
	});

	it.each([false, true])("degrades old peers and missing metadata inline/streamed=%s", async (streamed) => {
		for (const mode of ["old-daemon", "old-client", "missing"] as const) {
			const h = await fixture(mode === "old-daemon" ? 32 : 33, mode !== "old-client");
			try {
				const result = await h.workerResult();
				if (mode === "missing") Reflect.deleteProperty(result, "replay");
				const frames = await catchup(h, result, streamed);
				if (mode === "old-client") expect(frames[0]).not.toHaveProperty("replay");
				expect(h.events).toHaveLength(1);
				expect(h.sdk.latestSnapshot.replay).toBeUndefined();
			} finally {
				await h.connection.dispose();
				h.client.socket.destroy();
			}
		}
	});

	it.each([false, true])(
		"rejects invalid replay without publishing a complete snapshot inline/streamed=%s",
		async (streamed) => {
			const invalid: unknown[] = [
				null,
				{ status: "invented", toSequence: 10 },
				{ status: "complete", toSequence: 9 },
				{ status: "complete", toSequence: 10, toCursor: { generation: "foreign", sequence: 10 } },
				{ status: "complete", toSequence: 10, toCursor: { generation, sequence: 9 } },
				{ status: "complete", toSequence: 10, fromSequence: 11 },
				{ status: "complete", toSequence: 10, fromSequence: 1, fromCursor: { generation: "foreign", sequence: 1 } },
			];
			for (const replay of invalid) {
				const h = await fixture();
				try {
					const before = h.sdk.latestSnapshot;
					const result = await h.workerResult();
					result.replay = replay as DaemonReplayInfo;
					await catchup(h, result, streamed);
					expect(h.events.filter((event) => event.type === "session_resynced").length).toBe(0);
					expect(h.sdk.latestSnapshot).toBe(before);
				} finally {
					await h.connection.dispose();
					h.client.socket.destroy();
				}
			}
		},
	);
	it.each([false, true])("keeps direct worker emission compatible inline/streamed=%s", async (streamed) => {
		for (const eventSequence of [true, false]) {
			const h = await fixture(33, eventSequence);
			try {
				const frames = await workerCatchup(h, streamed);
				const replay = (frames[0] as { replay?: DaemonReplayInfo }).replay;
				expect(replay).toEqual(eventSequence ? (await h.workerResult()).replay : undefined);
				for (const frame of frames) await h.sdk.handleDaemonMessage(frame, h.sdk.attachmentEpoch);
				expect(h.events.filter((event) => event.type === "session_resynced").length).toBe(1);
				expect(h.sdk.latestSnapshot.replay).toEqual(replay);
			} finally {
				await h.connection.dispose();
				h.client.socket.destroy();
			}
		}
	});

	it.each([false, true])("does not let replay override snapshot identity inline/streamed=%s", async (streamed) => {
		const h = await fixture();
		try {
			const before = h.sdk.latestSnapshot;
			const result = await h.workerResult();
			result.snapshot = {
				...result.snapshot,
				state: { ...result.snapshot.state, sessionId: "foreign-session" },
				summary: { ...result.snapshot.summary, sessionId: "foreign-session" },
			};
			await catchup(h, result, streamed);
			expect(h.events.filter((event) => event.type === "session_resynced").length).toBe(0);
			expect(h.sdk.latestSnapshot).toBe(before);
		} finally {
			await h.connection.dispose();
			h.client.socket.destroy();
		}
	});

	it("cannot publish replay from a stale stream assembly", async () => {
		const h = await fixture();
		try {
			const before = h.sdk.latestSnapshot;
			const frames = await catchup(h, await h.workerResult(), true, false);
			await h.sdk.handleDaemonMessage(frames[0]!, h.sdk.attachmentEpoch);
			h.sdk.attachmentEpoch++;
			for (const frame of frames.slice(1)) await h.sdk.handleDaemonMessage(frame, h.sdk.attachmentEpoch);
			expect(h.events.filter((event) => event.type === "session_resynced").length).toBe(0);
			expect(h.sdk.latestSnapshot).toBe(before);
		} finally {
			await h.connection.dispose();
			h.client.socket.destroy();
		}
	});

	it("preserves worker replay through the supervisor cache and never invents complete for old workers", async () => {
		for (const status of ["complete", "partial", "unavailable", "missing", "invalid"] as const) {
			const h = await fixture();
			const supervisor = new DaemonSupervisor(join(h.harness.tempDir, "supervisor.sock"), {
				defaultSessionConfig: { agentDir: h.harness.tempDir, cwd: h.harness.tempDir },
				descriptorDir: join(h.harness.tempDir, "descriptors"),
			});
			const worker = {
				descriptor: { workerId: "worker", rootActiveSessionId: activeSessionId, lifecycle: "ready", pid: 1 },
				client: { close: vi.fn() },
				authorizedActiveSessionIds: new Set([activeSessionId]),
				summaries: new Map([[activeSessionId, h.snapshot.summary]]),
				snapshotCache: new Map<string, DaemonAttachResult>(),
				transcriptCaches: new Map<string, SnapshotTranscriptCache>(),
				snapshotGenerations: new Map(),
				snapshotLoads: new Map(),
				intentionalStop: false,
				stopRevision: 0,
			};
			const internals = supervisor as unknown as {
				workers: Map<string, object>;
				snapshotCacheRoot: string;
				handleWorkerFrame(worker: object, frame: PrivateFrame<DaemonWorkerFrameHeader>): void;
			};
			internals.workers.set("worker", worker);
			internals.snapshotCacheRoot = h.harness.tempDir;
			try {
				const frames = await workerCatchup(h, true);
				const begin = frames[0];
				if (begin?.type !== "session_snapshot_begin") throw new Error("Missing worker begin frame");
				if (status === "missing") delete begin.replay;
				else
					begin.replay = {
						...begin.replay!,
						status: status === "invalid" ? "complete" : status,
						...(status === "invalid" ? { toSequence: 99 } : {}),
					};
				for (const message of frames) {
					if (!("snapshotId" in message)) throw new Error("Unexpected worker frame");
					internals.handleWorkerFrame(worker, {
						header: {
							kind: "outbound",
							outboundType: message.type,
							activeSessionId,
							snapshotId: message.snapshotId,
							payloadEncoding: "jsonl",
							snapshotPurpose: "attach",
						},
						payload: Buffer.from(JSON.stringify(message)),
					});
				}
				const cached = worker.snapshotCache.get(activeSessionId);
				expect(worker.client.close).not.toHaveBeenCalled();
				if (status === "invalid") {
					expect(cached).toBeUndefined();
					continue;
				}
				if (!cached) throw new Error("Worker snapshot was not cached");
				expect(cached.replay).toEqual(
					status === "missing"
						? {
								status: "unavailable",
								toSequence: 10,
								toCursor: { generation, sequence: 10 },
								reason: "worker_snapshot_replay_not_reported",
							}
						: begin.replay,
				);
				const materialized = { ...cached, snapshot: { ...cached.snapshot, messages: h.snapshot.messages } };
				await catchup(h, materialized, true);
				expect(h.sdk.latestSnapshot.replay).toEqual(cached.replay);
				expect(worker.client.close).not.toHaveBeenCalled();
			} finally {
				for (const transcript of worker.transcriptCaches.values()) transcript.dispose();
				await h.connection.dispose();
				h.client.socket.destroy();
			}
		}
	});
});
