import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { DaemonCommand, DaemonRecoverableOwnedSessionPrepareResult } from "../src/modes/daemon/daemon-protocol.js";
import { DaemonSupervisor } from "../src/modes/daemon/daemon-supervisor.js";
import { RECOVERABLE_OWNED_CONFIRMATION_RETENTION_MS } from "../src/modes/daemon/owned-session-recovery-retention.js";

const directories: string[] = [];

afterEach(() => {
	vi.useRealTimers();
	for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

function supervisorFixture() {
	const root = mkdtempSync(join(tmpdir(), "daemon-recoverable-state-"));
	directories.push(root);
	const supervisor = new DaemonSupervisor(join(root, "daemon.sock"), {
		defaultSessionConfig: { agentDir: root, cwd: root },
		descriptorDir: join(root, "workers"),
	});
	return supervisor as unknown as {
		clients: Set<unknown>;
		workers: Map<string, unknown>;
		generation: string;
		ownedSessionRecoveryStore: Record<string, ReturnType<typeof vi.fn>>;
		stopWorker: ReturnType<typeof vi.fn>;
		scheduleOwnedWorkerCleanup: ReturnType<typeof vi.fn>;
		createRecoverableOwnedSession(
			client: unknown,
			command: Extract<DaemonCommand, { type: "create_recoverable_owned_session" }>,
		): Promise<unknown>;
		prepareRecoverableOwnedSnapshot(
			worker: unknown,
			adoption: unknown,
			receipt: unknown,
			command: Extract<DaemonCommand, { type: "prepare_recoverable_owned_session_adoption" }>,
			prepareDeadline: number,
		): Promise<DaemonRecoverableOwnedSessionPrepareResult>;
		prepareRecoverableOwnedSessionAdoption(
			client: unknown,
			command: Extract<DaemonCommand, { type: "prepare_recoverable_owned_session_adoption" }>,
		): Promise<DaemonRecoverableOwnedSessionPrepareResult>;
		rollbackRecoverableOwnedAdoption(worker: unknown, adoption: unknown): void;
		commitRecoverableOwnedSessionAdoption(
			client: unknown,
			command: Extract<DaemonCommand, { type: "commit_recoverable_owned_session_adoption" }>,
		): Promise<unknown>;
		retireRecoverableOwnedAuthorityForWorkerIncarnation(worker: unknown, incarnation: string): void;
		scheduleRecoverableOwnedConfirmationExpiry(worker: unknown, recordId: string): void;
	};
}

describe("DaemonSupervisor recoverable authority lifecycle", () => {
	it("revokes every stale recovery surface when a worker incarnation is replaced", () => {
		const supervisor = supervisorFixture();
		const oldIncarnation = "A".repeat(43);
		const adoptionTimeout = setTimeout(() => {}, 60_000);
		const confirmationTimer = setTimeout(() => {}, 60_000);
		const cleanupTimer = setTimeout(() => {}, 60_000);
		const worker = {
			descriptor: { workerId: "worker-1", ownerClientId: "connected-adopted-owner" },
			workerIncarnation: oldIncarnation,
			recoveryRecordId: "record-1",
			recoverableAdoption: {
				workerIncarnation: oldIncarnation,
				timeout: adoptionTimeout,
			},
			recoverableFinal: { recordId: "record-1" },
			recoveryConfirmationTimer: confirmationTimer,
			ownerCleanupTimer: cleanupTimer,
		};
		supervisor.ownedSessionRecoveryStore = {
			get: vi.fn(() => ({ authority: { workerIncarnation: oldIncarnation } })),
			remove: vi.fn(),
		};

		supervisor.retireRecoverableOwnedAuthorityForWorkerIncarnation(worker, "B".repeat(43));

		expect(supervisor.ownedSessionRecoveryStore.remove).toHaveBeenCalledWith("record-1");
		expect(worker).toMatchObject({
			descriptor: { ownerClientId: "connected-adopted-owner" },
			recoveryRecordId: undefined,
			recoverableAdoption: undefined,
			recoverableFinal: undefined,
			recoveryConfirmationTimer: undefined,
			ownerCleanupTimer: undefined,
		});
	});

	it("rejects a stale create generation before recovery lookup or worker writes", async () => {
		const supervisor = supervisorFixture();
		const getByCreateRequest = vi.fn();
		const create = vi.fn();
		supervisor.ownedSessionRecoveryStore = {
			digestRequest: vi.fn(),
			getByCreateRequest,
			create,
		};
		await expect(
			supervisor.createRecoverableOwnedSession(
				{ id: "creator" },
				{
					type: "create_recoverable_owned_session",
					requestId: "00112233445566778899aabbccddeeff",
					expectedSupervisorGeneration: "replacement-generation",
					correlationId: "correlation",
					mcpOwnerId: "mcp-owner",
					recoveryConfig: { cwd: "/tmp/project" },
					config: { cwd: "/tmp/project" },
					launchEnv: {},
					launchEnvMode: "replace",
				},
			),
		).rejects.toThrow("Recoverable owned session adoption is unavailable");
		expect(getByCreateRequest).not.toHaveBeenCalled();
		expect(create).not.toHaveBeenCalled();
		expect(supervisor.workers.size).toBe(0);
	});

	it("contains the exact launched worker and removes its receipt after post-launch setup fails", async () => {
		const supervisor = supervisorFixture();
		const summary = {
			id: "active-create",
			activeSessionId: "active-create",
			sessionId: "session-create",
			cwd: "/tmp/project",
		};
		const worker = {
			descriptor: { workerId: "worker-create", rootActiveSessionId: "active-create", ownerClientId: "creator" },
			workerIncarnation: "I".repeat(43),
			client: { matchesAuthenticatedIncarnation: vi.fn(() => true) },
			summaries: new Map([["active-create", summary]]),
			recoveryRecordId: undefined as string | undefined,
			ownerCleanupTimer: setTimeout(() => {}, 1_000) as NodeJS.Timeout | undefined,
		};
		const stopWorker = vi.fn(async () => undefined);
		const remove = vi.fn();
		supervisor.ownedSessionRecoveryStore = {
			digestRequest: vi.fn((value: unknown) => JSON.stringify(value)),
			digestAuthority: vi.fn((value: unknown) => JSON.stringify(value)),
			getByCreateRequest: vi.fn(() => undefined),
			create: vi.fn(() => ({
				recordId: "record-create",
				recoveryHandle: "R".repeat(43),
				ownershipGeneration: 1,
			})),
			remove,
		};
		Object.assign(supervisor, {
			createOrReuseWorker: vi.fn(async (_owner: string, _command: unknown, onLaunch: (value: unknown) => void) => {
				onLaunch(worker);
				return worker;
			}),
			stopWorker,
			scheduleOwnedWorkerCleanup: vi.fn(() => {
				throw new Error("timer setup failed");
			}),
		});
		supervisor.workers.set("worker-create", worker);
		const command = {
			type: "create_recoverable_owned_session",
			requestId: "00112233445566778899aabbccddeeff",
			expectedSupervisorGeneration: supervisor.generation,
			correlationId: "correlation-create",
			mcpOwnerId: "mcp-create",
			recoveryConfig: { cwd: "/tmp/project" },
			config: { cwd: "/tmp/project" },
			launchEnv: {},
			launchEnvMode: "replace",
		} as const satisfies Extract<DaemonCommand, { type: "create_recoverable_owned_session" }>;

		await expect(supervisor.createRecoverableOwnedSession({ id: "creator" }, command)).rejects.toThrow(
			"timer setup failed",
		);
		expect(remove).toHaveBeenCalledWith("record-create");
		expect(worker.recoveryRecordId).toBeUndefined();
		expect(worker.ownerCleanupTimer).toBeUndefined();
		expect(stopWorker).toHaveBeenCalledWith(worker, true, true);
	});

	it("single-flights one stable prepare before draining mutations", async () => {
		const supervisor = supervisorFixture();
		const workerIncarnation = "A".repeat(43);
		const authority = {
			workerId: "worker-prepare",
			workerIncarnation,
			activeSessionId: "active-prepare",
			sessionId: "session-prepare",
			correlationId: "correlation-prepare",
			mcpOwnerId: "mcp-owner-before",
			recoveryConfig: { cwd: "/tmp/project" },
			launchEnv: { PATH: "/caller/bin" },
		};
		const receipt = {
			recordId: "record-prepare",
			authority,
			recoveryHandle: "R".repeat(43),
			ownershipGeneration: 1,
			phase: "prepared",
			repeated: false,
		};
		const workerClient = { matchesAuthenticatedIncarnation: vi.fn(() => true) };
		const worker = {
			descriptor: { workerId: "worker-prepare", ownerClientId: "previous-owner" },
			workerIncarnation,
			client: workerClient,
			recoveryRecordId: receipt.recordId,
			stopRevision: 0,
		};
		supervisor.workers.set("worker-prepare", worker);
		supervisor.clients = new Set();
		supervisor.scheduleOwnedWorkerCleanup = vi.fn();
		const beginAdoption = vi.fn(() => receipt);
		supervisor.ownedSessionRecoveryStore = {
			getByHandle: vi.fn(() => receipt),
			digestRequest: vi.fn((value: unknown) => JSON.stringify(value)),
			digestAuthority: vi.fn((value: unknown) => JSON.stringify(value)),
			beginAdoption,
			get: vi.fn(() => receipt),
			rollbackAdoption: vi.fn(() => receipt),
		};
		let releaseSnapshot!: () => void;
		const snapshotGate = new Promise<void>((resolve) => {
			releaseSnapshot = resolve;
		});
		const result = {
			recoveryHandle: receipt.recoveryHandle,
			proof: {
				feature: "recoverable_owned_session_adoption_v1",
				status: "adopted",
				supervisorGeneration: supervisor.generation,
				ownershipGeneration: 1,
				activeSessionId: authority.activeSessionId,
				sessionId: authority.sessionId,
				correlationId: authority.correlationId,
				lifecycle: { correlationId: authority.correlationId, expired: true, deliveryCrossed: true },
				cursor: { generation: "cursor-generation", sequence: 4 },
				mcpOwnerId: "mcp-owner-after",
			},
		} as unknown as DaemonRecoverableOwnedSessionPrepareResult;
		supervisor.prepareRecoverableOwnedSnapshot = vi.fn(async () => {
			await snapshotGate;
			return result;
		});
		const physicalClient = {
			id: "claimant",
			socket: { destroyed: false },
			attachedActiveSessionIds: new Set<string>(),
			capabilities: new Set<string>(),
		};
		const otherPhysicalClient = { ...physicalClient, id: "other-claimant", socket: { destroyed: false } };
		const command = {
			type: "prepare_recoverable_owned_session_adoption",
			requestId: "00112233445566778899aabbccddeeff",
			recoveryHandle: receipt.recoveryHandle,
			expectedSupervisorGeneration: supervisor.generation,
			activeSessionId: authority.activeSessionId,
			sessionId: authority.sessionId,
			correlationId: authority.correlationId,
			cursor: { generation: "cursor-generation", sequence: 3 },
			previousMcpOwnerId: authority.mcpOwnerId,
			mcpOwnerId: "mcp-owner-after",
			recoveryConfig: authority.recoveryConfig,
			launchEnv: authority.launchEnv,
			clientId: "attached-claimant",
			capabilities: [
				"attach_snapshot",
				"event_sequence",
				"correlated_prompt_lifecycle_v1",
				"caller_owned_session_environment_cleanup_v1",
			],
		} as const satisfies Extract<DaemonCommand, { type: "prepare_recoverable_owned_session_adoption" }>;

		const first = supervisor.prepareRecoverableOwnedSessionAdoption(physicalClient, command);
		const samePhysicalRetry = supervisor.prepareRecoverableOwnedSessionAdoption(physicalClient, command);
		await expect(supervisor.prepareRecoverableOwnedSessionAdoption(otherPhysicalClient, command)).rejects.toThrow(
			"Recoverable owned session adoption is unavailable",
		);
		expect(beginAdoption).toHaveBeenCalledTimes(1);
		expect(supervisor.prepareRecoverableOwnedSnapshot).toHaveBeenCalledTimes(1);
		releaseSnapshot();
		await expect(Promise.all([first, samePhysicalRetry])).resolves.toEqual([result, result]);
		const adoption = (worker as { recoverableAdoption?: unknown }).recoverableAdoption;
		if (adoption) supervisor.rollbackRecoverableOwnedAdoption(worker, adoption);
	});

	it("commits the exact final retry when one daemon client recovers two sessions", async () => {
		const supervisor = supervisorFixture();
		const actions: string[] = [];
		const physicalClient = {
			id: "shared-client",
			socket: { destroyed: false },
			attachedActiveSessionIds: { add: (activeSessionId: string) => actions.push(`attach:${activeSessionId}`) },
		};
		const workerClient = { matchesAuthenticatedIncarnation: vi.fn(() => true) };
		const requestDigest = (requestId: string) =>
			JSON.stringify({ type: "prepare_recoverable_owned_session_adoption", requestId });
		const makeWorker = (suffix: string) => {
			const requestId = suffix.toLowerCase().repeat(32);
			const proof = {
				feature: "recoverable_owned_session_adoption_v1" as const,
				status: "adopted" as const,
				supervisorGeneration: supervisor.generation,
				ownershipGeneration: 1,
				activeSessionId: `active-${suffix}`,
				sessionId: `session-${suffix}`,
				correlationId: `correlation-${suffix}`,
				lifecycle: { correlationId: `correlation-${suffix}`, expired: true as const, deliveryCrossed: true },
				cursor: { generation: `cursor-${suffix}`, sequence: 3 },
				mcpOwnerId: `mcp-${suffix}`,
			};
			const result = { recoveryHandle: suffix.repeat(43), proof } as DaemonRecoverableOwnedSessionPrepareResult;
			const adoption = {
				recordId: `record-${suffix}`,
				requestIdDigest: requestDigest(requestId),
				requestDigest: `digest-${suffix}`,
				client: physicalClient,
				workerClient,
				workerIncarnation: `incarnation-${suffix}`,
				workerStopRevision: 0,
				previousOwnerClientId: "shared-client",
				activeSessionId: proof.activeSessionId,
				proof,
				result,
				frames: [{ payload: Buffer.from(`retained-${suffix}`) }],
				bufferedBytes: 10,
				finalRetry: true as const,
				timeout: setTimeout(() => {}, 60_000),
			};
			return {
				descriptor: { workerId: `worker-${suffix}`, ownerClientId: "shared-client", lifecycle: "ready" },
				workerIncarnation: `incarnation-${suffix}`,
				client: workerClient,
				recoveryRecordId: `record-${suffix}`,
				stopRevision: 0,
				recoverableAdoption: adoption,
				recoverableFinal: {
					recordId: `record-${suffix}`,
					requestIdDigest: adoption.requestIdDigest,
					requestDigest: adoption.requestDigest,
					ownerClientId: "shared-client",
					result,
					frames: [],
				},
			};
		};
		const first = makeWorker("A");
		const second = makeWorker("B");
		supervisor.workers = new Map([
			["worker-A", first],
			["worker-B", second],
		]);
		supervisor.clients = new Set([physicalClient]);
		supervisor.ownedSessionRecoveryStore = {
			digestRequest: vi.fn((value: unknown) => JSON.stringify(value)),
			getForConfirmation: vi.fn(() => ({
				recordId: "record-B",
				authority: { workerIncarnation: "incarnation-B" },
			})),
		};
		Object.assign(supervisor, {
			validateRecoverableOwnedBuffer: vi.fn(),
			writeSerialized: vi.fn((_client: unknown, payload: Buffer) => actions.push(payload.toString())),
			syncWorkerExtensionUi: vi.fn(),
		});
		const command = {
			type: "commit_recoverable_owned_session_adoption",
			requestId: "b".repeat(32),
			expectedSupervisorGeneration: supervisor.generation,
			recoveryHandle: "B".repeat(43),
			proof: second.recoverableFinal.result.proof,
		} as const satisfies Extract<DaemonCommand, { type: "commit_recoverable_owned_session_adoption" }>;

		await expect(supervisor.commitRecoverableOwnedSessionAdoption(physicalClient, command)).resolves.toEqual(
			command.proof,
		);
		expect(first.recoverableAdoption).toBeDefined();
		expect(second.recoverableAdoption).toBeUndefined();
		expect(actions).toEqual(["retained-B", "attach:active-B"]);
	});

	it("rejects incomplete replay, malformed snapshots, and lifecycle mismatches before ownership cut", async () => {
		const supervisor = supervisorFixture();
		const activeSessionId = "active-proof";
		const sessionId = "session-proof";
		const correlationId = "correlation-proof";
		const cursor = { generation: "generation-proof", sequence: 5 };
		const command = {
			type: "prepare_recoverable_owned_session_adoption",
			requestId: "00112233445566778899aabbccddeeff",
			recoveryHandle: "R".repeat(43),
			expectedSupervisorGeneration: supervisor.generation,
			activeSessionId,
			sessionId,
			correlationId,
			cursor: { generation: cursor.generation, sequence: 4 },
			previousMcpOwnerId: "mcp-before",
			mcpOwnerId: "mcp-after",
			recoveryConfig: { cwd: "/tmp/project" },
			launchEnv: { PATH: "/caller/bin" },
			clientId: "claimant",
			capabilities: ["attach_snapshot", "event_sequence", "correlated_prompt_lifecycle_v1"],
		} as const satisfies Extract<DaemonCommand, { type: "prepare_recoverable_owned_session_adoption" }>;
		const lifecycle = {
			correlationId,
			phase: "queued" as const,
			kind: "model_prompt" as const,
			revision: 1,
			deliveryCrossed: false,
		};
		const attached = {
			protocol: { name: "prime-agent.daemon" as const, version: 7 },
			activeSessionId,
			snapshot: {
				activeSessionId,
				summary: {
					id: activeSessionId,
					activeSessionId,
					lifecycle: "live" as const,
					activity: "idle" as const,
					isSessionActive: false,
					sessionId,
					cwd: "/tmp/project",
					isStreaming: false,
					isCompacting: false,
					attachedClients: 1,
					messageCount: 0,
					sessionActions: { queuedCount: 0, steering: [], followUps: [] },
				},
				state: { sessionId },
				messages: [],
				lastEventSequence: cursor.sequence,
				lastEventCursor: cursor,
				promptLifecycles: { records: [lifecycle], expired: [] },
			},
			replay: { status: "complete" as const, toSequence: cursor.sequence, toCursor: cursor },
			lastEventSequence: cursor.sequence,
			lastEventCursor: cursor,
			client: { id: "worker-client", capabilities: [...command.capabilities] },
		};
		const receipt = {
			recordId: "record-proof",
			authority: {},
			recoveryHandle: command.recoveryHandle,
			ownershipGeneration: 1,
			phase: "prepared",
			repeated: false,
		};
		const adoption = {
			frames: [],
			bufferedBytes: 0,
			client: { capabilities: new Set<string>(), supportsExtensionUi: false },
		};
		for (const mutate of [
			(value: typeof attached) => ({ ...value, replay: { ...value.replay, status: "partial" as const } }),
			(value: typeof attached) => ({
				...value,
				snapshot: { ...value.snapshot, state: { sessionId: "wrong-session" } },
			}),
			(value: typeof attached) => ({
				...value,
				snapshot: { ...value.snapshot, promptLifecycles: { records: [], expired: [] } },
			}),
		]) {
			const worker = {
				descriptor: { workerId: "worker-proof", lifecycle: "ready" as const },
				client: {
					request: vi.fn(async () => ({
						type: "response",
						command: "attach",
						success: true,
						data: mutate(attached),
					})),
				},
			};
			await expect(
				supervisor.prepareRecoverableOwnedSnapshot(worker, adoption, receipt, command, Date.now() + 1_000),
			).rejects.toThrow("Recoverable owned session adoption is unavailable");
		}
	});

	it("contains a disconnected worker when its unconfirmed final receipt disappears", async () => {
		vi.useFakeTimers();
		const supervisor = supervisorFixture();
		const worker = {
			descriptor: { workerId: "worker-2", ownerClientId: "disconnected-owner" },
			recoveryRecordId: "record-2",
			recoverableFinal: { recordId: "record-2" },
			ownerCleanupTimer: setTimeout(() => {}, 60_000) as NodeJS.Timeout | undefined,
		};
		supervisor.workers.set("worker-2", worker);
		supervisor.clients = new Set();
		supervisor.ownedSessionRecoveryStore = { get: vi.fn(() => undefined), remove: vi.fn() };
		supervisor.stopWorker = vi.fn(async () => undefined);

		supervisor.scheduleRecoverableOwnedConfirmationExpiry(worker, "record-2");
		await vi.advanceTimersByTimeAsync(RECOVERABLE_OWNED_CONFIRMATION_RETENTION_MS + 1);

		expect(supervisor.ownedSessionRecoveryStore.remove).toHaveBeenCalledWith("record-2");
		expect(supervisor.stopWorker).toHaveBeenCalledWith(worker, true, true);
		expect(worker).toMatchObject({
			recoveryRecordId: undefined,
			recoverableFinal: undefined,
			ownerCleanupTimer: undefined,
		});
	});
});
