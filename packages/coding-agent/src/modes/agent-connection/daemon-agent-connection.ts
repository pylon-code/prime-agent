import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ImageContent, ServiceTier, Transport } from "@earendil-works/pi-ai";
import { appendRotatingLog, getAgentLogPath, getDaemonLogPath } from "../../config.js";
import type { AgentSessionMessageReceipt, AgentSessionMessageSafetyStatus } from "../../core/agent-messages.js";
import type { AgentSessionEvent } from "../../core/agent-session.js";
import type { AgentSessionRuntimeConfig } from "../../core/agent-session-config.js";
import type { AgentAutonomousStatus } from "../../core/autonomous.js";
import type { BashResult } from "../../core/bash-executor.js";
import type { CompactionResult } from "../../core/compaction/index.js";
import type { ContextTreeNode } from "../../core/context-tree.js";
import type {
	AgentCronJob,
	AgentHeartbeatDeliveryMode,
	AgentHeartbeatManagementAction,
	AgentHeartbeatUpdateAction,
} from "../../core/cron-jobs.js";
import type { AcpMcpServerConfig } from "../../core/mcp/acp-mcp-types.js";
import {
	createPromptRequestFingerprint,
	isPromptEventAttribution,
	isPromptLifecycleSnapshot,
	isPromptLifecycleStateSnapshot,
	isPromptLifecycleSuccessor,
	isPromptLifecycleTerminal,
	type PromptLifecycleCancellationResult,
	type PromptLifecycleSnapshot,
	type PromptLifecycleStateSnapshot,
} from "../../core/prompt-lifecycle.js";
import type { RefinementResult } from "../../core/refinement/index.js";
import type { DeleteSessionFileResult } from "../../core/session-file-actions.js";
import { SessionAlreadyActiveError } from "../../core/session-lease.js";
import type { SessionStats } from "../../core/session-stats.js";
import {
	DaemonCapabilityUnavailableError,
	type DaemonClient,
	getDaemonSocketCloseReason,
} from "../daemon/daemon-client.js";
import { deserializeDaemonError } from "../daemon/daemon-errors.js";
import {
	collectDaemonClientEnv,
	collectDaemonLaunchEnv,
	DAEMON_SUPPORTED_CLIENT_CAPABILITIES,
	type DaemonAttachResult,
	type DaemonClientCapability,
	type DaemonCommand,
	type DaemonEventCursor,
	type DaemonOutbound,
	type DaemonReplayInfo,
	type DaemonSessionClosedReason,
	type DaemonSessionSnapshot,
	isUnknownDaemonCommandError,
} from "../daemon/daemon-protocol.js";
import type { SessionSummary } from "../daemon/daemon-session-list.js";
import { listDaemonHeartbeats } from "../daemon/heartbeat-catalog.js";
import {
	deleteDaemonSavedSession,
	listDaemonSavedSessions,
	renameDaemonSavedSession,
} from "../daemon/saved-session-catalog.js";
import type {
	AgentConnection,
	AgentConnectionBeforeSessionInvalidateListener,
	AgentConnectionCorrelatedPromptOptions,
	AgentConnectionCorrelatedPromptResult,
	AgentConnectionEvent,
	AgentConnectionEventListener,
	AgentConnectionExecuteBashOptions,
	AgentConnectionExtensionUiResponse,
	AgentConnectionForkOptions,
	AgentConnectionHeadlessCompletionOptions,
	AgentConnectionHeartbeat,
	AgentConnectionModel,
	AgentConnectionModelCatalog,
	AgentConnectionModelCycleResult,
	AgentConnectionNavigateTreeOptions,
	AgentConnectionNavigateTreeResult,
	AgentConnectionNewSessionOptions,
	AgentConnectionPromptOptions,
	AgentConnectionQueuedMessageLane,
	AgentConnectionQueuedMessageMutation,
	AgentConnectionQueuedMessageMutationStatus,
	AgentConnectionQueueMode,
	AgentConnectionQueueState,
	AgentConnectionResourceSnapshot,
	AgentConnectionRlmChildAgentSnapshot,
	AgentConnectionSavedSessionInfo,
	AgentConnectionSavedSessionScope,
	AgentConnectionScopedModel,
	AgentConnectionSessionContext,
	AgentConnectionSessionHeader,
	AgentConnectionSessionInputPause,
	AgentConnectionSessionListCallbacks,
	AgentConnectionSessionTreeFlatNode,
	AgentConnectionSessionTreeNode,
	AgentConnectionSessionWatcher,
	AgentConnectionSideQuestionEvent,
	AgentConnectionSideQuestionTurn,
	AgentConnectionSlashCommand,
	AgentConnectionSnapshot,
	AgentConnectionState,
	AgentConnectionSwitchSessionOptions,
	AgentConnectionToolDefinition,
	AgentConnectionUserMessage,
} from "./types.js";
import { AgentConnectionPromptAdmissionError } from "./types.js";

type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never;
type DaemonCommandBody = DistributiveOmit<DaemonCommand, "id">;
type DaemonSnapshotBegin = Extract<DaemonOutbound, { type: "session_snapshot_begin" }>;
type DaemonSnapshotPurpose = NonNullable<DaemonSnapshotBegin["purpose"]>;
type DaemonSessionReplaced = Extract<DaemonOutbound, { type: "session_replaced" }>;

interface DaemonSnapshotAssembly {
	begin?: DaemonSnapshotBegin;
	chunks: Map<number, AgentMessage[]>;
	settled: boolean;
	attachmentEpoch: number;
	requestAttemptId?: number;
	promise: Promise<DaemonSessionSnapshot>;
	resolve: (snapshot: DaemonSessionSnapshot) => void;
	reject: (error: Error) => void;
	timeout: ReturnType<typeof setTimeout>;
}

interface DaemonSnapshotExpectation {
	id: string;
	activeSessionId: string;
	sessionId: string;
	messageCount: number;
	targetChunkBytes: number;
	purpose: DaemonSnapshotPurpose;
	lastEventSequence: number;
	lastEventCursor?: DaemonEventCursor;
}

interface DaemonSnapshotRequestAttempt {
	id: number;
	activeSessionId: string;
	attachmentEpoch: number;
	candidateSnapshotIds: Set<string>;
	logicalPurpose: DaemonSnapshotPurpose;
	wirePurpose: DaemonSnapshotPurpose;
	state: "requesting" | "receiving" | "succeeded" | "failed";
	expected?: DaemonSnapshotExpectation;
}

interface DaemonRuntimeSnapshotAttempt {
	activeSessionId: string;
	attachmentEpoch: number;
	purpose: "replacement" | "resync";
	state: "receiving" | "retrying" | "succeeded" | "terminal";
	snapshotIds: Set<string>;
	recovery?: Promise<void>;
}

interface CorrelatedPromptRoute {
	activeSessionId: string;
	sessionId: string;
	requestFingerprint: string;
	pending: boolean;
}

interface PendingChunkedReplacement {
	header: DaemonSessionReplaced;
	snapshotId?: string;
	queuedMessages: DaemonOutbound[];
	queuedMessageWeight: number;
}

interface StagedEventProgress {
	lastEventCursor: DaemonEventCursor | undefined;
	lastEventSequence: number | undefined;
	retiredEventGenerations: Set<string>;
}

interface StagedSnapshotCommit extends StagedEventProgress {
	activeSessionId: string;
	attachedSessionId: string;
	attachedSessionFile: string | undefined;
	latestSnapshot: AgentConnectionSnapshot;
	childRosterSequence: number | undefined;
}

export const DAEMON_REFINE_REQUEST_TIMEOUT_MS = 10 * 60 * 1000;
const DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS = 24 * 60 * 60 * 1000;
export const DAEMON_RECONNECT_TIMEOUT_MS = 60_000;
export const DAEMON_SNAPSHOT_TIMEOUT_MS = 30_000;
const MAX_IGNORED_SNAPSHOT_IDS = 128;
const UPDATE_RECONNECT_TIMEOUT_MS = 120000;
const UPDATE_RECONNECT_RETRY_MS = 100;
const MAX_COMPLETED_SNAPSHOTS = 128;
const PROMPT_LIFECYCLE_TERMINAL_RETENTION = 256;
const PROMPT_LIFECYCLE_TOMBSTONE_RETENTION = 256;
const OWNED_SESSION_DISPOSE_RECONNECT_WAIT_MS = 10_000;
const updateTransportReconnects = new WeakMap<DaemonClient, Promise<void>>();

function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatErrorSentence(error: unknown): string {
	const message = (error instanceof Error ? error.message : String(error)).trim();
	if (!message) {
		return "Unknown daemon error.";
	}
	return /[.!?]$/.test(message) ? message : `${message}.`;
}

function isPromptLifecycleCancellationResult(
	value: unknown,
	correlationId: string,
): value is PromptLifecycleCancellationResult {
	if (typeof value !== "object" || value === null || !("status" in value)) return false;
	const result = value as Record<string, unknown>;
	if (result.status === "unknown") {
		return result.ownershipCrossed === "unknown" && result.deliveryCrossed === "unknown";
	}
	if (result.status === "expired") {
		return result.ownershipCrossed === true && typeof result.deliveryCrossed === "boolean";
	}
	if (result.status !== "cancelled" && result.status !== "too_late") return false;
	if (
		result.ownershipCrossed !== true ||
		typeof result.deliveryCrossed !== "boolean" ||
		!isPromptLifecycleSnapshot(result.lifecycle)
	) {
		return false;
	}
	return (
		result.lifecycle.correlationId === correlationId &&
		result.lifecycle.deliveryCrossed === result.deliveryCrossed &&
		(result.status === "cancelled"
			? result.lifecycle.phase === "cancelled" && result.deliveryCrossed === false
			: result.lifecycle.phase !== "cancelled" && (result.deliveryCrossed || result.lifecycle.phase === "failed"))
	);
}

function delayUntilRetryOrOwnerClose(client: DaemonClient, timeoutMs: number): Promise<void> {
	if (client.isClosed) return Promise.resolve();
	return new Promise((resolve) => {
		let settled = false;
		let unsubscribe = () => {};
		const finish = () => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			unsubscribe();
			resolve();
		};
		const timeout = setTimeout(finish, timeoutMs);
		unsubscribe = client.onClose(() => {
			if (client.isClosed) finish();
		});
		if (client.isClosed) finish();
	});
}

function reconnectDaemonTransportAfterUpdate(client: DaemonClient): Promise<void> {
	const existing = updateTransportReconnects.get(client);
	if (existing) {
		return existing;
	}
	const reconnectPromise = Promise.resolve()
		.then(async () => {
			if (client.isClosed) throw new Error("the daemon client was closed by its owner");
			client.disconnectForReconnect("update");
			const deadline = Date.now() + UPDATE_RECONNECT_TIMEOUT_MS;
			let lastError: unknown;
			while (!client.isClosed && Date.now() < deadline) {
				try {
					await client.reconnect(1000);
					if (client.isClosed) throw new Error("the daemon client was closed by its owner");
					return;
				} catch (error) {
					if (client.isClosed) throw new Error("the daemon client was closed by its owner");
					lastError = error;
				}
				await delayUntilRetryOrOwnerClose(client, UPDATE_RECONNECT_RETRY_MS);
			}
			if (client.isClosed) throw new Error("the daemon client was closed by its owner");
			throw lastError ?? new Error("the updated daemon did not become available");
		})
		.finally(() => {
			if (updateTransportReconnects.get(client) === reconnectPromise) {
				updateTransportReconnects.delete(client);
			}
		});
	updateTransportReconnects.set(client, reconnectPromise);
	return reconnectPromise;
}

export interface DaemonAgentConnectionOptions {
	closeClientOnDispose?: boolean;
	/** Restart/probe the detached supervisor after a transient socket loss. */
	recoverDaemon?: () => Promise<void>;
	/** Bound supervisor recovery before surfacing a fatal connection error. */
	reconnectTimeoutMs?: number;
	/** Bound an incomplete streamed snapshot before failing the attach or resync. */
	snapshotTimeoutMs?: number;
	/**
	 * Send this client's allowlisted env (herdr pane identity) with attach so
	 * an env-less session (e.g. cron-created) adopts it. Set only by the
	 * primary interactive connection — the daemon adopts-if-absent, never
	 * rebinds, so watchers must not send env at all.
	 */
	sendClientEnv?: boolean;
	/** Advertise support for interactive extension dialogs. */
	supportsExtensionUi?: boolean;
	/** Dispose the connection by stopping its hidden worker instead of detaching. */
	ownedSession?: boolean;
	/** Fresh runtime context used only if the owned worker must be relaunched. */
	ownedSessionRecoveryConfig?: AgentSessionRuntimeConfig;
	/** Require the target worker to have been created with telemetry disabled. */
	telemetryDisabled?: true;
}

/**
 * AgentConnection adapter for the local daemon JSONL socket transport.
 *
 * InteractiveMode depends only on AgentConnection; local socket ownership and
 * daemon command details stay inside this adapter.
 */
export function buildSessionTreeFromFlatNodes(
	flatNodes: readonly AgentConnectionSessionTreeFlatNode[],
): AgentConnectionSessionTreeNode[] {
	const byId = new Map<string, AgentConnectionSessionTreeNode>();
	const roots: AgentConnectionSessionTreeNode[] = [];
	for (const flatNode of flatNodes) {
		byId.set(flatNode.entry.id, { ...flatNode, children: [] });
	}
	for (const flatNode of flatNodes) {
		const entry = flatNode.entry;
		const node = byId.get(entry.id)!;
		const parent = entry.parentId === null || entry.parentId === entry.id ? undefined : byId.get(entry.parentId);
		if (parent) parent.children.push(node);
		else roots.push(node);
	}
	// Match SessionManager.getTree() ordering without recursively walking deep
	// chains: every node is already indexed, so sort each sibling array directly.
	for (const node of byId.values()) {
		node.children.sort(
			(left, right) => new Date(left.entry.timestamp).getTime() - new Date(right.entry.timestamp).getTime(),
		);
	}
	return roots;
}

const sharedAttachmentOwners = new WeakMap<DaemonClient, Map<string, DaemonAgentConnection>>();
const sharedAttachmentMutationTails = new WeakMap<DaemonClient, Promise<void>>();
const MAX_PENDING_NEGOTIATED_RUNTIME_FRAMES = 128;
const MAX_PENDING_NEGOTIATED_RUNTIME_FRAME_WEIGHT = 256 * 1024;

function boundedJsonStructuralWeight(value: unknown, maxWeight: number): number | undefined {
	let weight = 0;
	const values: unknown[] = [value];
	const seen = new WeakSet<object>();
	const add = (amount: number): boolean => {
		if (!Number.isSafeInteger(amount) || amount < 0 || amount > maxWeight - weight) return false;
		weight += amount;
		return true;
	};
	while (values.length > 0) {
		const current = values.pop();
		if (current === null || current === undefined) {
			if (!add(8)) return undefined;
			continue;
		}
		switch (typeof current) {
			case "boolean":
			case "number":
				if (!add(8)) return undefined;
				continue;
			case "string":
				if (!add(16 + current.length * 2)) return undefined;
				continue;
			case "object":
				break;
			default:
				return undefined;
		}
		if (seen.has(current)) return undefined;
		seen.add(current);
		if (Array.isArray(current)) {
			if (!add(24 + current.length * 8)) return undefined;
			for (let index = 0; index < current.length; index++) values.push(current[index]);
			continue;
		}
		if (!add(32)) return undefined;
		try {
			for (const key in current) {
				if (!Object.hasOwn(current, key)) continue;
				if (!add(16 + key.length * 2)) return undefined;
				values.push((current as Record<string, unknown>)[key]);
			}
		} catch {
			return undefined;
		}
	}
	return weight;
}

function isCorrelatedPromptRuntimeFrame(message: DaemonOutbound): boolean {
	switch (message.type) {
		case "prompt_lifecycle":
			return true;
		case "session_event":
			return message.attribution !== undefined || message.event.promptCorrelationId !== undefined;
		case "extension_ui_request":
		case "extension_error":
			return message.attribution !== undefined;
		default:
			return false;
	}
}

export class DaemonAgentConnection implements AgentConnection {
	private readonly listeners = new Set<AgentConnectionEventListener>();
	private readonly unsubscribeDaemonMessages: () => void;
	private readonly unsubscribeDaemonClose: () => void;
	private readonly clientId = `daemon-agent-connection:${randomUUID()}`;
	private readonly sessionInputPauses = new Map<string, Promise<AgentConnectionSessionInputPause>>();
	private readonly correlatedPromptRoutes = new Map<string, CorrelatedPromptRoute>();
	private sessionInputPauseGeneration = 0;
	private ownedSessionPromotionTail = Promise.resolve();
	private replacementMessageTail: Promise<void> | undefined;
	private pendingChunkedReplacement: PendingChunkedReplacement | undefined;
	private discardUnsolicitedRuntimeSnapshots = false;
	private replacementReconciliationFailed = false;
	private lastEventCursor: DaemonEventCursor | undefined;
	private readonly retiredEventGenerations = new Set<string>();
	private lastEventSequence: number | undefined;
	private childRosterSequence: number | undefined;
	private latestSnapshot: AgentConnectionSnapshot | undefined;
	private latestSnapshotIsFresh = false;
	private attachedSessionId: string | undefined;
	private attachedSessionFile: string | undefined;
	private daemonLogPath: string | undefined;
	private updateRestartPending = false;
	private updateReconnectFailed = false;
	private terminalCloseEmitted = false;
	private updateReconnectPromise?: Promise<void>;
	private readonly activeSideQuestionIds = new Set<string>();
	private readonly snapshotAssemblies = new Map<string, DaemonSnapshotAssembly>();
	private readonly completedSnapshots = new Map<string, DaemonSessionSnapshot>();
	private readonly completedSnapshotAttemptIds = new Map<string, number | undefined>();
	private readonly pendingReattachActiveSessionIds = new Set<string>();
	private runtimeSnapshotAttempt: DaemonRuntimeSnapshotAttempt | undefined;
	private readonly ignoredSnapshotIds = new Set<string>();
	private readonly snapshotRequestAttempts = new Map<number, DaemonSnapshotRequestAttempt>();
	private snapshotRequestAttemptSequence = 0;
	private attachmentEpoch = 0;
	private attachmentAdmissionRevision = 0;
	private attachmentInvalidationRevision = 0;
	private negotiatedCapabilities: ReadonlySet<DaemonClientCapability> = new Set();
	private negotiatedTransportGeneration: number | undefined;
	private negotiatedRuntimeCapabilities: ReadonlySet<DaemonClientCapability> = new Set();
	private negotiatedRuntimeTransportGeneration: number | undefined;
	private pendingNegotiatedRuntimeFrames: DaemonOutbound[] = [];
	private pendingNegotiatedRuntimeFrameWeight = 0;
	private sharedAttachmentActiveSessionId: string | undefined;
	private reconnectPromise?: Promise<void>;
	private readonly definitiveRequestErrors = new WeakSet<Error>();
	private disposing = false;
	private disposed = false;

	private dispatchDaemonMessage(message: DaemonOutbound): void {
		if (this.replacementReconciliationFailed) return;
		if (this.deferNegotiatedRuntimeFrame(message)) return;
		const pendingChunkedReplacement = this.pendingChunkedReplacement;
		if (pendingChunkedReplacement && !isSnapshotTransferMessage(message)) {
			if (
				message.type === "session_closed" &&
				message.activeSessionId === pendingChunkedReplacement.header.activeSessionId
			) {
				this.retirePendingChunkedReplacement();
			} else {
				const remainingWeight =
					MAX_PENDING_NEGOTIATED_RUNTIME_FRAME_WEIGHT - pendingChunkedReplacement.queuedMessageWeight;
				const messageWeight = boundedJsonStructuralWeight(message, remainingWeight);
				if (
					pendingChunkedReplacement.queuedMessages.length >= MAX_PENDING_NEGOTIATED_RUNTIME_FRAMES ||
					messageWeight === undefined
				) {
					this.failClosedReplacementReconciliation();
				} else {
					pendingChunkedReplacement.queuedMessages.push(message);
					pendingChunkedReplacement.queuedMessageWeight += messageWeight;
				}
				return;
			}
		}
		const fencesReplacement =
			message.type === "session_replaced" &&
			!message.snapshotFollows &&
			this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1");
		const previous = this.replacementMessageTail;
		const handling = previous
			? previous.then(() => {
					if (!this.replacementReconciliationFailed) return this.handleDaemonMessage(message);
				})
			: this.handleDaemonMessage(message);
		const settled = handling.catch((error: unknown) => {
			try {
				appendRotatingLog(
					getAgentLogPath(),
					`[${new Date().toISOString()}] daemon-message: ignored ${message.type} failure: ${String(error)}`,
				);
			} catch {
				// Logging failure must not turn an isolated message error into a connection failure.
			}
			if (fencesReplacement) this.failClosedReplacementReconciliation();
		});
		if (!previous && !fencesReplacement) return;
		this.replacementMessageTail = settled;
		void settled.finally(() => {
			if (this.replacementMessageTail === settled) this.replacementMessageTail = undefined;
		});
	}

	private failClosedReplacementReconciliation(): void {
		this.invalidateNegotiatedCapabilityProof();
		this.replacementReconciliationFailed = true;
		this.retirePendingChunkedReplacement();
		this.latestSnapshotIsFresh = false;
		if (this.terminalCloseEmitted) return;
		this.terminalCloseEmitted = true;
		void this.emit({
			type: "closed",
			error: "Daemon replacement lifecycle state could not be reconciled.",
		});
	}

	constructor(
		private readonly client: DaemonClient,
		private activeSessionId: string,
		private readonly options: DaemonAgentConnectionOptions = {},
	) {
		if (options.recoverDaemon) {
			this.client.enableRequestRecovery();
		}
		this.unsubscribeDaemonMessages = this.client.onMessage((message) => {
			this.dispatchDaemonMessage(message);
		});
		this.captureDaemonLogPath();
		this.unsubscribeDaemonClose = this.client.onClose((error) => {
			this.retirePendingChunkedReplacement();
			this.invalidateNegotiatedCapabilityProof();
			const invalidatedInputPause = this.sessionInputPauses.size > 0;
			this.sessionInputPauses.clear();
			this.sessionInputPauseGeneration++;
			this.rejectSnapshotAssemblies(error, true);
			if (this.disposed || this.terminalCloseEmitted) {
				return;
			}
			if (this.client.isClosed) {
				this.emitOwnerClosedTerminal();
				return;
			}
			if (invalidatedInputPause) {
				this.terminalCloseEmitted = true;
				void this.emit({
					type: "closed",
					error: "Daemon connection closed while session input was paused; the fence was invalidated.",
				});
				return;
			}
			const closeReason = getDaemonSocketCloseReason(error);
			if (closeReason === "shutdown") {
				this.terminalCloseEmitted = true;
				void this.emit({ type: "closed", error: this.formatDaemonSessionClosedError("shutdown") });
				return;
			}
			if ((this.updateRestartPending || closeReason === "update") && !this.updateReconnectFailed) {
				this.updateRestartPending = true;
				void this.reconnectAfterUpdate();
				return;
			}
			if (this.options.recoverDaemon) {
				void this.reconnect(error);
				return;
			}
			this.terminalCloseEmitted = true;
			void this.emit({ type: "closed", error: this.formatDaemonConnectionClosedError(error) });
		});
	}

	static async attach(
		client: DaemonClient,
		activeSessionId: string,
		options?: DaemonAgentConnectionOptions,
	): Promise<DaemonAgentConnection> {
		const connection = new DaemonAgentConnection(client, activeSessionId, options);
		try {
			await connection.attach();
			return connection;
		} catch (error) {
			await connection.dispose();
			throw error;
		}
	}

	async attach(): Promise<void> {
		if (this.disposing || this.disposed) throw new Error("Daemon connection is disposing");
		await this.attachSession(this.activeSessionId, this.lastEventCursor, false);
	}

	private attachSession(
		requestedActiveSessionId: string,
		resumeCursor: DaemonEventCursor | undefined,
		resetEventProgress: boolean,
		maxAttempts = 2,
		logicalPurpose: DaemonSnapshotPurpose = "attach",
		allowWhileDisposing = false,
	): Promise<void> {
		const admissionRevision = ++this.attachmentAdmissionRevision;
		this.retirePendingChunkedReplacement();
		this.invalidateNegotiatedCapabilityProof();
		if (this.replacementReconciliationFailed) {
			return Promise.reject(new Error("Daemon connection replacement reconciliation has failed"));
		}
		return this.withSharedAttachmentMutation(async () => {
			try {
				await this.attachSessionUnserialized(
					requestedActiveSessionId,
					resumeCursor,
					resetEventProgress,
					maxAttempts,
					logicalPurpose,
					allowWhileDisposing,
					admissionRevision,
				);
			} catch (error) {
				this.invalidateNegotiatedCapabilityProof();
				throw error;
			}
		});
	}

	private async attachSessionUnserialized(
		requestedActiveSessionId: string,
		resumeCursor: DaemonEventCursor | undefined,
		resetEventProgress: boolean,
		maxAttempts: number,
		logicalPurpose: DaemonSnapshotPurpose,
		allowWhileDisposing: boolean,
		admissionRevision: number,
	): Promise<void> {
		if (admissionRevision !== this.attachmentAdmissionRevision) {
			throw new Error("Daemon connection attachment changed during attach");
		}
		if (this.disposed || (this.disposing && !allowWhileDisposing)) {
			throw new Error("Daemon connection is disposing");
		}
		if (this.replacementReconciliationFailed) {
			throw new Error("Daemon connection replacement reconciliation has failed");
		}
		const supportsExtensionUi = this.options.supportsExtensionUi !== false;
		const capabilities: DaemonClientCapability[] = [
			"attach_snapshot",
			"event_sequence",
			...(supportsExtensionUi ? (["extension_ui"] as const) : []),
			"slim_attach",
			"chunked_snapshot",
			...(this.supportsCorrelatedPromptLifecycle() ? (["correlated_prompt_lifecycle_v1"] as const) : []),
			...(this.options.ownedSession ? (["client_owned_sessions"] as const) : []),
		];
		const attachmentEpoch = this.advanceAttachmentEpoch();
		this.reserveSharedAttachment(requestedActiveSessionId);
		const invalidationRevision = this.attachmentInvalidationRevision;
		const transportGeneration = this.client.getTransportGeneration();
		let result!: SessionSummary | DaemonAttachResult;
		let streamedSnapshot: DaemonSessionSnapshot | undefined;
		for (let attemptIndex = 0; attemptIndex < maxAttempts; attemptIndex++) {
			const requestAttempt = this.startSnapshotRequestAttempt(
				requestedActiveSessionId,
				attachmentEpoch,
				logicalPurpose,
				"attach",
			);
			try {
				result = await this.requestData<SessionSummary | DaemonAttachResult>({
					type: "attach",
					activeSessionId: requestedActiveSessionId,
					snapshotGenerationNonce: randomUUID(),
					supportsExtensionUi,
					clientId: this.clientId,
					capabilities,
					env: this.options.sendClientEnv ? collectDaemonClientEnv() : undefined,
					launchEnv: this.options.ownedSession ? collectDaemonLaunchEnv() : undefined,
					...(this.options.ownedSession &&
					this.options.ownedSessionRecoveryConfig &&
					this.client.supportsServerCapability("owned_session_recovery_context")
						? { recoveryConfig: this.options.ownedSessionRecoveryConfig }
						: {}),
					telemetryDisabled: this.options.telemetryDisabled,
					resumeCursor:
						resumeCursor === undefined
							? undefined
							: { activeSessionId: requestedActiveSessionId, ...resumeCursor },
				});
				this.assertAttachmentCommit(
					requestedActiveSessionId,
					attachmentEpoch,
					invalidationRevision,
					transportGeneration,
					admissionRevision,
					allowWhileDisposing,
				);
				if ("snapshot" in result && result.snapshotStream) {
					this.bindSnapshotExpectation(requestAttempt, result);
					requestAttempt.state = "receiving";
					streamedSnapshot = await this.waitForSnapshot(result.snapshotStream.id, requestAttempt);
				} else {
					streamedSnapshot = undefined;
				}
				requestAttempt.state = "succeeded";
				break;
			} catch (error) {
				requestAttempt.state = "failed";
				if (
					attemptIndex === maxAttempts - 1 ||
					this.disposing ||
					this.disposed ||
					this.attachmentAdmissionRevision !== admissionRevision ||
					this.attachmentEpoch !== attachmentEpoch ||
					this.attachmentInvalidationRevision !== invalidationRevision ||
					this.client.getTransportGeneration() !== transportGeneration ||
					sharedAttachmentOwners.get(this.client)?.get(requestedActiveSessionId) !== this
				) {
					throw error;
				}
			} finally {
				this.finishSnapshotRequestAttempt(requestAttempt);
			}
		}
		const negotiatedCapabilities =
			"snapshot" in result
				? this.negotiatedCapabilitiesFromAttach(result, capabilities)
				: new Set<DaemonClientCapability>();
		const nextActiveSessionId = getAttachActiveSessionId(result);
		this.retargetSharedAttachment(requestedActiveSessionId, nextActiveSessionId);

		if ("snapshot" in result) {
			const snapshot = streamedSnapshot ?? result.snapshot;
			let staged: StagedSnapshotCommit;
			try {
				validateDaemonSnapshotIdentity(result.snapshot, nextActiveSessionId);
				const includePromptLifecycles = negotiatedCapabilities.has("correlated_prompt_lifecycle_v1");
				mapDaemonSessionSnapshot(result.snapshot, undefined, includePromptLifecycles);
				staged = this.stageSnapshotCommit(snapshot, {
					purpose: "attach",
					includePromptLifecycles,
					envelopeActiveSessionId: nextActiveSessionId,
					expectedState: result.snapshot.state,
					replay: result.replay,
					resetEventProgress,
					progress: [
						{ cursor: getAttachLastEventCursor(result), sequence: getAttachLastEventSequence(result) },
						{ cursor: snapshot.lastEventCursor, sequence: snapshot.lastEventSequence },
					],
				});
			} catch (error) {
				if (negotiatedCapabilities.has("correlated_prompt_lifecycle_v1")) {
					await this.emit({ type: "correlated_prompt_protocol_violation" });
				}
				throw error;
			}
			this.assertAttachmentCommit(
				nextActiveSessionId,
				attachmentEpoch,
				invalidationRevision,
				transportGeneration,
				admissionRevision,
				allowWhileDisposing,
			);
			this.commitStagedSnapshot(staged);
			if (!this.disposing) this.publishNegotiatedCapabilityProof(negotiatedCapabilities, transportGeneration);
		} else {
			validateSummaryIdentity(result, nextActiveSessionId);
			this.assertAttachmentCommit(
				nextActiveSessionId,
				attachmentEpoch,
				invalidationRevision,
				transportGeneration,
				admissionRevision,
				allowWhileDisposing,
			);
			this.activeSessionId = nextActiveSessionId;
			this.attachedSessionId = result.sessionId;
			this.attachedSessionFile = result.sessionFile;
			this.latestSnapshot = undefined;
			this.latestSnapshotIsFresh = false;
			if (!this.disposing) this.publishNegotiatedCapabilityProof(negotiatedCapabilities, transportGeneration);
		}
		this.captureDaemonLogPath();
		this.updateReconnectFailed = false;
		this.terminalCloseEmitted = false;
	}

	subscribe(listener: AgentConnectionEventListener): () => void {
		this.listeners.add(listener);
		return () => {
			this.listeners.delete(listener);
		};
	}

	onBeforeSessionInvalidate(_listener: AgentConnectionBeforeSessionInvalidateListener): () => void {
		return () => {};
	}

	async getState(): Promise<AgentConnectionState> {
		if (this.latestSnapshotIsFresh && this.latestSnapshot) {
			return this.latestSnapshot.state;
		}
		return this.requestData<AgentConnectionState>({
			type: "get_connection_state",
			activeSessionId: this.activeSessionId,
		});
	}

	async getInitialSnapshot(): Promise<AgentConnectionSnapshot> {
		const snapshotRecovery = this.runtimeSnapshotAttempt?.recovery;
		if (snapshotRecovery) await snapshotRecovery;
		if (this.latestSnapshotIsFresh && this.latestSnapshot) {
			return this.latestSnapshot;
		}
		// The session tree is intentionally not fetched here: it is large on long
		// sessions and only needed when the user opens the tree/branch selector.
		// getSessionTree() fetches it lazily via get_session_tree on first use.
		const snapshotCursor = this.lastEventCursor;
		const snapshotSequence = this.lastEventSequence;
		const [state, messagesData, sessionContextData, promptLifecycles] = await Promise.all([
			this.requestData<AgentConnectionState>({
				type: "get_connection_state",
				activeSessionId: this.activeSessionId,
			}),
			this.requestData<{ messages: AgentMessage[] }>({
				type: "get_messages",
				activeSessionId: this.activeSessionId,
			}),
			this.requestData<{ context: AgentConnectionSessionContext }>({
				type: "get_session_context",
				activeSessionId: this.activeSessionId,
			}),
			this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")
				? this.getPromptLifecycles()
				: Promise.resolve(undefined),
		]);
		const observedSnapshot = this.latestSnapshot;
		const sameGeneration = observedSnapshot?.state.sessionId === state.sessionId;
		const children = sameGeneration ? observedSnapshot.children : undefined;
		const streamingMessage = sameGeneration ? observedSnapshot.streamingMessage : undefined;
		const reconciledPromptLifecycles = promptLifecycles
			? mergePromptLifecycleStates(promptLifecycles, sameGeneration ? observedSnapshot.promptLifecycles : undefined)
			: undefined;
		const nextSnapshot: AgentConnectionSnapshot = {
			state,
			messages: messagesData.messages,
			sessionContext: sessionContextData.context,
			...(children ? { children } : {}),
			...(streamingMessage ? { streamingMessage } : {}),
			...(reconciledPromptLifecycles ? { promptLifecycles: reconciledPromptLifecycles } : {}),
		};
		const generationChanged = this.attachedSessionId !== undefined && this.attachedSessionId !== state.sessionId;
		this.prunePromptRoutes(reconciledPromptLifecycles, this.activeSessionId, state.sessionId, generationChanged);
		this.latestSnapshot = nextSnapshot;
		if (snapshotSequence !== undefined) {
			this.latestSnapshot.lastEventSequence = snapshotSequence;
		}
		if (snapshotCursor) {
			this.latestSnapshot.lastEventCursor = snapshotCursor;
		}
		this.latestSnapshotIsFresh =
			snapshotSequence === this.lastEventSequence &&
			snapshotCursor?.generation === this.lastEventCursor?.generation &&
			snapshotCursor?.sequence === this.lastEventCursor?.sequence;
		return this.latestSnapshot;
	}

	async getRlmChildSnapshots(): Promise<AgentConnectionRlmChildAgentSnapshot[]> {
		if (!this.client.supportsServerCapability("authoritative_child_roster")) {
			throw new DaemonCapabilityUnavailableError("get_rlm_children", "authoritative_child_roster");
		}
		const data = await this.requestData<{
			children: AgentConnectionRlmChildAgentSnapshot[];
			eventSequence: number;
		}>({ type: "get_rlm_children", activeSessionId: this.activeSessionId });
		if (!Array.isArray(data.children) || !Number.isInteger(data.eventSequence)) {
			throw new Error("Daemon returned an invalid child roster");
		}
		if ((this.childRosterSequence ?? -1) > data.eventSequence) {
			return this.latestSnapshot?.children ?? data.children;
		}
		this.childRosterSequence = data.eventSequence;
		if (this.latestSnapshot) {
			this.latestSnapshot = { ...this.latestSnapshot, children: data.children };
		}
		return data.children;
	}

	async getMessages(): Promise<AgentMessage[]> {
		if (this.latestSnapshotIsFresh && this.latestSnapshot) {
			return this.latestSnapshot.messages;
		}
		const data = await this.requestData<{ messages: AgentMessage[] }>({
			type: "get_messages",
			activeSessionId: this.activeSessionId,
		});
		return data.messages;
	}

	async getSessionHeader(): Promise<AgentConnectionSessionHeader | undefined> {
		const data = await this.requestData<{ header?: AgentConnectionSessionHeader | null }>({
			type: "get_session_header",
			activeSessionId: this.activeSessionId,
		});
		return data.header ?? undefined;
	}

	async getCommands(): Promise<AgentConnectionSlashCommand[]> {
		const data = await this.requestData<{ commands: AgentConnectionSlashCommand[] }>({
			type: "get_commands",
			activeSessionId: this.activeSessionId,
		});
		return data.commands;
	}

	async getResourceSnapshot(): Promise<AgentConnectionResourceSnapshot> {
		return this.requestData<AgentConnectionResourceSnapshot>({
			type: "get_resource_snapshot",
			activeSessionId: this.activeSessionId,
		});
	}

	supportsAcpMcpServers(): boolean {
		return this.client.supportsServerCapability("acp_mcp_servers");
	}

	/**
	 * Whether the daemon proved this client capability for the current committed
	 * attachment. This is false while attach/reattach is pending and after the
	 * attachment is invalidated. Server hello offers alone are not proof.
	 */
	supportsNegotiatedCapability(capability: DaemonClientCapability): boolean {
		if (
			this.disposing ||
			this.disposed ||
			this.replacementReconciliationFailed ||
			this.negotiatedTransportGeneration !== this.client.getTransportGeneration()
		) {
			return false;
		}
		const activeSessionId = this.sharedAttachmentActiveSessionId;
		return (
			activeSessionId !== undefined &&
			sharedAttachmentOwners.get(this.client)?.get(activeSessionId) === this &&
			this.negotiatedCapabilities.has(capability)
		);
	}

	private supportsNegotiatedRuntimeCapability(capability: DaemonClientCapability): boolean {
		const activeSessionId = this.sharedAttachmentActiveSessionId;
		return (
			!this.disposed &&
			!this.replacementReconciliationFailed &&
			activeSessionId !== undefined &&
			sharedAttachmentOwners.get(this.client)?.get(activeSessionId) === this &&
			this.negotiatedRuntimeTransportGeneration === this.client.getTransportGeneration() &&
			this.negotiatedRuntimeCapabilities.has(capability)
		);
	}

	/** Server-offer evidence used to construct the pre-attach capability list. */
	supportsCorrelatedPromptLifecycle(): boolean {
		return this.client.supportsServerCapability("correlated_prompt_lifecycle_v1");
	}

	async getPromptLifecycles(): Promise<PromptLifecycleStateSnapshot> {
		if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
			throw new DaemonCapabilityUnavailableError("get_prompt_lifecycles", "correlated_prompt_lifecycle_v1");
		}
		return this.getPromptLifecyclesFor(this.activeSessionId, this.attachedSessionId);
	}

	private async getPromptLifecyclesFor(
		activeSessionId: string,
		sessionId: string | undefined,
	): Promise<PromptLifecycleStateSnapshot> {
		if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
			throw new DaemonCapabilityUnavailableError("get_prompt_lifecycles", "correlated_prompt_lifecycle_v1");
		}
		if (!sessionId) throw new Error("Prompt lifecycle query requires an attached session generation");
		const snapshot = await this.requestData<PromptLifecycleStateSnapshot>({
			type: "get_prompt_lifecycles",
			activeSessionId,
			sessionId,
		});
		if (!isPromptLifecycleStateSnapshot(snapshot)) {
			throw new Error("Daemon returned invalid prompt lifecycle state");
		}
		return snapshot;
	}

	async submitCorrelatedPrompt(
		message: string,
		options: AgentConnectionCorrelatedPromptOptions,
	): Promise<AgentConnectionCorrelatedPromptResult> {
		if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
			throw new DaemonCapabilityUnavailableError("submit_correlated_prompt", "correlated_prompt_lifecycle_v1");
		}
		if (options.signal?.aborted) {
			throw new AgentConnectionPromptAdmissionError("Prompt admission was cancelled.", "cancelled");
		}
		const sessionId = this.attachedSessionId;
		if (!sessionId) throw new Error("Correlated prompt submission requires an attached session generation");
		const requestFingerprint = createPromptRequestFingerprint({
			message,
			images: options.images,
			queueIfBusy: options.queueIfBusy,
		});
		const existingRoute = this.correlatedPromptRoutes.get(options.correlationId);
		if (
			existingRoute &&
			(existingRoute.activeSessionId !== this.activeSessionId ||
				existingRoute.sessionId !== sessionId ||
				existingRoute.requestFingerprint !== requestFingerprint)
		) {
			throw new Error("Prompt correlation id is reserved for another session generation or request");
		}
		const route =
			existingRoute ??
			({
				activeSessionId: this.activeSessionId,
				sessionId,
				requestFingerprint,
				pending: true,
			} satisfies CorrelatedPromptRoute);
		if (!existingRoute) this.correlatedPromptRoutes.set(options.correlationId, route);
		const onAbort = () => {
			void this.cancelPromptLifecycle(options.correlationId).catch(() => undefined);
		};
		options.signal?.addEventListener("abort", onAbort, { once: true });
		try {
			const result = await this.requestData<AgentConnectionCorrelatedPromptResult>(
				{
					type: "submit_correlated_prompt",
					activeSessionId: this.activeSessionId,
					sessionId,
					correlationId: options.correlationId,
					message,
					images: options.images,
					queueIfBusy: options.queueIfBusy,
				},
				DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS,
			);
			if (
				typeof result !== "object" ||
				result === null ||
				typeof result.duplicate !== "boolean" ||
				!isPromptLifecycleSnapshot(result.lifecycle) ||
				result.lifecycle.correlationId !== options.correlationId
			) {
				throw new Error("Daemon returned an invalid correlated prompt result");
			}
			const lifecycle = this.observePromptLifecycleResponse(result.lifecycle);
			route.pending = false;
			if (options.signal?.aborted) {
				const cancellation = await this.cancelPromptLifecycle(options.correlationId);
				if (cancellation.status === "cancelled") {
					throw new AgentConnectionPromptAdmissionError("Prompt admission was cancelled.", "cancelled");
				}
				if (cancellation.status === "unknown") {
					throw new Error("Correlated prompt cancellation could not be reconciled after reconnect");
				}
			}
			return { ...result, lifecycle };
		} catch (error) {
			if (!existingRoute && route.pending && error instanceof Error && this.definitiveRequestErrors.has(error)) {
				this.correlatedPromptRoutes.delete(options.correlationId);
			}
			throw error;
		} finally {
			options.signal?.removeEventListener("abort", onAbort);
		}
	}

	async cancelPromptLifecycle(correlationId: string): Promise<PromptLifecycleCancellationResult> {
		if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
			throw new DaemonCapabilityUnavailableError("cancel_correlated_prompt", "correlated_prompt_lifecycle_v1");
		}
		const cachedLifecycle = this.latestSnapshot?.promptLifecycles;
		const cachedInAttachedGeneration =
			this.attachedSessionId !== undefined && this.latestSnapshot?.state.sessionId === this.attachedSessionId;
		const currentGenerationProvesCorrelation =
			cachedInAttachedGeneration &&
			(cachedLifecycle?.records.some((record) => record.correlationId === correlationId) === true ||
				cachedLifecycle?.expired.some((tombstone) => tombstone.correlationId === correlationId) === true);
		const route =
			this.correlatedPromptRoutes.get(correlationId) ??
			(this.attachedSessionId && currentGenerationProvesCorrelation
				? { activeSessionId: this.activeSessionId, sessionId: this.attachedSessionId }
				: undefined);
		if (!route) {
			return { status: "unknown", ownershipCrossed: "unknown", deliveryCrossed: "unknown" };
		}
		const result = await this.requestData<PromptLifecycleCancellationResult>({
			type: "cancel_correlated_prompt",
			activeSessionId: route.activeSessionId,
			sessionId: route.sessionId,
			correlationId,
		});
		if (!isPromptLifecycleCancellationResult(result, correlationId)) {
			throw new Error("Daemon returned an invalid correlated prompt cancellation result");
		}
		return result;
	}

	async replaceAcpMcpServers(servers: readonly AcpMcpServerConfig[], ownerId: string): Promise<void> {
		if (!this.supportsAcpMcpServers()) {
			throw new DaemonCapabilityUnavailableError("replace_acp_mcp_servers", "acp_mcp_servers");
		}
		await this.requestOk({
			type: "replace_acp_mcp_servers",
			activeSessionId: this.activeSessionId,
			ownerId,
			servers: [...servers],
		});
	}

	async releaseAcpMcpServers(ownerId: string, _serverNames: readonly string[]): Promise<void> {
		await this.replaceAcpMcpServers([], ownerId);
	}

	async getAvailableModels(): Promise<AgentConnectionModel[]> {
		const data = await this.requestData<{ models: AgentConnectionModel[] }>({
			type: "get_available_models",
			activeSessionId: this.activeSessionId,
		});
		return data.models;
	}

	async getModelCatalog(): Promise<AgentConnectionModelCatalog> {
		if (!this.client.supportsServerCapability("model_catalog")) {
			const models = await this.getAvailableModels();
			return {
				models,
				configuredProviders: [...new Set(models.map((model) => model.provider))],
			};
		}
		return this.requestData<AgentConnectionModelCatalog>({
			type: "get_model_catalog",
			activeSessionId: this.activeSessionId,
		});
	}

	async getSessionStats(): Promise<SessionStats> {
		return this.requestData<SessionStats>({
			type: "get_session_stats",
			activeSessionId: this.activeSessionId,
		});
	}

	async getContextTree(): Promise<ContextTreeNode> {
		return this.requestData<ContextTreeNode>({
			type: "get_context_tree",
			activeSessionId: this.activeSessionId,
		});
	}

	async getSessionContext(): Promise<AgentConnectionSessionContext> {
		if (this.latestSnapshotIsFresh && this.latestSnapshot?.sessionContext) {
			return this.latestSnapshot.sessionContext;
		}
		const data = await this.requestData<{ context: AgentConnectionSessionContext }>({
			type: "get_session_context",
			activeSessionId: this.activeSessionId,
		});
		return data.context;
	}

	async getSessionTree(): Promise<{ tree: AgentConnectionSessionTreeNode[]; leafId: string | null }> {
		if (this.latestSnapshotIsFresh && this.latestSnapshot?.sessionTree) {
			return this.latestSnapshot.sessionTree;
		}
		const data = await this.requestData<{
			flatNodes: AgentConnectionSessionTreeFlatNode[];
			leafId: string | null;
		}>({
			type: "get_session_tree",
			activeSessionId: this.activeSessionId,
		});
		return { tree: buildSessionTreeFromFlatNodes(data.flatNodes), leafId: data.leafId };
	}

	async listSavedSessions(
		scope: AgentConnectionSavedSessionScope,
		callbacks?: AgentConnectionSessionListCallbacks,
	): Promise<AgentConnectionSavedSessionInfo[]> {
		return listDaemonSavedSessions(this.client, { activeSessionId: this.activeSessionId }, scope, callbacks);
	}

	async getQueue(): Promise<AgentConnectionQueueState> {
		return this.requestData<AgentConnectionQueueState>({
			type: "get_queue",
			activeSessionId: this.activeSessionId,
		});
	}

	async mutateQueuedMessage(
		lane: AgentConnectionQueuedMessageLane,
		index: number,
		expectedText: string,
		mutation: AgentConnectionQueuedMessageMutation,
	): Promise<AgentConnectionQueuedMessageMutationStatus> {
		if (!this.client.supportsServerCapability("queue_message_mutation")) return "unsupported";
		const data = await this.requestData<{ status: AgentConnectionQueuedMessageMutationStatus }>({
			type: "mutate_queued_message",
			activeSessionId: this.activeSessionId,
			lane,
			index,
			expectedText,
			mutation,
		});
		return data.status;
	}

	async clearQueue(): Promise<AgentConnectionQueueState> {
		return this.requestData<AgentConnectionQueueState>({
			type: "clear_queue",
			activeSessionId: this.activeSessionId,
		});
	}

	async abortAndClearQueue(): Promise<AgentConnectionQueueState> {
		try {
			return await this.requestData<AgentConnectionQueueState>({
				type: "abort_and_clear_queue",
				activeSessionId: this.activeSessionId,
			});
		} catch (error) {
			if (isUnknownDaemonCommandError(error, "abort_and_clear_queue")) {
				throw new Error("the daemon is running an older build; restart the daemon and try again");
			}
			throw error;
		}
	}

	async acquireSessionInputPause(leaseKey: string): Promise<AgentConnectionSessionInputPause> {
		if (this.terminalCloseEmitted) throw new Error("Daemon connection is closed; cannot acquire an input pause.");
		const activeSessionId = this.activeSessionId;
		const generation = this.sessionInputPauseGeneration;
		const acquisitionKey = JSON.stringify([activeSessionId, leaseKey]);
		const existing = this.sessionInputPauses.get(acquisitionKey);
		if (existing) return existing;
		const acquisition = (async (): Promise<AgentConnectionSessionInputPause> => {
			const { pauseId } = await this.requestData<{ pauseId: string }>({
				type: "acquire_session_input_pause",
				activeSessionId,
				leaseKey,
			});
			if (generation !== this.sessionInputPauseGeneration || this.terminalCloseEmitted) {
				try {
					await this.requestData({
						type: "release_session_input_pause",
						activeSessionId,
						pauseId,
					});
				} catch {
					this.client.close();
				}
				throw new Error("Session input pause acquisition was invalidated by a daemon reconnect.");
			}
			let released = false;
			return {
				release: async () => {
					if (released) return;
					if (generation !== this.sessionInputPauseGeneration) {
						throw new Error("Session input pause was invalidated by a daemon reconnect.");
					}
					await this.requestData({
						type: "release_session_input_pause",
						activeSessionId,
						pauseId,
					});
					released = true;
					if (this.sessionInputPauses.get(acquisitionKey) === acquisition) {
						this.sessionInputPauses.delete(acquisitionKey);
					}
				},
			};
		})();
		this.sessionInputPauses.set(acquisitionKey, acquisition);
		try {
			return await acquisition;
		} catch (error) {
			if (this.sessionInputPauses.get(acquisitionKey) === acquisition)
				this.sessionInputPauses.delete(acquisitionKey);
			throw error;
		}
	}

	async listCronJobs(options: { includeInactive?: boolean } = {}): Promise<AgentCronJob[]> {
		const data = await this.requestData<{ jobs: AgentCronJob[] }>({
			type: "cron_list",
			activeSessionId: this.activeSessionId,
			includeInactive: options.includeInactive,
		});
		return data.jobs;
	}

	async listHeartbeats(): Promise<AgentConnectionHeartbeat[]> {
		return listDaemonHeartbeats(this.client, this.options.ownedSession ? this.activeSessionId : undefined);
	}

	async manageHeartbeat(
		activeSessionId: string,
		jobId: string,
		action: AgentHeartbeatManagementAction,
	): Promise<AgentCronJob> {
		if (!this.client.supportsServerCapability("heartbeat_management")) {
			throw new Error("Heartbeat management requires a newer Prime Agent daemon.");
		}
		try {
			const data = await this.requestData<{ heartbeat: AgentCronJob }>({
				type: "heartbeat_manage",
				activeSessionId,
				jobId,
				action,
			});
			return data.heartbeat;
		} catch (error) {
			if (isUnknownDaemonCommandError(error, "heartbeat_manage")) {
				throw new Error("Heartbeat management requires a newer Prime Agent daemon.");
			}
			throw error;
		}
	}

	async addCronJob(schedule: string, prompt: string): Promise<AgentCronJob> {
		return this.withOwnedSessionPromotion(async (promoteOwnedSession) => {
			const data = await this.requestData<{ job: AgentCronJob }>({
				type: "cron_add",
				activeSessionId: this.activeSessionId,
				schedule,
				prompt,
				promoteOwnedSession,
			});
			return data.job;
		});
	}

	async cancelCronJob(jobId: string): Promise<AgentCronJob> {
		const data = await this.requestData<{ job: AgentCronJob }>({
			type: "cron_cancel",
			activeSessionId: this.activeSessionId,
			jobId,
		});
		return data.job;
	}

	async getHeartbeat(): Promise<AgentCronJob | undefined> {
		const data = await this.requestData<{ heartbeat?: AgentCronJob | null }>({
			type: "heartbeat_get",
			activeSessionId: this.activeSessionId,
		});
		return data.heartbeat ?? undefined;
	}

	async setHeartbeat(
		schedule: string,
		instruction: string,
		deliveryMode?: AgentHeartbeatDeliveryMode,
	): Promise<AgentCronJob> {
		return this.withOwnedSessionPromotion(async (promoteOwnedSession) => {
			const data = await this.requestData<{ heartbeat: AgentCronJob }>({
				type: "heartbeat_set",
				activeSessionId: this.activeSessionId,
				schedule,
				prompt: instruction,
				...(deliveryMode ? { deliveryMode } : {}),
				promoteOwnedSession,
			});
			return data.heartbeat;
		});
	}

	async updateHeartbeat(action: AgentHeartbeatUpdateAction): Promise<AgentCronJob | undefined> {
		const data = await this.requestData<{ heartbeat?: AgentCronJob | null }>({
			type: "heartbeat_update",
			activeSessionId: this.activeSessionId,
			action,
		});
		return data.heartbeat ?? undefined;
	}

	async sendAgentMessage(targetActiveSessionId: string, message: string): Promise<AgentSessionMessageReceipt> {
		return this.requestData<AgentSessionMessageReceipt>({
			type: "send_message",
			targetActiveSessionId,
			message,
			fromActiveSessionId: this.activeSessionId,
		});
	}

	async getAgentMessageStatus(): Promise<AgentSessionMessageSafetyStatus> {
		return this.requestData<AgentSessionMessageSafetyStatus>({
			type: "agent_messages_status",
			activeSessionId: this.activeSessionId,
		});
	}

	async pauseAgentMessages(): Promise<AgentSessionMessageSafetyStatus> {
		return this.requestData<AgentSessionMessageSafetyStatus>({
			type: "agent_messages_pause",
			activeSessionId: this.activeSessionId,
		});
	}

	async resumeAgentMessages(): Promise<AgentSessionMessageSafetyStatus> {
		return this.requestData<AgentSessionMessageSafetyStatus>({
			type: "agent_messages_resume",
			activeSessionId: this.activeSessionId,
		});
	}

	async clearAgentMessages(): Promise<number> {
		return this.requestData<number>({
			type: "agent_messages_clear",
			activeSessionId: this.activeSessionId,
		});
	}

	async getUserMessagesForForking(): Promise<AgentConnectionUserMessage[]> {
		const data = await this.requestData<{ messages: AgentConnectionUserMessage[] }>({
			type: "get_user_messages_for_forking",
			activeSessionId: this.activeSessionId,
		});
		return data.messages;
	}

	async getLastAssistantText(): Promise<string | undefined> {
		const data = await this.requestData<{ text?: string | null }>({
			type: "get_last_assistant_text",
			activeSessionId: this.activeSessionId,
		});
		return data.text ?? undefined;
	}

	async getSystemPrompt(): Promise<string> {
		const data = await this.requestData<{ systemPrompt: string }>({
			type: "get_system_prompt",
			activeSessionId: this.activeSessionId,
		});
		return data.systemPrompt;
	}

	async getToolDefinition(name: string): Promise<AgentConnectionToolDefinition | undefined> {
		const data = await this.requestData<{ toolDefinition?: AgentConnectionToolDefinition }>({
			type: "get_tool_definition",
			activeSessionId: this.activeSessionId,
			name,
		});
		return data.toolDefinition;
	}

	async setSessionEntryLabel(entryId: string, label: string | undefined): Promise<void> {
		await this.requestOk({
			type: "set_session_entry_label",
			activeSessionId: this.activeSessionId,
			entryId,
			label,
		});
	}

	async respondToExtensionUiRequest(requestId: string, response: AgentConnectionExtensionUiResponse): Promise<void> {
		await this.requestOk({
			type: "extension_ui_response",
			activeSessionId: this.activeSessionId,
			requestId,
			response,
		});
	}

	async prompt(message: string, options?: AgentConnectionPromptOptions): Promise<void> {
		await this.promptWithAdmissionCancellation("prompt", message, options);
	}

	async promptAndWait(message: string, options?: AgentConnectionPromptOptions): Promise<void> {
		await this.promptWithAdmissionCancellation("prompt_and_wait", message, options);
	}

	private async promptWithAdmissionCancellation(
		type: "prompt" | "prompt_and_wait",
		message: string,
		options?: AgentConnectionPromptOptions,
	): Promise<void> {
		const signal = options?.signal;
		if (signal?.aborted) {
			throw new AgentConnectionPromptAdmissionError("Prompt admission was cancelled.", "cancelled");
		}
		const admissionId = signal ? `prompt-admission:${randomUUID()}` : undefined;
		if (!signal) {
			await this.requestData<unknown>(
				{
					type,
					activeSessionId: this.activeSessionId,
					message,
					images: options?.images,
					streamingBehavior: options?.streamingBehavior,
					queueIfBusy: options?.queueIfBusy,
					source: options?.source,
					admissionId,
				},
				DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS,
			);
			return;
		}
		if (admissionId === undefined) throw new Error("Prompt admission id was not created for cancellable input");
		let resolveAbort = () => {};
		const aborted = new Promise<"abort">((resolve) => {
			resolveAbort = () => resolve("abort");
		});
		const onAbort = () => resolveAbort();
		signal.addEventListener("abort", onAbort, { once: true });
		// Close the listener-registration race before issuing the first request.
		if (signal.aborted) {
			signal.removeEventListener("abort", onAbort);
			throw new AgentConnectionPromptAdmissionError("Prompt admission was cancelled.", "cancelled");
		}
		const command = {
			type,
			activeSessionId: this.activeSessionId,
			message,
			images: options.images,
			streamingBehavior: options.streamingBehavior,
			queueIfBusy: options.queueIfBusy,
			source: options.source,
			admissionId,
		} as Extract<DaemonCommandBody, { type: typeof type }>;
		let promptError: unknown;
		const promptRequest = this.requestData<unknown>(command, DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS).catch(
			(error: unknown) => {
				promptError =
					error instanceof DaemonCapabilityUnavailableError && !error.afterReconnect
						? new AgentConnectionPromptAdmissionError(error.message, "unsupported", { cause: error })
						: error;
				return "failed" as const;
			},
		);
		try {
			const first = await Promise.race([promptRequest.then(() => "settled" as const), aborted]);
			if (first === "settled" && promptError === undefined) return;
			if (first === "settled" && promptError instanceof AgentConnectionPromptAdmissionError) throw promptError;
			if (
				first === "settled" &&
				!signal.aborted &&
				promptError instanceof Error &&
				this.definitiveRequestErrors.has(promptError)
			) {
				throw promptError;
			}
			let status: "cancelled" | "owned" | "unknown" = "unknown";
			try {
				const result = await this.requestData<{ status: "cancelled" | "owned" | "unknown" }>({
					type: "cancel_prompt_admission",
					activeSessionId: this.activeSessionId,
					admissionId,
					...(this.client.supportsServerCapability("owned_prompt_cancellation") ? { cancelOwned: true } : {}),
				});
				status = result.status;
			} catch {
				// Timeout/transport is indistinguishable from accepted ownership.
			}
			await promptRequest;
			if (promptError instanceof AgentConnectionPromptAdmissionError) throw promptError;
			const definitiveFailure = promptError instanceof Error && this.definitiveRequestErrors.has(promptError);
			if (promptError === undefined || (status === "owned" && type === "prompt" && !definitiveFailure)) return;
			throw new AgentConnectionPromptAdmissionError(
				promptError instanceof Error ? promptError.message : "Prompt admission did not complete.",
				status,
				promptError === undefined ? undefined : { cause: promptError },
			);
		} finally {
			signal.removeEventListener("abort", onAbort);
		}
	}

	async startSideQuestion(
		id: string,
		question: string,
		previousTurns?: AgentConnectionSideQuestionTurn[],
	): Promise<void> {
		if (previousTurns?.length && !this.client.supportsServerCapability("side_question_transcript")) {
			// An older daemon would silently ignore previousTurns and answer the
			// follow-up without the side-conversation context; fail loudly instead.
			throw new Error(
				"the daemon is running an older build without side-conversation follow-ups; restart the daemon and try again",
			);
		}
		this.activeSideQuestionIds.add(id);
		try {
			await this.requestOk({
				type: "start_side_question",
				activeSessionId: this.activeSessionId,
				sideQuestionId: id,
				question,
				previousTurns,
			});
		} catch (error) {
			this.activeSideQuestionIds.delete(id);
			if (isUnknownDaemonCommandError(error, "start_side_question")) {
				throw new Error("the daemon is running an older build; restart the daemon and try again");
			}
			throw error;
		}
	}

	async abortSideQuestion(id: string): Promise<boolean> {
		const data = await this.requestData<{ aborted: boolean }>({
			type: "abort_side_question",
			activeSessionId: this.activeSessionId,
			sideQuestionId: id,
		});
		this.activeSideQuestionIds.delete(id);
		return data.aborted;
	}

	async steer(message: string, images?: ImageContent[]): Promise<void> {
		await this.requestOk({ type: "steer", activeSessionId: this.activeSessionId, message, images });
	}

	async followUp(message: string, images?: ImageContent[]): Promise<void> {
		await this.requestOk({ type: "follow_up", activeSessionId: this.activeSessionId, message, images });
	}

	async abort(): Promise<void> {
		await this.requestOk({ type: "abort", activeSessionId: this.activeSessionId });
	}

	async cancelRlmChild(childId: string): Promise<boolean> {
		try {
			const result = await this.requestData<{ cancelled: boolean }>({
				type: "cancel_rlm_child",
				activeSessionId: this.activeSessionId,
				childId,
			});
			return result.cancelled;
		} catch (error) {
			if (isUnknownDaemonCommandError(error, "cancel_rlm_child")) {
				throw new Error("the daemon is running an older build; restart the daemon and try again");
			}
			throw error;
		}
	}

	async waitForIdle(): Promise<void> {
		await this.requestData<unknown>(
			{ type: "wait_for_idle", activeSessionId: this.activeSessionId },
			DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS,
		);
	}

	async waitForHeadlessCompletion(options?: AgentConnectionHeadlessCompletionOptions): Promise<AgentAutonomousStatus> {
		if (options?.waitForRlmQuiescence && !this.client.supportsServerCapability("rlm_quiescence_barrier")) {
			throw new Error(
				"the daemon is running an older build without RLM quiescence barriers; restart the daemon and try again",
			);
		}
		return this.requestData<AgentAutonomousStatus>(
			{
				type: "wait_for_headless_completion",
				activeSessionId: this.activeSessionId,
				...(options?.waitForRlmQuiescence ? { waitForRlmQuiescence: true } : {}),
			},
			DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS,
		);
	}

	async executeBash(command: string, options?: AgentConnectionExecuteBashOptions): Promise<void> {
		if (options?.transient && !this.client.supportsServerCapability("transient_bash")) {
			// An older daemon would record the run into the session, leaking the
			// side conversation into the main transcript; fail loudly instead.
			throw new Error(
				"the daemon is running an older build without side-conversation bash; restart the daemon and try again",
			);
		}
		try {
			await this.requestOk({
				type: "execute_bash",
				activeSessionId: this.activeSessionId,
				command,
				excludeFromContext: options?.excludeFromContext,
				transient: options?.transient,
				runId: options?.runId,
			});
		} catch (error) {
			if (isUnknownDaemonCommandError(error, "execute_bash")) {
				throw new Error("the daemon is running an older build; restart the daemon and try again");
			}
			throw error;
		}
	}

	async executeBashAndWait(command: string): Promise<BashResult> {
		return this.requestData<BashResult>(
			{
				type: "execute_bash_and_wait",
				activeSessionId: this.activeSessionId,
				command,
			},
			DAEMON_LONG_RUNNING_REQUEST_TIMEOUT_MS,
		);
	}

	async abortBash(): Promise<void> {
		try {
			await this.requestOk({ type: "abort_bash", activeSessionId: this.activeSessionId });
		} catch (error) {
			if (isUnknownDaemonCommandError(error, "abort_bash")) {
				throw new Error("the daemon is running an older build; restart the daemon and try again");
			}
			throw error;
		}
	}

	async setModel(provider: string, modelId: string): Promise<AgentConnectionModel> {
		return this.requestData<AgentConnectionModel>({
			type: "set_model",
			activeSessionId: this.activeSessionId,
			provider,
			modelId,
		});
	}

	async cycleModel(direction?: "forward" | "backward"): Promise<AgentConnectionModelCycleResult | undefined> {
		const result = await this.requestData<AgentConnectionModelCycleResult | null>({
			type: "cycle_model",
			activeSessionId: this.activeSessionId,
			direction,
		});
		return result ?? undefined;
	}

	async setScopedModels(scopedModels: AgentConnectionScopedModel[]): Promise<void> {
		await this.requestOk({
			type: "set_scoped_models",
			activeSessionId: this.activeSessionId,
			scopedModels,
		});
	}

	async setThinkingLevel(level: ThinkingLevel): Promise<void> {
		await this.requestOk({ type: "set_thinking_level", activeSessionId: this.activeSessionId, level });
	}

	async setServiceTier(serviceTier: ServiceTier): Promise<void> {
		await this.requestOk({ type: "set_service_tier", activeSessionId: this.activeSessionId, serviceTier });
	}

	async cycleThinkingLevel(): Promise<ThinkingLevel | undefined> {
		const result = await this.requestData<{ level: ThinkingLevel } | null>({
			type: "cycle_thinking_level",
			activeSessionId: this.activeSessionId,
		});
		return result?.level;
	}

	async setTransport(transport: Transport): Promise<void> {
		await this.requestOk({ type: "set_transport", activeSessionId: this.activeSessionId, transport });
	}

	async setSteeringMode(mode: AgentConnectionQueueMode): Promise<void> {
		await this.requestOk({ type: "set_steering_mode", activeSessionId: this.activeSessionId, mode });
	}

	async setFollowUpMode(mode: AgentConnectionQueueMode): Promise<void> {
		await this.requestOk({ type: "set_follow_up_mode", activeSessionId: this.activeSessionId, mode });
	}

	async setAutoCompactionEnabled(enabled: boolean): Promise<void> {
		await this.requestOk({ type: "set_auto_compaction", activeSessionId: this.activeSessionId, enabled });
	}

	async setAutoRetryEnabled(enabled: boolean): Promise<void> {
		await this.requestOk({ type: "set_auto_retry", activeSessionId: this.activeSessionId, enabled });
	}

	async compact(customInstructions?: string): Promise<CompactionResult> {
		return this.requestData<CompactionResult>({
			type: "compact",
			activeSessionId: this.activeSessionId,
			customInstructions,
		});
	}

	async refine(
		options: { instructions?: string; rollbackId?: string; global?: boolean } = {},
	): Promise<RefinementResult> {
		const command: {
			type: "refine";
			activeSessionId: string;
			instructions?: string;
			rollbackId?: string;
			global?: boolean;
		} = {
			type: "refine",
			activeSessionId: this.activeSessionId,
			instructions: options.instructions,
			rollbackId: options.rollbackId,
		};
		if (options.global !== undefined) {
			command.global = options.global;
		}
		return this.requestData<RefinementResult>(command, DAEMON_REFINE_REQUEST_TIMEOUT_MS);
	}

	async abortCompaction(): Promise<void> {
		await this.requestOk({ type: "abort_compaction", activeSessionId: this.activeSessionId });
	}

	async abortBranchSummary(): Promise<void> {
		await this.requestOk({ type: "abort_branch_summary", activeSessionId: this.activeSessionId });
	}

	async abortRetry(): Promise<void> {
		await this.requestOk({ type: "abort_retry", activeSessionId: this.activeSessionId });
	}

	async reload(): Promise<void> {
		await this.requestOk({ type: "reload", activeSessionId: this.activeSessionId });
	}

	async newSession(options?: AgentConnectionNewSessionOptions): Promise<{ cancelled: boolean }> {
		return this.requestData<{ cancelled: boolean }>({
			type: "new_session",
			activeSessionId: this.activeSessionId,
			parentSession: options?.parentSession,
		});
	}

	async switchSession(
		sessionPath: string,
		options?: AgentConnectionSwitchSessionOptions,
	): Promise<{ cancelled: boolean }> {
		const sourceActiveSessionId = this.activeSessionId;
		try {
			return await this.requestData<{ cancelled: boolean }>({
				type: "switch_session",
				activeSessionId: sourceActiveSessionId,
				sessionPath,
				cwdOverride: options?.cwdOverride,
			});
		} catch (error) {
			if (!(error instanceof SessionAlreadyActiveError) || !error.activeSessionId) {
				throw error;
			}
			if (this.options.ownedSession) {
				throw error;
			}
			if (error.activeSessionId === sourceActiveSessionId) {
				return { cancelled: false };
			}
			return this.reattachSession(sourceActiveSessionId, error.activeSessionId);
		}
	}

	private reattachSession(
		sourceActiveSessionId: string,
		targetActiveSessionId: string,
	): Promise<{ cancelled: false }> {
		const admissionRevision = ++this.attachmentAdmissionRevision;
		this.retirePendingChunkedReplacement();
		this.invalidateNegotiatedCapabilityProof();
		if (this.replacementReconciliationFailed) {
			return Promise.reject(new Error("Daemon connection replacement reconciliation has failed"));
		}
		return this.withSharedAttachmentMutation(async () => {
			try {
				return await this.reattachSessionUnserialized(
					sourceActiveSessionId,
					targetActiveSessionId,
					admissionRevision,
				);
			} catch (error) {
				this.invalidateNegotiatedCapabilityProof();
				throw error;
			}
		});
	}

	private async reattachSessionUnserialized(
		sourceActiveSessionId: string,
		targetActiveSessionId: string,
		admissionRevision: number,
	): Promise<{ cancelled: false }> {
		if (admissionRevision !== this.attachmentAdmissionRevision) {
			throw new Error("Daemon connection attachment changed during attach");
		}
		if (this.disposing || this.disposed) throw new Error("Daemon connection is disposing");
		if (this.replacementReconciliationFailed) {
			throw new Error("Daemon connection replacement reconciliation has failed");
		}
		this.pendingReattachActiveSessionIds.add(targetActiveSessionId);
		let reattached = false;
		let attachmentEpoch: number | undefined;
		let invalidationRevision: number | undefined;
		try {
			const supportsExtensionUi = this.options.supportsExtensionUi !== false;
			attachmentEpoch = this.advanceAttachmentEpoch();
			this.reserveSharedAttachment(targetActiveSessionId);
			invalidationRevision = this.attachmentInvalidationRevision;
			const transportGeneration = this.client.getTransportGeneration();
			const capabilities: DaemonClientCapability[] = [
				"attach_snapshot",
				"event_sequence",
				...(supportsExtensionUi ? (["extension_ui"] as const) : []),
				"slim_attach",
				"chunked_snapshot",
				...(this.supportsCorrelatedPromptLifecycle() ? (["correlated_prompt_lifecycle_v1"] as const) : []),
				...(this.options.ownedSession ? (["client_owned_sessions"] as const) : []),
			];
			let result!: DaemonAttachResult;
			let snapshot!: DaemonSessionSnapshot;
			for (let attemptIndex = 0; attemptIndex < 2; attemptIndex++) {
				const requestAttempt = this.startSnapshotRequestAttempt(
					targetActiveSessionId,
					attachmentEpoch,
					"replacement",
					attemptIndex === 0 ? "replacement" : "attach",
				);
				try {
					result = await this.requestData<DaemonAttachResult>(
						attemptIndex === 0
							? {
									type: "reattach",
									activeSessionId: sourceActiveSessionId,
									targetActiveSessionId,
									supportsExtensionUi,
									clientId: this.clientId,
									capabilities,
									env: this.options.sendClientEnv ? collectDaemonClientEnv() : undefined,
									launchEnv: this.options.ownedSession ? collectDaemonLaunchEnv() : undefined,
									telemetryDisabled: this.options.telemetryDisabled,
								}
							: {
									type: "attach",
									activeSessionId: targetActiveSessionId,
									snapshotGenerationNonce: randomUUID(),
									supportsExtensionUi,
									clientId: this.clientId,
									capabilities,
									env: this.options.sendClientEnv ? collectDaemonClientEnv() : undefined,
									telemetryDisabled: this.options.telemetryDisabled,
								},
					);
					reattached = true;
					this.assertAttachmentCommit(
						targetActiveSessionId,
						attachmentEpoch,
						invalidationRevision,
						transportGeneration,
						admissionRevision,
					);
					if (result.activeSessionId !== targetActiveSessionId) {
						throw new Error("Daemon returned an invalid session snapshot");
					}
					if (result.snapshotStream) {
						this.bindSnapshotExpectation(requestAttempt, result);
						requestAttempt.state = "receiving";
						snapshot = await this.waitForSnapshot(result.snapshotStream.id, requestAttempt);
					} else {
						snapshot = result.snapshot;
					}
					requestAttempt.state = "succeeded";
					break;
				} catch (error) {
					requestAttempt.state = "failed";
					if (
						attemptIndex === 1 ||
						!reattached ||
						this.disposing ||
						this.disposed ||
						this.attachmentAdmissionRevision !== admissionRevision ||
						this.attachmentEpoch !== attachmentEpoch ||
						this.attachmentInvalidationRevision !== invalidationRevision ||
						this.client.getTransportGeneration() !== transportGeneration ||
						sharedAttachmentOwners.get(this.client)?.get(targetActiveSessionId) !== this
					) {
						throw error;
					}
				} finally {
					this.finishSnapshotRequestAttempt(requestAttempt);
				}
			}
			this.assertAttachmentCommit(
				targetActiveSessionId,
				attachmentEpoch,
				invalidationRevision,
				transportGeneration,
				admissionRevision,
			);
			const negotiatedCapabilities = this.negotiatedCapabilitiesFromAttach(result, capabilities);
			const includePromptLifecycles = negotiatedCapabilities.has("correlated_prompt_lifecycle_v1");

			validateDaemonSnapshotIdentity(result.snapshot, result.activeSessionId);
			mapDaemonSessionSnapshot(result.snapshot, undefined, includePromptLifecycles);
			const staged = this.stageSnapshotCommit(snapshot, {
				purpose: "replacement",
				includePromptLifecycles,
				envelopeActiveSessionId: result.activeSessionId,
				expectedState: result.snapshot.state,
				replay: result.replay,
				progress: [
					{ cursor: result.lastEventCursor, sequence: result.lastEventSequence },
					{ cursor: snapshot.lastEventCursor, sequence: snapshot.lastEventSequence },
				],
			});
			this.assertAttachmentCommit(
				targetActiveSessionId,
				attachmentEpoch,
				invalidationRevision,
				transportGeneration,
				admissionRevision,
			);
			this.activeSideQuestionIds.clear();
			this.commitStagedSnapshot(staged);
			this.publishNegotiatedCapabilityProof(negotiatedCapabilities, transportGeneration);
			await this.emit({
				type: "session_replaced",
				state: snapshot.state,
				messages: snapshot.messages,
			});
			return { cancelled: false };
		} catch (error) {
			const stillCurrent =
				this.attachmentAdmissionRevision === admissionRevision &&
				this.attachmentEpoch === attachmentEpoch &&
				this.attachmentInvalidationRevision === invalidationRevision &&
				sharedAttachmentOwners.get(this.client)?.get(targetActiveSessionId) === this;
			if (reattached && stillCurrent) {
				if (this.supportsCorrelatedPromptLifecycle()) {
					await this.emit({ type: "correlated_prompt_protocol_violation" });
				}
				this.failClosedReplacementReconciliation();
			} else if (!reattached && stillCurrent) {
				this.invalidateNegotiatedCapabilityProof();
			}
			throw error;
		} finally {
			this.pendingReattachActiveSessionIds.delete(targetActiveSessionId);
		}
	}

	async fork(
		entryId: string,
		options?: AgentConnectionForkOptions,
	): Promise<{ cancelled: boolean; selectedText?: string }> {
		return this.requestData<{ cancelled: boolean; selectedText?: string }>({
			type: "fork",
			activeSessionId: this.activeSessionId,
			entryId,
			position: options?.position,
		});
	}

	async navigateTree(
		targetId: string,
		options?: AgentConnectionNavigateTreeOptions,
	): Promise<AgentConnectionNavigateTreeResult> {
		return this.requestData<AgentConnectionNavigateTreeResult>({
			type: "navigate_tree",
			activeSessionId: this.activeSessionId,
			targetId,
			summarize: options?.summarize,
			customInstructions: options?.customInstructions,
			replaceInstructions: options?.replaceInstructions,
			label: options?.label,
		});
	}

	async importFromJsonl(inputPath: string, cwdOverride?: string): Promise<{ cancelled: boolean }> {
		return this.requestData<{ cancelled: boolean }>({
			type: "import_jsonl",
			activeSessionId: this.activeSessionId,
			inputPath,
			cwdOverride,
		});
	}

	async exportToHtml(outputPath?: string): Promise<string> {
		const data = await this.requestData<{ path: string }>({
			type: "export_html",
			activeSessionId: this.activeSessionId,
			outputPath,
		});
		return data.path;
	}

	async exportToJsonl(outputPath?: string): Promise<string> {
		const data = await this.requestData<{ path: string }>({
			type: "export_jsonl",
			activeSessionId: this.activeSessionId,
			outputPath,
		});
		return data.path;
	}

	async setSessionName(name: string): Promise<void> {
		await this.requestOk({ type: "set_session_name", activeSessionId: this.activeSessionId, name });
	}

	async getRlmMaxDepthStatus() {
		return this.requestData<{ maxDepth: number; source: "default" | "env" | "global" | "inherited" | "chat" }>({
			type: "get_rlm_max_depth_status",
			activeSessionId: this.activeSessionId,
		});
	}

	async setRlmMaxDepth(maxDepth: number, options?: { global?: boolean }) {
		return this.requestData<{
			maxDepth: number;
			source: "default" | "env" | "global" | "inherited" | "chat";
			globalSaved: boolean;
			globalError?: string;
		}>({
			type: "set_rlm_max_depth",
			activeSessionId: this.activeSessionId,
			maxDepth,
			global: options?.global,
		});
	}

	async renameSavedSession(sessionPath: string, name: string): Promise<void> {
		await renameDaemonSavedSession(this.client, { activeSessionId: this.activeSessionId }, sessionPath, name);
	}

	async deleteSavedSession(sessionPath: string): Promise<DeleteSessionFileResult> {
		return deleteDaemonSavedSession(this.client, { activeSessionId: this.activeSessionId }, sessionPath);
	}

	async watchSession(activeSessionId: string): Promise<AgentConnectionSessionWatcher | undefined> {
		// A second connection on the shared client; each one filters to its own session id.
		// attach() rejects for an unknown/exited session — treat that as unreachable.
		let connection: DaemonAgentConnection;
		try {
			connection = await DaemonAgentConnection.attach(this.client, activeSessionId, { closeClientOnDispose: false });
		} catch {
			return undefined;
		}
		return {
			getMessages: () => connection.getMessages(),
			getCommands: () => connection.getCommands(),
			subscribe: (listener) => connection.subscribe(listener),
			getToolDefinition: (name) => connection.getToolDefinition(name),
			close: () => connection.dispose(),
		};
	}

	async dispose(): Promise<void> {
		if (this.disposed || this.disposing) {
			return;
		}
		this.disposing = true;
		this.advanceAttachmentEpoch();
		if (this.options.ownedSession && !this.client.isConnected && this.reconnectPromise) {
			await Promise.race([this.reconnectPromise, delay(OWNED_SESSION_DISPOSE_RECONNECT_WAIT_MS)]).catch(
				() => undefined,
			);
		}
		this.disposed = true;
		this.invalidateNegotiatedCapabilityProof();
		this.updateRestartPending = false;
		await Promise.allSettled([...this.activeSideQuestionIds].map((id) => this.abortSideQuestion(id)));
		this.unsubscribeDaemonMessages();
		this.unsubscribeDaemonClose();
		const pendingActiveSessionId = [...this.pendingReattachActiveSessionIds].at(-1);
		const serverRoutedActiveSessionId = pendingActiveSessionId ?? this.activeSessionId;
		this.invalidateSharedAttachment(serverRoutedActiveSessionId);
		if (this.options.ownedSession) {
			await this.requestOk({ type: "complete_owned_session", activeSessionId: serverRoutedActiveSessionId }).catch(
				() => undefined,
			);
		} else {
			await this.requestOk({ type: "detach", activeSessionId: serverRoutedActiveSessionId }).catch(() => undefined);
		}
		if (this.options.closeClientOnDispose) {
			this.client.close();
		}
		this.rejectSnapshotAssemblies(new Error("Daemon connection disposed during snapshot transfer"));
	}

	async promoteToResident(): Promise<void> {
		await this.withOwnedSessionPromotion(async (promoteOwnedSession) => {
			if (!promoteOwnedSession) return;
			await this.requestOk({ type: "promote_owned_session", activeSessionId: this.activeSessionId });
		});
	}

	private withOwnedSessionPromotion<T>(operation: (promoteOwnedSession: boolean) => Promise<T>): Promise<T> {
		const run = this.ownedSessionPromotionTail.then(async () => {
			const promoteOwnedSession = this.options.ownedSession === true;
			const result = await operation(promoteOwnedSession);
			if (promoteOwnedSession) {
				this.options.ownedSession = false;
			}
			return result;
		});
		this.ownedSessionPromotionTail = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
	}

	private async reconnect(cause: Error): Promise<void> {
		if (this.reconnectPromise) {
			return this.reconnectPromise;
		}
		this.reconnectPromise = (async () => {
			void this.emit({ type: "connection_status", status: "reconnecting", error: cause.message });
			const deadline = Date.now() + (this.options.reconnectTimeoutMs ?? DAEMON_RECONNECT_TIMEOUT_MS);
			let attempt = 0;
			let lastError: Error = cause;
			while (!this.disposed && !this.terminalCloseEmitted && !this.client.isClosed && Date.now() < deadline) {
				try {
					await this.options.recoverDaemon?.();
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					if (this.disposed || this.terminalCloseEmitted) return;
					await this.client.connect(1000);
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					await this.client.waitForHello(3000);
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					if (this.disposed || this.terminalCloseEmitted) return;
					// Owned disposal may need one authenticated reattach to reach the
					// authoritative completion command. Disposal keeps the public proof
					// false and suppresses restoration events throughout this path.
					await this.attachSession(
						this.activeSessionId,
						this.lastEventCursor,
						false,
						2,
						"attach",
						this.disposing && this.options.ownedSession === true,
					);
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					if (this.disposing || this.disposed || this.terminalCloseEmitted) return;
					const snapshot = await this.getInitialSnapshot();
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					if (this.disposed || this.terminalCloseEmitted) return;
					void this.emit({ type: "session_resynced", snapshot });
					void this.emit({ type: "connection_status", status: "connected" });
					return;
				} catch (error) {
					lastError = error instanceof Error ? error : new Error(String(error));
					if (this.client.isClosed) {
						this.emitOwnerClosedTerminal();
						return;
					}
					if (this.disposed || this.terminalCloseEmitted) return;
					this.client.resetTransportForReconnect();
					const remainingMs = deadline - Date.now();
					if (remainingMs <= 0) break;
					const delayMs = Math.min(remainingMs, 2000, 100 * 2 ** Math.min(attempt, 5));
					attempt++;
					await delayUntilRetryOrOwnerClose(this.client, delayMs);
				}
			}
			if (this.client.isClosed) {
				this.emitOwnerClosedTerminal();
				return;
			}
			if (!this.disposed && !this.terminalCloseEmitted) {
				this.terminalCloseEmitted = true;
				this.client.close();
				await this.emit({ type: "closed", error: `Daemon reconnection failed: ${lastError.message}` });
			}
		})().finally(() => {
			this.reconnectPromise = undefined;
		});
		return this.reconnectPromise;
	}

	private async requestOk(command: DaemonCommandBody): Promise<void> {
		await this.requestData<unknown>(command);
	}

	private async requestData<T>(
		command: DaemonCommandBody,
		timeoutMs?: number,
		options?: Parameters<DaemonClient["request"]>[2],
	): Promise<T> {
		const response = await this.client.request(command, timeoutMs, options);
		if (!response.success) {
			const error = deserializeDaemonError(response);
			this.definitiveRequestErrors.add(error);
			throw error;
		}
		if (invalidatesCachedSnapshot(command.type)) {
			this.latestSnapshotIsFresh = false;
		}
		return response.data as T;
	}

	private async handleDaemonMessage(message: DaemonOutbound): Promise<void> {
		if (message.type === "heartbeats_changed") {
			await this.emit({ type: "heartbeats_changed" });
			return;
		}
		if (!this.isMessageForActiveSession(message)) {
			return;
		}
		if (this.isPendingTargetInvalidation(message) || this.isInvalidationDuringPendingAttachment(message)) {
			this.failClosedReplacementReconciliation();
			return;
		}
		if (
			"snapshotId" in message &&
			message.type !== "session_snapshot_begin" &&
			this.ignoredSnapshotIds.has(message.snapshotId)
		) {
			if (message.type === "session_snapshot_end" || message.type === "session_snapshot_failed") {
				this.ignoredSnapshotIds.delete(message.snapshotId);
			}
			return;
		}
		if (message.type === "session_snapshot_begin") {
			const terminalRuntimeAttempt = this.runtimeSnapshotAttempt;
			if (
				terminalRuntimeAttempt?.state === "terminal" &&
				terminalRuntimeAttempt.attachmentEpoch === this.attachmentEpoch &&
				terminalRuntimeAttempt.activeSessionId === message.activeSessionId
			) {
				this.ignoreSnapshotId(message.snapshotId);
				return;
			}
			const pendingReplacement = this.pendingChunkedReplacement;
			const requestAttempt = this.latestSnapshotRequestAttempt(message.activeSessionId);
			if (this.discardUnsolicitedRuntimeSnapshots && !requestAttempt) {
				this.ignoreSnapshotId(message.snapshotId);
				return;
			}
			const retryingReplacement =
				pendingReplacement !== undefined &&
				requestAttempt?.logicalPurpose === "replacement" &&
				requestAttempt.wirePurpose === "attach";
			const acceptsUnsolicitedRuntimeSnapshot =
				this.attachedSessionId !== undefined && (message.purpose === "resync" || message.purpose === "replacement");
			if (!pendingReplacement && !requestAttempt && !acceptsUnsolicitedRuntimeSnapshot) return;
			this.ignoredSnapshotIds.delete(message.snapshotId);
			if (pendingReplacement) {
				if (message.purpose !== "replacement" && !retryingReplacement) {
					void this.transitionRuntimeSnapshotFailure(
						"replacement",
						new Error("Daemon returned an invalid replacement snapshot purpose"),
						message.snapshotId,
					);
					return;
				}
				if (pendingReplacement.snapshotId && pendingReplacement.snapshotId !== message.snapshotId) {
					const failedSnapshotId = pendingReplacement.snapshotId;
					const failedAssembly = this.snapshotAssemblies.get(failedSnapshotId);
					if (failedAssembly) {
						this.rejectSnapshotAssembly(
							failedSnapshotId,
							failedAssembly,
							new Error(`Snapshot ${failedSnapshotId} was replaced by retry ${message.snapshotId}`),
						);
						this.snapshotAssemblies.delete(failedSnapshotId);
					}
					this.ignoreSnapshotId(failedSnapshotId);
				}
				pendingReplacement.snapshotId = message.snapshotId;
			}
			let assembly = this.snapshotAssemblies.get(message.snapshotId);
			if (assembly && (assembly.settled || assembly.attachmentEpoch !== this.attachmentEpoch)) {
				clearTimeout(assembly.timeout);
				this.snapshotAssemblies.delete(message.snapshotId);
				assembly = undefined;
			}
			assembly ??= this.getSnapshotAssembly(message.snapshotId, requestAttempt);
			if (requestAttempt) {
				requestAttempt.candidateSnapshotIds.add(message.snapshotId);
				assembly.requestAttemptId = requestAttempt.id;
			}
			try {
				validateDaemonSnapshotBegin(message);
				if (requestAttempt) this.validateSnapshotAttemptBegin(requestAttempt, message);
			} catch (error) {
				await this.rejectInvalidSnapshotAssembly(
					message.snapshotId,
					assembly,
					error instanceof Error ? error : new Error(String(error)),
					message.activeSessionId,
					message.purpose,
				);
				return;
			}
			// A begin is the immutable boundary. Never retain chunks from an older transfer that reused an id.
			assembly.chunks.clear();
			this.completedSnapshots.delete(message.snapshotId);
			this.completedSnapshotAttemptIds.delete(message.snapshotId);
			assembly.begin = message;
			if (!requestAttempt && (message.purpose === "replacement" || message.purpose === "resync")) {
				this.startRuntimeSnapshotAttempt(message.purpose, message.activeSessionId, message.snapshotId);
			}
			return;
		}
		if (message.type === "session_snapshot_chunk") {
			const assembly = this.snapshotAssemblies.get(message.snapshotId);
			if (!assembly || assembly.settled || !assembly.begin || assembly.attachmentEpoch !== this.attachmentEpoch) {
				return;
			}
			if (
				message.activeSessionId !== assembly.begin.activeSessionId ||
				!Number.isSafeInteger(message.index) ||
				message.index < 0 ||
				!Array.isArray(message.messages) ||
				assembly.chunks.has(message.index)
			) {
				await this.rejectInvalidSnapshotAssembly(
					message.snapshotId,
					assembly,
					new Error(`Snapshot ${message.snapshotId} returned an invalid chunk`),
					message.activeSessionId,
				);
				return;
			}
			assembly.chunks.set(message.index, message.messages);
			return;
		}
		if (message.type === "session_snapshot_end") {
			const assembly = this.snapshotAssemblies.get(message.snapshotId);
			if (!assembly || assembly.settled || assembly.attachmentEpoch !== this.attachmentEpoch) return;
			await this.completeSnapshotAssembly(message);
			return;
		}
		if (message.type === "session_snapshot_failed") {
			const existingAssembly = this.snapshotAssemblies.get(message.snapshotId);
			const explicitPurpose = message.purpose;
			const assembly =
				existingAssembly ??
				(this.pendingChunkedReplacement || explicitPurpose
					? this.getSnapshotAssembly(message.snapshotId)
					: undefined);
			if (!assembly || assembly.settled || assembly.attachmentEpoch !== this.attachmentEpoch) return;
			const purpose =
				explicitPurpose ?? assembly.begin?.purpose ?? (this.pendingChunkedReplacement ? "replacement" : "attach");
			const recovery = this.transitionSnapshotFailure(
				message.snapshotId,
				assembly,
				new Error(message.error),
				message.activeSessionId,
				purpose,
			);
			if (recovery) await recovery;
			return;
		}
		if (this.isStaleSequencedMessage(message)) {
			return;
		}
		if (message.type === "session_event") {
			if (
				this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1") &&
				(!isPromptEventAttribution(message.attribution) ||
					message.event.promptCorrelationId === undefined ||
					(message.attribution.scope === "session" && message.event.promptCorrelationId !== null) ||
					(message.attribution.scope === "prompt" &&
						message.event.promptCorrelationId !== message.attribution.correlationId))
			) {
				await this.emit({ type: "correlated_prompt_protocol_violation" });
				return;
			}
			this.observeDaemonEventSequence(message);
			if (message.event.type !== "refine_complete" && message.event.type !== "refine_failed") {
				this.observeStreamingMessage(message.event);
			}
			if (message.event.type === "rlm_child_update") {
				this.childRosterSequence = maxEventSequence(this.childRosterSequence, getDaemonMessageSequence(message));
				this.observeRlmChildUpdate(message.event.child);
			}
			this.latestSnapshotIsFresh = false;
			await this.emit({ type: "session_event", event: message.event, attribution: message.attribution });
			return;
		}
		if (message.type === "prompt_lifecycle") {
			if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) return;
			if (!isPromptLifecycleSnapshot(message.lifecycle)) {
				await this.emit({ type: "correlated_prompt_protocol_violation" });
				return;
			}
			if (this.latestSnapshot) {
				let next: PromptLifecycleStateSnapshot | undefined;
				try {
					next = advancePromptLifecycleState(this.latestSnapshot.promptLifecycles, message.lifecycle);
				} catch {
					await this.emit({ type: "correlated_prompt_protocol_violation" });
					return;
				}
				if (!next) {
					this.prunePromptRoutes(
						this.latestSnapshot.promptLifecycles,
						message.activeSessionId,
						this.latestSnapshot.state.sessionId,
						false,
					);
					return;
				}
				this.observeDaemonEventSequence(message);
				this.commitPromptLifecycleState(next, true);
			} else {
				this.observeDaemonEventSequence(message);
			}
			await this.emit({ type: "prompt_lifecycle", lifecycle: message.lifecycle });
			return;
		}
		if (message.type === "session_resynced") {
			let staged: StagedSnapshotCommit;
			try {
				staged = this.stageSnapshotCommit(message.snapshot, {
					purpose: "resync",
					envelopeActiveSessionId: message.activeSessionId,
					expectedSessionId: this.attachedSessionId,
					progress: [
						{ cursor: message.snapshot.lastEventCursor, sequence: message.snapshot.lastEventSequence },
						{ cursor: getDaemonMessageCursor(message), sequence: getDaemonMessageSequence(message) },
					],
				});
			} catch {
				if (this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
					await this.emit({ type: "correlated_prompt_protocol_violation" });
				}
				return;
			}
			this.commitStagedSnapshot(staged);
			await this.emit({ type: "session_resynced", snapshot: staged.latestSnapshot });
			return;
		}
		if (message.type === "session_replaced") {
			this.invalidateNegotiatedCapabilityProof(false);
			try {
				validateConnectionStateIdentity(message.state, message.activeSessionId);
			} catch {
				if (this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) {
					await this.emit({ type: "correlated_prompt_protocol_violation" });
				}
				this.failClosedReplacementReconciliation();
				return;
			}
			if (message.snapshotFollows) {
				if (this.pendingChunkedReplacement) {
					throw new Error("Daemon returned an invalid replacement snapshot header");
				}
				this.pendingChunkedReplacement = { header: message, queuedMessages: [], queuedMessageWeight: 0 };
				return;
			}
			const shapingRevision = this.attachmentInvalidationRevision;
			const routeAdmissionRevision = this.attachmentAdmissionRevision;
			const routeAttachmentEpoch = this.attachmentEpoch;
			const routeTransportGeneration = this.client.getTransportGeneration();
			const routeActiveSessionId = this.activeSessionId;
			const queriedPromptLifecycles = this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")
				? await this.getPromptLifecyclesFor(message.activeSessionId, message.state.sessionId)
				: undefined;
			if (
				this.disposing ||
				this.disposed ||
				this.terminalCloseEmitted ||
				this.updateRestartPending ||
				this.replacementReconciliationFailed ||
				this.attachmentAdmissionRevision !== routeAdmissionRevision ||
				this.attachmentEpoch !== routeAttachmentEpoch ||
				this.client.getTransportGeneration() !== routeTransportGeneration ||
				this.activeSessionId !== routeActiveSessionId
			) {
				return;
			}
			const promptLifecycles =
				queriedPromptLifecycles &&
				this.attachmentInvalidationRevision === shapingRevision &&
				this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")
					? queriedPromptLifecycles
					: undefined;
			const reconciledPromptLifecycles =
				promptLifecycles && this.latestSnapshot?.state.sessionId === message.state.sessionId
					? reconcilePromptLifecycleSnapshot(promptLifecycles, this.latestSnapshot.promptLifecycles)
					: promptLifecycles;
			const latestSnapshot: AgentConnectionSnapshot = {
				state: message.state,
				messages: message.messages,
				...(reconciledPromptLifecycles ? { promptLifecycles: reconciledPromptLifecycles } : {}),
			};
			const staged = this.stageMappedSnapshotCommit(latestSnapshot, {
				activeSessionId: message.activeSessionId,
				attachedSessionId: message.state.sessionId,
				attachedSessionFile: message.state.sessionFile,
				progress: [{ cursor: getDaemonMessageCursor(message), sequence: getDaemonMessageSequence(message) }],
			});
			this.commitStagedSnapshot(staged);
			await this.emit({ type: "session_replaced", state: message.state, messages: message.messages });
			return;
		}

		if (
			(message.type === "extension_ui_request" || message.type === "extension_error") &&
			this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1") &&
			!isPromptEventAttribution(message.attribution)
		) {
			await this.emit({ type: "correlated_prompt_protocol_violation" });
			return;
		}
		this.observeDaemonEventSequence(message);
		if (message.type === "side_question_event") {
			this.observeSideQuestionEvent(message.event);
			await this.emit({ type: "side_question_event", event: message.event });
			return;
		}
		if (message.type === "session_status") {
			// Keep a cached snapshot's recap current so a later re-attach seeds it.
			if (this.latestSnapshot) {
				this.latestSnapshot = {
					...this.latestSnapshot,
					state: { ...this.latestSnapshot.state, recap: message.recap },
				};
			}
			await this.emit({ type: "session_status", recap: message.recap });
			return;
		}

		if (message.type === "extension_ui_request") {
			await this.emit({
				type: "extension_ui_request",
				request: {
					id: message.id,
					method: message.method,
					payload: message.payload,
					attribution: message.attribution,
				},
			});
			return;
		}
		if (message.type === "extension_error") {
			await this.emit({
				type: "extension_error",
				extensionPath: message.extensionPath,
				event: message.event,
				error: message.error,
				attribution: message.attribution,
			});
			return;
		}
		if (message.type === "session_closed") {
			this.invalidateNegotiatedCapabilityProof();
			if (message.reason === "update") {
				this.captureDaemonLogPath();
				this.updateRestartPending = true;
				void this.reconnectAfterUpdate();
				return;
			}
			this.terminalCloseEmitted = true;
			await this.emit({ type: "closed", error: this.formatDaemonSessionClosedError(message.reason) });
		}
	}

	private captureDaemonLogPath(): void {
		const socketPath = this.client.hello?.socketPath;
		if (socketPath) {
			this.daemonLogPath = getDaemonLogPath(socketPath);
		}
	}

	private emitOwnerClosedTerminal(): void {
		if (this.disposed || this.terminalCloseEmitted) return;
		this.terminalCloseEmitted = true;
		void this.emit({ type: "closed", error: "Prime Agent daemon client was closed." });
	}

	private formatDaemonSessionClosedError(reason: DaemonSessionClosedReason): string {
		const explanation: Record<DaemonSessionClosedReason, string> = {
			killed:
				"The daemon stopped this agent session. Its transcript remains saved and can be reopened from Agents View.",
			shutdown:
				"The Prime Agent daemon shut down while this window was attached. The session transcript remains saved; restart Prime Agent and reopen it from Agents View.",
			completed:
				"The daemon closed this agent session after it completed. Its transcript remains available from Agents View.",
			replaced:
				"The daemon replaced this agent session with another session. Reopen the current session from Agents View.",
			update:
				"The Prime Agent daemon restarted for an update, but this window did not restore automatically. The session transcript remains saved; restart Prime Agent and reopen it from Agents View.",
		};
		return `${explanation[reason]} ${this.formatDaemonDiagnosticContext()}`;
	}

	private formatDaemonConnectionClosedError(error: Error): string {
		return `Lost connection to the Prime Agent daemon. Cause: ${formatErrorSentence(error)} The session transcript remains saved; restart Prime Agent or reopen the session from Agents View. ${this.formatDaemonDiagnosticContext()}`;
	}

	private formatUpdateReconnectError(error: unknown): string {
		return `The Prime Agent daemon restarted for an update, but this window could not reconnect to its restored session before the recovery timeout expired. Last error: ${formatErrorSentence(error)} The session transcript remains saved; restart Prime Agent and reopen it from Agents View. ${this.formatDaemonDiagnosticContext()}`;
	}

	private formatDaemonDiagnosticContext(): string {
		const details: string[] = [];
		if (this.attachedSessionId) {
			details.push(`Session ID: ${this.attachedSessionId}.`);
		}
		if (this.attachedSessionFile) {
			details.push(`Session file: ${this.attachedSessionFile}.`);
		}
		details.push(`Diagnostic log: ${this.daemonLogPath ?? getAgentLogPath()}.`);
		return details.join(" ");
	}

	private reconnectAfterUpdate(): Promise<void> {
		if (this.updateReconnectPromise) {
			return this.updateReconnectPromise;
		}
		if (this.client.isClosed) {
			this.emitOwnerClosedTerminal();
			return Promise.resolve();
		}
		void this.emit({
			type: "connection_status",
			status: "reconnecting",
			error: "The Prime Agent daemon is restarting for an update.",
		});
		const reconnectPromise = reconnectDaemonTransportAfterUpdate(this.client)
			.then(() => this.restoreConnectionAfterUpdate())
			.then(() => {
				if (!this.disposed && !this.terminalCloseEmitted && !this.client.isClosed) {
					void this.emit({ type: "connection_status", status: "connected" });
				}
			})
			.catch(async (error: unknown) => {
				this.updateRestartPending = false;
				if (this.client.isClosed) {
					this.emitOwnerClosedTerminal();
					return;
				}
				this.updateReconnectFailed = true;
				if (!this.disposed && !this.terminalCloseEmitted) {
					this.terminalCloseEmitted = true;
					await this.emit({
						type: "closed",
						error: this.formatUpdateReconnectError(error),
					});
				}
			})
			.finally(() => {
				if (this.updateReconnectPromise === reconnectPromise) {
					this.updateReconnectPromise = undefined;
				}
			});
		this.updateReconnectPromise = reconnectPromise;
		return reconnectPromise;
	}

	private async restoreConnectionAfterUpdate(): Promise<void> {
		const sessionId = this.attachedSessionId;
		const sessionFile = this.attachedSessionFile;
		if (!sessionId && !sessionFile) {
			throw new Error("the previous session identity is unavailable");
		}
		const deadline = Date.now() + UPDATE_RECONNECT_TIMEOUT_MS;
		let lastError: unknown;
		while (!this.disposed && !this.terminalCloseEmitted && !this.client.isClosed && Date.now() < deadline) {
			try {
				await this.client.reconnect(1000);
				if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
				if (this.disposed || this.terminalCloseEmitted) return;
				const response = await this.client.request({ type: "list" }, 30000);
				if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
				if (this.disposed || this.terminalCloseEmitted) return;
				if (!response.success) {
					throw deserializeDaemonError(response);
				}
				const sessions = readSessionSummaries(response.data);
				const restored = sessions.find(
					(summary) =>
						summary.activeSessionId !== undefined &&
						((sessionFile !== undefined && summary.sessionFile === sessionFile) ||
							(sessionId !== undefined && summary.sessionId === sessionId)),
				);
				if (restored?.activeSessionId) {
					if (this.disposed || this.terminalCloseEmitted) return;
					this.pendingReattachActiveSessionIds.add(restored.activeSessionId);
					try {
						await this.attachSession(restored.activeSessionId, undefined, true);
					} finally {
						this.pendingReattachActiveSessionIds.delete(restored.activeSessionId);
					}
					if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
					if (this.disposed || this.terminalCloseEmitted) return;
					const snapshot = await this.getInitialSnapshot();
					if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
					if (this.disposed || this.terminalCloseEmitted) return;
					this.updateRestartPending = false;
					void this.emit({ type: "session_resynced", snapshot });
					return;
				}
			} catch (error) {
				if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
				lastError = error;
			}
			await delayUntilRetryOrOwnerClose(this.client, UPDATE_RECONNECT_RETRY_MS);
		}
		if (this.client.isClosed) throw new Error("the daemon client was closed by its owner");
		if (this.disposed || this.terminalCloseEmitted) return;
		throw lastError ?? new Error("the restored session did not become available");
	}

	private deferNegotiatedRuntimeFrame(message: DaemonOutbound): boolean {
		if (!isCorrelatedPromptRuntimeFrame(message)) return false;
		if (this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) return false;
		if ("activeSessionId" in message && typeof message.activeSessionId === "string") {
			const attempt = this.latestSnapshotRequestAttempt(message.activeSessionId);
			if (attempt?.state === "requesting" || attempt?.state === "receiving") {
				const remainingWeight =
					MAX_PENDING_NEGOTIATED_RUNTIME_FRAME_WEIGHT - this.pendingNegotiatedRuntimeFrameWeight;
				const frameWeight = boundedJsonStructuralWeight(message, remainingWeight);
				if (
					this.pendingNegotiatedRuntimeFrames.length >= MAX_PENDING_NEGOTIATED_RUNTIME_FRAMES ||
					frameWeight === undefined
				) {
					this.failClosedReplacementReconciliation();
				} else {
					this.pendingNegotiatedRuntimeFrames.push(message);
					this.pendingNegotiatedRuntimeFrameWeight += frameWeight;
				}
			}
		}
		return true;
	}

	private releasePendingNegotiatedRuntimeFrames(): void {
		const pending = this.pendingNegotiatedRuntimeFrames;
		this.pendingNegotiatedRuntimeFrames = [];
		this.pendingNegotiatedRuntimeFrameWeight = 0;
		if (!this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1")) return;
		for (const message of pending) this.dispatchDaemonMessage(message);
	}

	private withSharedAttachmentMutation<T>(operation: () => Promise<T>): Promise<T> {
		const previous = sharedAttachmentMutationTails.get(this.client) ?? Promise.resolve();
		const run = previous.then(operation, operation);
		const tail = run.then(
			() => undefined,
			() => undefined,
		);
		sharedAttachmentMutationTails.set(this.client, tail);
		void tail.finally(() => {
			if (sharedAttachmentMutationTails.get(this.client) === tail) {
				sharedAttachmentMutationTails.delete(this.client);
			}
		});
		return run;
	}

	private attachmentOwners(): Map<string, DaemonAgentConnection> {
		let owners = sharedAttachmentOwners.get(this.client);
		if (!owners) {
			owners = new Map();
			sharedAttachmentOwners.set(this.client, owners);
		}
		return owners;
	}

	private invalidateNegotiatedCapabilityProof(clearRuntimeCapabilities = true): void {
		this.attachmentInvalidationRevision++;
		this.negotiatedCapabilities = new Set();
		this.negotiatedTransportGeneration = undefined;
		if (clearRuntimeCapabilities) {
			this.negotiatedRuntimeCapabilities = new Set();
			this.negotiatedRuntimeTransportGeneration = undefined;
		}
		this.pendingNegotiatedRuntimeFrames = [];
		this.pendingNegotiatedRuntimeFrameWeight = 0;
		if (!clearRuntimeCapabilities) return;
		const activeSessionId = this.sharedAttachmentActiveSessionId;
		this.sharedAttachmentActiveSessionId = undefined;
		if (!activeSessionId) return;
		const owners = sharedAttachmentOwners.get(this.client);
		if (owners?.get(activeSessionId) === this) owners.delete(activeSessionId);
	}

	private reserveSharedAttachment(activeSessionId: string): void {
		const owners = this.attachmentOwners();
		for (const previous of new Set(owners.values())) {
			if (previous !== this) previous.invalidateNegotiatedCapabilityProof();
		}
		owners.clear();
		owners.set(activeSessionId, this);
		this.sharedAttachmentActiveSessionId = activeSessionId;
	}

	private retargetSharedAttachment(requestedActiveSessionId: string, activeSessionId: string): void {
		if (requestedActiveSessionId === activeSessionId) return;
		const owners = this.attachmentOwners();
		if (owners.get(requestedActiveSessionId) !== this) {
			throw new Error("Daemon connection attachment changed during attach");
		}
		owners.delete(requestedActiveSessionId);
		this.sharedAttachmentActiveSessionId = undefined;
		this.reserveSharedAttachment(activeSessionId);
	}

	private invalidateSharedAttachment(activeSessionId: string): void {
		const owners = sharedAttachmentOwners.get(this.client);
		const owner = owners?.get(activeSessionId);
		if (!owner) return;
		owners?.delete(activeSessionId);
		owner.invalidateNegotiatedCapabilityProof();
	}

	private assertAttachmentCommit(
		activeSessionId: string,
		attachmentEpoch: number,
		invalidationRevision: number,
		transportGeneration: number,
		admissionRevision: number,
		allowWhileDisposing = false,
	): void {
		if (
			(this.disposing && !allowWhileDisposing) ||
			this.disposed ||
			this.replacementReconciliationFailed ||
			this.attachmentAdmissionRevision !== admissionRevision ||
			this.attachmentEpoch !== attachmentEpoch ||
			this.attachmentInvalidationRevision !== invalidationRevision ||
			this.client.getTransportGeneration() !== transportGeneration ||
			this.sharedAttachmentActiveSessionId !== activeSessionId ||
			sharedAttachmentOwners.get(this.client)?.get(activeSessionId) !== this
		) {
			throw new Error("Daemon connection attachment changed during attach");
		}
	}

	private publishNegotiatedCapabilityProof(
		capabilities: ReadonlySet<DaemonClientCapability>,
		transportGeneration: number,
	): void {
		if (this.replacementReconciliationFailed) {
			throw new Error("Daemon connection replacement reconciliation has failed");
		}
		this.discardUnsolicitedRuntimeSnapshots = false;
		this.negotiatedCapabilities = capabilities;
		this.negotiatedTransportGeneration = transportGeneration;
		this.negotiatedRuntimeCapabilities = capabilities;
		this.negotiatedRuntimeTransportGeneration = transportGeneration;
		this.releasePendingNegotiatedRuntimeFrames();
	}

	private negotiatedCapabilitiesFromAttach(
		result: DaemonAttachResult,
		requestedCapabilities: readonly DaemonClientCapability[],
	): ReadonlySet<DaemonClientCapability> {
		if (
			typeof result.client !== "object" ||
			result.client === null ||
			result.client.id !== this.clientId ||
			!Array.isArray(result.client.capabilities)
		) {
			throw new Error("Daemon returned invalid attached client capability proof");
		}
		const requested = new Set(requestedCapabilities);
		const supported = new Set<unknown>(DAEMON_SUPPORTED_CLIENT_CAPABILITIES);
		const observed = new Set<DaemonClientCapability>();
		const negotiated = new Set<DaemonClientCapability>();
		for (const capability of result.client.capabilities as unknown[]) {
			if (
				typeof capability !== "string" ||
				!supported.has(capability) ||
				!requested.has(capability as DaemonClientCapability) ||
				observed.has(capability as DaemonClientCapability)
			) {
				throw new Error("Daemon returned invalid attached client capability proof");
			}
			const typed = capability as DaemonClientCapability;
			observed.add(typed);
			if (this.client.supportsServerCapability(typed)) negotiated.add(typed);
		}
		return negotiated;
	}

	private advanceAttachmentEpoch(): number {
		this.retirePendingChunkedReplacement();
		this.invalidateNegotiatedCapabilityProof();
		const nextEpoch = ++this.attachmentEpoch;
		for (const [snapshotId, assembly] of this.snapshotAssemblies) {
			if (assembly.attachmentEpoch === nextEpoch) continue;
			this.rejectSnapshotAssembly(
				snapshotId,
				assembly,
				new Error(`Attachment changed while waiting for snapshot ${snapshotId}`),
			);
			this.snapshotAssemblies.delete(snapshotId);
			this.completedSnapshots.delete(snapshotId);
			this.completedSnapshotAttemptIds.delete(snapshotId);
			this.ignoreSnapshotId(snapshotId);
		}
		return nextEpoch;
	}

	private bindSnapshotExpectation(attempt: DaemonSnapshotRequestAttempt, result: DaemonAttachResult): void {
		attempt.expected = this.snapshotExpectation(result, attempt.wirePurpose);
		for (const snapshotId of attempt.candidateSnapshotIds) {
			const begin = this.snapshotAssemblies.get(snapshotId)?.begin;
			if (begin) this.validateSnapshotAttemptBegin(attempt, begin);
		}
	}

	private snapshotExpectation(result: DaemonAttachResult, purpose: DaemonSnapshotPurpose): DaemonSnapshotExpectation {
		const stream = result.snapshotStream;
		if (!stream) throw new Error("Daemon snapshot response omitted its stream descriptor");
		return {
			id: stream.id,
			activeSessionId: result.activeSessionId,
			sessionId: result.snapshot.state.sessionId,
			messageCount: stream.messageCount,
			targetChunkBytes: stream.targetChunkBytes,
			purpose,
			lastEventSequence: result.snapshot.lastEventSequence,
			lastEventCursor: result.snapshot.lastEventCursor,
		};
	}

	private validateSnapshotAttemptBegin(attempt: DaemonSnapshotRequestAttempt, begin: DaemonSnapshotBegin): void {
		const expected = attempt.expected;
		if (!expected) return;
		const cursor = begin.snapshot.lastEventCursor;
		if (
			begin.snapshotId !== expected.id ||
			begin.activeSessionId !== expected.activeSessionId ||
			begin.snapshot.activeSessionId !== expected.activeSessionId ||
			begin.snapshot.state.sessionId !== expected.sessionId ||
			begin.snapshot.summary.sessionId !== expected.sessionId ||
			begin.messageCount !== expected.messageCount ||
			begin.targetChunkBytes !== expected.targetChunkBytes ||
			(begin.purpose ?? "attach") !== expected.purpose ||
			begin.snapshot.lastEventSequence !== expected.lastEventSequence ||
			cursor?.generation !== expected.lastEventCursor?.generation ||
			cursor?.sequence !== expected.lastEventCursor?.sequence
		) {
			throw new Error(`Snapshot ${begin.snapshotId} did not match its response descriptor`);
		}
	}

	private startSnapshotRequestAttempt(
		activeSessionId: string,
		attachmentEpoch: number,
		logicalPurpose: DaemonSnapshotPurpose,
		wirePurpose: DaemonSnapshotPurpose,
	): DaemonSnapshotRequestAttempt {
		const attempt: DaemonSnapshotRequestAttempt = {
			id: ++this.snapshotRequestAttemptSequence,
			activeSessionId,
			attachmentEpoch,
			candidateSnapshotIds: new Set(),
			logicalPurpose,
			wirePurpose,
			state: "requesting",
		};
		this.snapshotRequestAttempts.set(attempt.id, attempt);
		return attempt;
	}

	private latestSnapshotRequestAttempt(activeSessionId: string): DaemonSnapshotRequestAttempt | undefined {
		return [...this.snapshotRequestAttempts.values()]
			.reverse()
			.find(
				(attempt) =>
					attempt.activeSessionId === activeSessionId && attempt.attachmentEpoch === this.attachmentEpoch,
			);
	}

	private finishSnapshotRequestAttempt(attempt: DaemonSnapshotRequestAttempt): void {
		if (this.snapshotRequestAttempts.get(attempt.id) !== attempt) return;
		this.snapshotRequestAttempts.delete(attempt.id);
		for (const snapshotId of attempt.candidateSnapshotIds) {
			const assembly = this.snapshotAssemblies.get(snapshotId);
			if (assembly?.requestAttemptId === attempt.id) {
				this.rejectSnapshotAssembly(
					snapshotId,
					assembly,
					new Error(`Snapshot ${snapshotId} arrived after its request attempt settled`),
				);
				this.snapshotAssemblies.delete(snapshotId);
				this.ignoreSnapshotId(snapshotId);
			}
			if (this.completedSnapshotAttemptIds.get(snapshotId) === attempt.id) {
				this.completedSnapshots.delete(snapshotId);
				this.completedSnapshotAttemptIds.delete(snapshotId);
			}
		}
	}

	private getSnapshotAssembly(
		snapshotId: string,
		requestAttempt?: DaemonSnapshotRequestAttempt,
	): DaemonSnapshotAssembly {
		const existing = this.snapshotAssemblies.get(snapshotId);
		if (
			existing &&
			existing.attachmentEpoch === this.attachmentEpoch &&
			(!requestAttempt || existing.requestAttemptId === undefined || existing.requestAttemptId === requestAttempt.id)
		) {
			return existing;
		}
		if (existing) {
			this.rejectSnapshotAssembly(
				snapshotId,
				existing,
				new Error(`Snapshot ${snapshotId} was superseded by a fresh transfer`),
			);
			this.snapshotAssemblies.delete(snapshotId);
		}
		let resolveSnapshot!: (snapshot: DaemonSessionSnapshot) => void;
		let rejectSnapshot!: (error: Error) => void;
		const promise = new Promise<DaemonSessionSnapshot>((resolve, reject) => {
			resolveSnapshot = resolve;
			rejectSnapshot = reject;
		});
		void promise.catch(() => undefined);
		const timeout = setTimeout(() => {
			const current = this.snapshotAssemblies.get(snapshotId);
			if (current && !current.settled) {
				const activeSessionId =
					current.begin?.activeSessionId ?? requestAttempt?.activeSessionId ?? this.activeSessionId;
				void this.transitionSnapshotFailure(
					snapshotId,
					current,
					new Error(`Timed out waiting for snapshot ${snapshotId}`),
					activeSessionId,
					current.begin?.purpose,
				);
			}
		}, this.options.snapshotTimeoutMs ?? DAEMON_SNAPSHOT_TIMEOUT_MS);
		timeout.unref();
		const assembly: DaemonSnapshotAssembly = {
			chunks: new Map(),
			settled: false,
			attachmentEpoch: this.attachmentEpoch,
			...(requestAttempt ? { requestAttemptId: requestAttempt.id } : {}),
			promise,
			resolve(snapshot) {
				if (assembly.settled) return;
				assembly.settled = true;
				resolveSnapshot(snapshot);
			},
			reject(error) {
				if (assembly.settled) return;
				assembly.settled = true;
				rejectSnapshot(error);
			},
			timeout,
		};
		this.snapshotAssemblies.set(snapshotId, assembly);
		return assembly;
	}

	private rejectSnapshotAssemblies(error: Error, recoverRuntime = false): void {
		for (const [snapshotId, assembly] of [...this.snapshotAssemblies]) {
			clearTimeout(assembly.timeout);
			if (recoverRuntime) {
				void this.transitionSnapshotFailure(
					snapshotId,
					assembly,
					error,
					assembly.begin?.activeSessionId ?? this.activeSessionId,
					assembly.begin?.purpose,
				);
			} else {
				assembly.reject(error);
			}
		}
		this.snapshotAssemblies.clear();
		this.completedSnapshots.clear();
		this.completedSnapshotAttemptIds.clear();
		this.snapshotRequestAttempts.clear();
		this.ignoredSnapshotIds.clear();
		if (!recoverRuntime) this.runtimeSnapshotAttempt = undefined;
	}

	private ignoreSnapshotId(snapshotId: string): void {
		this.ignoredSnapshotIds.add(snapshotId);
		while (this.ignoredSnapshotIds.size > MAX_IGNORED_SNAPSHOT_IDS) {
			const oldest = this.ignoredSnapshotIds.values().next().value;
			if (oldest === undefined) {
				break;
			}
			this.ignoredSnapshotIds.delete(oldest);
		}
	}

	private rejectSnapshotAssembly(snapshotId: string, assembly: DaemonSnapshotAssembly, error: Error): void {
		assembly.reject(error);
		clearTimeout(assembly.timeout);
		if (assembly.begin?.purpose && assembly.begin.purpose !== "attach") {
			this.snapshotAssemblies.delete(snapshotId);
		}
	}

	private async rejectInvalidSnapshotAssembly(
		snapshotId: string,
		assembly: DaemonSnapshotAssembly,
		error: Error,
		activeSessionId: string,
		explicitPurpose?: DaemonSnapshotPurpose,
	): Promise<void> {
		const reportCorrelatedViolation = this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1");
		const recovery = this.transitionSnapshotFailure(
			snapshotId,
			assembly,
			error,
			activeSessionId,
			explicitPurpose ?? assembly.begin?.purpose ?? (this.pendingChunkedReplacement ? "replacement" : undefined),
		);
		if (reportCorrelatedViolation) {
			await this.emit({ type: "correlated_prompt_protocol_violation" });
		}
		if (recovery) await recovery;
	}

	private startRuntimeSnapshotAttempt(
		purpose: "replacement" | "resync",
		activeSessionId: string,
		snapshotId: string,
	): DaemonRuntimeSnapshotAttempt {
		const current = this.runtimeSnapshotAttempt;
		if (
			current &&
			current.attachmentEpoch === this.attachmentEpoch &&
			current.activeSessionId === activeSessionId &&
			current.state !== "succeeded"
		) {
			for (const previousId of current.snapshotIds) {
				if (previousId === snapshotId) continue;
				const previous = this.snapshotAssemblies.get(previousId);
				if (previous && !previous.settled) {
					this.rejectSnapshotAssembly(
						previousId,
						previous,
						new Error(`Snapshot ${previousId} was superseded by ${snapshotId}`),
					);
					this.snapshotAssemblies.delete(previousId);
					this.ignoreSnapshotId(previousId);
				}
			}
			current.snapshotIds.add(snapshotId);
			return current;
		}
		const attempt: DaemonRuntimeSnapshotAttempt = {
			activeSessionId,
			attachmentEpoch: this.attachmentEpoch,
			purpose,
			state: "receiving",
			snapshotIds: new Set([snapshotId]),
		};
		this.runtimeSnapshotAttempt = attempt;
		return attempt;
	}

	private transitionSnapshotFailure(
		snapshotId: string,
		assembly: DaemonSnapshotAssembly,
		error: Error,
		activeSessionId: string,
		explicitPurpose?: DaemonSnapshotPurpose,
	): Promise<void> | undefined {
		this.rejectSnapshotAssembly(snapshotId, assembly, error);
		this.ignoreSnapshotId(snapshotId);
		const requestAttempt =
			assembly.requestAttemptId === undefined
				? undefined
				: this.snapshotRequestAttempts.get(assembly.requestAttemptId);
		if (requestAttempt) {
			// Keep the settled assembly until the command response binds its descriptor.
			// This covers begin/failed frames that beat the attach response continuation.
			this.snapshotAssemblies.set(snapshotId, assembly);
			requestAttempt.state = "failed";
			return undefined;
		}
		this.snapshotAssemblies.delete(snapshotId);
		const purpose = explicitPurpose ?? assembly.begin?.purpose ?? "attach";
		if (
			(purpose === "replacement" || purpose === "resync") &&
			!this.pendingReattachActiveSessionIds.has(activeSessionId)
		) {
			return this.transitionRuntimeSnapshotFailure(purpose, error, snapshotId, activeSessionId);
		}
		return undefined;
	}

	private transitionRuntimeSnapshotFailure(
		purpose: "replacement" | "resync",
		snapshotError: Error,
		snapshotId: string,
		activeSessionId = this.activeSessionId,
	): Promise<void> {
		const attempt = this.startRuntimeSnapshotAttempt(purpose, activeSessionId, snapshotId);
		if (attempt.state === "terminal" || attempt.state === "succeeded") return Promise.resolve();
		if (attempt.recovery) return attempt.recovery;
		attempt.state = "retrying";
		this.latestSnapshotIsFresh = false;
		const recovery = this.recoverFailedSnapshot(attempt.purpose).then(
			async () => {
				if (this.runtimeSnapshotAttempt !== attempt || attempt.state === "terminal") return;
				attempt.state = "succeeded";
				if (attempt.purpose === "replacement") this.releaseChunkedReplacementFence();
				this.runtimeSnapshotAttempt = undefined;
			},
			async (recoveryError: unknown) => {
				if (this.disposed || this.runtimeSnapshotAttempt !== attempt || attempt.state === "terminal") return;
				attempt.state = "terminal";
				for (const failedId of attempt.snapshotIds) this.ignoreSnapshotId(failedId);
				if (attempt.purpose === "replacement") {
					this.replacementReconciliationFailed = true;
					this.pendingChunkedReplacement = undefined;
				}
				if (this.terminalCloseEmitted) return;
				this.terminalCloseEmitted = true;
				await this.emit({
					type: "closed",
					error: `Failed to recover from a ${attempt.purpose} snapshot transfer. Snapshot error: ${formatErrorSentence(snapshotError)} Recovery error: ${formatErrorSentence(recoveryError)} ${this.formatDaemonDiagnosticContext()}`,
				});
			},
		);
		attempt.recovery = recovery;
		return recovery;
	}

	private async recoverFailedSnapshot(purpose: "replacement" | "resync"): Promise<void> {
		await this.attachSession(this.activeSessionId, this.lastEventCursor, false, 1, purpose);
		const snapshot = this.latestSnapshot;
		if (!snapshot) throw new Error("Daemon snapshot retry returned no snapshot");
		if (this.disposed) return;
		if (purpose === "replacement") {
			await this.emit({ type: "session_replaced", state: snapshot.state, messages: snapshot.messages });
		} else {
			await this.emit({ type: "session_resynced", snapshot });
		}
	}

	private async waitForSnapshot(
		snapshotId: string,
		requestAttempt?: DaemonSnapshotRequestAttempt,
	): Promise<DaemonSessionSnapshot> {
		const completed = this.completedSnapshots.get(snapshotId);
		const completedAttemptId = this.completedSnapshotAttemptIds.get(snapshotId);
		if (completed && (!requestAttempt || completedAttemptId === requestAttempt.id)) {
			this.completedSnapshots.delete(snapshotId);
			this.completedSnapshotAttemptIds.delete(snapshotId);
			return completed;
		}
		const assembly = this.getSnapshotAssembly(snapshotId, requestAttempt);
		if (requestAttempt) {
			requestAttempt.candidateSnapshotIds.add(snapshotId);
			assembly.requestAttemptId = requestAttempt.id;
			if (assembly.begin) this.validateSnapshotAttemptBegin(requestAttempt, assembly.begin);
		}
		try {
			return await assembly.promise;
		} finally {
			clearTimeout(assembly.timeout);
			if (this.snapshotAssemblies.get(snapshotId) === assembly) {
				this.snapshotAssemblies.delete(snapshotId);
			}
			this.completedSnapshots.delete(snapshotId);
			this.completedSnapshotAttemptIds.delete(snapshotId);
		}
	}

	private stageSnapshotCommit(
		snapshot: DaemonSessionSnapshot,
		options: {
			purpose: DaemonSnapshotPurpose;
			envelopeActiveSessionId: string;
			expectedSessionId?: string;
			expectedState?: AgentConnectionState;
			replay?: DaemonReplayInfo;
			resetEventProgress?: boolean;
			includePromptLifecycles?: boolean;
			progress: Array<{ cursor: DaemonEventCursor | undefined; sequence: number | undefined }>;
		},
	): StagedSnapshotCommit {
		const latestSnapshot = reconcileDaemonSessionSnapshot(snapshot, {
			purpose: options.purpose,
			envelopeActiveSessionId: options.envelopeActiveSessionId,
			expectedSessionId: options.expectedSessionId,
			expectedState: options.expectedState,
			cachedSnapshot: this.latestSnapshot,
			replay: options.replay,
			includePromptLifecycles:
				options.includePromptLifecycles ??
				this.supportsNegotiatedRuntimeCapability("correlated_prompt_lifecycle_v1"),
		});
		return this.stageMappedSnapshotCommit(latestSnapshot, {
			activeSessionId: options.envelopeActiveSessionId,
			attachedSessionId: snapshot.state.sessionId,
			attachedSessionFile: snapshot.state.sessionFile,
			progress: options.progress,
			allowStaleProgress:
				options.purpose === "attach" && this.latestSnapshot?.state.sessionId === snapshot.state.sessionId,
			resetEventProgress: options.resetEventProgress,
			childRosterSequence: Array.isArray(snapshot.children) ? snapshot.lastEventSequence : undefined,
		});
	}

	private stageMappedSnapshotCommit(
		latestSnapshot: AgentConnectionSnapshot,
		options: {
			activeSessionId: string;
			attachedSessionId: string;
			attachedSessionFile: string | undefined;
			progress: Array<{ cursor: DaemonEventCursor | undefined; sequence: number | undefined }>;
			allowStaleProgress?: boolean;
			resetEventProgress?: boolean;
			childRosterSequence?: number;
		},
	): StagedSnapshotCommit {
		const progress = stageEventProgress(
			options.resetEventProgress ? undefined : this.lastEventCursor,
			options.resetEventProgress ? undefined : this.lastEventSequence,
			options.resetEventProgress ? new Set<string>() : this.retiredEventGenerations,
			options.progress,
			options.allowStaleProgress,
		);
		const committedSnapshot = {
			...latestSnapshot,
			...(latestSnapshot.promptLifecycles
				? { promptLifecycles: retainPromptLifecycleState(latestSnapshot.promptLifecycles) }
				: {}),
		};
		if (progress.lastEventSequence !== undefined) {
			committedSnapshot.lastEventSequence = progress.lastEventSequence;
		}
		if (progress.lastEventCursor) {
			committedSnapshot.lastEventCursor = progress.lastEventCursor;
		}
		return {
			...progress,
			activeSessionId: options.activeSessionId,
			attachedSessionId: options.attachedSessionId,
			attachedSessionFile: options.attachedSessionFile,
			latestSnapshot: committedSnapshot,
			childRosterSequence: options.childRosterSequence,
		};
	}

	private commitStagedSnapshot(staged: StagedSnapshotCommit): void {
		const generationChanged =
			this.attachedSessionId !== undefined &&
			(this.activeSessionId !== staged.activeSessionId || this.attachedSessionId !== staged.attachedSessionId);
		this.prunePromptRoutes(
			staged.latestSnapshot.promptLifecycles,
			staged.activeSessionId,
			staged.attachedSessionId,
			generationChanged,
		);
		this.activeSessionId = staged.activeSessionId;
		this.attachedSessionId = staged.attachedSessionId;
		this.attachedSessionFile = staged.attachedSessionFile;
		this.lastEventCursor = staged.lastEventCursor;
		this.lastEventSequence = staged.lastEventSequence;
		this.retiredEventGenerations.clear();
		for (const generation of staged.retiredEventGenerations) {
			this.retiredEventGenerations.add(generation);
		}
		this.latestSnapshot = staged.latestSnapshot;
		this.childRosterSequence = staged.childRosterSequence;
		this.latestSnapshotIsFresh = true;
	}

	private retirePendingChunkedReplacement(): void {
		const pending = this.pendingChunkedReplacement;
		if (!pending) return;
		this.pendingChunkedReplacement = undefined;
		this.discardUnsolicitedRuntimeSnapshots = true;
		const snapshotId = pending.snapshotId;
		if (!snapshotId) return;
		const assembly = this.snapshotAssemblies.get(snapshotId);
		if (assembly) {
			this.rejectSnapshotAssembly(
				snapshotId,
				assembly,
				new Error("Daemon replacement snapshot was superseded by attachment change"),
			);
			this.snapshotAssemblies.delete(snapshotId);
		}
		this.completedSnapshots.delete(snapshotId);
		this.completedSnapshotAttemptIds.delete(snapshotId);
		this.ignoreSnapshotId(snapshotId);
	}

	private releaseChunkedReplacementFence(): void {
		const pending = this.pendingChunkedReplacement;
		if (!pending) return;
		this.pendingChunkedReplacement = undefined;
		for (const message of pending.queuedMessages) {
			this.dispatchDaemonMessage(message);
		}
	}

	private async completeSnapshotAssembly(
		message: Extract<DaemonOutbound, { type: "session_snapshot_end" }>,
	): Promise<void> {
		const assembly = this.getSnapshotAssembly(message.snapshotId);
		if (!assembly.begin) {
			await this.rejectInvalidSnapshotAssembly(
				message.snapshotId,
				assembly,
				new Error(`Snapshot ${message.snapshotId} ended before it began`),
				message.activeSessionId,
			);
			return;
		}
		const beginCursor = assembly.begin.snapshot.lastEventCursor;
		if (
			message.activeSessionId !== assembly.begin.activeSessionId ||
			!Number.isSafeInteger(message.chunkCount) ||
			message.chunkCount < 0 ||
			message.lastEventSequence !== assembly.begin.snapshot.lastEventSequence ||
			(message.lastEventCursor !== undefined &&
				(message.lastEventCursor.generation !== beginCursor?.generation ||
					message.lastEventCursor.sequence !== beginCursor?.sequence))
		) {
			await this.rejectInvalidSnapshotAssembly(
				message.snapshotId,
				assembly,
				new Error(`Snapshot ${message.snapshotId} ended with invalid metadata`),
				message.activeSessionId,
			);
			return;
		}
		if (assembly.chunks.size !== message.chunkCount) {
			await this.rejectInvalidSnapshotAssembly(
				message.snapshotId,
				assembly,
				new Error(
					`Snapshot ${message.snapshotId} ended with ${assembly.chunks.size} of ${message.chunkCount} chunks`,
				),
				message.activeSessionId,
			);
			return;
		}
		const messages: AgentMessage[] = [];
		for (let index = 0; index < message.chunkCount; index++) {
			const chunk = assembly.chunks.get(index);
			if (!chunk) {
				await this.rejectInvalidSnapshotAssembly(
					message.snapshotId,
					assembly,
					new Error(`Snapshot ${message.snapshotId} is missing chunk ${index}`),
					message.activeSessionId,
				);
				return;
			}
			messages.push(...chunk);
		}
		if (messages.length !== assembly.begin.messageCount) {
			await this.rejectInvalidSnapshotAssembly(
				message.snapshotId,
				assembly,
				new Error(
					`Snapshot ${message.snapshotId} contained ${messages.length} of ${assembly.begin.messageCount} messages`,
				),
				message.activeSessionId,
			);
			return;
		}
		const snapshot: DaemonSessionSnapshot = {
			...assembly.begin.snapshot,
			messages,
			lastEventSequence: message.lastEventSequence,
			lastEventCursor: message.lastEventCursor ?? assembly.begin.snapshot.lastEventCursor,
		};
		const purpose = assembly.begin.purpose ?? "attach";
		const pendingReattach = this.pendingReattachActiveSessionIds.has(message.activeSessionId);
		if (purpose === "attach" || pendingReattach) {
			assembly.resolve(snapshot);
			clearTimeout(assembly.timeout);
			if (pendingReattach) {
				this.snapshotAssemblies.delete(message.snapshotId);
				this.completedSnapshots.set(message.snapshotId, snapshot);
				this.completedSnapshotAttemptIds.set(message.snapshotId, assembly.requestAttemptId);
				while (this.completedSnapshots.size > MAX_COMPLETED_SNAPSHOTS) {
					const oldest = this.completedSnapshots.keys().next().value;
					if (oldest === undefined) break;
					this.completedSnapshots.delete(oldest);
					this.completedSnapshotAttemptIds.delete(oldest);
				}
			}
			return;
		}
		const pendingReplacement = purpose === "replacement" ? this.pendingChunkedReplacement : undefined;
		try {
			if (pendingReplacement?.snapshotId !== undefined && pendingReplacement.snapshotId !== message.snapshotId) {
				throw new Error("Daemon returned an invalid replacement snapshot");
			}
			const staged = this.stageSnapshotCommit(snapshot, {
				purpose,
				envelopeActiveSessionId: message.activeSessionId,
				expectedSessionId: purpose === "resync" ? this.attachedSessionId : undefined,
				expectedState: pendingReplacement?.header.state,
				progress: [
					...(pendingReplacement
						? [
								{
									cursor: getDaemonMessageCursor(pendingReplacement.header),
									sequence: getDaemonMessageSequence(pendingReplacement.header),
								},
							]
						: []),
					{ cursor: message.lastEventCursor, sequence: message.lastEventSequence },
				],
			});
			this.commitStagedSnapshot(staged);
			assembly.resolve(snapshot);
			clearTimeout(assembly.timeout);
			this.snapshotAssemblies.delete(message.snapshotId);
			const runtimeAttempt = this.runtimeSnapshotAttempt;
			if (runtimeAttempt?.snapshotIds.has(message.snapshotId) && runtimeAttempt.state === "receiving") {
				runtimeAttempt.state = "succeeded";
				this.runtimeSnapshotAttempt = undefined;
			}
			if (purpose === "replacement") {
				await this.emit({ type: "session_replaced", state: snapshot.state, messages });
			} else {
				await this.emit({ type: "session_resynced", snapshot: staged.latestSnapshot });
			}
			if (pendingReplacement) this.releaseChunkedReplacementFence();
		} catch {
			await this.rejectInvalidSnapshotAssembly(
				message.snapshotId,
				assembly,
				new Error("Daemon returned an invalid session snapshot"),
				message.activeSessionId,
			);
		}
	}

	private observePromptLifecycleResponse(lifecycle: PromptLifecycleSnapshot): PromptLifecycleSnapshot {
		if (!this.latestSnapshot) return lifecycle;
		const current = this.latestSnapshot.promptLifecycles ?? { records: [], expired: [] };
		const cached = current.records.find((record) => record.correlationId === lifecycle.correlationId);
		if (cached) {
			if (isDeepStrictEqual(cached, lifecycle)) return cached;
			if (isLegalPromptLifecycleForward(lifecycle, cached)) return cached;
			if (!isLegalPromptLifecycleForward(cached, lifecycle)) {
				throw new Error("Daemon prompt lifecycle response could not be reconciled");
			}
			this.commitPromptLifecycleState(
				{
					records: [
						...current.records.filter((record) => record.correlationId !== lifecycle.correlationId),
						lifecycle,
					],
					expired: current.expired,
				},
				false,
			);
			return lifecycle;
		}
		if (current.expired.some((tombstone) => tombstone.correlationId === lifecycle.correlationId)) {
			throw new Error("Daemon prompt lifecycle response could not be reconciled");
		}
		this.commitPromptLifecycleState({ records: [...current.records, lifecycle], expired: current.expired }, false);
		return lifecycle;
	}

	private commitPromptLifecycleState(promptLifecycles: PromptLifecycleStateSnapshot, syncProgress: boolean): void {
		if (!this.latestSnapshot) return;
		const retained = retainPromptLifecycleState(promptLifecycles);
		this.prunePromptRoutes(retained, this.activeSessionId, this.latestSnapshot.state.sessionId, false);
		this.latestSnapshot = {
			...this.latestSnapshot,
			promptLifecycles: retained,
			...(syncProgress && this.lastEventSequence !== undefined ? { lastEventSequence: this.lastEventSequence } : {}),
			...(syncProgress && this.lastEventCursor !== undefined ? { lastEventCursor: this.lastEventCursor } : {}),
		};
	}

	private prunePromptRoutes(
		next: PromptLifecycleStateSnapshot | undefined,
		activeSessionId: string,
		sessionId: string,
		removeRetiredGenerations: boolean,
	): void {
		if (!next) return;
		const retainedIds = new Set([
			...next.records.map((record) => record.correlationId),
			...next.expired.map((tombstone) => tombstone.correlationId),
		]);
		for (const [correlationId, route] of this.correlatedPromptRoutes) {
			const matchesGeneration = route.activeSessionId === activeSessionId && route.sessionId === sessionId;
			if (matchesGeneration && retainedIds.has(correlationId)) {
				route.pending = false;
				continue;
			}
			if (!route.pending && (matchesGeneration || removeRetiredGenerations)) {
				this.correlatedPromptRoutes.delete(correlationId);
			}
		}
	}

	private observeRlmChildUpdate(child: AgentConnectionRlmChildAgentSnapshot): void {
		if (!this.latestSnapshot) return;
		const children = this.latestSnapshot.children ?? [];
		const index = children.findIndex((candidate) => candidate.id === child.id);
		const updatedChildren = [...children];
		if (index === -1) {
			updatedChildren.push(child);
		} else {
			updatedChildren[index] = child;
		}
		this.latestSnapshot = { ...this.latestSnapshot, children: updatedChildren };
	}

	private observeStreamingMessage(event: AgentSessionEvent): void {
		if (!this.latestSnapshot) {
			return;
		}
		if ((event.type === "message_start" || event.type === "message_update") && event.message.role === "assistant") {
			this.latestSnapshot = { ...this.latestSnapshot, streamingMessage: event.message };
			return;
		}
		if ((event.type === "message_end" && event.message.role === "assistant") || event.type === "agent_end") {
			const { streamingMessage: _streamingMessage, ...snapshot } = this.latestSnapshot;
			this.latestSnapshot = snapshot;
		}
	}

	private isInvalidationDuringPendingAttachment(message: DaemonOutbound): boolean {
		if (
			!("activeSessionId" in message) ||
			typeof message.activeSessionId !== "string" ||
			(message.type !== "session_closed" &&
				message.type !== "session_replaced" &&
				message.type !== "session_resynced")
		) {
			return false;
		}
		const attempt = this.latestSnapshotRequestAttempt(message.activeSessionId);
		return attempt?.state === "requesting" || attempt?.state === "receiving";
	}

	private isPendingTargetInvalidation(message: DaemonOutbound): boolean {
		return (
			"activeSessionId" in message &&
			typeof message.activeSessionId === "string" &&
			message.activeSessionId !== this.activeSessionId &&
			this.pendingReattachActiveSessionIds.has(message.activeSessionId) &&
			(message.type === "session_closed" ||
				message.type === "session_replaced" ||
				message.type === "session_resynced")
		);
	}

	private isMessageForActiveSession(message: DaemonOutbound): boolean {
		if (!("activeSessionId" in message) || typeof message.activeSessionId !== "string") {
			return false;
		}
		if (message.activeSessionId === this.activeSessionId) return true;
		return (
			this.pendingReattachActiveSessionIds.has(message.activeSessionId) &&
			(isSnapshotTransferMessage(message) || this.isPendingTargetInvalidation(message))
		);
	}

	private isStaleSequencedMessage(message: DaemonOutbound): boolean {
		const cursor = getDaemonMessageCursor(message);
		if (cursor) {
			if (this.retiredEventGenerations.has(cursor.generation)) {
				return true;
			}
			return (
				this.lastEventCursor?.generation === cursor.generation && cursor.sequence <= this.lastEventCursor.sequence
			);
		}
		const sequence = getDaemonMessageSequence(message);
		return sequence !== undefined && this.lastEventSequence !== undefined && sequence <= this.lastEventSequence;
	}

	private observeDaemonEventSequence(message: DaemonOutbound): void {
		const cursor = getDaemonMessageCursor(message);
		if (cursor) {
			this.observeEventCursor(cursor);
			this.lastEventSequence = cursor.sequence;
			return;
		}
		const sequence = getDaemonMessageSequence(message);
		if (sequence === undefined) {
			return;
		}
		this.lastEventSequence =
			this.lastEventSequence === undefined ? sequence : Math.max(this.lastEventSequence, sequence);
		if (this.lastEventCursor) {
			this.lastEventCursor = {
				...this.lastEventCursor,
				sequence: Math.max(this.lastEventCursor.sequence, sequence),
			};
		}
	}

	private observeEventCursor(cursor: DaemonEventCursor): void {
		const current = this.lastEventCursor;
		if (current && current.generation !== cursor.generation) {
			this.retiredEventGenerations.add(current.generation);
		}
		if (!current || current.generation !== cursor.generation || cursor.sequence > current.sequence) {
			this.lastEventCursor = cursor;
		}
	}

	private async emit(event: AgentConnectionEvent): Promise<void> {
		const deliveries: Promise<void>[] = [];
		for (const listener of [...this.listeners]) {
			try {
				deliveries.push(Promise.resolve(listener(event)));
			} catch {
				// One attachment must not interrupt delivery or transport recovery for the others.
			}
		}
		await Promise.allSettled(deliveries);
	}

	private observeSideQuestionEvent(event: AgentConnectionSideQuestionEvent): void {
		if (event.status !== "running") {
			this.activeSideQuestionIds.delete(event.id);
		}
	}
}

function readSessionSummaries(value: unknown): SessionSummary[] {
	if (!value || typeof value !== "object" || !Array.isArray((value as { sessions?: unknown }).sessions)) {
		throw new Error("Daemon returned an invalid session list response");
	}
	return (value as { sessions: SessionSummary[] }).sessions;
}

function getAttachActiveSessionId(result: SessionSummary | DaemonAttachResult): string {
	if ("snapshot" in result) {
		return result.activeSessionId;
	}
	return result.activeSessionId ?? result.id;
}

function getAttachLastEventSequence(result: SessionSummary | DaemonAttachResult): number | undefined {
	if ("lastEventSequence" in result) {
		return result.lastEventSequence;
	}
	return undefined;
}

function getAttachLastEventCursor(result: SessionSummary | DaemonAttachResult): DaemonEventCursor | undefined {
	if ("lastEventCursor" in result) {
		return result.lastEventCursor;
	}
	return undefined;
}

function maxEventSequence(current: number | undefined, observed: number | undefined): number | undefined {
	if (current === undefined) {
		return observed;
	}
	if (observed === undefined) {
		return current;
	}
	return Math.max(current, observed);
}

function validateSummaryIdentity(summary: SessionSummary, activeSessionId: string): void {
	if (
		summary.id !== activeSessionId ||
		(summary.activeSessionId !== undefined && summary.activeSessionId !== activeSessionId)
	) {
		throw new Error("Daemon returned an invalid session snapshot");
	}
}

function validateConnectionStateIdentity(state: AgentConnectionState, activeSessionId: string): void {
	if (state.activeSessionId !== activeSessionId || !state.sessionId) {
		throw new Error("Daemon returned an invalid session snapshot");
	}
}

function validateDaemonSnapshotBegin(begin: DaemonSnapshotBegin): void {
	validateDaemonSnapshotIdentity(begin.snapshot, begin.activeSessionId);
	if (
		!Number.isSafeInteger(begin.messageCount) ||
		begin.messageCount < 0 ||
		!Number.isSafeInteger(begin.targetChunkBytes) ||
		begin.targetChunkBytes <= 0 ||
		begin.snapshot.summary.messageCount !== begin.messageCount ||
		!Number.isSafeInteger(begin.snapshot.lastEventSequence) ||
		begin.snapshot.lastEventSequence < 0 ||
		(begin.snapshot.lastEventCursor !== undefined &&
			(begin.snapshot.lastEventCursor.sequence !== begin.snapshot.lastEventSequence ||
				typeof begin.snapshot.lastEventCursor.generation !== "string" ||
				begin.snapshot.lastEventCursor.generation.length === 0))
	) {
		throw new Error("Daemon returned an invalid snapshot begin frame");
	}
}

function validateDaemonSnapshotIdentity(
	snapshot: Omit<DaemonSessionSnapshot, "messages">,
	activeSessionId: string,
	expectedSessionId?: string,
	expectedState?: AgentConnectionState,
): void {
	validateSummaryIdentity(snapshot.summary, activeSessionId);
	validateConnectionStateIdentity(snapshot.state, activeSessionId);
	if (
		snapshot.activeSessionId !== activeSessionId ||
		snapshot.summary.sessionId !== snapshot.state.sessionId ||
		(expectedSessionId !== undefined && snapshot.state.sessionId !== expectedSessionId) ||
		(expectedState !== undefined && !isDeepStrictEqual(snapshot.state, expectedState)) ||
		(snapshot.summary.sessionFile !== undefined &&
			snapshot.state.sessionFile !== undefined &&
			snapshot.summary.sessionFile !== snapshot.state.sessionFile)
	) {
		throw new Error("Daemon returned an invalid session snapshot");
	}
}

function isLegalPromptLifecycleForward(current: PromptLifecycleSnapshot, next: PromptLifecycleSnapshot): boolean {
	if (
		current.correlationId !== next.correlationId ||
		current.kind !== next.kind ||
		next.revision <= current.revision ||
		(current.deliveryCrossed && !next.deliveryCrossed) ||
		isPromptLifecycleTerminal(current.phase)
	) {
		return false;
	}
	const reachable =
		current.phase === "owned"
			? new Set(["queued", "delivered", "completed", "cancelled", "failed"])
			: current.phase === "queued"
				? new Set(["delivered", "completed", "cancelled", "failed"])
				: new Set(["completed", "failed"]);
	return reachable.has(next.phase);
}

function retainPromptLifecycleState(snapshot: PromptLifecycleStateSnapshot): PromptLifecycleStateSnapshot {
	const nonterminal = snapshot.records.filter((record) => !isPromptLifecycleTerminal(record.phase));
	const terminal = snapshot.records
		.filter((record) => isPromptLifecycleTerminal(record.phase))
		.sort((left, right) => left.revision - right.revision);
	const retainedTerminal = terminal.slice(-PROMPT_LIFECYCLE_TERMINAL_RETENTION);
	const evictedTerminal = terminal.slice(0, -PROMPT_LIFECYCLE_TERMINAL_RETENTION).map((record) => ({
		correlationId: record.correlationId,
		deliveryCrossed: record.deliveryCrossed,
	}));
	return {
		records: [...nonterminal, ...retainedTerminal].sort((left, right) => left.revision - right.revision),
		expired: [...snapshot.expired, ...evictedTerminal].slice(-PROMPT_LIFECYCLE_TOMBSTONE_RETENTION),
	};
}

function advancePromptLifecycleState(
	current: PromptLifecycleStateSnapshot | undefined,
	next: PromptLifecycleSnapshot,
): PromptLifecycleStateSnapshot | undefined {
	const state = current ?? { records: [], expired: [] };
	const previous = state.records.find((record) => record.correlationId === next.correlationId);
	if (previous && isDeepStrictEqual(previous, next)) return undefined;
	if (
		state.expired.some((tombstone) => tombstone.correlationId === next.correlationId) ||
		(previous !== undefined && !isPromptLifecycleSuccessor(previous, next))
	) {
		throw new Error("Daemon prompt lifecycle state could not be reconciled");
	}
	return {
		records: [...state.records.filter((record) => record.correlationId !== next.correlationId), next],
		expired: state.expired,
	};
}

function reconcilePromptLifecycleSnapshot(
	candidate: PromptLifecycleStateSnapshot,
	cached: PromptLifecycleStateSnapshot | undefined,
): PromptLifecycleStateSnapshot {
	if (cached === undefined) return candidate;
	const candidateRecords = new Map(candidate.records.map((record) => [record.correlationId, record]));
	const candidateExpired = new Map(candidate.expired.map((tombstone) => [tombstone.correlationId, tombstone]));
	for (const current of cached.records) {
		const next = candidateRecords.get(current.correlationId);
		if (next) {
			if (isDeepStrictEqual(current, next)) continue;
			if (next.revision === current.revision || !isLegalPromptLifecycleForward(current, next)) {
				throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
			}
			continue;
		}
		const tombstone = candidateExpired.get(current.correlationId);
		if (
			!isPromptLifecycleTerminal(current.phase) ||
			(tombstone !== undefined && tombstone.deliveryCrossed !== current.deliveryCrossed)
		) {
			throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
		}
	}
	for (const current of cached.expired) {
		if (candidateRecords.has(current.correlationId)) {
			throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
		}
		const tombstone = candidateExpired.get(current.correlationId);
		if (tombstone && tombstone.deliveryCrossed !== current.deliveryCrossed) {
			throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
		}
	}
	return candidate;
}

function mergePromptLifecycleStates(
	loaded: PromptLifecycleStateSnapshot,
	observed: PromptLifecycleStateSnapshot | undefined,
): PromptLifecycleStateSnapshot {
	if (observed === undefined) return retainPromptLifecycleState(loaded);
	try {
		return retainPromptLifecycleState(reconcilePromptLifecycleSnapshot(loaded, observed));
	} catch {
		const records = new Map(loaded.records.map((record) => [record.correlationId, record]));
		const expired = new Map(loaded.expired.map((tombstone) => [tombstone.correlationId, tombstone]));
		for (const current of observed.records) {
			const candidate = records.get(current.correlationId);
			if (candidate) {
				if (isDeepStrictEqual(candidate, current)) continue;
				if (!isLegalPromptLifecycleForward(candidate, current)) {
					throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
				}
				records.set(current.correlationId, current);
				continue;
			}
			const tombstone = expired.get(current.correlationId);
			if (
				!isPromptLifecycleTerminal(current.phase) ||
				(tombstone !== undefined && tombstone.deliveryCrossed !== current.deliveryCrossed)
			) {
				throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
			}
		}
		for (const current of observed.expired) {
			const candidate = expired.get(current.correlationId);
			if (
				records.has(current.correlationId) ||
				(candidate && candidate.deliveryCrossed !== current.deliveryCrossed)
			) {
				throw new Error("Daemon snapshot prompt lifecycle state could not be reconciled");
			}
		}
		return retainPromptLifecycleState({ records: [...records.values()], expired: [...expired.values()] });
	}
}

function reconcileDaemonSessionSnapshot(
	snapshot: DaemonSessionSnapshot,
	options: {
		purpose: DaemonSnapshotPurpose;
		envelopeActiveSessionId: string;
		expectedSessionId?: string;
		expectedState?: AgentConnectionState;
		cachedSnapshot: AgentConnectionSnapshot | undefined;
		replay?: DaemonReplayInfo;
		includePromptLifecycles: boolean;
	},
): AgentConnectionSnapshot {
	validateDaemonSnapshotIdentity(
		snapshot,
		options.envelopeActiveSessionId,
		options.purpose === "resync" ? options.expectedSessionId : undefined,
		options.purpose === "replacement" || options.purpose === "attach" ? options.expectedState : undefined,
	);
	const mapped = mapDaemonSessionSnapshot(snapshot, options.replay, options.includePromptLifecycles);
	if (options.includePromptLifecycles && mapped.promptLifecycles) {
		const reconciled =
			options.cachedSnapshot?.state.sessionId === snapshot.state.sessionId
				? reconcilePromptLifecycleSnapshot(mapped.promptLifecycles, options.cachedSnapshot.promptLifecycles)
				: mapped.promptLifecycles;
		mapped.promptLifecycles = retainPromptLifecycleState(reconciled);
	}
	return mapped;
}

function mapDaemonSessionSnapshot(
	snapshot: DaemonSessionSnapshot,
	replay?: DaemonReplayInfo,
	includePromptLifecycles = false,
): AgentConnectionSnapshot {
	if (includePromptLifecycles && !isPromptLifecycleStateSnapshot(snapshot.promptLifecycles)) {
		throw new Error("Daemon snapshot is missing valid prompt lifecycle state");
	}
	const connectionSnapshot: AgentConnectionSnapshot = {
		state: snapshot.state,
		messages: snapshot.messages,
		...(snapshot.summary.streamingMessage ? { streamingMessage: snapshot.summary.streamingMessage } : {}),
		lastEventSequence: snapshot.lastEventSequence,
		lastEventCursor: snapshot.lastEventCursor,
	};
	if (snapshot.sessionContext) {
		connectionSnapshot.sessionContext = snapshot.sessionContext;
	}
	if (snapshot.sessionTree) {
		connectionSnapshot.sessionTree = snapshot.sessionTree;
	}
	if (snapshot.parent) {
		connectionSnapshot.parent = snapshot.parent;
	}
	if (snapshot.children) {
		connectionSnapshot.children = snapshot.children;
	}
	if (includePromptLifecycles && snapshot.promptLifecycles) {
		connectionSnapshot.promptLifecycles = snapshot.promptLifecycles;
	}
	if (replay) {
		connectionSnapshot.replay = replay;
	}
	return connectionSnapshot;
}

function stageEventProgress(
	lastEventCursor: DaemonEventCursor | undefined,
	lastEventSequence: number | undefined,
	retiredEventGenerations: ReadonlySet<string>,
	observations: Array<{ cursor: DaemonEventCursor | undefined; sequence: number | undefined }>,
	allowStaleObservations = false,
): StagedEventProgress {
	let cursor = lastEventCursor;
	let sequence = lastEventSequence;
	const retired = new Set(retiredEventGenerations);
	for (const observation of observations) {
		if (observation.cursor) {
			if (observation.sequence !== undefined && observation.sequence !== observation.cursor.sequence) {
				throw new Error("Daemon returned an invalid session snapshot cursor");
			}
			if (
				retired.has(observation.cursor.generation) ||
				(cursor?.generation === observation.cursor.generation && observation.cursor.sequence < cursor.sequence)
			) {
				if (allowStaleObservations) continue;
				throw new Error("Daemon returned an invalid session snapshot cursor");
			}
			if (cursor && cursor.generation !== observation.cursor.generation) {
				retired.add(cursor.generation);
			}
			if (
				!cursor ||
				cursor.generation !== observation.cursor.generation ||
				observation.cursor.sequence > cursor.sequence
			) {
				cursor = observation.cursor;
			}
			sequence = cursor.sequence;
			continue;
		}
		sequence = maxEventSequence(sequence, observation.sequence);
	}
	return { lastEventCursor: cursor, lastEventSequence: sequence, retiredEventGenerations: retired };
}

function isSnapshotTransferMessage(message: DaemonOutbound): boolean {
	return (
		message.type === "session_snapshot_begin" ||
		message.type === "session_snapshot_chunk" ||
		message.type === "session_snapshot_end" ||
		message.type === "session_snapshot_failed"
	);
}

function getDaemonMessageSequence(message: DaemonOutbound): number | undefined {
	if (!("meta" in message)) {
		return undefined;
	}
	return message.meta?.sequence;
}

function getDaemonMessageCursor(message: DaemonOutbound): DaemonEventCursor | undefined {
	if (!("meta" in message)) {
		return undefined;
	}
	return message.meta?.cursor;
}

function invalidatesCachedSnapshot(commandType: DaemonCommandBody["type"]): boolean {
	switch (commandType) {
		case "attach":
		case "reattach":
		case "detach":
		case "list":
		case "list_saved_sessions":
		case "wait_for_idle":
		case "get_state":
		case "get_connection_state":
		case "get_messages":
		case "get_session_stats":
		case "get_commands":
		case "get_resource_snapshot":
		case "get_model_catalog":
		case "get_available_models":
		case "get_queue":
		case "cron_list":
		case "heartbeats_list":
		case "get_session_context":
		case "get_session_tree":
		case "get_user_messages_for_forking":
		case "get_last_assistant_text":
		case "get_system_prompt":
		case "get_tool_definition":
		case "start_side_question":
		case "abort_side_question":
		case "export_html":
		case "export_jsonl":
			return false;
		default:
			return true;
	}
}
