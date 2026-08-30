import type { AgentMessage } from "@earendil-works/pi-agent-core";
import { getModel } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import type { ExpiredPromptLifecycle, PromptLifecycleSnapshot } from "../src/core/prompt-lifecycle.js";
import { MissingSessionCwdError } from "../src/core/session-cwd.js";
import { SessionImportFileNotFoundError } from "../src/core/session-import-errors.js";
import {
	DAEMON_REFINE_REQUEST_TIMEOUT_MS,
	DaemonAgentConnection,
} from "../src/modes/agent-connection/daemon-agent-connection.js";
import type {
	AgentConnectionEvent,
	AgentConnectionRlmChildAgentSnapshot,
	AgentConnectionSavedSessionInfo,
	AgentConnectionSnapshot,
	AgentConnectionState,
} from "../src/modes/agent-connection/types.js";

import {
	DaemonCapabilityUnavailableError,
	type DaemonClient,
	type DaemonClientCloseListener,
	type DaemonClientMessageListener,
	type DaemonClientRequestOptions,
	type DaemonHello,
	DaemonSocketClosedError,
} from "../src/modes/daemon/daemon-client.js";
import {
	DAEMON_PROTOCOL_INFO,
	DAEMON_SCHEMA_REVISION,
	type DaemonAttachResult,
	type DaemonCommand,
	type DaemonOutbound,
	type DaemonResponse,
} from "../src/modes/daemon/daemon-protocol.js";

class FakeDaemonClient {
	readonly requests: DaemonCommand[] = [];
	readonly requestTimeouts: number[] = [];
	attachResultFactory: ((command: Extract<DaemonCommand, { type: "attach" }>) => DaemonAttachResult) | undefined;
	reattachResultFactory: ((command: Extract<DaemonCommand, { type: "reattach" }>) => DaemonAttachResult) | undefined;
	switchSessionActiveSessionId: string | undefined;
	attachGate: Promise<void> | undefined;
	reattachGate: Promise<void> | undefined;
	reconnectGate: Promise<void> | undefined;
	restoredAttachGate: Promise<void> | undefined;
	restoredAttachCompleted = 0;
	closeCount = 0;
	connected = true;
	ownerClosed = false;
	transportGeneration = 1;
	reconnectCount = 0;
	resetTransportCount = 0;
	reconnectError: Error | undefined;
	attachFailures = 0;
	connectionStateGate: Promise<void> | undefined;
	connectionStateFactory: ((activeSessionId: string) => AgentConnectionState) | undefined;
	rlmChildren: AgentConnectionRlmChildAgentSnapshot[] = [];
	rlmChildrenEventSequence = 12;
	rlmChildrenGate: Promise<void> | undefined;
	abortBashUnknownCommand = false;
	abortAndClearQueueUnknownCommand = false;
	inputPauseAcquireGate: Promise<void> | undefined;
	cronAddGate: Promise<void> | undefined;
	promptGate: Promise<void> | undefined;
	promptError: Error | undefined;
	promptResponseError: string | undefined;
	promptLifecycleGate: Promise<void> | undefined;
	promptLifecycleResponse = { records: [] as PromptLifecycleSnapshot[], expired: [] as ExpiredPromptLifecycle[] };
	correlatedPromptGate: Promise<void> | undefined;
	correlatedPromptError: Error | undefined;
	correlatedPromptResponseFactory:
		| ((command: Extract<DaemonCommand, { type: "submit_correlated_prompt" }>) => unknown)
		| undefined;
	cancelCorrelatedPromptResponse: unknown;
	cancelPromptAdmissionStatus: "cancelled" | "owned" | "unknown" = "owned";
	serverCapabilities = new Set<string>();
	updateRestartSessions: Array<Record<string, unknown>> = [];
	hello: DaemonHello | undefined = {
		type: "daemon_hello",
		socketPath: "/tmp/fake.sock",
		protocol: DAEMON_PROTOCOL_INFO,
		schemaRevision: DAEMON_SCHEMA_REVISION,
		clientId: "fake-client",
		serverCapabilities: ["prompt_admission_cancellation", "session_input_admission"],
	};
	private readonly messageListeners = new Set<DaemonClientMessageListener>();
	private readonly closeListeners = new Set<DaemonClientCloseListener>();

	async request(
		command: DaemonCommand,
		timeoutMs = 30000,
		options: DaemonClientRequestOptions = {},
	): Promise<DaemonResponse> {
		this.requests.push(command);
		this.requestTimeouts.push(timeoutMs);
		switch (command.type) {
			case "prompt":
				if (this.promptGate) await this.promptGate;
				if (this.promptError) throw this.promptError;
				if (this.promptResponseError) {
					return { type: "response", command: command.type, success: false, error: this.promptResponseError };
				}
				return { type: "response", command: command.type, success: true };
			case "prompt_and_wait":
				if (this.promptGate) await this.promptGate;
				if (this.promptError) throw this.promptError;
				return { type: "response", command: command.type, success: true };
			case "cancel_prompt_admission":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { status: this.cancelPromptAdmissionStatus },
				};
			case "submit_correlated_prompt":
				await this.correlatedPromptGate;
				if (this.correlatedPromptError) throw this.correlatedPromptError;
				return {
					type: "response",
					command: command.type,
					success: true,
					data: this.correlatedPromptResponseFactory?.(command) ?? {
						lifecycle: {
							correlationId: command.correlationId,
							phase: "queued",
							kind: "model_prompt",
							revision: 2,
							deliveryCrossed: false,
						},
						duplicate: false,
					},
				};
			case "cancel_correlated_prompt":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: this.cancelCorrelatedPromptResponse ?? {
						status: "cancelled",
						ownershipCrossed: true,
						deliveryCrossed: false,
						lifecycle: {
							correlationId: command.correlationId,
							phase: "cancelled",
							kind: "model_prompt",
							revision: 3,
							deliveryCrossed: false,
						},
					},
				};
			case "get_prompt_lifecycles":
				if (this.promptLifecycleGate) await this.promptLifecycleGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data: this.promptLifecycleResponse,
				};
			case "list":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { sessions: this.updateRestartSessions },
				};
			case "attach":
				if (this.attachGate) await this.attachGate;
				if (this.attachFailures > 0) {
					this.attachFailures--;
					throw new Error("attach failed");
				}
				if (command.activeSessionId === "active-restored" && this.restoredAttachGate) {
					await this.restoredAttachGate;
					this.restoredAttachCompleted++;
				}
				if (command.activeSessionId === "missing") {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Unknown active session: missing",
					};
				}
				return {
					type: "response",
					command: command.type,
					success: true,
					data:
						this.attachResultFactory?.(command) ??
						createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12),
				};
			case "reattach":
				if (this.reattachGate) await this.reattachGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data:
						this.reattachResultFactory?.(command) ??
						createAttachResult(command.targetActiveSessionId, command.clientId, command.capabilities, 1),
				};
			case "get_queue":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { steering: ["steer"], followUp: ["follow"] },
				};
			case "get_connection_state":
				await this.connectionStateGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data:
						this.connectionStateFactory?.(command.activeSessionId) ??
						createConnectionState(command.activeSessionId, "session-current"),
				};
			case "get_messages":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { messages: [{ role: "user", content: "current prompt", timestamp: 4 }] },
				};
			case "get_rlm_children":
				await this.rlmChildrenGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { children: this.rlmChildren, eventSequence: this.rlmChildrenEventSequence },
				};
			case "get_resource_snapshot":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						contextFiles: [{ path: "/tmp/AGENTS.md" }],
						skills: [
							{
								name: "demo-skill",
								description: "Demo skill",
								filePath: "/tmp/skills/demo-skill/SKILL.md",
								sourceInfo: {
									path: "/tmp/skills/demo-skill/SKILL.md",
									source: "local",
									scope: "project",
									origin: "top-level",
									baseDir: "/tmp/skills",
								},
							},
						],
						prompts: [],
						extensions: [],
						themes: [],
						diagnostics: {
							skills: [],
							prompts: [],
							extensions: [],
							themes: [],
						},
					},
				};
			case "replace_acp_mcp_servers":
				return { type: "response", command: command.type, success: true };
			case "get_model_catalog":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						models: [getModel("openai", "gpt-5.1")],
						configuredProviders: ["openai"],
					},
				};
			case "get_available_models":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { models: [getModel("openai", "gpt-5.1")] },
				};
			case "get_session_context":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						context: {
							messages: [{ role: "user", content: "context prompt", timestamp: 3 }],
							thinkingLevel: "medium",
							model: { provider: "anthropic", modelId: "claude-sonnet-4-5" },
						},
					},
				};
			case "get_session_tree":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						flatNodes: [
							{
								entry: {
									type: "message",
									id: "user-1",
									parentId: null,
									timestamp: "2026-01-01T00:00:00.000Z",
									message: { role: "user", content: "hello", timestamp: 1 },
								},
							},
						],
						leafId: "user-1",
					},
				};
			case "get_tool_definition":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						toolDefinition: {
							name: command.name,
							label: command.name,
							description: `${command.name} description`,
							promptSnippet: `${command.name} prompt`,
							promptGuidelines: [`Use ${command.name}`],
							parameters: { type: "object" },
							renderShell: "self",
						},
					},
				};
			case "clear_queue":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { steering: ["cleared"], followUp: [] },
				};
			case "abort_and_clear_queue":
				if (this.abortAndClearQueueUnknownCommand) {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Unknown daemon command: abort_and_clear_queue",
					};
				}
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { steering: ["aborted"], followUp: ["cleared"] },
				};
			case "acquire_session_input_pause":
				if (this.inputPauseAcquireGate) await this.inputPauseAcquireGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { pauseId: "pause-1" },
				};
			case "release_session_input_pause":
				return { type: "response", command: command.type, success: true };
			case "heartbeats_list":
				return this.serverCapabilities.has("heartbeat_catalog")
					? { type: "response", command: command.type, success: true, data: { heartbeats: [] } }
					: {
							type: "response",
							command: command.type,
							success: false,
							error: "Unknown daemon command: heartbeats_list",
						};
			case "heartbeat_manage":
				return this.serverCapabilities.has("heartbeat_management")
					? {
							type: "response",
							command: command.type,
							success: true,
							data: { heartbeat: { id: command.jobId } },
						}
					: {
							type: "response",
							command: command.type,
							success: false,
							error: "Unknown daemon command: heartbeat_manage",
						};
			case "list_saved_sessions": {
				const activeSessionId = "activeSessionId" in command ? command.activeSessionId : undefined;
				options.onProgress?.({
					id: "daemon_test",
					type: "session_list_progress",
					command: "list_saved_sessions",
					...(activeSessionId ? { activeSessionId } : {}),
					loaded: 1,
					total: 2,
				});
				options.onProgress?.({
					id: "daemon_test",
					type: "session_list_item",
					command: "list_saved_sessions",
					...(activeSessionId ? { activeSessionId } : {}),
					session: {
						path: "/tmp/session-a.jsonl",
						id: "session-a",
						cwd: "/tmp",
						name: "Saved session",
						created: "2026-01-01T00:00:00.000Z",
						modified: "2026-01-02T00:00:00.000Z",
						messageCount: 2,
						firstMessage: "hello",
						allMessagesText: "hello world",
					},
				});
				options.onProgress?.({
					id: "daemon_test",
					type: "session_list_progress",
					command: "list_saved_sessions",
					...(activeSessionId ? { activeSessionId } : {}),
					loaded: 2,
					total: 2,
				});
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						sessions: [
							{
								path: "/tmp/session-a.jsonl",
								id: "session-a",
								cwd: "/tmp",
								name: "Saved session",
								created: "2026-01-01T00:00:00.000Z",
								modified: "2026-01-02T00:00:00.000Z",
								messageCount: 2,
								firstMessage: "hello",
								allMessagesText: "hello world",
							},
						],
					},
				};
			}
			case "wait_for_headless_completion":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						enabled: false,
						continuationsUsed: 0,
						turnsUsed: 0,
						tokensUsed: 0,
						limits: { maxContinuations: 0 },
					},
				};
			case "wait_for_idle":
			case "set_scoped_models":
			case "rename_saved_session":
			case "extension_ui_response":
			case "detach":
				return { type: "response", command: command.type, success: true };
			case "cancel_rlm_child":
				if (command.childId === "stale-daemon") {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Unknown daemon command: cancel_rlm_child",
					};
				}
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { cancelled: command.childId === "child-1" },
				};
			case "execute_bash":
				if (command.command === "stale-daemon") {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Unknown daemon command: execute_bash",
					};
				}
				return { type: "response", command: command.type, success: true };
			case "abort_bash":
				if (this.abortBashUnknownCommand) {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Unknown daemon command: abort_bash",
					};
				}
				return { type: "response", command: command.type, success: true };
			case "start_side_question":
				return { type: "response", command: command.type, success: true };
			case "delete_saved_session":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: { ok: true, method: "trash" },
				};
			case "refine":
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						id: "refine_daemon",
						summary: "Daemon refinement",
						rationale: "Test daemon refine timeout",
						expectedOutcome: "Refine request completes",
						appliedEdits: [],
						harnessStatePath: "/tmp/harness_state.json",
					},
				};
			case "cron_add":
				await this.cronAddGate;
				return {
					type: "response",
					command: command.type,
					success: true,
					data: {
						job: {
							id: `cron-${this.requests.filter((request) => request.type === "cron_add").length}`,
							status: "active",
							source: "cron",
							activeSessionId: command.activeSessionId,
							sessionId: "session-current",
							sessionFile: "/tmp/session-current.jsonl",
							cwd: "/tmp/project",
							prompt: command.prompt,
							schedule: { kind: "cron", expression: command.schedule },
							createdAt: "2026-01-01T00:00:00.000Z",
							updatedAt: "2026-01-01T00:00:00.000Z",
							runCount: 0,
						},
					},
				};
			case "switch_session":
				if (this.switchSessionActiveSessionId) {
					return {
						type: "response",
						command: command.type,
						success: false,
						error: "Session is already active",
						errorInfo: {
							code: "session_already_active",
							sessionPath: command.sessionPath,
							activeSessionId: this.switchSessionActiveSessionId,
						},
					};
				}
				return {
					type: "response",
					command: command.type,
					success: false,
					error: "Stored session working directory does not exist: /tmp/missing\nSession file: /tmp/session.jsonl\nCurrent working directory: /tmp/current",
					errorInfo: {
						code: "missing_session_cwd",
						issue: {
							sessionFile: "/tmp/session.jsonl",
							sessionCwd: "/tmp/missing",
							fallbackCwd: "/tmp/current",
						},
					},
				};
			case "import_jsonl":
				return {
					type: "response",
					command: command.type,
					success: false,
					error: "File not found: /tmp/not-found.jsonl",
					errorInfo: {
						code: "session_import_file_not_found",
						filePath: "/tmp/not-found.jsonl",
					},
				};
			default:
				throw new Error(`Unexpected command: ${command.type}`);
		}
	}

	supportsServerCapability(capability: string): boolean {
		return this.serverCapabilities.has(capability);
	}

	onMessage(listener: DaemonClientMessageListener): () => void {
		this.messageListeners.add(listener);
		return () => {
			this.messageListeners.delete(listener);
		};
	}

	onClose(listener: DaemonClientCloseListener): () => void {
		this.closeListeners.add(listener);
		return () => {
			this.closeListeners.delete(listener);
		};
	}

	emitMessage(message: DaemonOutbound): void {
		for (const listener of [...this.messageListeners]) {
			listener(message);
		}
	}

	emitClose(error: Error): void {
		if (this.connected) {
			this.connected = false;
			this.transportGeneration++;
		}
		for (const listener of [...this.closeListeners]) {
			listener(error);
		}
	}

	enableRequestRecovery(): void {}

	getTransportGeneration(): number {
		return this.transportGeneration;
	}

	get isClosed(): boolean {
		return this.ownerClosed;
	}

	async connect(): Promise<void> {
		if (this.ownerClosed) throw new Error("Prime Agent daemon client is closed");
		if (this.connected) {
			throw new Error("Prime Agent daemon client is already connected");
		}
		this.reconnectCount++;
		if (this.reconnectError) {
			throw this.reconnectError;
		}
		this.connected = true;
		this.transportGeneration++;
	}

	async waitForHello(): Promise<DaemonHello> {
		return this.hello!;
	}

	resetTransportForReconnect(): void {
		this.resetTransportCount++;
		if (this.connected) this.transportGeneration++;
		this.connected = false;
	}

	async reconnect(): Promise<void> {
		if (this.ownerClosed) throw new Error("Prime Agent daemon client is closed");
		if (this.connected) {
			return;
		}
		this.reconnectCount++;
		if (this.reconnectGate) await this.reconnectGate;
		if (this.ownerClosed) throw new Error("Prime Agent daemon client is closed");
		if (this.reconnectError) {
			throw this.reconnectError;
		}
		this.connected = true;
		this.transportGeneration++;
	}

	getMessageListenerCount(): number {
		return this.messageListeners.size;
	}

	getCloseListenerCount(): number {
		return this.closeListeners.size;
	}

	close(): void {
		if (this.ownerClosed) return;
		this.ownerClosed = true;
		this.closeCount++;
		if (this.connected) this.transportGeneration++;
		this.connected = false;
		this.emitClose(new Error("Daemon socket closed"));
	}

	disconnectForReconnect(reason: "shutdown" | "update"): void {
		this.closeCount++;
		if (this.connected) this.transportGeneration++;
		this.connected = false;
		this.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", reason));
	}
}

function asDaemonClient(client: FakeDaemonClient): DaemonClient {
	return client as unknown as DaemonClient;
}

function createConnectionState(activeSessionId: string, sessionId: string): AgentConnectionState {
	return {
		activeSessionId,
		cwd: "/tmp/project",
		model: undefined,
		thinkingLevel: "medium",
		serviceTier: "default",
		availableThinkingLevels: ["minimal", "low", "medium", "high", "xhigh"],
		isStreaming: false,
		isCompacting: false,
		isBashRunning: false,
		retryAttempt: 0,
		steeringMode: "all",
		followUpMode: "one-at-a-time",
		sessionFile: `/tmp/${sessionId}.jsonl`,
		sessionId,
		sessionName: `${sessionId} name`,
		sessionDir: "/tmp/sessions",
		leafId: `${sessionId}-leaf`,
		autoCompactionEnabled: true,
		messageCount: 1,
		sessionActions: { queuedCount: 0, steering: [], followUps: [] },
		compactionCount: 0,
		goal: {
			active: false,
			status: "idle",
			tokensUsed: 0,
			timeUsedSeconds: 0,
			continuationsUsed: 0,
		},
		scopedModels: [],
		activeToolNames: ["ipython"],
		contextUsage: undefined,
	};
}

interface CreateAttachResultOptions {
	state?: AgentConnectionState;
	messages?: AgentMessage[];
	streamingMessage?: AgentMessage;
	sessionContext?: DaemonAttachResult["snapshot"]["sessionContext"];
	omitSessionContext?: boolean;
	sessionTree?: DaemonAttachResult["snapshot"]["sessionTree"];
	parent?: DaemonAttachResult["snapshot"]["parent"];
	children?: DaemonAttachResult["snapshot"]["children"];
	replay?: DaemonAttachResult["replay"];
}

function createAttachResult(
	activeSessionId: string,
	clientId: string | undefined,
	capabilities: readonly string[] | undefined,
	lastEventSequence: number,
	options: CreateAttachResultOptions = {},
): DaemonAttachResult {
	const state = options.state ?? createConnectionState(activeSessionId, "session-current");
	const messages = options.messages ?? [];
	const lastEventCursor = { generation: `generation-${activeSessionId}`, sequence: lastEventSequence };
	const summary = {
		id: activeSessionId,
		activeSessionId,
		lifecycle: "live" as const,
		activity: "idle" as const,
		isSessionActive: state.isStreaming,
		sessionId: state.sessionId,
		cwd: "/tmp/project",
		isStreaming: state.isStreaming,
		isCompacting: false,
		attachedClients: 1,
		messageCount: messages.length,
		sessionActions: { queuedCount: 0, steering: [], followUps: [] },
		...(options.streamingMessage ? { streamingMessage: options.streamingMessage } : {}),
	};
	// Slim shape: the daemon omits top-level state/messages for clients with the
	// "slim_attach" capability, which DaemonAgentConnection always advertises.
	return {
		protocol: DAEMON_PROTOCOL_INFO,
		activeSessionId,
		snapshot: {
			activeSessionId,
			summary,
			state,
			messages,
			...(options.omitSessionContext
				? {}
				: {
						sessionContext:
							options.sessionContext ??
							({
								messages,
								thinkingLevel: state.thinkingLevel,
								serviceTier: state.serviceTier,
								model: state.model ? { provider: state.model.provider, modelId: state.model.id } : null,
							} satisfies NonNullable<DaemonAttachResult["snapshot"]["sessionContext"]>),
					}),
			sessionTree: options.sessionTree ?? { tree: [], leafId: state.leafId },
			lastEventSequence,
			lastEventCursor,
			...(capabilities?.includes("correlated_prompt_lifecycle_v1")
				? { promptLifecycles: { records: [], expired: [] } }
				: {}),
			...(options.parent ? { parent: options.parent } : {}),
			...(options.children ? { children: options.children } : {}),
		},
		replay: options.replay ?? {
			status: "complete",
			toSequence: lastEventSequence,
			toCursor: lastEventCursor,
		},
		lastEventSequence,
		lastEventCursor,
		client: {
			id: clientId ?? "client-1",
			capabilities: (capabilities ?? ["attach_snapshot", "event_sequence"]).filter(
				(capability): capability is DaemonAttachResult["client"]["capabilities"][number] =>
					capability === "attach_snapshot" ||
					capability === "event_sequence" ||
					capability === "extension_ui" ||
					capability === "slim_attach" ||
					capability === "chunked_snapshot" ||
					capability === "correlated_prompt_lifecycle_v1",
			),
		},
	};
}

function fillSnapshotRetirementBudget(client: FakeDaemonClient, activeSessionId: string, lastEventSequence = 20): void {
	const snapshot = createAttachResult(activeSessionId, "client-1", undefined, lastEventSequence, {
		messages: [{ role: "user", content: "retired close-budget tail", timestamp: lastEventSequence }],
	}).snapshot;
	const { messages: _messages, ...header } = snapshot;
	for (let index = 0; index < 129; index++) {
		client.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId,
			snapshotId: `close-budget-${index}`,
			snapshot: header,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync",
		});
	}
}

function emitRlmChildUpdate(
	client: FakeDaemonClient,
	activeSessionId: string,
	sequence: number,
	child: AgentConnectionRlmChildAgentSnapshot,
): void {
	client.emitMessage({
		type: "session_event",
		activeSessionId,
		event: { type: "rlm_child_update", child },
		meta: {
			id: `${activeSessionId}:${sequence}`,
			protocol: DAEMON_PROTOCOL_INFO,
			activeSessionId,
			sequence,
			cursor: { generation: `generation-${activeSessionId}`, sequence },
			emittedAt: "2026-01-01T00:00:00.000Z",
		},
	});
}

function emitSequencedQueueUpdate(client: FakeDaemonClient, activeSessionId: string, sequence: number): void {
	client.emitMessage({
		type: "session_event",
		activeSessionId,
		event: {
			type: "session_action_update",
			actions: { queuedCount: 0, steering: [], followUps: [] },
		},
		meta: {
			id: `${activeSessionId}:${sequence}`,
			protocol: DAEMON_PROTOCOL_INFO,
			activeSessionId,
			sequence,
			cursor: { generation: `generation-${activeSessionId}`, sequence },
			emittedAt: "2026-01-01T00:00:00.000Z",
		},
	});
}

describe("DaemonAgentConnection", () => {
	it("carries an opt-out-only telemetry policy on attach", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			telemetryDisabled: true,
		});

		await connection.attach();

		expect(fakeClient.requests[0]).toMatchObject({
			type: "attach",
			activeSessionId: "active-1",
			telemetryDisabled: true,
		});
	});

	it.each([true, false])("capability-gates owned-session recovery context: %s", async (supported) => {
		const fakeClient = new FakeDaemonClient();
		if (supported) fakeClient.serverCapabilities.add("owned_session_recovery_context");
		const recoveryConfig = { cwd: "/tmp/fresh-owner" };
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-owned", {
			ownedSession: true,
			ownedSessionRecoveryConfig: recoveryConfig,
		});

		await connection.attach();

		const request = fakeClient.requests[0];
		if (supported) expect(request).toMatchObject({ recoveryConfig });
		else expect(request).not.toHaveProperty("recoveryConfig");
	});

	it("forwards queueIfBusy for prompt admission", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.prompt("queued input", { streamingBehavior: "followUp", queueIfBusy: true });

		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "prompt",
			activeSessionId: "active-1",
			message: "queued input",
			streamingBehavior: "followUp",
			queueIfBusy: true,
		});
	});

	it.each([false, true])(
		"advertises correlated lifecycle on attach only when the daemon supports it (%s)",
		async (supported) => {
			const fakeClient = new FakeDaemonClient();
			if (supported) fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

			await connection.attach();

			const request = fakeClient.requests[0];
			if (request?.type !== "attach") throw new Error("Missing attach request");
			expect(request.capabilities?.includes("correlated_prompt_lifecycle_v1") ?? false).toBe(supported);
		},
	);

	it("proves negotiated capabilities only after the exact attach commits", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		releaseAttach();
		await attaching;

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("clears a proved capability synchronously when a later attach is admitted", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});

		const attaching = connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		releaseAttach();
		await attaching;

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("lets only the latest of two same-stack attach admissions reach the daemon", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});

		const superseded = connection.attach();
		const latest = connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(superseded).rejects.toThrow("Daemon connection attachment changed during attach");
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		releaseAttach();
		await latest;

		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("fails closed before ignored snapshot IDs can evict a queued-admission tombstone", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const active = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		const queued = connection.attach();
		const stale = createAttachResult("active-1", "client-1", undefined, 14, {
			messages: [{ role: "user", content: "retired overflow", timestamp: 1 }],
		}).snapshot;
		const { messages: _messages, ...staleHeader } = stale;
		const begin = (snapshotId: string) => ({
			type: "session_snapshot_begin" as const,
			activeSessionId: "active-1",
			snapshotId,
			snapshot: staleHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync" as const,
		});
		for (let index = 0; index < 129; index++) fakeClient.emitMessage(begin(`overflow-retired-${index}`));
		await vi.waitFor(() => expect(events.some((event) => event.type === "closed")).toBe(true));
		expect((connection as unknown as { ignoredSnapshotIds: Set<string> }).ignoredSnapshotIds.size).toBe(128);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);

		releaseAttach();
		await expect(active).rejects.toThrow("Daemon connection attachment changed during attach");
		await expect(queued).rejects.toThrow("Daemon connection replacement reconciliation has failed");
		fakeClient.emitMessage(begin("overflow-retired-0"));
		fakeClient.emitMessage({
			type: "session_snapshot_chunk",
			activeSessionId: "active-1",
			snapshotId: "overflow-retired-0",
			index: 0,
			messages: stale.messages,
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-1",
			snapshotId: "overflow-retired-0",
			chunkCount: 1,
			lastEventSequence: 14,
			lastEventCursor: { generation: "generation-active-1", sequence: 14 },
		});
		await Promise.resolve();

		expect(events).toEqual([
			{ type: "closed", error: "Daemon replacement lifecycle state could not be reconciled." },
		]);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("lets a newer admission retire a stalled streamed attach and take the mutation tail", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		let attachmentIndex = 0;
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			if (attachmentIndex++ > 0) return result;
			return {
				...result,
				snapshot: { ...result.snapshot, messages: [] },
				snapshotStream: { id: "stalled-superseded", messageCount: 0, targetChunkBytes: 512 * 1024 },
			};
		};

		const superseded = connection.attach();
		await vi.waitFor(() =>
			expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(
				1,
			),
		);
		const latest = connection.attach();

		await expect(superseded).rejects.toThrow("Daemon connection attachment changed during attach");
		await latest;
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(3);
		expect((connection as unknown as { attachmentAdmissionInFlight: boolean }).attachmentAdmissionInFlight).toBe(
			false,
		);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("prevents an active superseded attach from publishing or releasing buffered frames", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});

		const superseded = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "superseded-buffer",
				phase: "queued",
				kind: "model_prompt",
				revision: 1,
				deliveryCrossed: false,
			},
		});
		const latest = connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		releaseAttach();

		await expect(superseded).rejects.toThrow("Daemon connection attachment changed during attach");
		await latest;
		expect(events).toEqual([]);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(3);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("prevents a superseded reattach from publishing while a later reattach is pending", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.switchSessionActiveSessionId = "active-next";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		let releaseReattach!: () => void;
		fakeClient.reattachGate = new Promise<void>((resolve) => {
			releaseReattach = resolve;
		});
		const replacementProofs: boolean[] = [];
		connection.subscribe((event) => {
			if (event.type === "session_replaced") {
				replacementProofs.push(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1"));
			}
		});

		const superseded = connection.switchSession("/tmp/session-next.jsonl");
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "reattach")).toHaveLength(1),
		);
		const latest = connection.switchSession("/tmp/session-next.jsonl");
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "switch_session")).toHaveLength(2),
		);
		await vi.waitFor(() =>
			expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false),
		);
		releaseReattach();

		await expect(superseded).rejects.toThrow("Daemon connection attachment changed during attach");
		await expect(latest).resolves.toEqual({ cancelled: false });
		expect(fakeClient.requests.filter((request) => request.type === "reattach")).toHaveLength(2);
		expect(replacementProofs).toEqual([true]);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("buffers correlated runtime frames until the exact attach proof commits", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		await expect(
			connection.submitCorrelatedPrompt("must wait", { correlationId: "before-proof" }),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "prompt-early",
				phase: "delivered",
				kind: "model_prompt",
				revision: 1,
				deliveryCrossed: true,
			},
		});
		await Promise.resolve();
		expect(events).toEqual([]);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		const fence = connection as unknown as {
			pendingNegotiatedRuntimeFrames: unknown[];
			pendingNegotiatedRuntimeFrameWeight: number;
		};
		expect(fence.pendingNegotiatedRuntimeFrames).toHaveLength(2);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBeGreaterThan(0);

		releaseAttach();
		await attaching;
		await vi.waitFor(() => expect(events).toHaveLength(2));
		expect(events.map((event) => event.type)).toEqual(["session_event", "prompt_lifecycle"]);
		expect(fence.pendingNegotiatedRuntimeFrames).toEqual([]);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBe(0);
	});

	it("discards pre-proof correlated runtime frames when the attach echo omits the capability", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.client.capabilities = result.client.capabilities.filter(
				(capability) => capability !== "correlated_prompt_lifecycle_v1",
			);
			delete result.snapshot.promptLifecycles;
			return result;
		};
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "unproved-buffered",
				phase: "queued",
				kind: "model_prompt",
				revision: 1,
				deliveryCrossed: false,
			},
		});
		const fence = connection as unknown as {
			pendingNegotiatedRuntimeFrames: unknown[];
			pendingNegotiatedRuntimeFrameWeight: number;
		};
		expect(fence.pendingNegotiatedRuntimeFrames).toHaveLength(1);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBeGreaterThan(0);
		releaseAttach();
		await attaching;
		for (let flush = 0; flush < 3; flush++) await Promise.resolve();

		expect(events).toEqual([]);
		expect(fence.pendingNegotiatedRuntimeFrames).toEqual([]);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBe(0);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(
			connection.submitCorrelatedPrompt("must fail", { correlationId: "unproved-buffered" }),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);
	});

	it("fails closed when pre-proof correlated runtime buffering exceeds its bound", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});
		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));

		for (let revision = 1; revision <= 129; revision++) {
			fakeClient.emitMessage({
				type: "prompt_lifecycle",
				activeSessionId: "active-1",
				lifecycle: {
					correlationId: `pre-proof-${revision}`,
					phase: "queued",
					kind: "model_prompt",
					revision,
					deliveryCrossed: false,
				},
			});
		}
		await vi.waitFor(() => expect(closed).toHaveLength(1));
		releaseAttach();

		await expect(attaching).rejects.toThrow("Daemon connection attachment changed during attach");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(connection.attach()).rejects.toThrow("Daemon connection replacement reconciliation has failed");
	});

	it("fails closed on cumulative pre-proof runtime-frame weight below the count cap", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});
		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		const canary = `private-payload-canary:${"x".repeat(80 * 1024)}`;
		const emitLargeAttributedFrame = (id: string) => {
			fakeClient.emitMessage({
				type: "extension_ui_request",
				activeSessionId: "active-1",
				id,
				method: "render",
				payload: { value: canary },
				attribution: { scope: "session" },
			});
		};

		emitLargeAttributedFrame("large-1");
		const fence = connection as unknown as {
			pendingNegotiatedRuntimeFrames: unknown[];
			pendingNegotiatedRuntimeFrameWeight: number;
		};
		expect(fence.pendingNegotiatedRuntimeFrames).toHaveLength(1);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBeGreaterThan(0);
		emitLargeAttributedFrame("large-2");
		await vi.waitFor(() => expect(closed).toHaveLength(1));

		expect(fence.pendingNegotiatedRuntimeFrames).toEqual([]);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBe(0);
		expect(closed[0]).toEqual({
			type: "closed",
			error: "Daemon replacement lifecycle state could not be reconciled.",
		});
		expect(JSON.stringify(closed)).not.toContain("private-payload-canary");
		releaseAttach();
		await expect(attaching).rejects.toThrow("Daemon connection attachment changed during attach");
	});

	it("fails closed without retaining one individually overweight pre-proof frame", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});
		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));

		fakeClient.emitMessage({
			type: "extension_ui_request",
			activeSessionId: "active-1",
			id: "overweight",
			method: "render",
			payload: { value: `single-private-canary:${"x".repeat(140 * 1024)}` },
			attribution: { scope: "session" },
		});
		await vi.waitFor(() => expect(closed).toHaveLength(1));
		const fence = connection as unknown as {
			pendingNegotiatedRuntimeFrames: unknown[];
			pendingNegotiatedRuntimeFrameWeight: number;
		};
		expect(fence.pendingNegotiatedRuntimeFrames).toEqual([]);
		expect(fence.pendingNegotiatedRuntimeFrameWeight).toBe(0);
		expect(JSON.stringify(closed)).not.toContain("single-private-canary");
		releaseAttach();
		await expect(attaching).rejects.toThrow("Daemon connection attachment changed during attach");
	});

	it("does not treat a server offer as attach-side negotiation", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.client.capabilities = result.client.capabilities.filter(
				(capability) => capability !== "correlated_prompt_lifecycle_v1",
			);
			delete result.snapshot.promptLifecycles;
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();

		expect(connection.supportsCorrelatedPromptLifecycle()).toBe(true);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		const events: AgentConnectionEvent[] = [];
		const resynced = new Promise<void>((resolve) => {
			connection.subscribe((event) => {
				events.push(event);
				if (event.type === "session_resynced") resolve();
			});
		});
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "unproved-prompt",
				phase: "delivered",
				kind: "model_prompt",
				revision: 1,
				deliveryCrossed: true,
			},
		});
		await Promise.resolve();
		expect(events).toEqual([]);
		const snapshot = createAttachResult("active-1", "resync-client", [], 13).snapshot;
		fakeClient.emitMessage({ type: "session_resynced", activeSessionId: "active-1", snapshot });
		await resynced;
		expect(events.map((event) => event.type)).toEqual(["session_resynced"]);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("rejects a wrong-client attach proof without retaining capability state", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.client.id = "private-client-canary";
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		const error = await connection.attach().catch((cause: unknown) => cause);

		expect(error).toEqual(new Error("Daemon returned invalid attached client capability proof"));
		expect(String(error)).not.toContain("private-client-canary");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it.each(["duplicate", "unrequested", "unknown", "non-array", "non-string"] as const)(
		"rejects %s attached capability proof without retaining state",
		async (mode) => {
			const fakeClient = new FakeDaemonClient();
			if (mode !== "unrequested") fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			fakeClient.attachResultFactory = (command) => {
				const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
				if (mode === "duplicate") {
					result.client.capabilities.push("correlated_prompt_lifecycle_v1");
				} else if (mode === "unrequested") {
					result.client.capabilities.push("correlated_prompt_lifecycle_v1");
					result.snapshot.promptLifecycles = { records: [], expired: [] };
				} else if (mode === "unknown") {
					(result.client.capabilities as unknown[]).push("private-capability-canary");
				} else if (mode === "non-array") {
					(result.client as unknown as { capabilities: unknown }).capabilities = null;
				} else {
					(result.client.capabilities as unknown[]).push(undefined);
				}
				return result;
			};
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

			const error = await connection.attach().catch((cause: unknown) => cause);
			expect(error).toEqual(new Error("Daemon returned invalid attached client capability proof"));
			expect(String(error)).not.toContain("private-capability-canary");
			expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		},
	);

	it("clears negotiated proof synchronously when the daemon socket closes", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		fakeClient.emitClose(new Error("private close canary"));

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("treats explicit DaemonClient.close as terminal instead of recovering it", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const recoverDaemon = vi.fn(async () => {});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", { recoverDaemon });
		await connection.attach();
		const closed = new Promise<AgentConnectionEvent>((resolve) => {
			connection.subscribe((event) => {
				if (event.type === "closed") resolve(event);
			});
		});

		fakeClient.ownerClosed = true;
		fakeClient.emitClose(new Error("private owner-close canary"));

		await expect(closed).resolves.toEqual({
			type: "closed",
			error: "Prime Agent daemon client was closed.",
		});
		expect(recoverDaemon).not.toHaveBeenCalled();
		expect(fakeClient.reconnectCount).toBe(0);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("stops an in-flight normal recovery when the owner closes a socketless client", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseRecovery!: () => void;
		const recoveryGate = new Promise<void>((resolve) => {
			releaseRecovery = resolve;
		});
		const recoverDaemon = vi.fn(async () => recoveryGate);
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			recoverDaemon,
			reconnectTimeoutMs: 500,
		});
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		fakeClient.emitClose(new Error("private transport-loss canary"));
		await vi.waitFor(() => expect(recoverDaemon).toHaveBeenCalledTimes(1));
		const recovery = (connection as unknown as { reconnectPromise?: Promise<void> }).reconnectPromise;
		expect(recovery).toBeDefined();
		fakeClient.close();
		await vi.waitFor(() =>
			expect(events.filter((event) => event.type === "closed")).toEqual([
				{ type: "closed", error: "Prime Agent daemon client was closed." },
			]),
		);
		releaseRecovery();
		await recovery;

		expect(recoverDaemon).toHaveBeenCalledTimes(1);
		expect(fakeClient.reconnectCount).toBe(0);
		expect(events.filter((event) => event.type === "connection_status" && event.status === "connected")).toEqual([]);
		expect(JSON.stringify(events.filter((event) => event.type === "closed"))).not.toContain("private");
	});

	it("stops an in-flight update recovery when the owner closes a socketless client", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseReconnect!: () => void;
		fakeClient.reconnectGate = new Promise<void>((resolve) => {
			releaseReconnect = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/private-update-canary.sock", "update"));
		await vi.waitFor(() => expect(fakeClient.reconnectCount).toBe(1));
		const recovery = (connection as unknown as { updateReconnectPromise?: Promise<void> }).updateReconnectPromise;
		expect(recovery).toBeDefined();
		fakeClient.close();
		await vi.waitFor(() =>
			expect(events.filter((event) => event.type === "closed")).toEqual([
				{ type: "closed", error: "Prime Agent daemon client was closed." },
			]),
		);
		releaseReconnect();
		await recovery;

		expect(fakeClient.reconnectCount).toBe(1);
		expect(fakeClient.requests.filter((request) => request.type === "list")).toEqual([]);
		expect(events.filter((event) => event.type === "connection_status" && event.status === "connected")).toEqual([]);
		expect(JSON.stringify(events.filter((event) => event.type === "closed"))).not.toContain("private");
	});

	it("serializes shared-client attaches and proves only the latest completed attachment", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseFirst!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const first = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const second = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-b");

		const firstAttach = first.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1),
		);
		const secondAttach = second.attach();
		await Promise.resolve();
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
		releaseFirst();
		await Promise.all([firstAttach, secondAttach]);

		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("fences an in-flight attach across a silent shared transport reset", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		fakeClient.resetTransportForReconnect();
		releaseAttach();

		await expect(attaching).rejects.toThrow("Daemon connection attachment changed during attach");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("fences an in-flight attach when its session closes before the response", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});

		const attaching = connection.attach();
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "attach")).toBe(true));
		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-1", reason: "killed" });
		releaseAttach();

		await expect(attaching).rejects.toThrow("Daemon connection attachment changed during attach");
		await vi.waitFor(() => expect(closed).toHaveLength(1));
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(connection.attach()).rejects.toThrow("Daemon connection replacement reconciliation has failed");
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("fences a pending target reattach when the target generation closes", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.switchSessionActiveSessionId = "active-target";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-source");
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		let releaseReattach!: () => void;
		fakeClient.reattachGate = new Promise<void>((resolve) => {
			releaseReattach = resolve;
		});

		const switching = connection.switchSession("/tmp/target.jsonl");
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "reattach")).toBe(true));
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-target", reason: "replaced" });
		releaseReattach();

		await expect(switching).rejects.toThrow("Daemon connection attachment changed during attach");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("rejects attach re-entry throughout the owned-session dispose wait", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-owned", {
			ownedSession: true,
		});
		await connection.attach();
		let releaseReconnect!: () => void;
		const reconnect = new Promise<void>((resolve) => {
			releaseReconnect = resolve;
		});
		fakeClient.connected = false;
		(connection as unknown as { reconnectPromise?: Promise<void> }).reconnectPromise = reconnect;

		const disposing = connection.dispose();
		await Promise.resolve();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(connection.attach()).rejects.toThrow("Daemon connection is disposing");
		releaseReconnect();
		await disposing;
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("allows only an unproved owned-session cleanup reattach once disposal begins", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseRecovery!: () => void;
		const recoverDaemon = vi.fn(
			() =>
				new Promise<void>((resolve) => {
					releaseRecovery = resolve;
				}),
		);
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-owned", {
			ownedSession: true,
			recoverDaemon,
			reconnectTimeoutMs: 2000,
		});
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fakeClient.connected = false;
		fakeClient.emitClose(new Error("Daemon socket closed"));
		await vi.waitFor(() => expect(recoverDaemon).toHaveBeenCalledOnce());

		const disposing = connection.dispose();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(connection.attach()).rejects.toThrow("Daemon connection is disposing");
		releaseRecovery();
		await disposing;

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "complete_owned_session",
			activeSessionId: "active-owned",
		});
		expect(events).not.toContainEqual(expect.objectContaining({ type: "session_resynced" }));
	});

	it("invalidates same-session peers on shared-client attach and detach", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.serverCapabilities.add("extension_ui");
		const first = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-shared");
		const second = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-shared", {
			supportsExtensionUi: false,
		});
		await first.attach();
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		expect(first.supportsNegotiatedCapability("extension_ui")).toBe(true);

		await second.attach();
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		expect(second.supportsNegotiatedCapability("extension_ui")).toBe(false);

		await first.dispose();
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("does not replay a retired same-route snapshot begin after a fresh attach commits", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await connection.attach();
		const retired = createAttachResult("active-a", "client-1", ["correlated_prompt_lifecycle_v1"], 14, {
			messages: [{ role: "user", content: "retired", timestamp: 1 }],
		}).snapshot;
		const { messages: _retiredMessages, ...retiredHeader } = retired;
		const retiredBegin = {
			type: "session_snapshot_begin" as const,
			activeSessionId: "active-a",
			snapshotId: "retired-same-route-snapshot",
			snapshot: retiredHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync" as const,
		};
		const retiredChunk = {
			type: "session_snapshot_chunk" as const,
			activeSessionId: "active-a",
			snapshotId: "retired-same-route-snapshot",
			index: 0,
			messages: retired.messages,
		};
		const retiredEnd = {
			type: "session_snapshot_end" as const,
			activeSessionId: "active-a",
			snapshotId: "retired-same-route-snapshot",
			chunkCount: 1,
			lastEventSequence: 14,
			lastEventCursor: { generation: "generation-active-a", sequence: 14 },
		};
		fakeClient.emitMessage(retiredBegin);
		fakeClient.emitMessage(retiredChunk);
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 14, {
				messages: [{ role: "user", content: "fresh attach", timestamp: 2 }],
			});
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		fakeClient.emitMessage(retiredBegin);
		fakeClient.emitMessage(retiredChunk);
		fakeClient.emitMessage(retiredEnd);
		fakeClient.emitMessage(retiredEnd);
		await Promise.resolve();

		expect(events).toEqual([]);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			messages: [{ role: "user", content: "fresh attach", timestamp: 2 }],
		});
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it("lets an exact current streamed attach rebind a retired snapshot ID with a fresh begin", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await connection.attach();
		const fresh = createAttachResult("active-a", "client-1", ["correlated_prompt_lifecycle_v1"], 15, {
			messages: [{ role: "user", content: "fresh rebound", timestamp: 3 }],
		});
		const { messages: _freshMessages, ...freshHeader } = fresh.snapshot;
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			snapshot: freshHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync",
		});
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 15, {
				messages: fresh.snapshot.messages,
			});
			return {
				...result,
				snapshot: { ...result.snapshot, messages: [] },
				snapshotStream: { id: "rebound-current-request", messageCount: 1, targetChunkBytes: 512 * 1024 },
			};
		};

		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const attaching = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			snapshot: freshHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_chunk",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			index: 0,
			messages: [{ role: "user", content: "retired during request", timestamp: 2 }],
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			chunkCount: 1,
			lastEventSequence: 15,
			lastEventCursor: { generation: "generation-active-a", sequence: 15 },
		});
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			snapshot: freshHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "attach",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_chunk",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			index: 0,
			messages: fresh.snapshot.messages,
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-a",
			snapshotId: "rebound-current-request",
			chunkCount: 1,
			lastEventSequence: 15,
			lastEventCursor: { generation: "generation-active-a", sequence: 15 },
		});
		releaseAttach();
		await attaching;

		expect(events).toEqual([]);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			messages: [{ role: "user", content: "fresh rebound", timestamp: 3 }],
		});
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("keeps a retired same-route failed snapshot ID fenced across duplicate terminal frames", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await connection.attach();
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});

		const attaching = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2),
		);
		const delayedFailure = {
			type: "session_snapshot_failed" as const,
			activeSessionId: "active-a",
			snapshotId: "same-route-retired-failure",
			purpose: "resync" as const,
			error: "private retired failure",
		};
		fakeClient.emitMessage(delayedFailure);
		releaseAttach();
		await attaching;
		let releaseNextAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseNextAttach = resolve;
		});
		const nextAttach = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(3),
		);
		fakeClient.emitMessage(delayedFailure);
		fakeClient.emitMessage(delayedFailure);
		releaseNextAttach();
		await nextAttach;
		fakeClient.emitMessage(delayedFailure);
		fakeClient.emitMessage(delayedFailure);
		await Promise.resolve();

		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(3);
		expect((connection as unknown as { runtimeSnapshotAttempt?: unknown }).runtimeSnapshotAttempt).toBeUndefined();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("suppresses headerless old-route snapshot recovery during a newer attachment admission", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.switchSessionActiveSessionId = "active-b";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await connection.attach();
		let releaseReattach!: () => void;
		fakeClient.reattachGate = new Promise<void>((resolve) => {
			releaseReattach = resolve;
		});

		const switching = connection.switchSession("/tmp/session-b.jsonl");
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "reattach")).toBe(true));
		const delayedSnapshot = createAttachResult(
			"active-a",
			"client-1",
			["correlated_prompt_lifecycle_v1"],
			99,
		).snapshot;
		const { messages: _messages, ...delayedSnapshotHeader } = delayedSnapshot;
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-a",
			snapshotId: "headerless-old-a-snapshot",
			snapshot: delayedSnapshotHeader,
			messageCount: 0,
			targetChunkBytes: 512 * 1024,
			purpose: "resync",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-a",
			snapshotId: "headerless-old-a-snapshot",
			chunkCount: 0,
			lastEventSequence: 99,
			lastEventCursor: { generation: "generation-active-a", sequence: 99 },
		});
		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-a",
			snapshotId: "headerless-old-a-failure",
			purpose: "resync",
			error: "private old A resync failure",
		});
		await Promise.resolve();
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
		expect((connection as unknown as { runtimeSnapshotAttempt?: unknown }).runtimeSnapshotAttempt).toBeUndefined();
		releaseReattach();
		await expect(switching).resolves.toEqual({ cancelled: false });

		expect((connection as unknown as { activeSessionId: string }).activeSessionId).toBe("active-b");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
	});

	it("retires a header-only chunked replacement before reattaching to a new route", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.switchSessionActiveSessionId = "active-b";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-a-replacement"),
			messages: [],
			snapshotFollows: true,
		});
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-a",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 99, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		let releaseReattach!: () => void;
		fakeClient.reattachGate = new Promise<void>((resolve) => {
			releaseReattach = resolve;
		});
		const switching = connection.switchSession("/tmp/session-b.jsonl");
		await vi.waitFor(() => expect(fakeClient.requests.some((request) => request.type === "reattach")).toBe(true));
		const staleSnapshot = createAttachResult("active-a", "client-1", undefined, 12, {
			state: createConnectionState("active-a", "session-a-replacement"),
		}).snapshot;
		const { messages: _staleMessages, ...staleHeader } = staleSnapshot;
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-a",
			snapshotId: "stale-after-b-admission",
			snapshot: staleHeader,
			messageCount: 0,
			targetChunkBytes: 512 * 1024,
			purpose: "replacement",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-a",
			snapshotId: "stale-after-b-admission",
			chunkCount: 0,
			lastEventSequence: staleSnapshot.lastEventSequence,
			lastEventCursor: staleSnapshot.lastEventCursor,
		});
		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-a",
			snapshotId: "stale-failed-before-begin",
			purpose: "replacement",
			error: "private retired A failure",
		});
		await Promise.resolve();
		expect(events.filter((event) => event.type === "session_replaced")).toEqual([]);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
		releaseReattach();
		await expect(switching).resolves.toEqual({ cancelled: false });
		const fence = connection as unknown as { pendingChunkedReplacement?: unknown };
		expect(fence.pendingChunkedReplacement).toBeUndefined();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-b",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		await vi.waitFor(() =>
			expect(events.filter((event) => event.type === "session_event")).toEqual([
				expect.objectContaining({
					type: "session_event",
					event: expect.objectContaining({ actions: expect.objectContaining({ queuedCount: 0 }) }),
				}),
			]),
		);

		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-b", reason: "killed" });
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await vi.waitFor(() => expect(events.some((event) => event.type === "closed")).toBe(true));
	});

	it("retires a header-only chunked replacement across socket recovery", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const recoverDaemon = vi.fn(async () => {});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a", { recoverDaemon });
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-a-replacement"),
			messages: [],
			snapshotFollows: true,
		});
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-a",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 99, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});

		fakeClient.emitClose(new Error("transport lost"));
		await vi.waitFor(() =>
			expect(events.some((event) => event.type === "connection_status" && event.status === "connected")).toBe(true),
		);
		const fence = connection as unknown as { pendingChunkedReplacement?: unknown };
		expect(fence.pendingChunkedReplacement).toBeUndefined();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-a",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		await vi.waitFor(() =>
			expect(events.filter((event) => event.type === "session_event")).toEqual([
				expect.objectContaining({
					type: "session_event",
					event: expect.objectContaining({ actions: expect.objectContaining({ queuedCount: 0 }) }),
				}),
			]),
		);

		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-a", reason: "killed" });
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("bounds queued chunked-replacement frames by cumulative structural weight", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-a-replacement"),
			messages: [],
			snapshotFollows: true,
		});
		const canary = `chunk-fence-private-canary:${"x".repeat(80 * 1024)}`;
		const emitLargeFrame = (id: string) => {
			fakeClient.emitMessage({
				type: "extension_ui_request",
				activeSessionId: "active-a",
				id,
				method: "render",
				payload: { value: canary },
				attribution: { scope: "session" },
			});
		};

		emitLargeFrame("large-1");
		const fence = connection as unknown as {
			pendingChunkedReplacement?: { queuedMessages: unknown[]; queuedMessageWeight: number };
		};
		expect(fence.pendingChunkedReplacement?.queuedMessages).toHaveLength(1);
		expect(fence.pendingChunkedReplacement?.queuedMessageWeight).toBeGreaterThan(0);
		emitLargeFrame("large-2");
		await vi.waitFor(() => expect(closed).toHaveLength(1));

		expect(fence.pendingChunkedReplacement).toBeUndefined();
		expect(closed).toEqual([
			{ type: "closed", error: "Daemon replacement lifecycle state could not be reconciled." },
		]);
		expect(JSON.stringify(closed)).not.toContain("chunk-fence-private-canary");
	});

	it("lets session close retire a header-only chunked replacement fence", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const closed: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") closed.push(event);
		});
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-a-replacement"),
			messages: [],
			snapshotFollows: true,
		});

		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-a", reason: "killed" });

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await vi.waitFor(() => expect(closed).toHaveLength(1));
		expect(
			(connection as unknown as { pendingChunkedReplacement?: unknown }).pendingChunkedReplacement,
		).toBeUndefined();
	});

	it("discards an old replacement query after the same connection reattaches to a new route", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseLifecycleQuery!: () => void;
		fakeClient.promptLifecycleGate = new Promise<void>((resolve) => {
			releaseLifecycleQuery = resolve;
		});
		fakeClient.switchSessionActiveSessionId = "active-b";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await connection.attach();
		const replacementSessionIds: string[] = [];
		connection.subscribe((event) => {
			if (event.type === "session_replaced") replacementSessionIds.push(event.state.sessionId);
		});

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-a-stale"),
			messages: [{ role: "user", content: "stale replacement", timestamp: 1 }],
		});
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "get_prompt_lifecycles")).toBe(true),
		);
		await expect(connection.switchSession("/tmp/session-b.jsonl")).resolves.toEqual({ cancelled: false });
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		const tailDrained = new Promise<void>((resolve) => {
			connection.subscribe((event) => {
				if (event.type === "heartbeats_changed") resolve();
			});
		});
		releaseLifecycleQuery();
		fakeClient.emitMessage({ type: "heartbeats_changed" });
		await tailDrained;

		const route = connection as unknown as {
			activeSessionId: string;
			latestSnapshot?: AgentConnectionSnapshot;
		};
		expect(route.activeSessionId).toBe("active-b");
		expect(route.latestSnapshot).toMatchObject({
			state: { activeSessionId: "active-b", sessionId: "session-current" },
		});
		expect(replacementSessionIds).toEqual(["session-current"]);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		await connection.submitCorrelatedPrompt("route to B", { correlationId: "route-b" });
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "submit_correlated_prompt",
			activeSessionId: "active-b",
			correlationId: "route-b",
		});
	});

	it("drops stale replacement shaping when shared-client capabilities change during its query", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.promptLifecycleResponse = {
			records: [
				{
					correlationId: "stale-replacement-shaping",
					phase: "delivered",
					kind: "model_prompt",
					revision: 3,
					deliveryCrossed: true,
				},
			],
			expired: [],
		};
		let releaseLifecycleQuery!: () => void;
		fakeClient.promptLifecycleGate = new Promise<void>((resolve) => {
			releaseLifecycleQuery = resolve;
		});
		const first = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await first.attach();
		const replaced = new Promise<void>((resolve) => {
			first.subscribe((event) => {
				if (event.type === "session_replaced") resolve();
			});
		});
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-replaced"),
			messages: [],
		});
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "get_prompt_lifecycles")).toBe(true),
		);

		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.client.capabilities = result.client.capabilities.filter(
				(capability) => capability !== "correlated_prompt_lifecycle_v1",
			);
			delete result.snapshot.promptLifecycles;
			return result;
		};
		const second = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-b");
		await second.attach();
		releaseLifecycleQuery();
		await replaced;

		const snapshot = (first as unknown as { latestSnapshot?: AgentConnectionSnapshot }).latestSnapshot;
		expect(snapshot).toMatchObject({ state: { sessionId: "session-replaced" } });
		expect(snapshot).not.toHaveProperty("promptLifecycles");
		await expect(
			first.submitCorrelatedPrompt("must fail", { correlationId: "stale-replacement-shaping" }),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("invalidates retained replacement shaping when a shared client mutates capabilities", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const first = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		await first.attach();
		const replaced = new Promise<void>((resolve) => {
			first.subscribe((event) => {
				if (event.type === "session_replaced") resolve();
			});
		});
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-a",
			state: createConnectionState("active-a", "session-replaced"),
			messages: [],
		});
		await replaced;
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		await expect(
			first.submitCorrelatedPrompt("still shaped", { correlationId: "before-shared-mutation" }),
		).resolves.toMatchObject({ lifecycle: { correlationId: "before-shared-mutation" } });

		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.client.capabilities = result.client.capabilities.filter(
				(capability) => capability !== "correlated_prompt_lifecycle_v1",
			);
			delete result.snapshot.promptLifecycles;
			return result;
		};
		const second = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-b");
		await second.attach();

		await expect(
			first.submitCorrelatedPrompt("must fail", { correlationId: "after-shared-mutation" }),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("keeps only the latest shared-client attachment proved across different sessions", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const first = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const second = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-b");
		await first.attach();
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		await second.attach();
		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		fakeClient.resetTransportForReconnect();

		expect(first.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(second.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
	});

	it("clears negotiated proof before a failed replacement attach and restores it only after a proved reattach", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-source");
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		fakeClient.attachFailures = 2;
		await expect(connection.attach()).rejects.toThrow("attach failed");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);

		fakeClient.switchSessionActiveSessionId = "active-target";
		fakeClient.reattachResultFactory = (command) =>
			createAttachResult(command.targetActiveSessionId, command.clientId, command.capabilities, 1, {
				state: createConnectionState(command.targetActiveSessionId, "session-target"),
			});
		await connection.switchSession("/tmp/target.jsonl");
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("clears negotiated proof when the attached session generation is replaced", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "session-next"),
			messages: [],
		});
		await vi.waitFor(() =>
			expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false),
		);
	});

	it("retains negotiated proof across a valid same-generation resync", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const resynced = new Promise<void>((resolve) => {
			connection.subscribe((event) => {
				if (event.type === "session_resynced") resolve();
			});
		});
		await connection.attach();
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		const snapshot = createAttachResult("active-1", "resync-client", ["correlated_prompt_lifecycle_v1"], 13).snapshot;

		fakeClient.emitMessage({ type: "session_resynced", activeSessionId: "active-1", snapshot });
		await resynced;

		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
	});

	it("does not expose lifecycle frames when correlated lifecycle was not negotiated", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "prompt-1",
				phase: "owned",
				kind: "model_prompt",
				revision: 1,
				deliveryCrossed: false,
			},
		});
		await Promise.resolve();

		expect(events).toEqual([]);
		await expect(connection.getInitialSnapshot()).resolves.not.toHaveProperty("promptLifecycles");
	});

	it("strips lifecycle state from every unsupported snapshot entry path without a protocol marker", async () => {
		const withLifecycle = (result: DaemonAttachResult, correlationId: string): DaemonAttachResult => {
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId,
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [],
			};
			return result;
		};

		const directAttachClient = new FakeDaemonClient();
		directAttachClient.attachResultFactory = (command) =>
			withLifecycle(
				createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12),
				"direct-attach",
			);
		const directAttach = new DaemonAgentConnection(asDaemonClient(directAttachClient), "active-direct");
		await directAttach.attach();
		await expect(directAttach.getInitialSnapshot()).resolves.not.toHaveProperty("promptLifecycles");

		const streamedAttachClient = new FakeDaemonClient();
		streamedAttachClient.attachResultFactory = (command) => {
			const result = withLifecycle(
				createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12),
				"streamed-attach",
			);
			const { messages: _messages, ...snapshot } = result.snapshot;
			queueMicrotask(() => {
				streamedAttachClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: command.activeSessionId,
					snapshotId: "unsupported-attach",
					snapshot,
					messageCount: 0,
					targetChunkBytes: 512 * 1024,
				});
				streamedAttachClient.emitMessage({
					type: "session_snapshot_end",
					activeSessionId: command.activeSessionId,
					snapshotId: "unsupported-attach",
					chunkCount: 0,
					lastEventSequence: 12,
					lastEventCursor: { generation: `generation-${command.activeSessionId}`, sequence: 12 },
				});
			});
			return {
				...result,
				snapshot: { ...result.snapshot, messages: [] },
				snapshotStream: { id: "unsupported-attach", messageCount: 0, targetChunkBytes: 512 * 1024 },
			};
		};
		const streamedAttach = new DaemonAgentConnection(asDaemonClient(streamedAttachClient), "active-streamed");
		await streamedAttach.attach();
		await expect(streamedAttach.getInitialSnapshot()).resolves.not.toHaveProperty("promptLifecycles");

		const runtimeClient = new FakeDaemonClient();
		const runtime = new DaemonAgentConnection(asDaemonClient(runtimeClient), "active-runtime");
		const runtimeEvents: AgentConnectionEvent[] = [];
		runtime.subscribe((event) => {
			runtimeEvents.push(event);
		});
		await runtime.attach();
		const directResync = withLifecycle(
			createAttachResult("active-runtime", "client-1", undefined, 13),
			"direct-resync",
		);
		runtimeClient.emitMessage({
			type: "session_resynced",
			activeSessionId: "active-runtime",
			snapshot: directResync.snapshot,
			meta: {
				id: "active-runtime:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-runtime",
				sequence: 13,
				cursor: { generation: "generation-active-runtime", sequence: 13 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		await vi.waitFor(() => expect(runtimeEvents).toHaveLength(1));
		expect(runtimeEvents[0]).toMatchObject({ type: "session_resynced" });
		if (runtimeEvents[0]?.type !== "session_resynced") throw new Error("Missing direct resync");
		expect(runtimeEvents[0].snapshot).not.toHaveProperty("promptLifecycles");

		const chunkedResync = withLifecycle(
			createAttachResult("active-runtime", "client-1", undefined, 14),
			"chunked-resync",
		);
		const { messages: _resyncMessages, ...resyncHeader } = chunkedResync.snapshot;
		runtimeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-runtime",
			snapshotId: "unsupported-resync",
			snapshot: resyncHeader,
			messageCount: 0,
			targetChunkBytes: 512 * 1024,
			purpose: "resync",
		});
		runtimeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-runtime",
			snapshotId: "unsupported-resync",
			chunkCount: 0,
			lastEventSequence: 14,
			lastEventCursor: { generation: "generation-active-runtime", sequence: 14 },
		});
		await vi.waitFor(() => expect(runtimeEvents).toHaveLength(2));
		if (runtimeEvents[1]?.type !== "session_resynced") throw new Error("Missing chunked resync");
		expect(runtimeEvents[1].snapshot).not.toHaveProperty("promptLifecycles");

		const replacementState = createConnectionState("active-runtime", "session-next");
		const replacement = withLifecycle(
			createAttachResult("active-runtime", "client-1", undefined, 1, { state: replacementState }),
			"chunked-replacement",
		);
		replacement.snapshot.lastEventCursor = { generation: "generation-next", sequence: 1 };
		const { messages: _replacementMessages, ...replacementHeader } = replacement.snapshot;
		runtimeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-runtime",
			state: replacementState,
			messages: [],
			snapshotFollows: true,
			meta: {
				id: "active-runtime:next:1",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-runtime",
				sequence: 1,
				cursor: { generation: "generation-next", sequence: 1 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		runtimeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-runtime",
			snapshotId: "unsupported-replacement",
			snapshot: replacementHeader,
			messageCount: 0,
			targetChunkBytes: 512 * 1024,
			purpose: "replacement",
		});
		runtimeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-runtime",
			snapshotId: "unsupported-replacement",
			chunkCount: 0,
			lastEventSequence: 1,
			lastEventCursor: { generation: "generation-next", sequence: 1 },
		});
		await vi.waitFor(() => expect(runtimeEvents).toHaveLength(3));
		await expect(runtime.getInitialSnapshot()).resolves.not.toHaveProperty("promptLifecycles");

		const reattachClient = new FakeDaemonClient();
		reattachClient.switchSessionActiveSessionId = "active-target";
		reattachClient.reattachResultFactory = (command) =>
			withLifecycle(
				createAttachResult(command.targetActiveSessionId, command.clientId, command.capabilities, 1, {
					state: createConnectionState(command.targetActiveSessionId, "session-target"),
				}),
				"direct-reattach",
			);
		const reattach = new DaemonAgentConnection(asDaemonClient(reattachClient), "active-source");
		const reattachEvents: AgentConnectionEvent[] = [];
		reattach.subscribe((event) => {
			reattachEvents.push(event);
		});
		await reattach.attach();
		await reattach.switchSession("/tmp/target.jsonl");
		await expect(reattach.getInitialSnapshot()).resolves.not.toHaveProperty("promptLifecycles");

		expect([...runtimeEvents, ...reattachEvents]).not.toContainEqual({
			type: "correlated_prompt_protocol_violation",
		});
	});

	it("uses dedicated capability-gated commands for correlated submit and cancel", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(
			connection.submitCorrelatedPrompt("queued input", {
				correlationId: "prompt-1",
				queueIfBusy: true,
			}),
		).resolves.toMatchObject({
			lifecycle: { correlationId: "prompt-1", phase: "queued" },
			duplicate: false,
		});
		Object.assign(connection as unknown as { activeSessionId: string; attachedSessionId: string }, {
			activeSessionId: "replacement-active",
			attachedSessionId: "replacement-session",
		});
		await expect(connection.cancelPromptLifecycle("prompt-1")).resolves.toMatchObject({
			status: "cancelled",
			deliveryCrossed: false,
		});
		expect(fakeClient.requests.at(-2)).toMatchObject({
			type: "submit_correlated_prompt",
			activeSessionId: "active-1",
			sessionId: "session-current",
			correlationId: "prompt-1",
			message: "queued input",
			queueIfBusy: true,
		});
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "cancel_correlated_prompt",
			activeSessionId: "active-1",
			sessionId: "session-current",
			correlationId: "prompt-1",
		});
	});

	it("rejects too-late cancellation without terminal or delivery proof", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId: "prompt-queued",
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [],
			};
			return result;
		};
		fakeClient.cancelCorrelatedPromptResponse = {
			status: "too_late",
			ownershipCrossed: true,
			deliveryCrossed: false,
			lifecycle: {
				correlationId: "prompt-queued",
				phase: "queued",
				kind: "model_prompt",
				revision: 2,
				deliveryCrossed: false,
			},
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.cancelPromptLifecycle("prompt-queued")).rejects.toThrow(
			"Daemon returned an invalid correlated prompt cancellation result",
		);
	});

	it("reconciles active and expired lifecycle state from an attach snapshot", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId: "queued-1",
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [{ correlationId: "expired-1", deliveryCrossed: false }],
			};
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			promptLifecycles: {
				records: [{ correlationId: "queued-1", phase: "queued" }],
				expired: [{ correlationId: "expired-1", deliveryCrossed: false }],
			},
		});
	});

	it("rejects a capable attach snapshot without lifecycle state", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			delete result.snapshot.promptLifecycles;
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.attach()).rejects.toThrow("Daemon snapshot is missing valid prompt lifecycle state");
	});

	it.each(["direct", "streamed"] as const)(
		"keeps routing and resume state coherent when a %s attach snapshot is rejected",
		async (transport) => {
			const fakeClient = new FakeDaemonClient();
			fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			let attempt = 0;
			fakeClient.attachResultFactory = (command) => {
				attempt += 1;
				const responseActiveSessionId =
					attempt === 2 && transport === "direct" ? "poison-route" : command.activeSessionId;
				const sequence = attempt === 2 ? 99 : attempt === 3 ? 13 : 12;
				const result = createAttachResult(
					responseActiveSessionId,
					command.clientId,
					command.capabilities,
					sequence,
				);
				result.snapshot.promptLifecycles = {
					records: [
						attempt === 1
							? {
									correlationId: "prompt-attach",
									phase: "queued",
									kind: "model_prompt",
									revision: 2,
									deliveryCrossed: false,
								}
							: attempt === 2
								? {
										correlationId: "prompt-attach",
										phase: "owned",
										kind: "model_prompt",
										revision: 2,
										deliveryCrossed: false,
									}
								: {
										correlationId: "prompt-attach",
										phase: "delivered",
										kind: "model_prompt",
										revision: 3,
										deliveryCrossed: true,
									},
					],
					expired: [],
				};
				if (attempt !== 2 || transport === "direct") return result;
				const { messages: _messages, ...snapshot } = result.snapshot;
				queueMicrotask(() => {
					fakeClient.emitMessage({
						type: "session_snapshot_begin",
						activeSessionId: command.activeSessionId,
						snapshotId: "invalid-attach-stream",
						snapshot,
						messageCount: 0,
						targetChunkBytes: 512 * 1024,
					});
					fakeClient.emitMessage({
						type: "session_snapshot_end",
						activeSessionId: command.activeSessionId,
						snapshotId: "invalid-attach-stream",
						chunkCount: 0,
						lastEventSequence: 99,
						lastEventCursor: { generation: `generation-${command.activeSessionId}`, sequence: 99 },
					});
				});
				return {
					...result,
					snapshot: { ...result.snapshot, messages: [] },
					snapshotStream: { id: "invalid-attach-stream", messageCount: 0, targetChunkBytes: 512 * 1024 },
				};
			};
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
			await connection.attach();

			await expect(connection.attach()).rejects.toThrow("could not be reconciled");
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				state: { activeSessionId: "active-1" },
				lastEventCursor: { generation: "generation-active-1", sequence: 12 },
				promptLifecycles: { records: [{ phase: "queued", revision: 2 }] },
			});
			await connection.attach();

			expect(fakeClient.requests.filter((request) => request.type === "attach").at(-1)).toMatchObject({
				activeSessionId: "active-1",
				resumeCursor: { activeSessionId: "active-1", generation: "generation-active-1", sequence: 12 },
			});
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				state: { activeSessionId: "active-1" },
				lastEventCursor: { generation: "generation-active-1", sequence: 13 },
				promptLifecycles: { records: [{ phase: "delivered", revision: 3 }] },
			});
		},
	);

	it.each(["direct", "streamed"] as const)(
		"quarantines a server-side %s reattach whose snapshot cannot be reconciled",
		async (transport) => {
			const fakeClient = new FakeDaemonClient();
			fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			fakeClient.switchSessionActiveSessionId = "active-target";
			fakeClient.reattachResultFactory = (command) => {
				const result = createAttachResult(
					command.targetActiveSessionId,
					command.clientId,
					command.capabilities,
					1,
					{ state: createConnectionState(command.targetActiveSessionId, "session-target") },
				);
				result.snapshot.promptLifecycles = {
					records: [
						{
							correlationId: "invalid-reattach",
							phase: "owned",
							kind: "model_prompt",
							revision: 0,
							deliveryCrossed: false,
						},
					],
					expired: [],
				};
				if (transport === "direct") return result;
				const { messages: _messages, ...snapshot } = result.snapshot;
				queueMicrotask(() => {
					fakeClient.emitMessage({
						type: "session_snapshot_begin",
						activeSessionId: command.targetActiveSessionId,
						snapshotId: "invalid-reattach-stream",
						snapshot,
						messageCount: 0,
						targetChunkBytes: 512 * 1024,
						purpose: "replacement",
					});
					fakeClient.emitMessage({
						type: "session_snapshot_end",
						activeSessionId: command.targetActiveSessionId,
						snapshotId: "invalid-reattach-stream",
						chunkCount: 0,
						lastEventSequence: 1,
						lastEventCursor: { generation: "generation-active-target", sequence: 1 },
					});
				});
				return {
					...result,
					snapshot: { ...result.snapshot, messages: [] },
					snapshotStream: { id: "invalid-reattach-stream", messageCount: 0, targetChunkBytes: 512 * 1024 },
				};
			};
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-source");
			const events: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				events.push(event);
			});
			await connection.attach();

			await expect(connection.switchSession("/tmp/target.jsonl")).rejects.toThrow(
				"missing valid prompt lifecycle state",
			);
			await vi.waitFor(() => expect(events.at(-1)).toMatchObject({ type: "closed" }));

			expect((connection as unknown as { activeSessionId: string }).activeSessionId).toBe("active-source");
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				state: { activeSessionId: "active-source", sessionId: "session-current" },
				lastEventCursor: { generation: "generation-active-source", sequence: 12 },
			});
		},
	);

	it("reconciles lifecycle state before publishing a JSON replacement", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		fakeClient.promptLifecycleResponse = {
			records: [
				{
					correlationId: "replacement-1",
					phase: "failed",
					kind: "model_prompt",
					revision: 4,
					deliveryCrossed: true,
				},
			],
			expired: [],
		};
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const replacementState = createConnectionState("active-1", "replacement-session");

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: replacementState,
			messages: [],
		});

		await vi.waitFor(() => expect(events.some((event) => event.type === "session_replaced")).toBe(true));
		expect(fakeClient.requests.filter((request) => request.type === "get_prompt_lifecycles").at(-1)).toMatchObject({
			activeSessionId: "active-1",
			sessionId: "replacement-session",
		});
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "replacement-session" },
			promptLifecycles: {
				records: [{ correlationId: "replacement-1", phase: "failed", deliveryCrossed: true }],
			},
		});
	});

	it("serializes replacement reconciliation before new-generation lifecycle events", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		fakeClient.promptLifecycleResponse = {
			records: [
				{
					correlationId: "replacement-race",
					phase: "queued",
					kind: "model_prompt",
					revision: 2,
					deliveryCrossed: false,
				},
			],
			expired: [],
		};
		let releaseLifecycleQuery = () => {};
		fakeClient.promptLifecycleGate = new Promise<void>((resolve) => {
			releaseLifecycleQuery = resolve;
		});
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "replacement-race-session"),
			messages: [],
		});
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "get_prompt_lifecycles")).toBe(true),
		);
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "replacement-race",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
		});
		releaseLifecycleQuery();

		await vi.waitFor(() =>
			expect(events.map((event) => event.type)).toEqual(["session_replaced", "prompt_lifecycle"]),
		);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "replacement-race-session" },
			promptLifecycles: {
				records: [{ correlationId: "replacement-race", phase: "delivered", revision: 3 }],
			},
		});
	});

	it("quarantines new-generation frames when replacement lifecycle reconciliation fails", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		let releaseLifecycleQuery = () => {};
		fakeClient.promptLifecycleGate = new Promise<void>((resolve) => {
			releaseLifecycleQuery = resolve;
		});
		fakeClient.promptLifecycleResponse = {
			records: [{ correlationId: "replacement-invalid", phase: "queued" }],
			expired: [],
		} as never;
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "replacement-invalid-session"),
			messages: [],
		});
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "get_prompt_lifecycles")).toBe(true),
		);
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "replacement-invalid",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
		});
		releaseLifecycleQuery();

		await vi.waitFor(() => expect(events).toEqual([{ type: "closed", error: expect.any(String) }]));
		expect(events[0]).toEqual({
			type: "closed",
			error: "Daemon replacement lifecycle state could not be reconciled.",
		});
		expect(
			(connection as unknown as { latestSnapshot?: AgentConnectionSnapshot }).latestSnapshot?.state.sessionId,
		).toBe("session-current");
	});

	it("does not regress a lifecycle observed during a slow snapshot reload", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId: "prompt-1",
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [],
			};
			return result;
		};
		fakeClient.promptLifecycleResponse = {
			records: [
				{
					correlationId: "prompt-1",
					phase: "queued",
					kind: "model_prompt",
					revision: 2,
					deliveryCrossed: false,
				},
			],
			expired: [],
		};
		let releaseLifecycleQuery = () => {};
		fakeClient.promptLifecycleGate = new Promise<void>((resolve) => {
			releaseLifecycleQuery = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		const reload = connection.getInitialSnapshot();
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "get_prompt_lifecycles")).toBe(true),
		);
		expect(fakeClient.requests.find((request) => request.type === "get_prompt_lifecycles")).toMatchObject({
			activeSessionId: "active-1",
			sessionId: "session-current",
		});
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "prompt-1",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
		});
		releaseLifecycleQuery();

		await expect(reload).resolves.toMatchObject({
			promptLifecycles: { records: [{ correlationId: "prompt-1", phase: "delivered", revision: 3 }] },
		});
	});

	it("returns a newer lifecycle event when the submit response arrives stale", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		let releaseResponse = () => {};
		fakeClient.correlatedPromptGate = new Promise<void>((resolve) => {
			releaseResponse = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const submission = connection.submitCorrelatedPrompt("race prompt", { correlationId: "race-prompt" });
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "submit_correlated_prompt")).toBe(true),
		);
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "race-prompt",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
		});
		releaseResponse();

		await expect(submission).resolves.toMatchObject({
			lifecycle: { correlationId: "race-prompt", phase: "delivered", revision: 3 },
		});
		const snapshot = (connection as unknown as { latestSnapshot: AgentConnectionSnapshot }).latestSnapshot;
		expect(snapshot.promptLifecycles?.records).toEqual([
			expect.objectContaining({ correlationId: "race-prompt", phase: "delivered", revision: 3 }),
		]);
		const route = (
			connection as unknown as {
				correlatedPromptRoutes: Map<string, { pending: boolean; requestFingerprint: string }>;
			}
		).correlatedPromptRoutes.get("race-prompt");
		expect(route).toMatchObject({ pending: false, requestFingerprint: expect.stringMatching(/^[0-9a-f]{64}$/) });
	});

	it.each(["malformed", "conflicting"] as const)(
		"keeps route proof coherent after a %s submit response",
		async (responseKind) => {
			const fakeClient = new FakeDaemonClient();
			fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			let releaseResponse = () => {};
			if (responseKind === "conflicting") {
				fakeClient.correlatedPromptGate = new Promise<void>((resolve) => {
					releaseResponse = resolve;
				});
			}
			fakeClient.correlatedPromptResponseFactory = (command) => ({
				duplicate: false,
				lifecycle:
					responseKind === "malformed"
						? { correlationId: command.correlationId, phase: "queued" }
						: {
								correlationId: command.correlationId,
								phase: "queued",
								kind: "session_command",
								revision: 2,
								deliveryCrossed: false,
							},
			});
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
			await connection.attach();
			const submission = connection.submitCorrelatedPrompt("original request", {
				correlationId: `${responseKind}-response`,
			});
			if (responseKind === "conflicting") {
				await vi.waitFor(() =>
					expect(fakeClient.requests.some((request) => request.type === "submit_correlated_prompt")).toBe(true),
				);
				fakeClient.emitMessage({
					type: "prompt_lifecycle",
					activeSessionId: "active-1",
					lifecycle: {
						correlationId: "conflicting-response",
						phase: "delivered",
						kind: "model_prompt",
						revision: 3,
						deliveryCrossed: true,
					},
				});
				releaseResponse();
			}
			await expect(submission).rejects.toThrow(
				responseKind === "malformed"
					? "invalid correlated prompt result"
					: "lifecycle response could not be reconciled",
			);
			const route = (
				connection as unknown as {
					correlatedPromptRoutes: Map<string, { pending: boolean; requestFingerprint: string }>;
				}
			).correlatedPromptRoutes.get(`${responseKind}-response`);
			expect(route).toMatchObject({
				pending: responseKind === "malformed",
				requestFingerprint: expect.stringMatching(/^[0-9a-f]{64}$/),
			});
			const requestCount = fakeClient.requests.length;
			await expect(
				connection.submitCorrelatedPrompt("different request", {
					correlationId: `${responseKind}-response`,
				}),
			).rejects.toThrow("reserved for another session generation or request");
			expect(fakeClient.requests).toHaveLength(requestCount);
		},
	);

	it("bounds terminal lifecycle cache, tombstones, and routes while preserving active prompts", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const usage = {
			input: 1,
			output: 2,
			cacheRead: 3,
			cacheWrite: 4,
			totalTokens: 10,
			cost: { input: 0.1, output: 0.2, cacheRead: 0.3, cacheWrite: 0.4, total: 1 },
		};
		const terminalLifecycle = (index: number): PromptLifecycleSnapshot => ({
			correlationId: `terminal-${index}`,
			phase: "failed",
			kind: "model_prompt",
			revision: index * 2 + 3,
			deliveryCrossed: false,
			...(index === 520 ? { usage } : {}),
		});
		for (let index = 0; index < 520; index++) {
			await connection.submitCorrelatedPrompt(`terminal prompt ${index}`, {
				correlationId: `terminal-${index}`,
			});
			fakeClient.emitMessage({
				type: "prompt_lifecycle",
				activeSessionId: "active-1",
				lifecycle: terminalLifecycle(index),
			});
		}
		await connection.submitCorrelatedPrompt("active prompt", { correlationId: "active-prompt" });
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: terminalLifecycle(264),
		});
		await connection.submitCorrelatedPrompt("terminal prompt 520", { correlationId: "terminal-520" });
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: terminalLifecycle(520),
		});

		const bounded = (connection as unknown as { latestSnapshot: AgentConnectionSnapshot }).latestSnapshot;
		expect(bounded.promptLifecycles?.records).toHaveLength(257);
		expect(bounded.promptLifecycles?.expired).toHaveLength(256);
		expect(bounded.promptLifecycles?.records).toContainEqual(
			expect.objectContaining({ correlationId: "active-prompt", phase: "queued" }),
		);
		expect(bounded.promptLifecycles?.records).toContainEqual(
			expect.objectContaining({ correlationId: "terminal-520", usage }),
		);
		expect(bounded.promptLifecycles?.records).not.toContainEqual(
			expect.objectContaining({ correlationId: "terminal-264" }),
		);
		expect(bounded.promptLifecycles?.expired).toContainEqual({
			correlationId: "terminal-264",
			deliveryCrossed: false,
		});
		const routes = (connection as unknown as { correlatedPromptRoutes: Map<string, unknown> }).correlatedPromptRoutes;
		expect(routes.size).toBe(513);
		expect(routes.has("active-prompt")).toBe(true);
		expect(routes.has("terminal-8")).toBe(false);

		const authoritative = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 13).snapshot;
		authoritative.promptLifecycles = {
			records: [
				{
					correlationId: "active-prompt",
					phase: "queued",
					kind: "model_prompt",
					revision: 2,
					deliveryCrossed: false,
				},
			],
			expired: [],
		};
		fakeClient.emitMessage({
			type: "session_resynced",
			activeSessionId: "active-1",
			snapshot: authoritative,
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				cursor: { generation: "generation-active-1", sequence: 13 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		await vi.waitFor(() => expect(routes.size).toBe(1));

		const replaced = await connection.getInitialSnapshot();
		expect(replaced).toMatchObject({
			promptLifecycles: {
				records: [{ correlationId: "active-prompt", phase: "queued" }],
				expired: [],
			},
		});
		expect(JSON.stringify(replaced.promptLifecycles)).not.toContain("terminal-520");
		expect(routes.has("active-prompt")).toBe(true);
		expect(routes.has("terminal-520")).toBe(false);
		const cancellationRequests = fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt");
		await expect(connection.cancelPromptLifecycle("terminal-520")).resolves.toEqual({
			status: "unknown",
			ownershipCrossed: "unknown",
			deliveryCrossed: "unknown",
		});
		expect(fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt")).toEqual(
			cancellationRequests,
		);
	});

	it("retires a lost-response route after validated terminal aging while preserving unresolved ownership", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		let releaseLostResponse = () => {};
		fakeClient.correlatedPromptGate = new Promise<void>((resolve) => {
			releaseLostResponse = resolve;
		});
		fakeClient.correlatedPromptError = new Error("submit response lost");
		const lostSubmission = connection.submitCorrelatedPrompt("lost response", {
			correlationId: "lost-response",
		});
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "submit_correlated_prompt")).toBe(true),
		);
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "lost-response",
				phase: "failed",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: false,
			},
		});
		releaseLostResponse();
		await expect(lostSubmission).rejects.toThrow("submit response lost");
		fakeClient.correlatedPromptGate = undefined;
		const routes = (
			connection as unknown as {
				correlatedPromptRoutes: Map<string, { pending: boolean; requestFingerprint: string }>;
			}
		).correlatedPromptRoutes;
		expect(routes.get("lost-response")?.pending).toBe(false);

		fakeClient.correlatedPromptError = new Error("ownership unresolved");
		await expect(
			connection.submitCorrelatedPrompt("unresolved request", { correlationId: "unresolved-pending" }),
		).rejects.toThrow("ownership unresolved");
		fakeClient.correlatedPromptError = undefined;
		expect(routes.get("unresolved-pending")?.pending).toBe(true);
		for (let index = 0; index < 513; index++) {
			await connection.submitCorrelatedPrompt(`later terminal ${index}`, {
				correlationId: `later-terminal-${index}`,
			});
			fakeClient.emitMessage({
				type: "prompt_lifecycle",
				activeSessionId: "active-1",
				lifecycle: {
					correlationId: `later-terminal-${index}`,
					phase: "failed",
					kind: "model_prompt",
					revision: index + 10,
					deliveryCrossed: false,
				},
			});
		}

		expect(routes.size).toBe(513);
		expect(routes.has("lost-response")).toBe(false);
		expect(routes.get("unresolved-pending")?.pending).toBe(true);
		expect(new Set([...routes.values()].map((route) => route.requestFingerprint)).size).toBe(routes.size);
		const cancellationRequests = fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt");
		await expect(connection.cancelPromptLifecycle("lost-response")).resolves.toEqual({
			status: "unknown",
			ownershipCrossed: "unknown",
			deliveryCrossed: "unknown",
		});
		expect(fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt")).toEqual(
			cancellationRequests,
		);
	});

	it("drops proven routes from each retired generation without retargeting cancellation", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fakeClient.correlatedPromptError = new Error("pending response lost");
		await expect(
			connection.submitCorrelatedPrompt("unresolved across generations", {
				correlationId: "retired-unresolved",
			}),
		).rejects.toThrow("pending response lost");
		fakeClient.correlatedPromptError = undefined;
		let retiredCorrelationId = "";
		for (let generation = 0; generation < 6; generation++) {
			for (let index = 0; index < 40; index++) {
				retiredCorrelationId = `generation-${generation}-terminal-${index}`;
				await connection.submitCorrelatedPrompt(`generation ${generation} prompt ${index}`, {
					correlationId: retiredCorrelationId,
				});
				fakeClient.emitMessage({
					type: "prompt_lifecycle",
					activeSessionId: "active-1",
					lifecycle: {
						correlationId: retiredCorrelationId,
						phase: "failed",
						kind: "model_prompt",
						revision: index + 3,
						deliveryCrossed: false,
					},
				});
			}
			fakeClient.emitMessage({
				type: "session_replaced",
				activeSessionId: "active-1",
				state: createConnectionState("active-1", `session-generation-${generation + 1}`),
				messages: [],
			});
			await vi.waitFor(() =>
				expect(
					events.filter(
						(event) =>
							event.type === "session_replaced" &&
							event.state.sessionId === `session-generation-${generation + 1}`,
					),
				).toHaveLength(1),
			);
			const routes = (
				connection as unknown as {
					correlatedPromptRoutes: Map<string, { pending: boolean; requestFingerprint: string }>;
				}
			).correlatedPromptRoutes;
			expect(routes.size).toBe(1);
			expect(routes.get("retired-unresolved")?.pending).toBe(true);
		}
		const routes = (
			connection as unknown as {
				correlatedPromptRoutes: Map<string, { pending: boolean; requestFingerprint: string }>;
			}
		).correlatedPromptRoutes;
		expect(new Set([...routes.values()].map((route) => route.requestFingerprint)).size).toBe(1);
		const cancellationRequests = fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt");
		await expect(connection.cancelPromptLifecycle(retiredCorrelationId)).resolves.toEqual({
			status: "unknown",
			ownershipCrossed: "unknown",
			deliveryCrossed: "unknown",
		});
		expect(fakeClient.requests.filter((request) => request.type === "cancel_correlated_prompt")).toEqual(
			cancellationRequests,
		);
	});

	it("does not retarget a reserved correlation id across session generations", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		await connection.submitCorrelatedPrompt("generation one", { correlationId: "generation-reservation" });
		const firstRequestCount = fakeClient.requests.length;
		await expect(
			connection.submitCorrelatedPrompt("different request", { correlationId: "generation-reservation" }),
		).rejects.toThrow("reserved for another session generation or request");
		expect(fakeClient.requests).toHaveLength(firstRequestCount);
		await connection.submitCorrelatedPrompt("generation one", { correlationId: "generation-reservation" });
		Object.assign(connection as unknown as { activeSessionId: string; attachedSessionId: string }, {
			activeSessionId: "active-2",
			attachedSessionId: "session-2",
		});
		const requestCount = fakeClient.requests.length;

		await expect(
			connection.submitCorrelatedPrompt("generation two", { correlationId: "generation-reservation" }),
		).rejects.toThrow("reserved for another session generation or request");
		expect(fakeClient.requests).toHaveLength(requestCount);
		await connection.cancelPromptLifecycle("generation-reservation");
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "cancel_correlated_prompt",
			activeSessionId: "active-1",
			sessionId: "session-current",
			correlationId: "generation-reservation",
		});
	});

	it("reports stale and regressing lifecycle receipts without rejecting exact duplicates", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId: "prompt-1",
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [],
			};
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const phases: string[] = [];
		let protocolViolations = 0;
		connection.subscribe((event) => {
			if (event.type === "prompt_lifecycle") phases.push(event.lifecycle.phase);
			if (event.type === "correlated_prompt_protocol_violation") protocolViolations += 1;
		});
		await connection.attach();
		const emitLifecycle = (
			phase: "owned" | "queued" | "delivered" | "completed" | "cancelled" | "failed",
			revision: number,
			deliveryCrossed: boolean,
			kind: "model_prompt" | "session_command" = "model_prompt",
		) =>
			fakeClient.emitMessage({
				type: "prompt_lifecycle",
				activeSessionId: "active-1",
				lifecycle: { correlationId: "prompt-1", phase, kind, revision, deliveryCrossed },
			});

		emitLifecycle("failed", 3, true);
		emitLifecycle("delivered", 3, true);
		emitLifecycle("delivered", 3, true);
		emitLifecycle("owned", 1, false);
		emitLifecycle("completed", 4, false);
		emitLifecycle("failed", 5, true, "session_command");
		emitLifecycle("completed", 4, true);

		await vi.waitFor(() => {
			expect(phases).toEqual(["delivered", "completed"]);
			expect(protocolViolations).toBe(4);
		});
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			promptLifecycles: { records: [{ correlationId: "prompt-1", phase: "completed", revision: 4 }] },
		});
	});

	it("reports malformed negotiated events without advancing the event cursor", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		const meta = {
			id: "active-1:13",
			protocol: DAEMON_PROTOCOL_INFO,
			activeSessionId: "active-1",
			sequence: 13,
			cursor: { generation: "generation-active-1", sequence: 13 },
			emittedAt: "2026-01-01T00:00:00.000Z",
		};
		const event = {
			type: "session_action_update" as const,
			actions: { queuedCount: 0, steering: [], followUps: [] },
			promptCorrelationId: null,
		};
		fakeClient.emitMessage({ type: "session_event", activeSessionId: "active-1", event, meta });
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "prompt-1",
				phase: "owned",
				kind: "model_prompt",
				revision: 0,
				deliveryCrossed: false,
			},
		});
		fakeClient.emitMessage({
			type: "extension_ui_request",
			activeSessionId: "active-1",
			id: "request-1",
			method: "confirm",
			payload: { title: "Confirm", message: "Proceed?" },
		});
		const invalidResync = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 13).snapshot;
		invalidResync.promptLifecycles = {
			records: [
				{
					correlationId: "prompt-1",
					phase: "owned",
					kind: "model_prompt",
					revision: 0,
					deliveryCrossed: false,
				},
			],
			expired: [],
		};
		fakeClient.emitMessage({
			type: "session_resynced",
			activeSessionId: "active-1",
			snapshot: invalidResync,
			meta,
		});
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event,
			attribution: { scope: "session" },
			meta,
		});

		await vi.waitFor(() => expect(events).toHaveLength(5));
		expect(events.slice(0, 4)).toEqual([
			{ type: "correlated_prompt_protocol_violation" },
			{ type: "correlated_prompt_protocol_violation" },
			{ type: "correlated_prompt_protocol_violation" },
			{ type: "correlated_prompt_protocol_violation" },
		]);
		expect(events[4]).toMatchObject({ type: "session_event", attribution: { scope: "session" } });
	});

	it.each([
		["direct", "regression"],
		["direct", "conflict"],
		["direct", "generation"],
		["chunked", "regression"],
		["chunked", "conflict"],
		["chunked", "generation"],
	] as const)(
		"rejects a shape-valid %s resync snapshot with a lifecycle %s before commit",
		async (entry, violation) => {
			const fakeClient = new FakeDaemonClient();
			fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			fakeClient.attachResultFactory = (command) => {
				const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
				result.snapshot.promptLifecycles = {
					records: [
						{
							correlationId: "snapshot-prompt",
							phase: "queued",
							kind: "model_prompt",
							revision: 2,
							deliveryCrossed: false,
						},
					],
					expired: [],
				};
				return result;
			};
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
			const events: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				events.push(event);
			});
			await connection.attach();
			const sessionId = violation === "generation" ? "session-other" : "session-current";
			const candidate = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 13, {
				state: createConnectionState("active-1", sessionId),
			}).snapshot;
			candidate.promptLifecycles = {
				records: [
					violation === "regression"
						? {
								correlationId: "snapshot-prompt",
								phase: "owned",
								kind: "model_prompt",
								revision: 1,
								deliveryCrossed: false,
							}
						: violation === "conflict"
							? {
									correlationId: "snapshot-prompt",
									phase: "owned",
									kind: "model_prompt",
									revision: 2,
									deliveryCrossed: false,
								}
							: {
									correlationId: "snapshot-prompt",
									phase: "queued",
									kind: "model_prompt",
									revision: 2,
									deliveryCrossed: false,
								},
				],
				expired: [],
			};
			if (entry === "direct") {
				fakeClient.emitMessage({
					type: "session_resynced",
					activeSessionId: "active-1",
					snapshot: candidate,
					meta: {
						id: "active-1:13",
						protocol: DAEMON_PROTOCOL_INFO,
						activeSessionId: "active-1",
						sequence: 13,
						cursor: { generation: "generation-active-1", sequence: 13 },
						emittedAt: "2026-01-01T00:00:00.000Z",
					},
				});
			} else {
				const { messages: _messages, ...snapshot } = candidate;
				fakeClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: "active-1",
					snapshotId: `invalid-${violation}`,
					snapshot,
					messageCount: 0,
					targetChunkBytes: 512 * 1024,
					purpose: "resync",
				});
				fakeClient.emitMessage({
					type: "session_snapshot_end",
					activeSessionId: "active-1",
					snapshotId: `invalid-${violation}`,
					chunkCount: 0,
					lastEventSequence: 13,
					lastEventCursor: { generation: "generation-active-1", sequence: 13 },
				});
			}

			await vi.waitFor(() => expect(events).toEqual([{ type: "correlated_prompt_protocol_violation" }]));
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				state: { sessionId: "session-current" },
				lastEventCursor: { generation: "generation-active-1", sequence: 12 },
				promptLifecycles: { records: [{ phase: "queued", revision: 2 }] },
			});
		},
	);

	it.each(["tombstone resurrection", "tombstone delivery conflict", "nonterminal expiry"] as const)(
		"rejects %s during snapshot reconciliation",
		async (violation) => {
			const fakeClient = new FakeDaemonClient();
			fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
			fakeClient.attachResultFactory = (command) => {
				const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
				result.snapshot.promptLifecycles =
					violation === "tombstone resurrection"
						? { records: [], expired: [{ correlationId: "snapshot-prompt", deliveryCrossed: false }] }
						: violation === "tombstone delivery conflict"
							? { records: [], expired: [{ correlationId: "snapshot-prompt", deliveryCrossed: true }] }
							: {
									records: [
										{
											correlationId: "snapshot-prompt",
											phase: "queued",
											kind: "model_prompt",
											revision: 2,
											deliveryCrossed: false,
										},
									],
									expired: [],
								};
				return result;
			};
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
			const events: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				events.push(event);
			});
			await connection.attach();
			const candidate = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 13).snapshot;
			candidate.promptLifecycles =
				violation === "tombstone resurrection"
					? {
							records: [
								{
									correlationId: "snapshot-prompt",
									phase: "owned",
									kind: "model_prompt",
									revision: 1,
									deliveryCrossed: false,
								},
							],
							expired: [],
						}
					: violation === "tombstone delivery conflict"
						? { records: [], expired: [{ correlationId: "snapshot-prompt", deliveryCrossed: false }] }
						: { records: [], expired: [{ correlationId: "snapshot-prompt", deliveryCrossed: false }] };
			fakeClient.emitMessage({
				type: "session_resynced",
				activeSessionId: "active-1",
				snapshot: candidate,
				meta: {
					id: "active-1:13",
					protocol: DAEMON_PROTOCOL_INFO,
					activeSessionId: "active-1",
					sequence: 13,
					cursor: { generation: "generation-active-1", sequence: 13 },
					emittedAt: "2026-01-01T00:00:00.000Z",
				},
			});

			await vi.waitFor(() => expect(events).toEqual([{ type: "correlated_prompt_protocol_violation" }]));
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				lastEventCursor: { sequence: 12 },
			});
		},
	);

	it("accepts exact lifecycle snapshots and legal skipped forward transitions", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			result.snapshot.promptLifecycles = {
				records: [
					{
						correlationId: "snapshot-prompt",
						phase: "queued",
						kind: "model_prompt",
						revision: 2,
						deliveryCrossed: false,
					},
				],
				expired: [],
			};
			return result;
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		const emitSnapshot = (sequence: number, lifecycle: PromptLifecycleSnapshot) => {
			const snapshot = createAttachResult(
				"active-1",
				"client-1",
				["correlated_prompt_lifecycle_v1"],
				sequence,
			).snapshot;
			snapshot.promptLifecycles = { records: [lifecycle], expired: [] };
			fakeClient.emitMessage({
				type: "session_resynced",
				activeSessionId: "active-1",
				snapshot,
				meta: {
					id: `active-1:${sequence}`,
					protocol: DAEMON_PROTOCOL_INFO,
					activeSessionId: "active-1",
					sequence,
					cursor: { generation: "generation-active-1", sequence },
					emittedAt: "2026-01-01T00:00:00.000Z",
				},
			});
		};
		emitSnapshot(13, {
			correlationId: "snapshot-prompt",
			phase: "queued",
			kind: "model_prompt",
			revision: 2,
			deliveryCrossed: false,
		});
		emitSnapshot(14, {
			correlationId: "snapshot-prompt",
			phase: "completed",
			kind: "model_prompt",
			revision: 4,
			deliveryCrossed: true,
		});

		await vi.waitFor(() =>
			expect(events.map((event) => event.type)).toEqual(["session_resynced", "session_resynced"]),
		);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			promptLifecycles: { records: [{ phase: "completed", revision: 4 }] },
		});
	});

	it("maps explicit session provenance and dedicated lifecycle events", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: null,
			},
			attribution: { scope: "session" },
		});
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "prompt-1",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
		});

		await vi.waitFor(() => expect(events).toHaveLength(2));
		expect(events[0]).toMatchObject({ type: "session_event", attribution: { scope: "session" } });
		expect(events[1]).toMatchObject({
			type: "prompt_lifecycle",
			lifecycle: { correlationId: "prompt-1", phase: "delivered" },
		});
	});

	it("fails locally when correlated lifecycle was not negotiated", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(
			connection.submitCorrelatedPrompt("queued input", { correlationId: "prompt-1", queueIfBusy: true }),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);
		expect(fakeClient.requests).toEqual([]);
	});

	it("forwards signal-backed prompts with a unique cancellable admission id", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();

		await connection.prompt("startup", { signal: abort.signal, queueIfBusy: true });

		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "prompt",
			activeSessionId: "active-1",
			message: "startup",
			queueIfBusy: true,
			admissionId: expect.stringMatching(/^prompt-admission:/),
		});
	});

	it("cancels rejected signal-backed admission and removes its abort listener", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.promptError = new Error("transport failed");
		fakeClient.cancelPromptAdmissionStatus = "cancelled";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();
		const add = vi.spyOn(abort.signal, "addEventListener");
		const remove = vi.spyOn(abort.signal, "removeEventListener");

		await expect(connection.prompt("startup", { signal: abort.signal })).rejects.toMatchObject({
			message: "transport failed",
			cancelled: true,
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["prompt", "cancel_prompt_admission"]);
		expect(add).toHaveBeenCalledOnce();
		expect(remove).toHaveBeenCalledOnce();
		expect(remove.mock.calls[0]?.[1]).toBe(add.mock.calls[0]?.[1]);
	});

	it("preserves a definitive prompt rejection when the signal remains live", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.promptResponseError = "session rejected prompt";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.prompt("startup", { signal: new AbortController().signal })).rejects.toEqual(
			new Error("session rejected prompt"),
		);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["prompt"]);
	});

	it("sends zero requests for a pre-aborted prompt", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();
		abort.abort();

		await expect(connection.prompt("startup", { signal: abort.signal })).rejects.toMatchObject({
			status: "cancelled",
		});
		expect(fakeClient.requests).toEqual([]);
	});

	it("accepts a successful prompt response when cancellation reports unknown", async () => {
		const fakeClient = new FakeDaemonClient();
		let releasePrompt = () => {};
		fakeClient.promptGate = new Promise<void>((resolve) => {
			releasePrompt = resolve;
		});
		fakeClient.cancelPromptAdmissionStatus = "unknown";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();

		const prompt = connection.prompt("startup", { signal: abort.signal });
		abort.abort();
		await vi.waitFor(() =>
			expect(fakeClient.requests.map((request) => request.type)).toContain("cancel_prompt_admission"),
		);
		expect(fakeClient.requests.find((request) => request.type === "cancel_prompt_admission")).not.toHaveProperty(
			"cancelOwned",
		);
		releasePrompt();

		await expect(prompt).resolves.toBeUndefined();
	});

	it("requests owned prompt cancellation when the daemon advertises it", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("owned_prompt_cancellation");
		let releasePrompt = () => {};
		fakeClient.promptGate = new Promise<void>((resolve) => {
			releasePrompt = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();

		const prompt = connection.prompt("startup", { signal: abort.signal });
		abort.abort();
		await vi.waitFor(() =>
			expect(fakeClient.requests.map((request) => request.type)).toContain("cancel_prompt_admission"),
		);
		expect(fakeClient.requests.find((request) => request.type === "cancel_prompt_admission")).toMatchObject({
			cancelOwned: true,
		});
		releasePrompt();
		await expect(prompt).resolves.toBeUndefined();
	});

	it("preserves a definitive prompt rejection when cancellation reports owned", async () => {
		const fakeClient = new FakeDaemonClient();
		let releasePrompt = () => {};
		fakeClient.promptGate = new Promise<void>((resolve) => {
			releasePrompt = resolve;
		});
		fakeClient.promptResponseError = "post-ownership validation failed";
		fakeClient.cancelPromptAdmissionStatus = "owned";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const abort = new AbortController();

		const prompt = connection.prompt("startup", { signal: abort.signal });
		abort.abort();
		await vi.waitFor(() =>
			expect(fakeClient.requests.map((request) => request.type)).toContain("cancel_prompt_admission"),
		);
		releasePrompt();

		await expect(prompt).rejects.toThrow("post-ownership validation failed");
	});

	it("treats a lost prompt response plus unknown cancellation as uncertain", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.promptError = new Error("lost response");
		fakeClient.cancelPromptAdmissionStatus = "unknown";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.prompt("startup", { signal: new AbortController().signal })).rejects.toMatchObject({
			message: "lost response",
			status: "unknown",
			cancelled: false,
		});
	});

	it("translates unsupported admission cancellation into an admission error", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.promptError = new DaemonCapabilityUnavailableError("prompt", "prompt_admission_cancellation");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.prompt("startup", { signal: new AbortController().signal })).rejects.toMatchObject({
			status: "unsupported",
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["prompt"]);
	});

	it("uses cancellable admission for promptAndWait", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.promptError = new Error("lost response");
		fakeClient.cancelPromptAdmissionStatus = "cancelled";
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.promptAndWait("wait", { signal: new AbortController().signal })).rejects.toMatchObject({
			status: "cancelled",
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual([
			"prompt_and_wait",
			"cancel_prompt_admission",
		]);
	});

	it("uses fleet heartbeat scope for residents and session scope for owned workers", async () => {
		const residentClient = new FakeDaemonClient();
		residentClient.serverCapabilities.add("heartbeat_catalog");
		const resident = new DaemonAgentConnection(asDaemonClient(residentClient), "resident-1");
		await resident.listHeartbeats();

		const ownedClient = new FakeDaemonClient();
		ownedClient.serverCapabilities.add("heartbeat_catalog");
		const owned = new DaemonAgentConnection(asDaemonClient(ownedClient), "owned-1", { ownedSession: true });
		await owned.listHeartbeats();

		expect(residentClient.requests.at(-1)).toEqual(expect.objectContaining({ type: "heartbeats_list" }));
		expect(residentClient.requests.at(-1)).not.toHaveProperty("activeSessionId");
		expect(ownedClient.requests.at(-1)).toEqual(
			expect.objectContaining({ type: "heartbeats_list", activeSessionId: "owned-1" }),
		);
	});

	it("serializes concurrent owned-session promotion commands", async () => {
		const fakeClient = new FakeDaemonClient();
		let releaseFirst = () => {};
		fakeClient.cronAddGate = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			ownedSession: true,
		});

		const first = connection.addCronJob("0 * * * *", "first");
		const second = connection.addCronJob("30 * * * *", "second");
		await vi.waitFor(() => {
			expect(fakeClient.requests.filter((request) => request.type === "cron_add")).toHaveLength(1);
		});
		releaseFirst();
		await Promise.all([first, second]);

		const commands = fakeClient.requests.filter(
			(command): command is Extract<DaemonCommand, { type: "cron_add" }> => command.type === "cron_add",
		);
		expect(commands.map((command) => command.promoteOwnedSession)).toEqual([true, false]);
	});

	it("gates side-question follow-up transcripts on the daemon capability", async () => {
		const oldDaemonClient = new FakeDaemonClient();
		const oldConnection = new DaemonAgentConnection(asDaemonClient(oldDaemonClient), "active-original");

		// First questions carry no transcript and must keep working on old daemons.
		await oldConnection.startSideQuestion("turn-1", "What changed?");
		expect(oldDaemonClient.requests.map((request) => request.type)).toEqual(["start_side_question"]);

		// Follow-ups would silently lose their side context on an old daemon.
		await expect(
			oldConnection.startSideQuestion("turn-2", "And then?", [{ question: "What changed?", answer: "The parser." }]),
		).rejects.toThrow("older build without side-conversation follow-ups");
		expect(oldDaemonClient.requests).toHaveLength(1);

		const newDaemonClient = new FakeDaemonClient();
		newDaemonClient.serverCapabilities.add("side_question_transcript");
		const newConnection = new DaemonAgentConnection(asDaemonClient(newDaemonClient), "active-original");

		await newConnection.startSideQuestion("turn-2", "And then?", [
			{ question: "What changed?", answer: "The parser." },
		]);
		const sent = newDaemonClient.requests.find(
			(command): command is Extract<DaemonCommand, { type: "start_side_question" }> =>
				command.type === "start_side_question",
		);
		expect(sent?.previousTurns).toEqual([{ question: "What changed?", answer: "The parser." }]);
	});

	it("gates transient bash on the daemon capability", async () => {
		const oldDaemonClient = new FakeDaemonClient();
		const oldConnection = new DaemonAgentConnection(asDaemonClient(oldDaemonClient), "active-original");

		// Regular bash keeps working on old daemons.
		await oldConnection.executeBash("ls");
		expect(oldDaemonClient.requests.map((request) => request.type)).toEqual(["execute_bash"]);

		// A transient run on an old daemon would be recorded into the session.
		await expect(oldConnection.executeBash("ls", { transient: true })).rejects.toThrow(
			"older build without side-conversation bash",
		);
		expect(oldDaemonClient.requests).toHaveLength(1);

		const newDaemonClient = new FakeDaemonClient();
		newDaemonClient.serverCapabilities.add("transient_bash");
		const newConnection = new DaemonAgentConnection(asDaemonClient(newDaemonClient), "active-original");

		await newConnection.executeBash("ls", { excludeFromContext: true, transient: true, runId: "side-run-1" });
		const sent = newDaemonClient.requests.find(
			(command): command is Extract<DaemonCommand, { type: "execute_bash" }> => command.type === "execute_bash",
		);
		expect(sent).toMatchObject({ command: "ls", excludeFromContext: true, transient: true, runId: "side-run-1" });
	});

	it("degrades an unavailable heartbeat catalog without sending an unsupported command", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");

		await expect(connection.listHeartbeats()).resolves.toEqual([]);
		expect(fakeClient.requests).toEqual([]);
		await expect(connection.manageHeartbeat("active-original", "job-1", "pause")).rejects.toThrow(
			"requires a newer Prime Agent daemon",
		);
		expect(fakeClient.requests).toEqual([]);
	});

	it("reattaches an open window to its restored session after an update restart", async () => {
		const fakeClient = new FakeDaemonClient();
		const restoredMessages: AgentMessage[] = [{ role: "user", content: "restored prompt", timestamp: 2 }];
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-current",
				sessionFile: "/tmp/session-current.jsonl",
			},
		];
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 1, {
				state: createConnectionState(command.activeSessionId, "session-current"),
				messages: command.activeSessionId === "active-restored" ? restoredMessages : [],
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const events: AgentConnectionEvent[] = [];
		const restored = new Promise<AgentConnectionEvent>((resolve) => {
			connection.subscribe((event) => {
				events.push(event);
				if (event.type === "session_resynced") {
					resolve(event);
				}
			});
		});
		await connection.attach();
		const staleUpdate = createAttachResult("active-original", "client-1", [], 20, {
			state: createConnectionState("active-original", "session-current"),
			messages: [{ role: "user", content: "stale update tail", timestamp: 20 }],
		}).snapshot;
		const { messages: _staleMessages, ...staleUpdateHeader } = staleUpdate;
		const staleBegin = {
			type: "session_snapshot_begin" as const,
			activeSessionId: "active-original",
			snapshotId: "stale-update-tail",
			snapshot: staleUpdateHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync" as const,
		};
		const staleChunk = {
			type: "session_snapshot_chunk" as const,
			activeSessionId: "active-original",
			snapshotId: "stale-update-tail",
			index: 0,
			messages: staleUpdate.messages,
		};
		const staleEnd = {
			type: "session_snapshot_end" as const,
			activeSessionId: "active-original",
			snapshotId: "stale-update-tail",
			chunkCount: 1,
			lastEventSequence: 20,
			lastEventCursor: { generation: "generation-active-original", sequence: 20 },
		};
		fakeClient.emitMessage(staleBegin);
		fakeClient.emitMessage(staleChunk);
		fakeClient.emitMessage({
			type: "session_closed",
			activeSessionId: "active-original",
			reason: "update",
		});
		fakeClient.emitMessage(staleEnd);
		fakeClient.emitMessage(staleEnd);
		fakeClient.emitMessage(staleBegin);
		fakeClient.emitMessage(staleChunk);
		fakeClient.emitMessage(staleEnd);
		await vi.waitFor(() => {
			expect(fakeClient.closeCount).toBe(1);
			expect(fakeClient.reconnectCount).toBe(1);
		});

		await expect(restored).resolves.toMatchObject({
			type: "session_resynced",
			snapshot: {
				state: { activeSessionId: "active-restored", sessionId: "session-current" },
				messages: restoredMessages,
			},
		});
		expect(fakeClient.reconnectCount).toBe(1);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "list", "attach"]);
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-restored",
			resumeCursor: undefined,
		});
		await vi.waitFor(() => {
			expect(events).toEqual([
				expect.objectContaining({ type: "connection_status", status: "reconnecting" }),
				expect.objectContaining({
					type: "session_resynced",
					snapshot: expect.objectContaining({
						state: expect.objectContaining({ activeSessionId: "active-restored" }),
					}),
				}),
				{ type: "connection_status", status: "connected" },
			]);
		});
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({ messages: restoredMessages });
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it("restores authoritatively when socket-first update retires a full snapshot tombstone budget", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-current",
				sessionFile: "/tmp/session-current.jsonl",
			},
		];
		const restoredMessages: AgentMessage[] = [{ role: "user", content: "restored after full budget", timestamp: 2 }];
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 21, {
				state: createConnectionState(command.activeSessionId, "session-current"),
				messages: command.activeSessionId === "active-restored" ? restoredMessages : [],
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fillSnapshotRetirementBudget(fakeClient, "active-original");
		expect((connection as unknown as { ignoredSnapshotIds: Set<string> }).ignoredSnapshotIds.size).toBe(128);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(1);

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "update"));
		await vi.waitFor(() =>
			expect(events).toEqual([
				expect.objectContaining({ type: "connection_status", status: "reconnecting" }),
				expect.objectContaining({
					type: "session_resynced",
					snapshot: expect.objectContaining({ messages: restoredMessages }),
				}),
				{ type: "connection_status", status: "connected" },
			]),
		);

		expect(
			(connection as unknown as { replacementReconciliationFailed: boolean }).replacementReconciliationFailed,
		).toBe(false);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(true);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it("reattaches when an update socket close arrives before the session notice", async () => {
		const fakeClient = new FakeDaemonClient();
		const restoredMessages: AgentMessage[] = [{ role: "user", content: "restored prompt", timestamp: 2 }];
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-current",
				sessionFile: "/tmp/session-current.jsonl",
			},
		];
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 1, {
				state: createConnectionState(command.activeSessionId, "session-current"),
				messages: command.activeSessionId === "active-restored" ? restoredMessages : [],
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const restored = new Promise<AgentConnectionEvent>((resolve) => {
			connection.subscribe((event) => {
				if (event.type === "session_resynced") {
					resolve(event);
				}
			});
		});
		await connection.attach();

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "update"));

		await expect(restored).resolves.toMatchObject({
			type: "session_resynced",
			snapshot: {
				state: { activeSessionId: "active-restored", sessionId: "session-current" },
				messages: restoredMessages,
			},
		});
		expect(fakeClient.reconnectCount).toBe(1);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "list", "attach"]);
	});

	it("coordinates one transport reconnect across connections sharing a daemon client", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.updateRestartSessions = [
			{
				id: "restored-a",
				activeSessionId: "restored-a",
				sessionId: "session-a",
				sessionFile: "/tmp/session-a.jsonl",
			},
			{
				id: "restored-b",
				activeSessionId: "restored-b",
				sessionId: "session-b",
				sessionFile: "/tmp/session-b.jsonl",
			},
		];
		const sessionIds: Record<string, string> = {
			"active-a": "session-a",
			"active-b": "session-b",
			"restored-a": "session-a",
			"restored-b": "session-b",
		};
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 1, {
				state: createConnectionState(command.activeSessionId, sessionIds[command.activeSessionId]!),
			});
		const connectionA = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-a");
		const connectionB = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-b");
		await connectionA.attach();
		await connectionB.attach();
		const restoredA = new Promise<AgentConnectionEvent>((resolve) => {
			connectionA.subscribe((event) => {
				if (event.type === "session_resynced") {
					resolve(event);
				}
			});
		});
		const restoredB = new Promise<AgentConnectionEvent>((resolve) => {
			connectionB.subscribe((event) => {
				if (event.type === "session_resynced") {
					resolve(event);
				}
			});
		});

		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-a", reason: "update" });

		await expect(Promise.all([restoredA, restoredB])).resolves.toEqual([
			expect.objectContaining({
				type: "session_resynced",
				snapshot: expect.objectContaining({
					state: expect.objectContaining({ sessionId: "session-a" }),
				}),
			}),
			expect.objectContaining({
				type: "session_resynced",
				snapshot: expect.objectContaining({
					state: expect.objectContaining({ sessionId: "session-b" }),
				}),
			}),
		]);
		expect(fakeClient.closeCount).toBe(1);
		expect(fakeClient.reconnectCount).toBe(1);
	});

	it("keeps socket-first shutdown authoritative with a full snapshot tombstone budget", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fillSnapshotRetirementBudget(fakeClient, "active-original");
		expect((connection as unknown as { ignoredSnapshotIds: Set<string> }).ignoredSnapshotIds.size).toBe(128);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(1);

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "shutdown"));
		await Promise.resolve();

		expect(events).toHaveLength(1);
		expect(events[0]).toMatchObject({
			type: "closed",
			error: expect.stringContaining("The Prime Agent daemon shut down while this window was attached."),
		});
		expect(
			(connection as unknown as { replacementReconciliationFailed: boolean }).replacementReconciliationFailed,
		).toBe(false);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
	});

	it("keeps explicit owner close authoritative with a full snapshot tombstone budget", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		fillSnapshotRetirementBudget(fakeClient, "active-original");
		expect((connection as unknown as { ignoredSnapshotIds: Set<string> }).ignoredSnapshotIds.size).toBe(128);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(1);

		fakeClient.close();
		await Promise.resolve();

		expect(events).toEqual([{ type: "closed", error: "Prime Agent daemon client was closed." }]);
		expect(
			(connection as unknown as { replacementReconciliationFailed: boolean }).replacementReconciliationFailed,
		).toBe(false);
		expect(connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")).toBe(false);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
	});

	it("does not reconnect after an explicit shutdown session close", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const closedEvents: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") {
				closedEvents.push(event);
			}
		});
		await connection.attach();

		fakeClient.emitMessage({
			type: "session_closed",
			activeSessionId: "active-original",
			reason: "shutdown",
		});
		fakeClient.emitClose(new Error("Daemon socket closed"));
		await Promise.resolve();

		expect(fakeClient.reconnectCount).toBe(0);
		expect(closedEvents).toHaveLength(1);
		expect(closedEvents[0]).toMatchObject({
			type: "closed",
			error: expect.stringContaining("The Prime Agent daemon shut down while this window was attached."),
		});
		const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
		expect(closedError).toContain("Session ID: session-current.");
		expect(closedError).toContain("Session file: /tmp/session-current.jsonl.");
		expect(closedError).toContain("Diagnostic log:");
	});

	it("does not infer an update when shutdown closes the socket before the session notice", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const closedEvents: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") {
				closedEvents.push(event);
			}
		});
		await connection.attach();

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "shutdown"));
		await Promise.resolve();

		expect(fakeClient.reconnectCount).toBe(0);
		expect(closedEvents).toHaveLength(1);
		const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
		expect(closedError).toContain("The Prime Agent daemon shut down while this window was attached.");
	});

	it("retires ordinary snapshot tails before publishing a terminal session close", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		await connection.attach();
		const stale = createAttachResult("active-original", "client-1", [], 20, {
			messages: [{ role: "user", content: "stale terminal tail", timestamp: 20 }],
		}).snapshot;
		const { messages: _messages, ...staleHeader } = stale;
		const begin = {
			type: "session_snapshot_begin" as const,
			activeSessionId: "active-original",
			snapshotId: "terminal-snapshot-tail",
			snapshot: staleHeader,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "resync" as const,
		};
		const chunk = {
			type: "session_snapshot_chunk" as const,
			activeSessionId: "active-original",
			snapshotId: "terminal-snapshot-tail",
			index: 0,
			messages: stale.messages,
		};
		const end = {
			type: "session_snapshot_end" as const,
			activeSessionId: "active-original",
			snapshotId: "terminal-snapshot-tail",
			chunkCount: 1,
			lastEventSequence: 20,
			lastEventCursor: { generation: "generation-active-original", sequence: 20 },
		};
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		fakeClient.emitMessage(begin);
		fakeClient.emitMessage(chunk);
		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-original", reason: "killed" });
		fakeClient.emitMessage(end);
		fakeClient.emitMessage(end);
		fakeClient.emitMessage(begin);
		fakeClient.emitMessage(chunk);
		fakeClient.emitMessage(end);
		await Promise.resolve();

		expect(events.map((event) => event.type)).toEqual(["closed"]);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({ messages: [] });
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
		expect((connection as unknown as { runtimeSnapshotAttempt?: unknown }).runtimeSnapshotAttempt).toBeUndefined();
	});

	it.each([
		["killed", "The daemon stopped this agent session."],
		["completed", "The daemon closed this agent session after it completed."],
		["replaced", "The daemon replaced this agent session with another session."],
	] as const)("explains a %s session close instead of exposing the raw reason", async (reason, explanation) => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const closedEvents: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") {
				closedEvents.push(event);
			}
		});
		await connection.attach();

		fakeClient.emitMessage({ type: "session_closed", activeSessionId: "active-original", reason });
		await Promise.resolve();

		expect(closedEvents).toHaveLength(1);
		const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
		expect(closedError).toContain(explanation);
		expect(closedError).not.toBe(reason);
		expect(closedError).toContain("Session ID: session-current.");
	});

	it("adds recovery and diagnostic context to unexpected daemon disconnects", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const closedEvents: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") {
				closedEvents.push(event);
			}
		});
		await connection.attach();

		fakeClient.emitClose(new Error("ECONNRESET"));
		await Promise.resolve();

		expect(closedEvents).toHaveLength(1);
		const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
		expect(closedError).toContain("Lost connection to the Prime Agent daemon. Cause: ECONNRESET");
		expect(closedError).toContain("restart Prime Agent or reopen the session from Agents View");
		expect(closedError).toContain("Session file: /tmp/session-current.jsonl.");
		expect(closedError).toContain("Diagnostic log:");
	});

	it("reports an unannounced clean socket close without treating it as an update", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const closedEvents: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			if (event.type === "closed") {
				closedEvents.push(event);
			}
		});
		await connection.attach();

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock"));
		await Promise.resolve();

		expect(fakeClient.reconnectCount).toBe(0);
		expect(closedEvents).toHaveLength(1);
		const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
		expect(closedError).toContain("Lost connection to the Prime Agent daemon.");
	});

	it("does not emit a restored session after disposal begins", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-current",
				sessionFile: "/tmp/session-current.jsonl",
			},
		];
		let releaseRestoredAttach: (() => void) | undefined;
		fakeClient.restoredAttachGate = new Promise<void>((resolve) => {
			releaseRestoredAttach = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "update"));
		await vi.waitFor(() => {
			expect(
				fakeClient.requests.some(
					(request) => request.type === "attach" && request.activeSessionId === "active-restored",
				),
			).toBe(true);
		});
		await connection.dispose();
		releaseRestoredAttach?.();
		await vi.waitFor(() => {
			expect(fakeClient.restoredAttachCompleted).toBe(1);
		});
		for (let flush = 0; flush < 5; flush++) {
			await Promise.resolve();
		}

		expect(events).toEqual([expect.objectContaining({ type: "connection_status", status: "reconnecting" })]);
		expect(fakeClient.requests.at(-1)).toMatchObject({ type: "detach", activeSessionId: "active-restored" });
	});

	it("returns to normal close handling after update restoration times out", async () => {
		vi.useFakeTimers();
		try {
			const fakeClient = new FakeDaemonClient();
			fakeClient.reconnectError = new Error("daemon unavailable");
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
			const closedEvents: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				if (event.type === "closed") {
					closedEvents.push(event);
				}
			});
			await connection.attach();

			fakeClient.emitMessage({
				type: "session_closed",
				activeSessionId: "active-original",
				reason: "update",
			});
			await vi.advanceTimersByTimeAsync(120100);

			expect(closedEvents).toHaveLength(1);
			const closedError = closedEvents[0]?.type === "closed" ? closedEvents[0].error : undefined;
			expect(closedError).toContain(
				"The Prime Agent daemon restarted for an update, but this window could not reconnect",
			);
			expect(closedError).toContain("Last error: daemon unavailable");
			expect(closedError).toContain("restart Prime Agent and reopen it from Agents View");
			expect(closedError).toContain("Session ID: session-current.");
			expect(closedError).toContain("Session file: /tmp/session-current.jsonl.");
			expect(closedError).toContain("Diagnostic log:");
			const reconnectCountAfterFailure = fakeClient.reconnectCount;
			fakeClient.emitClose(new Error("Daemon socket closed"));
			await Promise.resolve();

			expect(fakeClient.reconnectCount).toBe(reconnectCountAfterFailure);
			expect(closedEvents).toHaveLength(1);
			await connection.dispose();
		} finally {
			vi.useRealTimers();
		}
	});

	it("reattaches using the replacement session identity after a session switch", async () => {
		const fakeClient = new FakeDaemonClient();
		const restoredMessages: AgentMessage[] = [{ role: "user", content: "switched prompt", timestamp: 3 }];
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-next",
				sessionFile: "/tmp/session-next.jsonl",
			},
		];
		fakeClient.attachResultFactory = (command) => {
			const sessionId = command.activeSessionId === "active-restored" ? "session-next" : "session-current";
			return createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 1, {
				state: createConnectionState(command.activeSessionId, sessionId),
				messages: command.activeSessionId === "active-restored" ? restoredMessages : [],
			});
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-original",
			state: createConnectionState("active-original", "session-next"),
			messages: [{ role: "user", content: "switched prompt", timestamp: 2 }],
		});

		const restored = new Promise<AgentConnectionEvent>((resolve) => {
			connection.subscribe((event) => {
				if (event.type === "session_resynced") {
					resolve(event);
				}
			});
		});
		fakeClient.emitMessage({
			type: "session_closed",
			activeSessionId: "active-original",
			reason: "update",
		});
		fakeClient.emitClose(new Error("Daemon socket closed"));

		await expect(restored).resolves.toMatchObject({
			type: "session_resynced",
			snapshot: {
				state: { activeSessionId: "active-restored", sessionId: "session-next" },
				messages: restoredMessages,
			},
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "list", "attach"]);
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-restored",
		});
	});

	it("loads connection state and forwards replacement snapshots through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		await expect(connection.getState()).resolves.toMatchObject({
			activeSessionId: "active-1",
			cwd: "/tmp/project",
			sessionId: "session-current",
			sessionName: "session-current name",
			activeToolNames: ["ipython"],
		});

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "session-next"),
			messages: [{ role: "user", content: "replacement prompt", timestamp: 1 }],
		});
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "other",
			state: createConnectionState("other", "ignored"),
			messages: [{ role: "user", content: "ignored", timestamp: 2 }],
		});

		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
		expect(events).toEqual([
			{
				type: "session_replaced",
				state: expect.objectContaining({
					activeSessionId: "active-1",
					sessionId: "session-next",
					sessionName: "session-next name",
					activeToolNames: ["ipython"],
				}),
				messages: [{ role: "user", content: "replacement prompt", timestamp: 1 }],
			},
		]);
	});

	it("forwards catch-up snapshots as non-destructive resync events", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		const messages: AgentMessage[] = [{ role: "user", content: "caught up", timestamp: 2 }];
		const streamingMessage = {
			role: "assistant",
			content: [{ type: "thinking", thinking: "Still reasoning" }],
		} as AgentMessage;
		const snapshot = createAttachResult("active-1", "client-1", undefined, 13, {
			state: { ...createConnectionState("active-1", "session-current"), isStreaming: true },
			messages,
			streamingMessage,
		}).snapshot;

		fakeClient.emitMessage({
			type: "session_resynced",
			activeSessionId: "active-1",
			snapshot,
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				cursor: { generation: "generation-active-1", sequence: 13 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});

		expect(events).toEqual([
			{
				type: "session_resynced",
				snapshot: expect.objectContaining({
					state: expect.objectContaining({ sessionId: "session-current" }),
					messages,
					streamingMessage,
					lastEventSequence: 13,
				}),
			},
		]);
	});

	it("exposes attach snapshots as the initial connection snapshot", async () => {
		const fakeClient = new FakeDaemonClient();
		const snapshotMessage: AgentMessage = { role: "user", content: "snapshot prompt", timestamp: 1 };
		const messages: AgentMessage[] = [snapshotMessage];
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 15, {
				state: createConnectionState(command.activeSessionId, "session-snapshot"),
				messages,
				sessionContext: {
					messages,
					thinkingLevel: "medium",
					serviceTier: "default",
					model: null,
				},
				sessionTree: {
					tree: [
						{
							entry: {
								type: "message",
								id: "user-1",
								parentId: null,
								timestamp: "2026-01-01T00:00:00.000Z",
								message: snapshotMessage,
							},
							children: [],
						},
					],
					leafId: "user-1",
				},
				parent: {
					activeSessionId: "parent-active",
					sessionId: "parent-session",
					nodeId: "parent-node",
					childId: "child-1",
				},
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();

		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: {
				activeSessionId: "active-1",
				sessionId: "session-snapshot",
			},
			messages,
			sessionContext: {
				messages,
				thinkingLevel: "medium",
				model: null,
			},
			sessionTree: {
				leafId: "user-1",
			},
			parent: {
				activeSessionId: "parent-active",
				sessionId: "parent-session",
				nodeId: "parent-node",
				childId: "child-1",
			},
			lastEventSequence: 15,
			replay: {
				status: "complete",
				toSequence: 15,
			},
		});
		await expect(connection.getState()).resolves.toMatchObject({
			sessionId: "session-snapshot",
		});
		await expect(connection.getMessages()).resolves.toEqual(messages);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
	});

	it("preserves the in-flight assistant message when refreshing a stale snapshot", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const streamingMessage: AgentMessage = {
			role: "assistant",
			content: [{ type: "text", text: "partial response" }],
			api: "test-api",
			provider: "test-provider",
			model: "test-model",
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: 2,
		};
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: { type: "message_start", message: streamingMessage },
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				cursor: { generation: "generation-active-1", sequence: 13 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});

		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({ streamingMessage });
		expect(fakeClient.requests.map((request) => request.type)).toEqual([
			"attach",
			"get_connection_state",
			"get_messages",
			"get_session_context",
		]);
	});

	it("does not stamp independently fetched snapshot data with an event cursor observed mid-fetch", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		emitSequencedQueueUpdate(fakeClient, "active-1", 13);
		let releaseState!: () => void;
		fakeClient.connectionStateGate = new Promise<void>((resolveState) => {
			releaseState = resolveState;
		});

		const snapshotPromise = connection.getInitialSnapshot();
		await Promise.resolve();
		emitSequencedQueueUpdate(fakeClient, "active-1", 14);
		releaseState();
		const snapshot = await snapshotPromise;

		expect(snapshot.lastEventCursor).toEqual({ generation: "generation-active-1", sequence: 13 });
		await connection.getState();
		expect(fakeClient.requests.filter((request) => request.type === "get_connection_state")).toHaveLength(2);
	});

	it("times out an attach whose streamed snapshot never completes", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			return {
				...result,
				snapshotStream: { id: "snapshot-stalled", messageCount: 0, targetChunkBytes: 512 * 1024 },
			};
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			snapshotTimeoutMs: 10,
		});

		await expect(connection.attach()).rejects.toThrow("Timed out waiting for snapshot snapshot-stalled");
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-1",
			snapshotId: "snapshot-stalled",
			snapshot: createAttachResult("active-1", "client-1", undefined, 12).snapshot,
			messageCount: 0,
			targetChunkBytes: 512 * 1024,
		});
		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-1",
			snapshotId: "snapshot-stalled",
			chunkCount: 0,
			lastEventSequence: 12,
		});
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it("retries one failed attach request and clears attempt state after success", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.attachFailures = 1;
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.attach()).resolves.toBeUndefined();
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(
			(connection as unknown as { snapshotRequestAttempts: Map<number, unknown> }).snapshotRequestAttempts.size,
		).toBe(0);
	});

	it("reuses a settled same-id assembly with a fresh timer and resolves the original attach caller", async () => {
		const fakeClient = new FakeDaemonClient();
		let attachAttempt = 0;
		fakeClient.attachResultFactory = (command) => {
			const attempt = ++attachAttempt;
			const full = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12, {
				messages: [{ role: "user", content: `generation-${attempt}`, timestamp: attempt }],
			});
			const { messages: _messages, ...snapshot } = full.snapshot;
			queueMicrotask(() => {
				fakeClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: command.activeSessionId,
					snapshotId: "same-id",
					snapshot,
					messageCount: 1,
					targetChunkBytes: 512 * 1024,
				});
				if (attempt === 1) {
					fakeClient.emitMessage({
						type: "session_snapshot_failed",
						activeSessionId: command.activeSessionId,
						snapshotId: "same-id",
						error: "first generation failed",
					});
					return;
				}
				fakeClient.emitMessage({
					type: "session_snapshot_chunk",
					activeSessionId: command.activeSessionId,
					snapshotId: "same-id",
					index: 0,
					messages: full.snapshot.messages,
				});
				fakeClient.emitMessage({
					type: "session_snapshot_end",
					activeSessionId: command.activeSessionId,
					snapshotId: "same-id",
					chunkCount: 1,
					lastEventSequence: full.lastEventSequence,
					lastEventCursor: full.lastEventCursor,
				});
			});
			return {
				...full,
				snapshot: { ...full.snapshot, messages: [] },
				snapshotStream: { id: "same-id", messageCount: 1, targetChunkBytes: 512 * 1024 },
			};
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(connection.attach()).resolves.toBeUndefined();
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			messages: [{ role: "user", content: "generation-2", timestamp: 2 }],
		});
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
		expect(
			(connection as unknown as { snapshotRequestAttempts: Map<number, unknown> }).snapshotRequestAttempts.size,
		).toBe(0);
	});

	it("aliases a failed command reattach to one fresh attach and resolves the switch caller", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-source", {
			snapshotTimeoutMs: 50,
		});
		await connection.attach();
		fakeClient.requests.length = 0;
		fakeClient.switchSessionActiveSessionId = "active-target";
		const target = createAttachResult("active-target", "client-1", undefined, 1, {
			state: createConnectionState("active-target", "session-target"),
			messages: [{ role: "user", content: "target", timestamp: 1 }],
		});
		const emitTransfer = (
			snapshotId: string,
			fail: boolean,
			purpose: "attach" | "replacement",
			clientId: string | undefined,
		) => {
			const { messages: _messages, ...snapshot } = target.snapshot;
			setImmediate(() => {
				fakeClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: "active-target",
					snapshotId,
					snapshot,
					messageCount: 1,
					targetChunkBytes: 512 * 1024,
					purpose,
				});
				if (fail) {
					fakeClient.emitMessage({
						type: "session_snapshot_failed",
						activeSessionId: "active-target",
						snapshotId,
						error: "reattach generation failed",
					});
					return;
				}
				fakeClient.emitMessage({
					type: "session_snapshot_chunk",
					activeSessionId: "active-target",
					snapshotId,
					index: 0,
					messages: target.snapshot.messages,
				});
				fakeClient.emitMessage({
					type: "session_snapshot_end",
					activeSessionId: "active-target",
					snapshotId,
					chunkCount: 1,
					lastEventSequence: target.lastEventSequence,
					lastEventCursor: target.lastEventCursor,
				});
			});
			return {
				...target,
				snapshot: { ...target.snapshot, messages: [] },
				snapshotStream: { id: snapshotId, messageCount: 1, targetChunkBytes: 512 * 1024 },
				client: { ...target.client, id: clientId ?? "missing-client" },
			};
		};
		fakeClient.reattachResultFactory = (command) =>
			emitTransfer("reattach-g1", true, "replacement", command.clientId);
		fakeClient.attachResultFactory = (command) => emitTransfer("reattach-g2", false, "attach", command.clientId);
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		await expect(connection.switchSession("/tmp/target.jsonl")).resolves.toEqual({ cancelled: false });
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["switch_session", "reattach", "attach"]);
		expect(events).toContainEqual(
			expect.objectContaining({
				type: "session_replaced",
				state: expect.objectContaining({ sessionId: "session-target" }),
				messages: [{ role: "user", content: "target", timestamp: 1 }],
			}),
		);
	});

	it("rejects one failed snapshot without interrupting another session on the shared client", async () => {
		const fakeClient = new FakeDaemonClient();
		const sibling = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-2");
		await sibling.attach();
		const siblingEvents: AgentConnectionEvent[] = [];
		sibling.subscribe((event) => {
			siblingEvents.push(event);
		});
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			if (command.activeSessionId !== "active-1") {
				return result;
			}
			const { messages: _messages, ...snapshot } = result.snapshot;
			queueMicrotask(() => {
				fakeClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-failed",
					snapshot,
					messageCount: 0,
					targetChunkBytes: 512 * 1024,
				});
				fakeClient.emitMessage({
					type: "session_snapshot_failed",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-failed",
					error: "snapshot encoder failed",
				});
			});
			return {
				...result,
				snapshot: { ...result.snapshot, messages: [] },
				snapshotStream: { id: "snapshot-failed", messageCount: 0, targetChunkBytes: 512 * 1024 },
			};
		};
		const failed = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await expect(failed.attach()).rejects.toThrow("snapshot encoder failed");
		expect(
			fakeClient.requests.filter((request) => request.type === "attach" && request.activeSessionId === "active-1"),
		).toHaveLength(2);
		expect((failed as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
		expect(
			(failed as unknown as { snapshotRequestAttempts: Map<number, unknown> }).snapshotRequestAttempts.size,
		).toBe(0);
		emitSequencedQueueUpdate(fakeClient, "active-2", 13);
		await vi.waitFor(() => expect(siblingEvents).toHaveLength(1));

		expect(siblingEvents[0]).toMatchObject({ type: "session_event", event: { type: "session_action_update" } });
		expect(fakeClient.closeCount).toBe(0);
		await failed.dispose();
		await sibling.dispose();
	});

	it("does not start runtime recovery from an unfenced failure-before-begin frame", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-1",
			snapshotId: "unfenced-runtime-failure",
			purpose: "resync",
			error: "private unfenced failure",
		});
		await Promise.resolve();

		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1);
		expect((connection as unknown as { runtimeSnapshotAttempt?: unknown }).runtimeSnapshotAttempt).toBeUndefined();
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it("binds a failure-before-begin frame to the exact current attach attempt", async () => {
		const fakeClient = new FakeDaemonClient();
		let releaseAttach!: () => void;
		fakeClient.attachGate = new Promise<void>((resolve) => {
			releaseAttach = resolve;
		});
		let attachResultCount = 0;
		fakeClient.attachResultFactory = (command) => {
			const result = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12);
			if (attachResultCount++ > 0) return result;
			return {
				...result,
				snapshot: { ...result.snapshot, messages: [] },
				snapshotStream: { id: "failed-before-begin", messageCount: 0, targetChunkBytes: 512 * 1024 },
			};
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		const attaching = connection.attach();
		await vi.waitFor(() =>
			expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(1),
		);
		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-1",
			snapshotId: "failed-before-begin",
			purpose: "attach",
			error: "snapshot failed before begin",
		});
		releaseAttach();
		await attaching;

		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(2);
		expect(
			(connection as unknown as { snapshotRequestAttempts: Map<number, unknown> }).snapshotRequestAttempts.size,
		).toBe(0);
		expect((connection as unknown as { runtimeSnapshotAttempt?: unknown }).runtimeSnapshotAttempt).toBeUndefined();
	});

	it("assembles chunked attach snapshots even when chunks arrive before the attach response continuation", async () => {
		const fakeClient = new FakeDaemonClient();
		const messages: AgentMessage[] = [
			{ role: "user", content: "first", timestamp: 1 },
			{ role: "user", content: "second", timestamp: 2 },
		];
		fakeClient.attachResultFactory = (command) => {
			const full = createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 23, {
				state: createConnectionState(command.activeSessionId, "session-streamed"),
				messages,
			});
			const { messages: _messages, ...snapshotHeader } = full.snapshot;
			queueMicrotask(() => {
				fakeClient.emitMessage({
					type: "session_snapshot_begin",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-streamed",
					snapshot: snapshotHeader,
					messageCount: messages.length,
					targetChunkBytes: 512 * 1024,
				});
				fakeClient.emitMessage({
					type: "session_snapshot_chunk",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-streamed",
					index: 0,
					messages: [messages[0]!],
				});
				fakeClient.emitMessage({
					type: "session_snapshot_chunk",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-streamed",
					index: 1,
					messages: [messages[1]!],
				});
				fakeClient.emitMessage({
					type: "session_snapshot_end",
					activeSessionId: command.activeSessionId,
					snapshotId: "snapshot-streamed",
					chunkCount: 2,
					lastEventSequence: 23,
				});
			});
			return {
				...full,
				snapshot: { ...full.snapshot, messages: [] },
				snapshotStream: {
					id: "snapshot-streamed",
					messageCount: messages.length,
					targetChunkBytes: 512 * 1024,
				},
			};
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		await connection.attach();

		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "session-streamed" },
			messages,
			lastEventSequence: 23,
		});
		expect(events).toEqual([]);
	});

	it("commits a chunked replacement header and snapshot atomically before fenced frames", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const replacementState = createConnectionState("active-1", "session-next");
		const full = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 1, {
			state: replacementState,
			messages: [{ role: "user", content: "replacement", timestamp: 1 }],
		});
		full.snapshot.lastEventCursor = { generation: "generation-next", sequence: 1 };
		full.snapshot.promptLifecycles = {
			records: [
				{
					correlationId: "replacement-prompt",
					phase: "queued",
					kind: "model_prompt",
					revision: 2,
					deliveryCrossed: false,
				},
			],
			expired: [],
		};
		const { messages: _messages, ...snapshot } = full.snapshot;

		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: replacementState,
			messages: [],
			snapshotFollows: true,
			meta: {
				id: "active-1:replacement:1",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 1,
				cursor: { generation: "generation-next", sequence: 1 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-1",
			snapshotId: "atomic-replacement",
			snapshot,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "replacement",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_chunk",
			activeSessionId: "active-1",
			snapshotId: "atomic-replacement",
			index: 0,
			messages: full.snapshot.messages,
		});
		fakeClient.emitMessage({
			type: "prompt_lifecycle",
			activeSessionId: "active-1",
			lifecycle: {
				correlationId: "replacement-prompt",
				phase: "delivered",
				kind: "model_prompt",
				revision: 3,
				deliveryCrossed: true,
			},
			meta: {
				id: "active-1:replacement:2",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 2,
				cursor: { generation: "generation-next", sequence: 2 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		await Promise.resolve();

		expect(events).toEqual([]);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "session-current" },
			lastEventCursor: { generation: "generation-active-1", sequence: 12 },
		});

		fakeClient.emitMessage({
			type: "session_snapshot_end",
			activeSessionId: "active-1",
			snapshotId: "atomic-replacement",
			chunkCount: 1,
			lastEventSequence: 1,
			lastEventCursor: { generation: "generation-next", sequence: 1 },
		});
		await vi.waitFor(() =>
			expect(events.map((event) => event.type)).toEqual(["session_replaced", "prompt_lifecycle"]),
		);

		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "session-next" },
			lastEventCursor: { generation: "generation-next", sequence: 2 },
			promptLifecycles: { records: [{ correlationId: "replacement-prompt", phase: "delivered", revision: 3 }] },
		});
	});

	it("keeps a correlated replacement fence across one client-owned attach retry", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const replacementState = createConnectionState("active-1", "session-retried");
		const full = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 1, {
			state: replacementState,
			messages: [{ role: "user", content: "fresh", timestamp: 2 }],
		});
		full.snapshot.lastEventCursor = { generation: "generation-retried", sequence: 1 };
		full.lastEventCursor = full.snapshot.lastEventCursor;
		full.replay = { status: "complete", toSequence: 1, toCursor: full.lastEventCursor };
		const { messages: _messages, ...snapshot } = full.snapshot;
		fakeClient.attachResultFactory = (command) => ({
			...full,
			client: { ...full.client, id: command.clientId ?? "missing-client" },
		});
		fakeClient.requests.length = 0;
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: replacementState,
			messages: [],
			snapshotFollows: true,
			meta: {
				id: "active-1:retried:1",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 1,
				cursor: { generation: "generation-retried", sequence: 1 },
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-1",
			snapshotId: "failed-generation",
			snapshot,
			messageCount: 1,
			targetChunkBytes: 512 * 1024,
			purpose: "replacement",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-1",
			snapshotId: "failed-generation",
			error: "first generation failed",
		});

		await vi.waitFor(() => expect(events).toHaveLength(1));
		expect(events).toEqual([
			expect.objectContaining({
				type: "session_replaced",
				state: expect.objectContaining({ sessionId: "session-retried" }),
				messages: [{ role: "user", content: "fresh", timestamp: 2 }],
			}),
		]);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
		expect(events.some((event) => event.type === "closed")).toBe(false);
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: { sessionId: "session-retried" },
			messages: [{ role: "user", content: "fresh", timestamp: 2 }],
		});
	});

	it("fails a correlated replacement after its one client-owned attach retry fails", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("correlated_prompt_lifecycle_v1");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		const replacementState = createConnectionState("active-1", "session-terminal");
		const full = createAttachResult("active-1", "client-1", ["correlated_prompt_lifecycle_v1"], 1, {
			state: replacementState,
		});
		full.snapshot.lastEventCursor = { generation: "generation-terminal", sequence: 1 };
		const { messages: _messages, ...snapshot } = full.snapshot;
		fakeClient.attachResultFactory = () => {
			throw new Error("retry generation failed");
		};
		fakeClient.requests.length = 0;
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: replacementState,
			messages: [],
			snapshotFollows: true,
		});
		fakeClient.emitMessage({
			type: "session_snapshot_begin",
			activeSessionId: "active-1",
			snapshotId: "failed-once",
			snapshot,
			messageCount: full.snapshot.messages.length,
			targetChunkBytes: 512 * 1024,
			purpose: "replacement",
		});
		fakeClient.emitMessage({
			type: "session_snapshot_failed",
			activeSessionId: "active-1",
			snapshotId: "failed-once",
			error: "first generation failed",
		});

		await vi.waitFor(() => expect(events.some((event) => event.type === "closed")).toBe(true));
		expect(events).toContainEqual({
			type: "closed",
			error: expect.stringContaining("retry generation failed"),
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
	});

	it("distinguishes chunked catch-up snapshots from runtime replacements", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		const emitSnapshot = (
			purpose: "replacement" | "resync",
			snapshotId: string,
			sequence: number,
			sessionId: string,
		) => {
			const messages: AgentMessage[] = [{ role: "user", content: purpose, timestamp: sequence }];
			const full = createAttachResult("active-1", "client-1", undefined, sequence, {
				state: createConnectionState("active-1", sessionId),
				messages,
			});
			const { messages: _messages, ...snapshot } = full.snapshot;
			fakeClient.emitMessage({
				type: "session_snapshot_begin",
				activeSessionId: "active-1",
				snapshotId,
				snapshot,
				messageCount: messages.length,
				targetChunkBytes: 512 * 1024,
				purpose,
			});
			fakeClient.emitMessage({
				type: "session_snapshot_chunk",
				activeSessionId: "active-1",
				snapshotId,
				index: 0,
				messages,
			});
			fakeClient.emitMessage({
				type: "session_snapshot_end",
				activeSessionId: "active-1",
				snapshotId,
				chunkCount: 1,
				lastEventSequence: sequence,
				lastEventCursor: { generation: "generation-active-1", sequence },
			});
		};

		emitSnapshot("resync", "snapshot-resync", 13, "session-current");
		emitSnapshot("replacement", "snapshot-replacement", 14, "session-next");
		await vi.waitFor(() => expect(events).toHaveLength(2));

		expect(events).toEqual([
			expect.objectContaining({
				type: "session_resynced",
				snapshot: expect.objectContaining({
					state: expect.objectContaining({ sessionId: "session-current" }),
				}),
			}),
			expect.objectContaining({
				type: "session_replaced",
				state: expect.objectContaining({ sessionId: "session-next" }),
			}),
		]);
		expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(0);
	});

	it.each(["replacement", "resync"] as const)(
		"recovers a failed chunked %s snapshot without interrupting a sibling session",
		async (purpose) => {
			const fakeClient = new FakeDaemonClient();
			const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
			const sibling = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-2");
			await Promise.all([connection.attach(), sibling.attach()]);
			const events: AgentConnectionEvent[] = [];
			const siblingEvents: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				events.push(event);
			});
			sibling.subscribe((event) => {
				siblingEvents.push(event);
			});
			const recoveredSessionId = purpose === "replacement" ? "session-next" : "session-current";
			fakeClient.connectionStateFactory = (activeSessionId) =>
				createConnectionState(
					activeSessionId,
					activeSessionId === "active-1" ? recoveredSessionId : "session-sibling",
				);
			fakeClient.requests.length = 0;
			const recoveredMessages: AgentMessage[] = [{ role: "user", content: "current prompt", timestamp: 4 }];
			fakeClient.attachResultFactory = (command) =>
				createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 13, {
					state: createConnectionState(command.activeSessionId, recoveredSessionId),
					messages: recoveredMessages,
				});
			const snapshotId = `snapshot-failed-${purpose}`;
			const full = createAttachResult("active-1", "client-1", undefined, 13, {
				state: createConnectionState("active-1", recoveredSessionId),
				messages: recoveredMessages,
			});
			const { messages: _messages, ...snapshot } = full.snapshot;
			if (purpose === "replacement") {
				fakeClient.emitMessage({
					type: "session_replaced",
					activeSessionId: "active-1",
					state: createConnectionState("active-1", recoveredSessionId),
					messages: [],
					snapshotFollows: true,
					meta: {
						id: "active-1:13",
						protocol: DAEMON_PROTOCOL_INFO,
						activeSessionId: "active-1",
						sequence: 13,
						cursor: { generation: "generation-active-1", sequence: 13 },
						emittedAt: "2026-01-01T00:00:00.000Z",
					},
				});
			}
			fakeClient.emitMessage({
				type: "session_snapshot_begin",
				activeSessionId: "active-1",
				snapshotId,
				snapshot,
				messageCount: 1,
				targetChunkBytes: 512 * 1024,
				purpose,
			});
			fakeClient.emitMessage({
				type: "session_snapshot_failed",
				activeSessionId: "active-1",
				snapshotId,
				error: `${purpose} snapshot failed`,
			});

			await vi.waitFor(() => expect(events).toHaveLength(1));
			if (purpose === "replacement") {
				expect(events[0]).toMatchObject({
					type: "session_replaced",
					state: { sessionId: recoveredSessionId },
					messages: [{ role: "user", content: "current prompt", timestamp: 4 }],
				});
			} else {
				expect(events[0]).toMatchObject({
					type: "session_resynced",
					snapshot: {
						state: { sessionId: recoveredSessionId },
						messages: [{ role: "user", content: "current prompt", timestamp: 4 }],
					},
				});
			}
			expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);
			emitSequencedQueueUpdate(fakeClient, "active-2", 13);
			await vi.waitFor(() => expect(siblingEvents).toHaveLength(1));
			expect(siblingEvents[0]).toMatchObject({ type: "session_event", event: { type: "session_action_update" } });
			expect(fakeClient.closeCount).toBe(0);
			expect((connection as unknown as { snapshotAssemblies: Map<string, unknown> }).snapshotAssemblies.size).toBe(
				0,
			);
			await connection.dispose();
			await sibling.dispose();
		},
	);

	it("keeps attach snapshots usable when the daemon omits duplicate session context", async () => {
		const fakeClient = new FakeDaemonClient();
		const messages: AgentMessage[] = [{ role: "user", content: "snapshot prompt", timestamp: 1 }];
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 15, {
				state: createConnectionState(command.activeSessionId, "session-snapshot"),
				messages,
				omitSessionContext: true,
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();

		const snapshot = await connection.getInitialSnapshot();
		expect(snapshot).toMatchObject({
			state: {
				activeSessionId: "active-1",
				sessionId: "session-snapshot",
			},
			messages,
			lastEventSequence: 15,
		});
		expect(snapshot.sessionContext).toBeUndefined();
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach"]);

		await expect(connection.getSessionContext()).resolves.toMatchObject({
			messages: [{ role: "user", content: "context prompt", timestamp: 3 }],
		});
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "get_session_context"]);
	});

	it("keeps reconnect usable from attach snapshots when replay is unavailable", async () => {
		const fakeClient = new FakeDaemonClient();
		const reconnectedMessages: AgentMessage[] = [{ role: "user", content: "reconnected prompt", timestamp: 2 }];
		let attachCount = 0;
		fakeClient.attachResultFactory = (command) => {
			attachCount++;
			if (attachCount === 1) {
				return createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12, {
					state: createConnectionState(command.activeSessionId, "session-initial"),
				});
			}
			return createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 20, {
				state: createConnectionState(command.activeSessionId, "session-reconnected"),
				messages: reconnectedMessages,
				sessionContext: {
					messages: reconnectedMessages,
					thinkingLevel: "medium",
					serviceTier: "default",
					model: null,
				},
				replay: {
					status: "unavailable",
					fromSequence: 14,
					toSequence: 20,
					reason: "event_replay_not_available",
				},
			});
		};
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();
		emitSequencedQueueUpdate(fakeClient, "active-1", 14);
		await connection.attach();

		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-1",
			capabilities: ["attach_snapshot", "event_sequence", "extension_ui", "slim_attach", "chunked_snapshot"],
			resumeCursor: {
				activeSessionId: "active-1",
				generation: "generation-active-1",
				sequence: 14,
			},
		});
		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			state: {
				sessionId: "session-reconnected",
			},
			messages: reconnectedMessages,
			replay: {
				status: "unavailable",
				fromSequence: 14,
				toSequence: 20,
				reason: "event_replay_not_available",
			},
			lastEventSequence: 20,
		});
		await expect(connection.getState()).resolves.toMatchObject({
			sessionId: "session-reconnected",
		});
		await expect(connection.getMessages()).resolves.toEqual(reconnectedMessages);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "attach"]);
	});

	it("retries one reattach on the recovered transport before resetting it", async () => {
		const fakeClient = new FakeDaemonClient();
		const recoverDaemon = vi.fn(async () => undefined);
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			recoverDaemon,
			reconnectTimeoutMs: 2000,
		});
		const statuses: string[] = [];
		connection.subscribe((event) => {
			if (event.type === "connection_status") {
				statuses.push(event.status);
			}
		});
		await connection.attach();

		fakeClient.attachFailures = 1;
		fakeClient.connected = false;
		fakeClient.emitClose(new Error("Daemon socket closed"));

		await vi.waitFor(() => expect(statuses).toEqual(["reconnecting", "connected"]));
		expect(recoverDaemon).toHaveBeenCalledOnce();
		expect(fakeClient.reconnectCount).toBe(1);
		expect(fakeClient.resetTransportCount).toBe(0);
		expect(fakeClient.requests.filter((request) => request.type === "attach")).toHaveLength(3);
	});

	it("does not reconnect after disposal while daemon recovery is pending", async () => {
		const fakeClient = new FakeDaemonClient();
		let finishRecovery: (() => void) | undefined;
		const recoverDaemon = vi.fn(
			() =>
				new Promise<void>((resolve) => {
					finishRecovery = resolve;
				}),
		);
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			recoverDaemon,
			reconnectTimeoutMs: 2000,
		});
		await connection.attach();

		fakeClient.connected = false;
		fakeClient.emitClose(new Error("Daemon socket closed"));
		await vi.waitFor(() => expect(recoverDaemon).toHaveBeenCalledOnce());
		await connection.dispose();
		finishRecovery?.();
		for (let flush = 0; flush < 5; flush++) {
			await Promise.resolve();
		}

		expect(fakeClient.reconnectCount).toBe(0);
	});

	it("isolates subscriber failures during transport recovery", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			recoverDaemon: async () => undefined,
			reconnectTimeoutMs: 2000,
		});
		const statuses: string[] = [];
		connection.subscribe(async () => {
			throw new Error("broken subscriber");
		});
		connection.subscribe((event) => {
			if (event.type === "connection_status") {
				statuses.push(event.status);
			}
		});
		await connection.attach();

		fakeClient.connected = false;
		fakeClient.emitClose(new Error("Daemon socket closed"));

		await vi.waitFor(() => expect(statuses).toEqual(["reconnecting", "connected"]));
		expect(fakeClient.reconnectCount).toBe(1);
	});

	it("does not let a stalled subscriber block update recovery", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.updateRestartSessions = [
			{
				id: "active-restored",
				activeSessionId: "active-restored",
				sessionId: "session-current",
				sessionFile: "/tmp/session-current.jsonl",
			},
		];
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-original");
		const statuses: string[] = [];
		connection.subscribe(() => new Promise<void>(() => undefined));
		connection.subscribe((event) => {
			if (event.type === "connection_status") {
				statuses.push(event.status);
			}
		});
		await connection.attach();

		fakeClient.emitClose(new DaemonSocketClosedError("/tmp/prime-agent.sock", "update"));

		await vi.waitFor(() => expect(statuses).toEqual(["reconnecting", "connected"]));
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-restored",
		});
	});

	it.each([true, false])("capability-gates authoritative child-roster reads: %s", async (supported) => {
		const fakeClient = new FakeDaemonClient();
		if (supported) fakeClient.serverCapabilities.add("authoritative_child_roster");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		const children = connection.getRlmChildSnapshots();
		if (supported) await expect(children).resolves.toEqual([]);
		else await expect(children).rejects.toThrow("authoritative_child_roster");
	});

	it("preserves a newer live child update over an in-flight roster read", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("authoritative_child_roster");
		let releaseRoster!: () => void;
		fakeClient.rlmChildrenGate = new Promise<void>((resolve) => {
			releaseRoster = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		const children = connection.getRlmChildSnapshots();
		emitRlmChildUpdate(fakeClient, "active-1", 13, {
			id: "child-live",
			label: "live child",
			status: "running",
			sessionDir: "/tmp/child-live",
		});
		releaseRoster();

		await expect(children).resolves.toEqual([expect.objectContaining({ id: "child-live", status: "running" })]);
	});

	it("keeps the live child roster after an empty attach snapshot", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12, { children: [] });
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();
		emitRlmChildUpdate(fakeClient, "active-1", 13, {
			id: "child-live",
			label: "live child",
			status: "running",
			sessionDir: "/tmp/child-live",
		});

		await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
			children: [expect.objectContaining({ id: "child-live", status: "running" })],
		});
	});

	it("refreshes initial snapshots after live events make the cached snapshot stale", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.attachResultFactory = (command) =>
			createAttachResult(command.activeSessionId, command.clientId, command.capabilities, 12, {
				state: createConnectionState(command.activeSessionId, "session-attached"),
				messages: [{ role: "user", content: "attached prompt", timestamp: 1 }],
			});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");

		await connection.attach();
		emitSequencedQueueUpdate(fakeClient, "active-1", 13);

		const snapshot = await connection.getInitialSnapshot();
		expect(snapshot).toMatchObject({
			state: {
				sessionId: "session-current",
			},
			messages: [{ role: "user", content: "current prompt", timestamp: 4 }],
			sessionContext: {
				messages: [{ role: "user", content: "context prompt", timestamp: 3 }],
			},
		});
		// The session tree is fetched lazily (only when the tree/branch selector is
		// opened), so refreshing the initial snapshot must not request it.
		expect(snapshot.sessionTree).toBeUndefined();
		expect(fakeClient.requests.map((request) => request.type)).toEqual([
			"attach",
			"get_connection_state",
			"get_messages",
			"get_session_context",
		]);
	});

	it("ignores older sequenced events after an attach snapshot", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		await connection.attach();
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "session-old"),
			messages: [{ role: "user", content: "old prompt", timestamp: 1 }],
			meta: {
				id: "active-1:10",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 10,
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		fakeClient.emitMessage({
			type: "session_replaced",
			activeSessionId: "active-1",
			state: createConnectionState("active-1", "session-new"),
			messages: [{ role: "user", content: "new prompt", timestamp: 2 }],
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});

		expect(events).toEqual([
			{
				type: "session_replaced",
				state: expect.objectContaining({
					sessionId: "session-new",
				}),
				messages: [{ role: "user", content: "new prompt", timestamp: 2 }],
			},
		]);
		await expect(connection.getState()).resolves.toMatchObject({
			sessionId: "session-new",
		});
	});

	it("sends queue commands through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("session_input_pause");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.getQueue()).resolves.toEqual({ steering: ["steer"], followUp: ["follow"] });
		await expect(connection.clearQueue()).resolves.toEqual({ steering: ["cleared"], followUp: [] });
		await expect(connection.abortAndClearQueue()).resolves.toEqual({ steering: ["aborted"], followUp: ["cleared"] });
		const [inputPause, inputPauseAlias] = await Promise.all([
			connection.acquireSessionInputPause("lease-1"),
			connection.acquireSessionInputPause("lease-1"),
		]);
		expect(inputPauseAlias).toBe(inputPause);
		(connection as unknown as { activeSessionId: string }).activeSessionId = "active-2";
		await inputPause.release();
		(connection as unknown as { activeSessionId: string }).activeSessionId = "active-1";
		await connection.waitForIdle();
		const model = getModel("anthropic", "claude-sonnet-4-5");
		if (!model) {
			throw new Error("Test model not found");
		}
		await connection.setScopedModels([{ model, thinkingLevel: "high" }]);

		expect(fakeClient.requests.map((request) => request.type)).toEqual([
			"attach",
			"get_queue",
			"clear_queue",
			"abort_and_clear_queue",
			"acquire_session_input_pause",
			"release_session_input_pause",
			"wait_for_idle",
			"set_scoped_models",
		]);
		expect(fakeClient.requests[1]).toMatchObject({ type: "get_queue", activeSessionId: "active-1" });
		expect(fakeClient.requests[2]).toMatchObject({ type: "clear_queue", activeSessionId: "active-1" });
		expect(fakeClient.requests[3]).toMatchObject({ type: "abort_and_clear_queue", activeSessionId: "active-1" });
		expect(fakeClient.requests[4]).toMatchObject({
			type: "acquire_session_input_pause",
			activeSessionId: "active-1",
			leaseKey: "lease-1",
		});
		expect(fakeClient.requests[5]).toMatchObject({
			type: "release_session_input_pause",
			activeSessionId: "active-1",
			pauseId: "pause-1",
		});
		expect(fakeClient.requests[6]).toMatchObject({ type: "wait_for_idle", activeSessionId: "active-1" });
		expect(fakeClient.requests[7]).toMatchObject({
			type: "set_scoped_models",
			activeSessionId: "active-1",
			scopedModels: [{ model, thinkingLevel: "high" }],
		});

		fakeClient.abortAndClearQueueUnknownCommand = true;
		await expect(connection.abortAndClearQueue()).rejects.toThrow(
			"the daemon is running an older build; restart the daemon and try again",
		);
	});

	it("fails closed when a daemon disconnect invalidates an input pause", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("session_input_pause");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});

		const pause = await connection.acquireSessionInputPause("lease-1");
		fakeClient.disconnectForReconnect("shutdown");

		await expect(pause.release()).rejects.toThrow("invalidated by a daemon reconnect");
		await expect(connection.acquireSessionInputPause("lease-1")).rejects.toThrow("connection is closed");
		expect(events).toContainEqual(
			expect.objectContaining({
				type: "closed",
				error: expect.stringContaining("fence was invalidated"),
			}),
		);
	});

	it("releases an input pause whose acquisition resolves after disconnect", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("session_input_pause");
		let releaseAcquire!: () => void;
		fakeClient.inputPauseAcquireGate = new Promise<void>((resolve) => {
			releaseAcquire = resolve;
		});
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		const acquisition = connection.acquireSessionInputPause("lease-1");
		await vi.waitFor(() =>
			expect(fakeClient.requests.some((request) => request.type === "acquire_session_input_pause")).toBe(true),
		);
		fakeClient.disconnectForReconnect("shutdown");
		releaseAcquire();

		await expect(acquisition).rejects.toThrow("acquisition was invalidated by a daemon reconnect");
		expect(fakeClient.requests.filter((request) => request.type === "release_session_input_pause")).toHaveLength(1);
	});

	it("capability-gates the strong RLM completion barrier", async () => {
		const oldDaemonClient = new FakeDaemonClient();
		const oldConnection = new DaemonAgentConnection(asDaemonClient(oldDaemonClient), "active-old");
		await oldConnection.attach();
		await oldConnection.waitForHeadlessCompletion();
		expect(oldDaemonClient.requests.at(-1)).toEqual({
			type: "wait_for_headless_completion",
			activeSessionId: "active-old",
		});
		await expect(oldConnection.waitForHeadlessCompletion({ waitForRlmQuiescence: true })).rejects.toThrow(
			"without RLM quiescence barriers",
		);
		expect(oldDaemonClient.requests.map((request) => request.type)).toEqual([
			"attach",
			"wait_for_headless_completion",
		]);

		const newDaemonClient = new FakeDaemonClient();
		newDaemonClient.serverCapabilities.add("rlm_quiescence_barrier");
		const newConnection = new DaemonAgentConnection(asDaemonClient(newDaemonClient), "active-new");
		await newConnection.attach();
		await newConnection.waitForHeadlessCompletion({ waitForRlmQuiescence: true });
		expect(newDaemonClient.requests.at(-1)).toMatchObject({
			type: "wait_for_headless_completion",
			activeSessionId: "active-new",
			waitForRlmQuiescence: true,
		});
	});

	it("cancels rlm child runs through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.cancelRlmChild("child-1")).resolves.toBe(true);
		await expect(connection.cancelRlmChild("finished-child")).resolves.toBe(false);

		expect(fakeClient.requests[1]).toMatchObject({
			type: "cancel_rlm_child",
			activeSessionId: "active-1",
			childId: "child-1",
		});

		// A daemon from a build that predates the command reports a restart hint
		// instead of the raw protocol error.
		await expect(connection.cancelRlmChild("stale-daemon")).rejects.toThrow(
			"the daemon is running an older build; restart the daemon and try again",
		);
	});

	it("sends bash commands through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await connection.executeBash("echo hi", { excludeFromContext: true });
		await connection.abortBash();

		expect(fakeClient.requests[1]).toMatchObject({
			type: "execute_bash",
			activeSessionId: "active-1",
			command: "echo hi",
			excludeFromContext: true,
		});
		expect(fakeClient.requests[2]).toMatchObject({ type: "abort_bash", activeSessionId: "active-1" });

		// A daemon from a build that predates the command reports a restart hint
		// instead of the raw protocol error.
		await expect(connection.executeBash("stale-daemon")).rejects.toThrow(
			"the daemon is running an older build; restart the daemon and try again",
		);
		fakeClient.abortBashUnknownCommand = true;
		await expect(connection.abortBash()).rejects.toThrow(
			"the daemon is running an older build; restart the daemon and try again",
		);
	});

	it("loads resource snapshots through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.getResourceSnapshot()).resolves.toMatchObject({
			contextFiles: [{ path: "/tmp/AGENTS.md" }],
			skills: [{ name: "demo-skill", filePath: "/tmp/skills/demo-skill/SKILL.md" }],
			diagnostics: { skills: [], prompts: [], extensions: [], themes: [] },
		});

		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_resource_snapshot",
			activeSessionId: "active-1",
		});
	});

	it("capability-gates ACP MCP server replacement", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "session-1");
		expect(connection.supportsAcpMcpServers()).toBe(false);
		await expect(
			connection.replaceAcpMcpServers(
				[
					{
						name: "task",
						type: "http",
						url: "https://task.example/mcp",
						headers: { Authorization: "Bearer task" },
					},
				],
				"owner-a",
			),
		).rejects.toBeInstanceOf(DaemonCapabilityUnavailableError);

		fakeClient.serverCapabilities.add("acp_mcp_servers");
		expect(connection.supportsAcpMcpServers()).toBe(true);
		await connection.releaseAcpMcpServers("owner-a", ["task"]);
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "replace_acp_mcp_servers",
			ownerId: "owner-a",
			servers: [],
		});
		await connection.dispose();
	});

	it("loads the full model catalog through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		fakeClient.serverCapabilities.add("model_catalog");
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		const catalog = await connection.getModelCatalog();

		expect(catalog.configuredProviders).toEqual(["openai"]);
		expect(catalog.models[0]).toMatchObject({ provider: "openai", id: "gpt-5.1" });
		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_model_catalog",
			activeSessionId: "active-1",
		});
	});

	it("falls back to configured models when the daemon lacks model catalog support", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		const catalog = await connection.getModelCatalog();

		expect(catalog.configuredProviders).toEqual(["openai"]);
		expect(catalog.models[0]).toMatchObject({ provider: "openai", id: "gpt-5.1" });
		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_available_models",
			activeSessionId: "active-1",
		});
	});

	it("loads session context through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
			},
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});

		await expect(connection.getSessionContext()).resolves.toEqual({
			messages: [{ role: "user", content: "context prompt", timestamp: 3 }],
			thinkingLevel: "medium",
			model: { provider: "anthropic", modelId: "claude-sonnet-4-5" },
		});

		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_session_context",
			activeSessionId: "active-1",
		});
	});

	it("loads session trees through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
			},
			meta: {
				id: "active-1:13",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 13,
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});

		await expect(connection.getSessionTree()).resolves.toEqual({
			tree: [
				{
					entry: {
						type: "message",
						id: "user-1",
						parentId: null,
						timestamp: "2026-01-01T00:00:00.000Z",
						message: { role: "user", content: "hello", timestamp: 1 },
					},
					children: [],
				},
			],
			leafId: "user-1",
		});

		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_session_tree",
			activeSessionId: "active-1",
		});
	});

	it("loads serializable tool metadata through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.getToolDefinition("custom_tool")).resolves.toEqual({
			name: "custom_tool",
			label: "custom_tool",
			description: "custom_tool description",
			promptSnippet: "custom_tool prompt",
			promptGuidelines: ["Use custom_tool"],
			parameters: { type: "object" },
			renderShell: "self",
		});

		expect(fakeClient.requests[1]).toMatchObject({
			type: "get_tool_definition",
			activeSessionId: "active-1",
			name: "custom_tool",
		});
	});

	it("forwards daemon session events through the connection boundary", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();

		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 2, steering: ["interrupt"], followUps: ["later"] },
			},
			meta: {
				id: "active-1:14",
				protocol: DAEMON_PROTOCOL_INFO,
				activeSessionId: "active-1",
				sequence: 14,
				emittedAt: "2026-01-01T00:00:00.000Z",
			},
		});
		fakeClient.emitMessage({
			type: "session_event",
			activeSessionId: "other",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 1, steering: ["ignored"], followUps: [] },
			},
		});

		expect(events).toEqual([
			{
				type: "session_event",
				event: {
					type: "session_action_update",
					actions: { queuedCount: 2, steering: ["interrupt"], followUps: ["later"] },
				},
			},
		]);

		await connection.attach();
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-1",
			clientId: expect.any(String),
			capabilities: ["attach_snapshot", "event_sequence", "extension_ui", "slim_attach", "chunked_snapshot"],
			resumeCursor: {
				activeSessionId: "active-1",
				generation: "generation-active-1",
				sequence: 14,
			},
		});

		await connection.attach();
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			activeSessionId: "active-1",
			capabilities: ["attach_snapshot", "event_sequence", "extension_ui", "slim_attach", "chunked_snapshot"],
			resumeCursor: {
				activeSessionId: "active-1",
				generation: "generation-active-1",
				sequence: 14,
			},
		});
	});

	it("ignores delayed events from a retired daemon generation", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const steeringUpdates: Array<readonly string[]> = [];
		connection.subscribe((event) => {
			if (event.type === "session_event" && event.event.type === "session_action_update") {
				steeringUpdates.push(event.event.actions.steering);
			}
		});
		await connection.attach();
		const emitQueue = (generation: string, sequence: number, steering: string) => {
			fakeClient.emitMessage({
				type: "session_event",
				activeSessionId: "active-1",
				event: { type: "session_action_update", actions: { queuedCount: 1, steering: [steering], followUps: [] } },
				meta: {
					id: `${generation}:${sequence}`,
					protocol: DAEMON_PROTOCOL_INFO,
					activeSessionId: "active-1",
					sequence,
					cursor: { generation, sequence },
					emittedAt: "2026-01-01T00:00:00.000Z",
				},
			});
		};

		emitQueue("generation-new", 1, "new");
		emitQueue("generation-active-1", 13, "old");
		await vi.waitFor(() => expect(steeringUpdates).toEqual([["new"]]));

		await connection.attach();
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "attach",
			resumeCursor: { generation: "generation-new", sequence: 1 },
		});
	});

	it("forwards extension UI requests and responses", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		const events: AgentConnectionEvent[] = [];
		connection.subscribe((event) => {
			events.push(event);
		});
		await connection.attach();
		expect(fakeClient.requests[0]).toMatchObject({
			type: "attach",
			activeSessionId: "active-1",
			supportsExtensionUi: true,
			clientId: expect.any(String),
			capabilities: ["attach_snapshot", "event_sequence", "extension_ui", "slim_attach", "chunked_snapshot"],
		});

		fakeClient.emitMessage({
			type: "extension_ui_request",
			activeSessionId: "active-1",
			id: "request-1",
			method: "confirm",
			payload: { title: "Confirm", message: "Proceed?" },
		});
		fakeClient.emitMessage({
			type: "extension_ui_request",
			activeSessionId: "other",
			id: "request-2",
			method: "confirm",
			payload: { title: "Ignore", message: "Wrong session" },
		});

		expect(events).toEqual([
			{
				type: "extension_ui_request",
				request: {
					id: "request-1",
					method: "confirm",
					payload: { title: "Confirm", message: "Proceed?" },
				},
			},
		]);

		await connection.respondToExtensionUiRequest("request-1", { confirmed: true });
		expect(fakeClient.requests.at(-1)).toMatchObject({
			type: "extension_ui_response",
			activeSessionId: "active-1",
			requestId: "request-1",
			response: { confirmed: true },
		});
	});

	it("uses an extended timeout for refine requests through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(
			connection.refine({ instructions: "remember this", rollbackId: "refine_previous" }),
		).resolves.toMatchObject({
			id: "refine_daemon",
			appliedEdits: [],
		});

		expect(fakeClient.requests[1]).toMatchObject({
			type: "refine",
			activeSessionId: "active-1",
			instructions: "remember this",
			rollbackId: "refine_previous",
		});
		expect(fakeClient.requests[1]).not.toHaveProperty("global");
		expect(fakeClient.requestTimeouts[0]).toBe(30000);
		expect(fakeClient.requestTimeouts[1]).toBe(DAEMON_REFINE_REQUEST_TIMEOUT_MS);
	});

	it("lists and renames saved sessions through the daemon protocol", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();
		const progress: Array<[number, number]> = [];
		const discovered: AgentConnectionSavedSessionInfo[] = [];

		const sessions = await connection.listSavedSessions("current", {
			onProgress: (loaded, total) => {
				progress.push([loaded, total]);
			},
			onSession: (session) => {
				discovered.push(session);
			},
		});
		await connection.renameSavedSession("/tmp/session-a.jsonl", "Next name");
		await expect(connection.deleteSavedSession("/tmp/session-a.jsonl")).resolves.toEqual({
			ok: true,
			method: "trash",
		});

		expect(sessions).toEqual([
			{
				path: "/tmp/session-a.jsonl",
				id: "session-a",
				cwd: "/tmp",
				name: "Saved session",
				created: new Date("2026-01-01T00:00:00.000Z"),
				modified: new Date("2026-01-02T00:00:00.000Z"),
				messageCount: 2,
				firstMessage: "hello",
				allMessagesText: "hello world",
			},
		]);
		expect(progress).toEqual([
			[1, 2],
			[2, 2],
		]);
		expect(discovered).toEqual(sessions);
		expect(fakeClient.requests[1]).toMatchObject({
			type: "list_saved_sessions",
			activeSessionId: "active-1",
			scope: "current",
		});
		expect(fakeClient.requests[2]).toMatchObject({
			type: "rename_saved_session",
			activeSessionId: "active-1",
			sessionPath: "/tmp/session-a.jsonl",
			name: "Next name",
		});
		expect(fakeClient.requests[3]).toMatchObject({
			type: "delete_saved_session",
			activeSessionId: "active-1",
			sessionPath: "/tmp/session-a.jsonl",
		});
	});

	it("rehydrates daemon recoverable errors for interactive recovery flows", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1");
		await connection.attach();

		await expect(connection.switchSession("/tmp/session.jsonl")).rejects.toBeInstanceOf(MissingSessionCwdError);
		await expect(connection.switchSession("/tmp/session.jsonl")).rejects.toMatchObject({
			issue: {
				sessionFile: "/tmp/session.jsonl",
				sessionCwd: "/tmp/missing",
				fallbackCwd: "/tmp/current",
			},
		});
		await expect(connection.importFromJsonl("/tmp/not-found.jsonl")).rejects.toBeInstanceOf(
			SessionImportFileNotFoundError,
		);
		await expect(connection.importFromJsonl("/tmp/not-found.jsonl")).rejects.toMatchObject({
			filePath: "/tmp/not-found.jsonl",
		});
	});

	it("can close the daemon client when disposed by an owning caller", async () => {
		const fakeClient = new FakeDaemonClient();
		const connection = new DaemonAgentConnection(asDaemonClient(fakeClient), "active-1", {
			closeClientOnDispose: true,
		});
		await connection.attach();

		await connection.dispose();

		expect(fakeClient.requests.at(-1)).toMatchObject({ type: "detach", activeSessionId: "active-1" });
		expect(fakeClient.closeCount).toBe(1);
	});

	it("cleans up daemon client subscriptions when static attach fails", async () => {
		const fakeClient = new FakeDaemonClient();

		await expect(
			DaemonAgentConnection.attach(asDaemonClient(fakeClient), "missing", { closeClientOnDispose: true }),
		).rejects.toThrow("Unknown active session: missing");

		expect(fakeClient.getMessageListenerCount()).toBe(0);
		expect(fakeClient.getCloseListenerCount()).toBe(0);
		expect(fakeClient.requests.map((request) => request.type)).toEqual(["attach", "attach", "detach"]);
		expect(fakeClient.closeCount).toBe(1);
	});
});
