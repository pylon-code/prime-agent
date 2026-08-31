import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
	cloneCallerOwnedSessionLaunchEnv,
	collectDaemonLaunchEnv,
	createDaemonCommandEnvelope,
	createDaemonEventEnvelope,
	createDaemonEventMeta,
	createDaemonReplayInfo,
	DAEMON_COMMAND_COMPATIBILITY,
	DAEMON_DEFAULT_CLIENT_CAPABILITIES,
	DAEMON_DEFAULT_SERVER_CAPABILITIES,
	DAEMON_OUTBOUND_COMPATIBILITY,
	DAEMON_PROTOCOL_INFO,
	DAEMON_PROTOCOL_VERSION,
	DAEMON_SCHEMA_ID,
	DAEMON_SCHEMA_REVISION,
	DAEMON_SNAPSHOT_GENERATION_NONCE_MIN_SCHEMA_REVISION,
	DAEMON_SUPERVISOR_SERVER_CAPABILITIES,
	DAEMON_SUPPORTED_CLIENT_CAPABILITIES,
	type DaemonCommand,
	type DaemonOutbound,
	daemonOutboundForCorrelatedPromptCapability,
	getDaemonCommandCompatibilities,
	isDaemonCommandEnvelope,
	isDaemonMutatingCommand,
	salvageDaemonCommandId,
} from "../src/modes/daemon/daemon-protocol.js";
import {
	type DaemonWorkerDescriptor,
	durableDaemonWorkerDescriptor,
} from "../src/modes/daemon/daemon-worker-protocol.js";
import { CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE } from "../src/sdk-features.js";

describe("daemon protocol helpers", () => {
	it("serializes worker descriptors as identity-only version 2 state", () => {
		const descriptor = {
			version: 1,
			workerId: "worker",
			pid: 123,
			processStartId: "process-start",
			socketPath: "/tmp/worker.sock",
			recoveryJournalPath: "/state/recovery.jsonl",
			orphanProcessJournalPath: "/state/orphans.jsonl",
			supervisorSocketPath: "/tmp/supervisor.sock",
			authenticationToken: "local-worker-token",
			rootActiveSessionId: "active",
			callerOwnedEnvironmentContract: true,
			sessionFile: "/sessions/root.jsonl",
			createdAt: "2026-01-01T00:00:00.000Z",
			updatedAt: "2026-01-01T00:00:00.000Z",
			lifecycle: "ready",
			createCommand: {
				type: "create",
				sessionPath: "/sessions/root.jsonl",
				config: {
					sessionDir: "/legacy/sessions",
					telemetryDisabled: true,
					apiKey: "secret-api-key",
					extensionFlagValues: { providerSecretKey: "secret-extension" },
				},
				env: { PROVIDER_TOKEN: "secret-client-env" },
				launchEnv: { PROVIDER_TOKEN: "secret-launch-env" },
				runtimeMetadata: { parentActiveSessionId: "secret-runtime" },
			},
			launchEnv: { PROVIDER_TOKEN: "secret-top-level-env" },
			consecutiveFailures: 0,
			lastError: "secret-error",
		} as unknown as DaemonWorkerDescriptor;

		const durable = durableDaemonWorkerDescriptor(descriptor);

		expect(durable.version).toBe(2);
		expect(durable.createCommand).toEqual({ type: "create", sessionPath: "/sessions/root.jsonl" });
		expect(durable).toMatchObject({
			workerId: "worker",
			callerOwnedEnvironmentContract: true,
			sessionFile: "/sessions/root.jsonl",
			sessionDir: "/legacy/sessions",
			telemetryDisabled: true,
		});
		expect(JSON.stringify(durable)).not.toContain("secret-");
	});

	it("defensively clones exact caller-owned environments without exposing rejected values", () => {
		const source = { PROVIDER_TOKEN: "secret-a", Path: "/caller/bin" };
		const snapshot = cloneCallerOwnedSessionLaunchEnv(source, "linux");
		source.PROVIDER_TOKEN = "secret-c";

		expect(snapshot).toEqual({ PROVIDER_TOKEN: "secret-a", Path: "/caller/bin" });
		expect(Object.isFrozen(snapshot)).toBe(true);
		expect(cloneCallerOwnedSessionLaunchEnv({ Path: "a", PATH: "b" }, "linux")).toEqual({
			Path: "a",
			PATH: "b",
		});
		expect(() => cloneCallerOwnedSessionLaunchEnv({ Path: "a", PATH: "b" }, "win32")).toThrow(
			"duplicate Windows environment keys",
		);
		for (const invalid of [
			{ "": "value" },
			{ "BAD=NAME": "value" },
			{ "BAD\0NAME": "value" },
			{ BAD_VALUE: "value\0suffix" },
			{ prime_agent_internal_private: "value" },
			{ BAD_TYPE: 1 } as unknown as Record<string, string>,
		]) {
			expect(() => cloneCallerOwnedSessionLaunchEnv(invalid)).toThrow(TypeError);
		}
		const accessor = {} as Record<string, string>;
		Object.defineProperty(accessor, "PRIVATE", { get: () => "value" });
		expect(() => cloneCallerOwnedSessionLaunchEnv(accessor)).toThrow(TypeError);
		const symbolKeyed = {} as Record<string, string>;
		Object.defineProperty(symbolKeyed, Symbol("private"), { value: "value" });
		expect(() => cloneCallerOwnedSessionLaunchEnv(symbolKeyed)).toThrow("string environment keys");
		const inherited = Object.create({ INHERITED_PRIVATE: "ignored" }) as Record<string, string>;
		expect(cloneCallerOwnedSessionLaunchEnv(inherited)).toEqual({});
		const reserved = "private-environment-canary";
		const error = (() => {
			try {
				cloneCallerOwnedSessionLaunchEnv({ PRIME_AGENT_INTERNAL_PRIVATE: reserved });
			} catch (cause) {
				return cause;
			}
		})();
		expect(error).toBeInstanceOf(TypeError);
		expect(String(error)).not.toContain(reserved);

		const proxyCanary = "hostile-proxy-private-canary";
		const hostileProxy = new Proxy(Object.create(null) as Record<string, string>, {
			ownKeys() {
				throw new Error(proxyCanary);
			},
		});
		let proxyError: unknown;
		try {
			cloneCallerOwnedSessionLaunchEnv(hostileProxy);
		} catch (cause) {
			proxyError = cause;
		}
		expect(proxyError).toBeInstanceOf(TypeError);
		expect(String(proxyError)).toBe(
			"TypeError: ownedSessionLaunchEnv must be an inspectable string environment record",
		);
		expect(String(proxyError)).not.toContain(proxyCanary);
		const revoked = Proxy.revocable(Object.create(null) as Record<string, string>, {});
		revoked.revoke();
		expect(() => cloneCallerOwnedSessionLaunchEnv(revoked.proxy)).toThrow(
			"ownedSessionLaunchEnv must be an inspectable string environment record",
		);

		for (const key of ["RLM_DEPTH", "rlm_depth", "Rlm_Depth"]) {
			expect(() => cloneCallerOwnedSessionLaunchEnv({ [key]: "private-depth" })).toThrow(TypeError);
		}
		expect(
			collectDaemonLaunchEnv({ RLM_DEPTH: "1", rlm_depth: "2", KEEP_EXACT: "yes" } as NodeJS.ProcessEnv),
		).toEqual({ KEEP_EXACT: "yes" });
	});

	it("capability-gates exact launch replacement without changing legacy commands", () => {
		const exact = getDaemonCommandCompatibilities({
			type: "create",
			lifecycle: "client_owned",
			launchEnv: { PROVIDER_TOKEN: "private" },
			launchEnvMode: "replace",
		});
		expect(exact).toEqual([
			{
				minProtocol: 7,
				minSchemaRevision: 29,
				capability: CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE,
			},
			{ minProtocol: 7 },
		]);
		expect(getDaemonCommandCompatibilities({ type: "create", launchEnv: { PROVIDER_TOKEN: "legacy" } })).toEqual([
			{ minProtocol: 7 },
		]);
		expect(DAEMON_SUPPORTED_CLIENT_CAPABILITIES).toContain(CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE);
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).not.toContain(CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE);
		expect(DAEMON_SUPERVISOR_SERVER_CAPABILITIES).toContain(CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE);
	});

	it("keeps the advertised schema identity synchronized with wire type shapes", () => {
		const source = readFileSync(resolve(__dirname, "../src/modes/daemon/daemon-protocol.ts"), "utf8");
		const commandSource = source.slice(
			source.indexOf("export type DaemonCommand ="),
			source.indexOf("type DaemonCommandName"),
		);
		const savedSessionSource = source.slice(
			source.indexOf("export interface DaemonSavedSessionInfo"),
			source.indexOf("export type DaemonDeleteSavedSessionResult"),
		);
		const outboundSource = source.slice(
			source.indexOf("export type DaemonOutbound ="),
			source.indexOf("export const DAEMON_OUTBOUND_COMPATIBILITY"),
		);
		const ownedSessionSource = source.slice(
			source.indexOf("export type DaemonOwnedSessionCleanupStatus"),
			source.indexOf("export type DaemonServerCapability"),
		);
		const launchEnvironmentSource = source.slice(
			source.indexOf("export type DaemonLaunchEnvMode"),
			source.indexOf("/**\n * The allowlist of env vars"),
		);
		const errorInfoSource = source.slice(
			source.indexOf("export type DaemonErrorInfo ="),
			source.indexOf("export type DaemonSessionClosedReason"),
		);
		const digest = createHash("sha256")
			.update(
				`${commandSource}\n${savedSessionSource}\n${outboundSource}\n${ownedSessionSource}\n${launchEnvironmentSource}\n${errorInfoSource}`,
			)
			.digest("hex")
			.slice(0, 12);
		expect(DAEMON_SCHEMA_ID).toBe(`protocol-${DAEMON_PROTOCOL_VERSION}-schema-${DAEMON_SCHEMA_REVISION}-${digest}`);
	});

	it("requires compatibility metadata for the heartbeat protocol surface", () => {
		expect(DAEMON_PROTOCOL_VERSION).toBe(7);
		expect(DAEMON_SCHEMA_ID).toContain(`protocol-${DAEMON_PROTOCOL_VERSION}`);
		expect(DAEMON_COMMAND_COMPATIBILITY.heartbeats_list).toEqual({
			minProtocol: 7,
			capability: "heartbeat_catalog",
		});
		expect(DAEMON_COMMAND_COMPATIBILITY.heartbeat_manage).toEqual({
			minProtocol: 7,
			capability: "heartbeat_management",
		});
		expect(DAEMON_COMMAND_COMPATIBILITY.complete_owned_session).toEqual({
			minProtocol: 7,
			capability: "client_owned_sessions",
		});
		expect(DAEMON_COMMAND_COMPATIBILITY.get_owned_session_cleanup).toEqual({
			minProtocol: 7,
			minSchemaRevision: 27,
			capability: "authoritative_owned_session_cleanup_v1",
		});
		expect(DAEMON_DEFAULT_CLIENT_CAPABILITIES).not.toContain("authoritative_owned_session_cleanup_v1");
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).not.toContain("authoritative_owned_session_cleanup_v1");
		expect(DAEMON_SUPERVISOR_SERVER_CAPABILITIES).toContain("authoritative_owned_session_cleanup_v1");
		expect(isDaemonMutatingCommand({ type: "get_owned_session_cleanup" })).toBe(false);
		expect(DAEMON_OUTBOUND_COMPATIBILITY.heartbeats_changed).toEqual({
			minProtocol: 7,
			capability: "heartbeat_catalog",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toEqual(
			expect.arrayContaining(["heartbeat_catalog", "heartbeat_management"]),
		);
	});

	it("capability-gates explicit subagent deletion instead of schema-gating it", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.delete_rlm_subagent).toEqual({
			minProtocol: 7,
			capability: "delete_rlm_subagent",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("delete_rlm_subagent");
	});

	it("capability- and schema-gates ACP MCP server replacement", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.replace_acp_mcp_servers).toEqual({
			minProtocol: 7,
			minSchemaRevision: 22,
			capability: "acp_mcp_servers",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("acp_mcp_servers");
	});

	it("capability-gates the optional model catalog surface", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.get_model_catalog).toEqual({
			minProtocol: 7,
			capability: "model_catalog",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("model_catalog");
	});

	it("capability- and schema-gates queued message mutation at its introducing revision", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.mutate_queued_message).toEqual({
			minProtocol: 7,
			minSchemaRevision: 15,
			capability: "queue_message_mutation",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("queue_message_mutation");
	});

	it("schema-gates the RLM max depth commands at their introducing revision", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.get_rlm_max_depth_status).toEqual({ minProtocol: 7, minSchemaRevision: 11 });
		expect(DAEMON_COMMAND_COMPATIBILITY.set_rlm_max_depth).toEqual({ minProtocol: 7, minSchemaRevision: 11 });
	});

	it("capability- and schema-gates fresh snapshot generation nonces", () => {
		expect(DAEMON_SCHEMA_REVISION).toBe(29);
		expect(DAEMON_SNAPSHOT_GENERATION_NONCE_MIN_SCHEMA_REVISION).toBe(28);
		expect(
			getDaemonCommandCompatibilities({
				type: "attach",
				activeSessionId: "active-1",
				snapshotGenerationNonce: "nonce-1",
			}),
		).toEqual([
			{ minProtocol: 7, minSchemaRevision: 28, capability: "snapshot_generation_nonce_v1" },
			{ minProtocol: 7 },
		]);
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("snapshot_generation_nonce_v1");
	});

	it("schema-gates session commands that carry the telemetry policy", () => {
		expect(getDaemonCommandCompatibilities({ type: "create", config: { cwd: "/tmp" } })).toEqual([
			{ minProtocol: 7 },
		]);
		expect(
			getDaemonCommandCompatibilities({ type: "create", config: { cwd: "/tmp", telemetryDisabled: true } }),
		).toEqual([{ minProtocol: 7, minSchemaRevision: 14 }, { minProtocol: 7 }]);
		expect(getDaemonCommandCompatibilities({ type: "attach", activeSessionId: "active-1" })).toEqual([
			{ minProtocol: 7 },
		]);
		expect(
			getDaemonCommandCompatibilities({ type: "attach", activeSessionId: "active-1", telemetryDisabled: true }),
		).toEqual([{ minProtocol: 7, minSchemaRevision: 14 }, { minProtocol: 7 }]);
		expect(
			getDaemonCommandCompatibilities({
				type: "reattach",
				activeSessionId: "active-1",
				targetActiveSessionId: "active-2",
				telemetryDisabled: true,
			}),
		).toEqual([{ minProtocol: 7, minSchemaRevision: 14 }, { minProtocol: 7 }]);
	});

	it("capability-gates authoritative rosters and transient owned-session recovery context", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.get_rlm_children).toEqual({
			minProtocol: 7,
			minSchemaRevision: 17,
			capability: "authoritative_child_roster",
		});
		expect(
			getDaemonCommandCompatibilities({
				type: "attach",
				activeSessionId: "active-1",
				recoveryConfig: { cwd: "/tmp/fresh-owner" },
			}),
		).toEqual([
			{ minProtocol: 7, minSchemaRevision: 17, capability: "owned_session_recovery_context" },
			{ minProtocol: 7 },
		]);
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toEqual(
			expect.arrayContaining([
				"authoritative_child_roster",
				"owned_session_recovery_context",
				"rlm_quiescence_barrier",
			]),
		);
	});

	it("gates the opt-in RLM quiescence wire field", () => {
		expect(
			getDaemonCommandCompatibilities({
				type: "wait_for_headless_completion",
				activeSessionId: "active-1",
				waitForRlmQuiescence: true,
			}),
		).toEqual([{ minProtocol: 7, minSchemaRevision: 18, capability: "rlm_quiescence_barrier" }, { minProtocol: 7 }]);
		expect(
			getDaemonCommandCompatibilities({
				type: "wait_for_headless_completion",
				activeSessionId: "active-1",
			}),
		).toEqual([{ minProtocol: 7 }]);
	});

	it("capability- and schema-gates session input pause leases", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.acquire_session_input_pause).toEqual({
			minProtocol: 7,
			minSchemaRevision: 19,
			capability: "session_input_pause",
		});
		expect(DAEMON_COMMAND_COMPATIBILITY.release_session_input_pause).toEqual(
			DAEMON_COMMAND_COMPATIBILITY.acquire_session_input_pause,
		);
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("session_input_pause");
	});

	it("version- and capability-gates prompt admission cancellation", () => {
		expect(DAEMON_COMMAND_COMPATIBILITY.cancel_prompt_admission).toEqual({
			minProtocol: 7,
			minSchemaRevision: 8,
			capability: "prompt_admission_cancellation",
		});
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("prompt_admission_cancellation");
	});

	it("capability-gates cancellation after prompt ownership", () => {
		const legacy = { type: "cancel_prompt_admission", activeSessionId: "active-1", admissionId: "a-1" } as const;
		expect(getDaemonCommandCompatibilities(legacy)).toEqual([DAEMON_COMMAND_COMPATIBILITY.cancel_prompt_admission]);
		expect(getDaemonCommandCompatibilities({ ...legacy, cancelOwned: true })).toEqual([
			{ minProtocol: 7, minSchemaRevision: 20, capability: "owned_prompt_cancellation" },
			DAEMON_COMMAND_COMPATIBILITY.cancel_prompt_admission,
		]);
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("owned_prompt_cancellation");
	});

	it("capability-gates correlated submit, cancel, query, and lifecycle events", () => {
		const expected = {
			minProtocol: 7,
			minSchemaRevision: 24,
			capability: "correlated_prompt_lifecycle_v1",
		};
		expect(DAEMON_COMMAND_COMPATIBILITY.submit_correlated_prompt).toEqual(expected);
		expect(DAEMON_COMMAND_COMPATIBILITY.cancel_correlated_prompt).toEqual(expected);
		expect(DAEMON_COMMAND_COMPATIBILITY.get_prompt_lifecycles).toEqual(expected);
		expect(DAEMON_OUTBOUND_COMPATIBILITY.prompt_lifecycle).toEqual(expected);
		expect(DAEMON_DEFAULT_CLIENT_CAPABILITIES).not.toContain("correlated_prompt_lifecycle_v1");
		expect(DAEMON_SUPPORTED_CLIENT_CAPABILITIES).toContain("correlated_prompt_lifecycle_v1");
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("correlated_prompt_lifecycle_v1");
		expect(isDaemonMutatingCommand({ type: "submit_correlated_prompt" })).toBe(true);
		expect(isDaemonMutatingCommand({ type: "cancel_correlated_prompt" })).toBe(true);
		expect(isDaemonMutatingCommand({ type: "get_prompt_lifecycles" })).toBe(false);
	});

	it("negotiates immutable worker snapshots without changing legacy client defaults", () => {
		expect(DAEMON_DEFAULT_CLIENT_CAPABILITIES).not.toContain("immutable_snapshot_transfer_v1");
		expect(DAEMON_SUPPORTED_CLIENT_CAPABILITIES).toContain("immutable_snapshot_transfer_v1");
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toContain("immutable_snapshot_transfer_v1");
	});

	it("removes correlated provenance from legacy daemon events", () => {
		const event: DaemonOutbound = {
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
				promptCorrelationId: "correlation-1",
			},
			attribution: { scope: "prompt", correlationId: "correlation-1" },
		};
		expect(daemonOutboundForCorrelatedPromptCapability(event, true)).toBe(event);
		expect(daemonOutboundForCorrelatedPromptCapability(event, false)).toEqual({
			type: "session_event",
			activeSessionId: "active-1",
			event: {
				type: "session_action_update",
				actions: { queuedCount: 0, steering: [], followUps: [] },
			},
		});
		expect(
			daemonOutboundForCorrelatedPromptCapability(
				{
					type: "prompt_lifecycle",
					activeSessionId: "active-1",
					lifecycle: {
						correlationId: "correlation-1",
						phase: "owned",
						kind: "model_prompt",
						revision: 1,
						deliveryCrossed: false,
					},
				},
				false,
			),
		).toBeUndefined();
	});

	it("gates honest worker-state reporting at its introducing schema revision", () => {
		// Revision 16 adds the "stopping" workerState and stops reporting
		// disconnected workers as "ready". The field is optional and old clients
		// ignore unknown values, so no capability gate is needed; the revision
		// lets version probes distinguish daemons with the old semantics.
		expect(DAEMON_SCHEMA_REVISION).toBeGreaterThanOrEqual(16);
	});

	it("keeps refine failure events backward-compatible on the existing session event channel", () => {
		const event: DaemonOutbound = {
			type: "session_event",
			activeSessionId: "active-1",
			event: { type: "refine_failed", error: "disk full" },
		};

		// Refine events remain on the original session-event channel across later schema revisions.
		expect(DAEMON_SCHEMA_REVISION).toBeGreaterThanOrEqual(6);
		expect(DAEMON_OUTBOUND_COMPATIBILITY.session_event).toEqual({ minProtocol: 7 });
		expect(event).toMatchObject({ event: { type: "refine_failed", error: "disk full" } });
	});

	it("accepts legacy side-question and bash shapes in new daemons and clients", () => {
		const oldClientSideQuestion: DaemonCommand = {
			type: "start_side_question",
			activeSessionId: "active-1",
			sideQuestionId: "side-1",
			question: "What changed?",
		};
		const oldClientBash: DaemonCommand = {
			type: "execute_bash",
			activeSessionId: "active-1",
			command: "ls",
		};
		const oldDaemonBashStart: DaemonOutbound = {
			type: "session_event",
			activeSessionId: "active-1",
			event: { type: "bash_start", command: "ls", excludeFromContext: false },
		};
		const oldDaemonBashEnd: DaemonOutbound = {
			type: "session_event",
			activeSessionId: "active-1",
			event: { type: "bash_end", exitCode: 0, cancelled: false, truncated: false },
		};

		expect(DAEMON_COMMAND_COMPATIBILITY.start_side_question).toEqual({ minProtocol: 7 });
		expect(DAEMON_COMMAND_COMPATIBILITY.execute_bash).toEqual({ minProtocol: 7 });
		expect(DAEMON_OUTBOUND_COMPATIBILITY.session_event).toEqual({ minProtocol: 7 });
		expect(oldClientSideQuestion).not.toHaveProperty("previousTurns");
		expect(oldClientBash).not.toHaveProperty("transient");
		expect(oldClientBash).not.toHaveProperty("runId");
		expect(oldDaemonBashStart.event).not.toHaveProperty("transient");
		expect(oldDaemonBashStart.event).not.toHaveProperty("runId");
		expect(oldDaemonBashEnd.event).not.toHaveProperty("transient");
		expect(oldDaemonBashEnd.event).not.toHaveProperty("runId");
		expect(DAEMON_DEFAULT_SERVER_CAPABILITIES).toEqual(
			expect.arrayContaining(["side_question_transcript", "transient_bash"]),
		);
	});

	it("creates versioned command and event envelopes", () => {
		const command = { id: "cmd-1", type: "attach", activeSessionId: "active-1" } as const;
		const commandEnvelope = createDaemonCommandEnvelope(command, "cmd-1", "client-1");
		const eventMeta = createDaemonEventMeta("active-1", 3, "2026-01-01T00:00:00.000Z");
		const event: DaemonOutbound = {
			type: "session_event",
			activeSessionId: "active-1",
			event: { type: "agent_end", messages: [] },
			meta: eventMeta,
		};

		expect(commandEnvelope).toEqual({
			type: "command",
			id: "cmd-1",
			protocol: DAEMON_PROTOCOL_INFO,
			clientId: "client-1",
			command,
		});
		expect(createDaemonEventEnvelope(event, eventMeta)).toEqual({
			type: "event",
			id: "active-1:3",
			protocol: DAEMON_PROTOCOL_INFO,
			activeSessionId: "active-1",
			sequence: 3,
			cursor: { generation: "active-1", sequence: 3 },
			emittedAt: "2026-01-01T00:00:00.000Z",
			event,
		});
		expect(eventMeta.cursor).toEqual({ generation: "active-1", sequence: 3 });
	});

	it("rejects command envelopes from pre-session-action protocols", () => {
		const command = { id: "cmd-1", type: "attach", activeSessionId: "active-1" } as const;

		expect(isDaemonCommandEnvelope(createDaemonCommandEnvelope(command, "cmd-1", "client-1", 7))).toBe(true);
		expect(isDaemonCommandEnvelope(createDaemonCommandEnvelope(command, "cmd-1", "client-1", 6))).toBe(false);
	});

	it("keeps attachment routing out of the durable mutation journal", () => {
		expect(isDaemonMutatingCommand({ type: "attach" })).toBe(false);
		expect(isDaemonMutatingCommand({ type: "reattach" })).toBe(false);
		expect(isDaemonMutatingCommand({ type: "wait_for_headless_completion" })).toBe(true);
		expect(isDaemonMutatingCommand({ type: "switch_session" })).toBe(true);
	});

	it("reports replay availability from resume cursors", () => {
		expect(createDaemonReplayInfo(undefined, 5, "generation-1")).toEqual({
			status: "complete",
			toSequence: 5,
			toCursor: { generation: "generation-1", sequence: 5 },
		});
		expect(
			createDaemonReplayInfo(
				{ activeSessionId: "active-1", generation: "generation-1", sequence: 5 },
				5,
				"generation-1",
			),
		).toEqual({
			status: "complete",
			fromSequence: 5,
			toSequence: 5,
			fromCursor: { generation: "generation-1", sequence: 5 },
			toCursor: { generation: "generation-1", sequence: 5 },
		});
		expect(createDaemonReplayInfo({ generation: "generation-1", sequence: 10 }, 5, "generation-1")).toEqual({
			status: "unavailable",
			fromSequence: 10,
			toSequence: 5,
			fromCursor: { generation: "generation-1", sequence: 10 },
			toCursor: { generation: "generation-1", sequence: 5 },
			reason: "resume_cursor_ahead_of_session",
		});
		expect(createDaemonReplayInfo({ generation: "generation-1", sequence: 2 }, 5, "generation-1")).toEqual({
			status: "unavailable",
			fromSequence: 2,
			toSequence: 5,
			fromCursor: { generation: "generation-1", sequence: 2 },
			toCursor: { generation: "generation-1", sequence: 5 },
			reason: "event_replay_not_available",
		});
		expect(createDaemonReplayInfo({ generation: "old", sequence: 5 }, 0, "new")).toMatchObject({
			status: "unavailable",
			reason: "event_generation_changed",
			fromCursor: { generation: "old", sequence: 5 },
			toCursor: { generation: "new", sequence: 0 },
		});
	});

	it("salvages command ids from rejected lines regardless of shape validity", () => {
		const oldEnvelope = JSON.stringify(
			createDaemonCommandEnvelope({ type: "list" } as DaemonCommand, "list-1", "old-client", 6),
		);
		expect(salvageDaemonCommandId(oldEnvelope)).toBe("list-1");
		expect(salvageDaemonCommandId(JSON.stringify({ type: "list", id: "bare-1" }))).toBe("bare-1");
		expect(salvageDaemonCommandId(JSON.stringify({ type: null, id: "typeless-1" }))).toBe("typeless-1");
		expect(salvageDaemonCommandId(JSON.stringify({ id: "no-type" }))).toBe("no-type");
		expect(salvageDaemonCommandId(JSON.stringify({ type: "command", id: 7 }))).toBeUndefined();
		expect(salvageDaemonCommandId(JSON.stringify("command"))).toBeUndefined();
		expect(salvageDaemonCommandId("{ not json")).toBeUndefined();
	});
});
