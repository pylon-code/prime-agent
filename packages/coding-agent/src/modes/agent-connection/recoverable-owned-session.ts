import type { AgentSessionRuntimeConfig } from "../../core/agent-session-config.js";
import type { AgentSessionRuntimeMetadata } from "../../core/agent-session-runtime.js";
import {
	CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE,
	PRIME_AGENT_SDK_FEATURES,
	RECOVERABLE_OWNED_SESSION_ADOPTION_FEATURE,
} from "../../sdk-features.js";
import type { DaemonClient } from "../daemon/daemon-client.js";
import { deserializeDaemonError } from "../daemon/daemon-errors.js";
import {
	cloneCallerOwnedSessionLaunchEnv,
	collectDaemonClientEnv,
	DAEMON_SCHEMA_REVISION,
	type DaemonEventCursor,
	type DaemonRecoverableOwnedSessionAdoptionProof,
	type DaemonRecoverableOwnedSessionConfirmResult,
	type DaemonRecoverableOwnedSessionCreateResult,
	isDaemonRecoveryRequestId,
} from "../daemon/daemon-protocol.js";
import type { SessionSummary } from "../daemon/daemon-session-list.js";
import { DaemonAgentConnection, type DaemonAgentConnectionOptions } from "./daemon-agent-connection.js";

export interface RecoverableOwnedSessionCreateOptions {
	requestId: string;
	correlationId: string;
	mcpOwnerId: string;
	config: AgentSessionRuntimeConfig;
	sessionPath?: string;
	continueRecent?: boolean;
	noSession?: boolean;
	name?: string;
	runtimeMetadata?: AgentSessionRuntimeMetadata;
	launchEnv: Readonly<Record<string, string>>;
	connectionOptions?: Omit<
		DaemonAgentConnectionOptions,
		"ownedSession" | "ownedSessionRecoveryConfig" | "ownedSessionLaunchEnv"
	>;
}

export interface RecoverableOwnedSessionCreation {
	connection: DaemonAgentConnection;
	state: SessionSummary;
	recoveryHandle: string;
	supervisorGeneration: string;
	ownershipGeneration: number;
}

export interface RecoverableOwnedSessionAdoptionOptions {
	requestId: string;
	recoveryHandle: string;
	expectedSupervisorGeneration: string;
	activeSessionId: string;
	sessionId: string;
	correlationId: string;
	cursor: DaemonEventCursor;
	previousMcpOwnerId: string;
	mcpOwnerId: string;
	config: AgentSessionRuntimeConfig;
	launchEnv: Readonly<Record<string, string>>;
	connectionOptions?: Omit<
		DaemonAgentConnectionOptions,
		"ownedSession" | "ownedSessionRecoveryConfig" | "ownedSessionLaunchEnv"
	>;
}

export interface RecoverableOwnedSessionAdoption {
	connection: DaemonAgentConnection;
	recoveryHandle: string;
	proof: DaemonRecoverableOwnedSessionAdoptionProof;
}

export interface RecoverableOwnedSessionAdoptionConfirmation {
	requestId: string;
	recoveryHandle: string;
	proof: DaemonRecoverableOwnedSessionAdoptionProof;
}

function assertRecoverableOwnedSessionFeature(client: DaemonClient): void {
	if (
		process.platform === "win32" ||
		!Object.isFrozen(PRIME_AGENT_SDK_FEATURES) ||
		!PRIME_AGENT_SDK_FEATURES.includes(RECOVERABLE_OWNED_SESSION_ADOPTION_FEATURE) ||
		!PRIME_AGENT_SDK_FEATURES.includes(CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE) ||
		!client.supportsServerCapability("daemon_recoverable_owned_session_adoption_v1") ||
		!client.supportsServerCapability(CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE) ||
		!client.supportsServerCapability("authoritative_owned_session_cleanup_v1") ||
		(client.hello?.schemaRevision ?? 0) < DAEMON_SCHEMA_REVISION
	) {
		throw new Error("Recoverable owned session adoption is unavailable");
	}
}

function assertRequestId(requestId: string): void {
	if (!isDaemonRecoveryRequestId(requestId)) {
		throw new Error("Recoverable owned session requestId must encode at least 128 bits");
	}
}

function assertRecoveryHandle(recoveryHandle: string): void {
	if (!/^[A-Za-z0-9_-]{43}$/.test(recoveryHandle)) {
		throw new Error("Recoverable owned session adoption returned an invalid recovery handle");
	}
}

async function createRecoverableOwnedSessionInternal(
	client: DaemonClient,
	options: RecoverableOwnedSessionCreateOptions,
): Promise<RecoverableOwnedSessionCreation> {
	const launchEnv = cloneCallerOwnedSessionLaunchEnv(options.launchEnv) as Record<string, string>;
	await client.waitForHello();
	assertRecoverableOwnedSessionFeature(client);
	assertRequestId(options.requestId);
	const expectedSupervisorGeneration = client.hello?.supervisorGeneration;
	if (!expectedSupervisorGeneration) {
		throw new Error("Recoverable owned session adoption is unavailable");
	}
	const response = await client.request(
		{
			type: "create_recoverable_owned_session",
			requestId: options.requestId,
			expectedSupervisorGeneration,
			correlationId: options.correlationId,
			mcpOwnerId: options.mcpOwnerId,
			recoveryConfig: options.config,
			...(options.sessionPath !== undefined ? { sessionPath: options.sessionPath } : {}),
			...(options.continueRecent !== undefined ? { continueRecent: options.continueRecent } : {}),
			...(options.noSession !== undefined ? { noSession: options.noSession } : {}),
			...(options.name !== undefined ? { name: options.name } : {}),
			config: options.config,
			...(options.runtimeMetadata !== undefined ? { runtimeMetadata: options.runtimeMetadata } : {}),
			env: collectDaemonClientEnv(),
			launchEnv,
			launchEnvMode: "replace",
		},
		30_000,
		{ recoverAcrossReconnect: true },
	);
	if (!response.success) throw deserializeDaemonError(response);
	const created = response.data as DaemonRecoverableOwnedSessionCreateResult;
	assertRecoveryHandle(created.recoveryHandle);
	if (
		created.supervisorGeneration !== expectedSupervisorGeneration ||
		created.ownershipGeneration !== 0 ||
		!created.state?.activeSessionId ||
		!created.state.sessionId
	) {
		throw new Error("Recoverable owned session creation returned an invalid authority receipt");
	}
	const connection = await DaemonAgentConnection.attachRecoverableOwnedSessionCreation(
		client,
		created.state.activeSessionId,
		created.state.sessionId,
		expectedSupervisorGeneration,
		{
			...options.connectionOptions,
			ownedSession: true,
			ownedSessionRecoveryConfig: options.config,
			ownedSessionLaunchEnv: launchEnv,
		},
	);
	return { connection, ...created };
}

async function adoptRecoverableOwnedSessionInternal(
	client: DaemonClient,
	options: RecoverableOwnedSessionAdoptionOptions,
): Promise<RecoverableOwnedSessionAdoption> {
	const launchEnv = cloneCallerOwnedSessionLaunchEnv(options.launchEnv);
	await client.waitForHello();
	assertRecoverableOwnedSessionFeature(client);
	assertRequestId(options.requestId);
	assertRecoveryHandle(options.recoveryHandle);
	const adopted = await DaemonAgentConnection.adoptRecoverableOwnedSession(client, {
		requestId: options.requestId,
		recoveryHandle: options.recoveryHandle,
		expectedSupervisorGeneration: options.expectedSupervisorGeneration,
		activeSessionId: options.activeSessionId,
		sessionId: options.sessionId,
		correlationId: options.correlationId,
		cursor: options.cursor,
		previousMcpOwnerId: options.previousMcpOwnerId,
		mcpOwnerId: options.mcpOwnerId,
		recoveryConfig: options.config,
		launchEnv,
		connectionOptions: options.connectionOptions,
	});
	assertRecoveryHandle(adopted.recoveryHandle);
	return adopted;
}

/** Confirm only after the caller has durably persisted the rotated handle and exact proof. */
async function confirmRecoverableOwnedSessionAdoptionInternal(
	client: DaemonClient,
	confirmation: RecoverableOwnedSessionAdoptionConfirmation,
): Promise<void> {
	await client.waitForHello();
	assertRecoverableOwnedSessionFeature(client);
	assertRequestId(confirmation.requestId);
	assertRecoveryHandle(confirmation.recoveryHandle);
	const response = await client.request(
		{
			type: "confirm_recoverable_owned_session_adoption",
			requestId: confirmation.requestId,
			expectedSupervisorGeneration: confirmation.proof.supervisorGeneration,
			recoveryHandle: confirmation.recoveryHandle,
			proof: confirmation.proof,
		},
		30_000,
		{ recoverAcrossReconnect: true },
	);
	if (!response.success) throw deserializeDaemonError(response);
	if ((response.data as Partial<DaemonRecoverableOwnedSessionConfirmResult> | undefined)?.status !== "confirmed") {
		throw new Error("Recoverable owned session adoption returned an invalid confirmation");
	}
}

function unavailable(): Error {
	return new Error("Recoverable owned session adoption is unavailable");
}

export async function createRecoverableOwnedSession(
	client: DaemonClient,
	options: RecoverableOwnedSessionCreateOptions,
): Promise<RecoverableOwnedSessionCreation> {
	try {
		return await createRecoverableOwnedSessionInternal(client, options);
	} catch {
		throw unavailable();
	}
}

export async function adoptRecoverableOwnedSession(
	client: DaemonClient,
	options: RecoverableOwnedSessionAdoptionOptions,
): Promise<RecoverableOwnedSessionAdoption> {
	try {
		return await adoptRecoverableOwnedSessionInternal(client, options);
	} catch {
		throw unavailable();
	}
}

export async function confirmRecoverableOwnedSessionAdoption(
	client: DaemonClient,
	confirmation: RecoverableOwnedSessionAdoptionConfirmation,
): Promise<void> {
	try {
		await confirmRecoverableOwnedSessionAdoptionInternal(client, confirmation);
	} catch {
		throw unavailable();
	}
}
