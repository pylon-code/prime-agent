import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ENV_AGENT_DIR, getCronJobsPath } from "../src/config.js";
import { AgentCronJobStore } from "../src/core/cron-jobs.js";
import { readActiveOrphanProcesses } from "../src/core/orphan-process-journal.js";
import {
	acquireSessionLease,
	getProcessStartId,
	SESSION_LEASE_OWNER_ID_ENV,
	SESSION_LEASES_ENABLED_ENV,
} from "../src/core/session-lease.js";
import { readSessionInfo, SessionManager } from "../src/core/session-manager.js";
import { DaemonAgentConnection } from "../src/modes/agent-connection/daemon-agent-connection.js";
import {
	adoptRecoverableOwnedSession,
	confirmRecoverableOwnedSessionAdoption,
	createRecoverableOwnedSession,
} from "../src/modes/agent-connection/recoverable-owned-session.js";
import { DaemonClient, getDaemonSocketCloseReason } from "../src/modes/daemon/daemon-client.js";
import {
	collectDaemonClientEnv,
	collectDaemonLaunchEnv,
	createDaemonCommandEnvelope,
} from "../src/modes/daemon/daemon-protocol.js";
import type { SessionSummary } from "../src/modes/daemon/daemon-session-list.js";
import type { DaemonWorkerDescriptor } from "../src/modes/daemon/daemon-worker-protocol.js";

const cliPath = resolve(__dirname, "../src/cli.ts");
const tsxPath = resolve(__dirname, "../../../node_modules/tsx/dist/cli.mjs");
const blockingProcessPath = resolve(__dirname, "fixtures/blocking-process.mjs");
const fauxExtensionPath = resolve(__dirname, "fixtures/eng-4600-faux-extension.ts");
const tempDirs: string[] = [];
const children = new Set<ChildProcess>();
const workerPids = new Set<number>();
const identityTrackedProcesses = new Map<number, string>();
const daemonSockets = new Set<string>();
const childDiagnostics = new WeakMap<ChildProcess, { stdout: string; stderr: string }>();
const PROCESS_STRESS_WORKERS = Number.parseInt(process.env.PRIME_AGENT_STRESS_WORKERS ?? "10", 10);

afterEach(async () => {
	for (const socketPath of daemonSockets) {
		const client = new DaemonClient(socketPath);
		try {
			await client.connect(250);
			await client.request({ type: "shutdown" }, 2000);
		} catch {
			// Already gone.
		} finally {
			client.close();
		}
	}
	daemonSockets.clear();
	for (const child of children) {
		if (child.exitCode === null && child.signalCode === null) {
			child.kill("SIGTERM");
		}
	}
	children.clear();
	for (const [pid, processStartId] of identityTrackedProcesses) {
		const identity = { pid, processStartId };
		try {
			signalIdentityVerifiedProcess(identity, "SIGCONT");
			signalIdentityVerifiedProcess(identity, "SIGTERM");
		} catch {
			continue;
		}
		try {
			await waitForProcessGone(pid, 1000);
		} catch {
			try {
				signalIdentityVerifiedProcess(identity, "SIGKILL");
				await waitForProcessGone(pid, 1000);
			} catch {
				// Gone or no longer the process identity tracked by this test.
			}
		}
	}
	identityTrackedProcesses.clear();
	for (const pid of workerPids) {
		try {
			process.kill(pid, "SIGCONT");
		} catch {
			// Already gone.
		}
		try {
			process.kill(-pid, "SIGTERM");
		} catch {
			try {
				process.kill(pid, "SIGTERM");
			} catch {
				// Already gone.
			}
		}
	}
	workerPids.clear();
	for (const directory of tempDirs.splice(0)) {
		rmSync(directory, { recursive: true, force: true });
	}
});

function signalIdentityVerifiedProcess(
	identity: { pid?: number; processStartId?: string },
	signal: NodeJS.Signals,
): void {
	const { pid, processStartId } = identity;
	if (!pid || !processStartId) throw new Error("Process identity is incomplete");
	const observedProcessStartId = getProcessStartId(pid);
	if (!observedProcessStartId || observedProcessStartId !== processStartId) {
		throw new Error("Process identity changed before the requested signal");
	}
	process.kill(pid, signal);
}

function tempDir(): string {
	const directory = mkdtempSync(join(tmpdir(), "prime-daemon-supervisor-test-"));
	tempDirs.push(directory);
	return directory;
}

function spawnSupervisor(
	agentDir: string,
	socketPath: string,
	cwd: string,
	extraArgs: readonly string[] = [],
	extraEnv: NodeJS.ProcessEnv = {},
): ChildProcess {
	daemonSockets.add(socketPath);
	const inheritedEnv = { ...process.env };
	for (const name of Object.keys(inheritedEnv)) {
		if (name.startsWith("PRIME_AGENT_INTERNAL_DAEMON_") || name.startsWith("RLM_")) delete inheritedEnv[name];
	}
	const child = spawn(
		process.execPath,
		[tsxPath, cliPath, "--mode", "daemon", "--daemon-socket", socketPath, "--offline", ...extraArgs],
		{
			cwd,
			env: {
				...inheritedEnv,
				...extraEnv,
				[ENV_AGENT_DIR]: agentDir,
				PI_OFFLINE: "1",
				TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json"),
			},
			stdio: ["ignore", "pipe", "pipe"],
		},
	);
	children.add(child);
	const diagnostics = { stdout: "", stderr: "" };
	childDiagnostics.set(child, diagnostics);
	child.stdout?.on("data", (chunk: Buffer) => {
		diagnostics.stdout += chunk.toString("utf8");
	});
	child.stderr?.on("data", (chunk: Buffer) => {
		diagnostics.stderr += chunk.toString("utf8");
	});
	return child;
}

function readDaemonLogs(agentDir: string): string {
	const logsDir = join(agentDir, "logs");
	try {
		return readdirSync(logsDir)
			.map((name) => `${name}:\n${readFileSync(join(logsDir, name), "utf8")}`)
			.join("\n");
	} catch {
		return "no daemon logs";
	}
}

function readWorkerDescriptor(agentDir: string): DaemonWorkerDescriptor {
	const workersRoot = join(agentDir, "daemon-workers");
	for (const directory of readdirSync(workersRoot)) {
		const descriptorDirectory = join(workersRoot, directory);
		for (const name of readdirSync(descriptorDirectory)) {
			if (name.endsWith(".json")) {
				return JSON.parse(readFileSync(join(descriptorDirectory, name), "utf8")) as DaemonWorkerDescriptor;
			}
		}
	}
	throw new Error("Worker descriptor was not persisted");
}

function readWorkerDescriptors(agentDir: string): DaemonWorkerDescriptor[] {
	const workersRoot = join(agentDir, "daemon-workers");
	try {
		return readdirSync(workersRoot).flatMap((directory) => {
			const descriptorDirectory = join(workersRoot, directory);
			return readdirSync(descriptorDirectory)
				.filter((name) => name.endsWith(".json"))
				.map((name) => JSON.parse(readFileSync(join(descriptorDirectory, name), "utf8")) as DaemonWorkerDescriptor);
		});
	} catch {
		return [];
	}
}

function countWorkerDescriptors(agentDir: string): number {
	const workersRoot = join(agentDir, "daemon-workers");
	try {
		return readdirSync(workersRoot).reduce((count, directory) => {
			return count + readdirSync(join(workersRoot, directory)).filter((name) => name.endsWith(".json")).length;
		}, 0);
	} catch {
		return 0;
	}
}

async function waitForWorkerStopTombstone(agentDir: string): Promise<DaemonWorkerDescriptor> {
	const deadline = Date.now() + 5000;
	while (Date.now() < deadline) {
		try {
			const descriptor = readWorkerDescriptor(agentDir);
			if (descriptor.stopRequestedAt) {
				return descriptor;
			}
		} catch {
			// The descriptor directory may still be appearing.
		}
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
	}
	throw new Error("Worker stop tombstone was not persisted");
}

function readSupervisorConfig(agentDir: string): { defaultSessionConfig?: { sessionDir?: string; noTools?: boolean } } {
	const workersRoot = join(agentDir, "daemon-workers");
	for (const directory of readdirSync(workersRoot)) {
		const path = join(workersRoot, directory, "supervisor-config");
		try {
			return JSON.parse(readFileSync(path, "utf8")) as {
				defaultSessionConfig?: { sessionDir?: string; noTools?: boolean };
			};
		} catch {
			// Continue looking for the descriptor directory for this daemon socket.
		}
	}
	throw new Error("Supervisor config was not persisted");
}

async function connectEventually(socketPath: string, child?: ChildProcess): Promise<DaemonClient> {
	const deadline = Date.now() + 15_000;
	let lastError: unknown;
	while (Date.now() < deadline) {
		if (child && (child.exitCode !== null || child.signalCode !== null)) {
			const diagnostics = childDiagnostics.get(child);
			throw new Error(
				`Supervisor exited before becoming ready (code ${child.exitCode}, signal ${child.signalCode})\n` +
					`stdout:\n${diagnostics?.stdout ?? ""}\nstderr:\n${diagnostics?.stderr ?? ""}`,
			);
		}
		const client = new DaemonClient(socketPath);
		try {
			await client.connect(250);
			await client.waitForHello(1000);
			return client;
		} catch (error) {
			lastError = error;
			client.close();
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
		}
	}
	const diagnostics = child ? childDiagnostics.get(child) : undefined;
	throw new Error(
		`Timed out waiting for supervisor: ${String(lastError)}\nstdout:\n${diagnostics?.stdout ?? ""}\nstderr:\n${diagnostics?.stderr ?? ""}`,
	);
}

async function disconnectDaemonClientWithServerCloseBarrier(client: DaemonClient): Promise<void> {
	const socket = (client as unknown as { socket?: Socket }).socket;
	if (!socket || socket.destroyed) return;
	const closed = new Promise<void>((resolveClose) => socket.once("close", () => resolveClose()));
	socket.end();
	await closed;
}

async function createSnapshotRetryProxy(
	proxyPath: string,
	targetPath: string,
	fault: "corrupt-first-chunk" | "corrupt-every-first-chunk" | "drop-first-end" = "corrupt-first-chunk",
): Promise<{
	server: Server;
	attachRequests: () => number;
	transferIds: () => string[];
}> {
	let attachCount = 0;
	const observedTransferIds: string[] = [];
	const server = createServer((downstream: Socket) => {
		const upstream = createConnection(targetPath);
		let requestBuffer = Buffer.alloc(0);
		let responseBuffer = Buffer.alloc(0);
		let firstTransferId: string | undefined;
		let firstChunk: Buffer | undefined;
		let firstEnd: Buffer | undefined;
		const corruptedTransferIds = new Set<string>();
		downstream.on("data", (chunk: Buffer) => {
			requestBuffer = Buffer.concat([requestBuffer, chunk]);
			while (true) {
				const newline = requestBuffer.indexOf(0x0a);
				if (newline < 0) break;
				const line = requestBuffer.subarray(0, newline);
				requestBuffer = requestBuffer.subarray(newline + 1);
				try {
					const parsed = JSON.parse(line.toString("utf8")) as {
						type?: unknown;
						command?: { type?: unknown };
					};
					if (parsed.type === "attach" || parsed.command?.type === "attach") attachCount++;
				} catch {
					// The real daemon owns command validation.
				}
				upstream.write(Buffer.concat([line, Buffer.from("\n")]));
			}
		});
		upstream.on("data", (chunk: Buffer) => {
			responseBuffer = Buffer.concat([responseBuffer, chunk]);
			while (true) {
				const newline = responseBuffer.indexOf(0x0a);
				if (newline < 0) break;
				const original = Buffer.from(responseBuffer.subarray(0, newline + 1));
				responseBuffer = responseBuffer.subarray(newline + 1);
				let parsed: { type?: unknown; snapshotId?: unknown } | undefined;
				try {
					parsed = JSON.parse(original.toString("utf8")) as { type?: unknown; snapshotId?: unknown };
				} catch {
					downstream.write(original);
					continue;
				}
				if (parsed.type === "session_snapshot_begin" && typeof parsed.snapshotId === "string") {
					if (!observedTransferIds.includes(parsed.snapshotId)) observedTransferIds.push(parsed.snapshotId);
					firstTransferId ??= parsed.snapshotId;
				}
				if (parsed.type === "session_snapshot_chunk" && typeof parsed.snapshotId === "string") {
					if (parsed.snapshotId === firstTransferId && !firstChunk) firstChunk = original;
					const corruptsThisTransfer =
						(fault === "corrupt-first-chunk" && parsed.snapshotId === firstTransferId) ||
						fault === "corrupt-every-first-chunk";
					if (corruptsThisTransfer && !corruptedTransferIds.has(parsed.snapshotId)) {
						corruptedTransferIds.add(parsed.snapshotId);
						parsed.snapshotId = `${parsed.snapshotId}-corrupted`;
						downstream.write(`${JSON.stringify(parsed)}\n`);
						continue;
					}
				}
				if (
					parsed.type === "session_snapshot_end" &&
					typeof parsed.snapshotId === "string" &&
					parsed.snapshotId === firstTransferId
				) {
					firstEnd = original;
					if (fault === "drop-first-end") continue;
				}
				downstream.write(original);
				if (
					parsed.type === "session_snapshot_end" &&
					typeof parsed.snapshotId === "string" &&
					parsed.snapshotId !== firstTransferId &&
					firstChunk &&
					firstEnd
				) {
					// Old-generation frames arrive after the fresh attempt's terminal end.
					downstream.write(firstChunk);
					downstream.write(firstEnd);
					firstChunk = undefined;
					firstEnd = undefined;
				}
			}
		});
		upstream.on("error", (error) => downstream.destroy(error));
		downstream.on("error", () => upstream.destroy());
		upstream.on("close", () => downstream.destroy());
		downstream.on("close", () => upstream.destroy());
	});
	await new Promise<void>((resolveListen, reject) => {
		server.once("error", reject);
		server.listen(proxyPath, () => resolveListen());
	});
	return {
		server,
		attachRequests: () => attachCount,
		transferIds: () => [...observedTransferIds],
	};
}

async function createCommandResponseLossProxy(
	proxyPath: string,
	targetPath: string,
	commandType: string,
): Promise<{ server: Server; responseObserved: Promise<void>; release(): void }> {
	let resolveResponseObserved!: () => void;
	const responseObserved = new Promise<void>((resolveObserved) => {
		resolveResponseObserved = resolveObserved;
	});
	let targetCommandId: string | undefined;
	let downstreamSocket: Socket | undefined;
	let upstreamSocket: Socket | undefined;
	const server = createServer((downstream) => {
		downstreamSocket = downstream;
		const upstream = createConnection(targetPath);
		upstreamSocket = upstream;
		let requestBuffer = Buffer.alloc(0);
		let responseBuffer = Buffer.alloc(0);
		downstream.on("data", (chunk: Buffer) => {
			requestBuffer = Buffer.concat([requestBuffer, chunk]);
			while (true) {
				const newline = requestBuffer.indexOf(0x0a);
				if (newline < 0) break;
				const original = Buffer.from(requestBuffer.subarray(0, newline + 1));
				requestBuffer = requestBuffer.subarray(newline + 1);
				try {
					const parsed = JSON.parse(original.toString("utf8")) as {
						id?: unknown;
						command?: { type?: unknown };
					};
					if (parsed.command?.type === commandType && typeof parsed.id === "string") targetCommandId = parsed.id;
				} catch {}
				upstream.write(original);
			}
		});
		upstream.on("data", (chunk: Buffer) => {
			responseBuffer = Buffer.concat([responseBuffer, chunk]);
			while (true) {
				const newline = responseBuffer.indexOf(0x0a);
				if (newline < 0) break;
				const original = Buffer.from(responseBuffer.subarray(0, newline + 1));
				responseBuffer = responseBuffer.subarray(newline + 1);
				let dropsResponse = false;
				try {
					const parsed = JSON.parse(original.toString("utf8")) as {
						id?: unknown;
						type?: unknown;
						command?: unknown;
					};
					dropsResponse =
						parsed.type === "response" && parsed.command === commandType && parsed.id === targetCommandId;
				} catch {}
				if (dropsResponse) {
					resolveResponseObserved();
					continue;
				}
				downstream.write(original);
			}
		});
		upstream.on("error", (error) => downstream.destroy(error));
		downstream.on("error", () => upstream.destroy());
		upstream.on("close", () => downstream.destroy());
		downstream.on("close", () => upstream.destroy());
	});
	await new Promise<void>((resolveListen, reject) => {
		server.once("error", reject);
		server.listen(proxyPath, () => resolveListen());
	});
	return {
		server,
		responseObserved,
		release() {
			downstreamSocket?.destroy();
			upstreamSocket?.destroy();
		},
	};
}

async function createSnapshotDrainAbortProxy(
	proxyPath: string,
	targetPath: string,
): Promise<{ server: Server; aborted: Promise<number> }> {
	let resolveAborted!: (bufferedBytes: number) => void;
	const aborted = new Promise<number>((resolve) => {
		resolveAborted = resolve;
	});
	const server = createServer((downstream: Socket) => {
		const upstream = createConnection(targetPath);
		let responseBuffer = Buffer.alloc(0);
		let paused = false;
		downstream.on("data", (chunk: Buffer) => {
			upstream.write(chunk);
		});
		upstream.on("data", (chunk: Buffer) => {
			if (paused) return;
			responseBuffer = Buffer.concat([responseBuffer, chunk]);
			while (true) {
				const newline = responseBuffer.indexOf(0x0a);
				if (newline < 0) break;
				const original = Buffer.from(responseBuffer.subarray(0, newline + 1));
				responseBuffer = responseBuffer.subarray(newline + 1);
				downstream.write(original);
				let type: unknown;
				try {
					type = (JSON.parse(original.toString("utf8")) as { type?: unknown }).type;
				} catch {
					continue;
				}
				if (type !== "session_snapshot_begin") continue;
				paused = true;
				upstream.pause();
				let settled = false;
				const abort = () => {
					if (settled) return;
					settled = true;
					clearTimeout(deadline);
					const bufferedBytes = responseBuffer.length + upstream.readableLength;
					resolveAborted(bufferedBytes);
					upstream.destroy();
					downstream.destroy();
				};
				const deadline = setTimeout(abort, 1_000);
				deadline.unref();
				upstream.once("readable", abort);
				return;
			}
		});
		upstream.on("error", () => downstream.destroy());
		downstream.on("error", () => upstream.destroy());
		upstream.on("close", () => downstream.destroy());
		downstream.on("close", () => upstream.destroy());
	});
	await new Promise<void>((resolveListen, reject) => {
		server.once("error", reject);
		server.listen(proxyPath, () => resolveListen());
	});
	return { server, aborted };
}

async function waitForSocketGone(socketPath: string): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		const client = new DaemonClient(socketPath);
		try {
			await client.connect(100);
		} catch {
			client.close();
			return;
		}
		client.close();
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
	}
	throw new Error("Daemon supervisor socket remained available after shutdown");
}

function requireSummary(responseData: unknown): SessionSummary {
	if (!responseData || typeof responseData !== "object") {
		throw new Error("Missing daemon session summary");
	}
	return responseData as SessionSummary;
}

function createSnapshotSessionFile(agentDir: string, projectDir: string, label: string): string {
	const sessionManager = SessionManager.create(projectDir, join(agentDir, "sessions"));
	sessionManager.appendMessage({ role: "user", content: label, timestamp: 1 });
	sessionManager.appendMessage({
		role: "assistant",
		content: [{ type: "text", text: "fixture complete" }],
		api: "openai-responses",
		provider: "faux",
		model: "faux",
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
	});
	const sessionFile = sessionManager.getSessionFile();
	if (!sessionFile) throw new Error("Snapshot fixture did not persist");
	return sessionFile;
}

function requireSessionList(responseData: unknown): SessionSummary[] {
	if (!responseData || typeof responseData !== "object" || !("sessions" in responseData)) {
		throw new Error("Missing daemon session list");
	}
	const sessions = (responseData as { sessions: unknown }).sessions;
	if (!Array.isArray(sessions)) {
		throw new Error("Invalid daemon session list");
	}
	return sessions as SessionSummary[];
}

async function waitForExit(child: ChildProcess): Promise<void> {
	if (child.exitCode !== null || child.signalCode !== null) {
		return;
	}
	await new Promise<void>((resolveExit, reject) => {
		const timeout = setTimeout(() => reject(new Error("Timed out waiting for process exit")), 10_000);
		child.once("exit", () => {
			clearTimeout(timeout);
			resolveExit();
		});
	});
}

async function waitForProcessGone(pid: number, timeoutMs = 10_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			process.kill(pid, 0);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") {
				return;
			}
		}
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
	}
	throw new Error(`Process ${pid} remained alive after daemon shutdown`);
}

async function waitForCondition(predicate: () => boolean, failureMessage: string, timeoutMs = 10_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (predicate()) {
			return;
		}
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
	}
	throw new Error(failureMessage);
}

function quoteShellArgument(argument: string): string {
	return `'${argument.replaceAll("'", `'"'"'`)}'`;
}

async function startBlockingBash(client: DaemonClient, activeSessionId: string, readyPath: string): Promise<void> {
	const command = [process.execPath, blockingProcessPath, readyPath].map(quoteShellArgument).join(" ");
	const response = await client.request({ type: "execute_bash", activeSessionId, command });
	if (!response.success) {
		throw new Error(response.error);
	}
	await waitForCondition(() => existsSync(readyPath), `Blocking bash process did not become ready: ${readyPath}`);
}

describe("daemon supervisor resident workers", () => {
	it("accepts the canonical socket path when launched with duplicate slashes", async () => {
		if (process.platform === "win32") return;
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const socketPath = join(root, "daemon.sock");
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, `${root}//daemon.sock`, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const response = await client.request({ type: "list" });

		expect(response.success).toBe(true);
		expect(requireSessionList(response.success ? response.data : undefined)).toHaveLength(0);
		await client.request({ type: "shutdown", force: true });
		client.close();
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("creates top-level sessions at depth zero when the supervisor inherits a child depth", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-depth-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir, [], { RLM_DEPTH: "1" });
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			config: {
				cwd: projectDir,
				agentDir,
				sessionDir,
				provider: "faux",
				model: "faux",
				noTools: true,
				noExtensions: true,
			},
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.workerPid || !summary.sessionFile) throw new Error("Worker did not expose its session identity");
		workerPids.add(summary.workerPid);

		expect(summary).toMatchObject({ runtimeKind: "top-level", rlmDepth: 0 });
		expect(await readSessionInfo(summary.sessionFile)).toMatchObject({ rlmDepth: 0, parentSessionPath: undefined });

		await client.request({ type: "shutdown" });
		client.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("lists, creates, and attaches passive children through their owning worker", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-passive-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const parentManager = SessionManager.create(projectDir, sessionDir);
		parentManager.appendMessage({ role: "user", content: "parent fixture", timestamp: 1 });
		parentManager.flushNow();
		const parentSessionFile = parentManager.getSessionFile();
		const parentArtifactDir = parentManager.getSessionArtifactDir();
		if (!parentSessionFile || !parentArtifactDir) throw new Error("Missing parent fixture paths");

		const makeChild = (childId: string, sessionName: string, timestamp: number) => {
			const childSessionDir = join(parentArtifactDir, childId);
			const manager = SessionManager.create(projectDir, childSessionDir);
			manager.newSession({ parentSession: parentSessionFile });
			manager.appendSessionInfo(sessionName);
			manager.appendMessage({ role: "user", content: `completed ${childId} fixture`, timestamp });
			manager.flushNow();
			const sessionFile = manager.getSessionFile();
			if (!sessionFile) throw new Error("Missing child fixture path");
			return { childId, sessionName, childSessionDir, manager, sessionFile };
		};
		const child = makeChild("passive-child", "passive-child-worker", 2);
		const createChild = makeChild("passive-create-child", "passive-create-worker", 3);
		writeFileSync(
			join(parentArtifactDir, "rlm-subagents.jsonl"),
			`${[child, createChild]
				.map((fixture) =>
					JSON.stringify({
						type: "rlm_subagent",
						childId: fixture.childId,
						sessionName: fixture.sessionName,
						sessionDir: fixture.childSessionDir,
						sessionFile: fixture.sessionFile,
						parentSessionId: parentManager.getSessionId(),
						parentSessionFile,
						rlmDepth: 1,
						rlmMaxDepth: 4,
						rlmParentNodeId: fixture.childId,
						status: "completed",
						createdAt: 1,
						updatedAt: "2026-01-01T00:00:00.000Z",
					}),
				)
				.join("\n")}
`,
		);

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: parentSessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		expect(created.success).toBe(true);
		const parentSummary = requireSummary(created.success ? created.data : undefined);
		if (!parentSummary.workerPid) throw new Error("Parent worker did not expose its pid");
		workerPids.add(parentSummary.workerPid);

		const beforeAttach = await client.request({ type: "list" });
		expect(beforeAttach.success).toBe(true);
		const passiveSummary = requireSessionList(beforeAttach.success ? beforeAttach.data : undefined).find(
			(summary) => summary.sessionFile === child.sessionFile,
		);
		expect(passiveSummary).toMatchObject({
			sessionId: child.manager.getSessionId(),
			sessionName: "passive-child-worker",
			runtimeKind: "subagent",
			rlmChildId: child.childId,
			workerPid: parentSummary.workerPid,
		});
		expect(passiveSummary?.activeSessionId).toBeUndefined();

		const createdChild = await client.request({
			type: "create",
			sessionPath: createChild.sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!createdChild.success) throw new Error(createdChild.error);
		expect(requireSummary(createdChild.data)).toMatchObject({
			workerPid: parentSummary.workerPid,
			rlmChildId: createChild.childId,
		});
		expect(countWorkerDescriptors(agentDir)).toBe(1);

		const attached = await client.request({
			type: "attach",
			activeSessionId: child.manager.getSessionId(),
			capabilities: ["attach_snapshot", "event_sequence", "slim_attach"],
		});
		if (!attached.success) throw new Error(attached.error);

		const afterAttach = await client.request({ type: "list" });
		const hydratedSummary = requireSessionList(afterAttach.success ? afterAttach.data : undefined).find(
			(summary) => summary.sessionFile === child.sessionFile,
		);
		expect(hydratedSummary).toMatchObject({
			activeSessionId: expect.any(String),
			workerPid: parentSummary.workerPid,
			rlmChildId: child.childId,
		});
		expect(countWorkerDescriptors(agentDir)).toBe(1);

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("retries one corrupted real worker snapshot with fresh transfer identity and ignores stale terminal frames", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const suffix = `${process.pid}-${randomUUID().slice(0, 8)}`;
		const socketPath = join(tmpdir(), `prime-snapshot-retry-${suffix}.sock`);
		const proxyPath = join(tmpdir(), `prime-snapshot-proxy-${suffix}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "real snapshot retry fixture", timestamp: 1 });
		sessionManager.appendMessage({
			role: "assistant",
			content: [{ type: "text", text: "fixture complete" }],
			api: "openai-responses",
			provider: "faux",
			model: "faux",
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
		});
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) throw new Error("Snapshot retry fixture did not persist");

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const creator = await connectEventually(socketPath, supervisor);
		const created = await creator.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.activeSessionId || !summary.workerPid) throw new Error("Snapshot retry worker was incomplete");
		workerPids.add(summary.workerPid);
		creator.close();

		const proxy = await createSnapshotRetryProxy(proxyPath, socketPath);
		const client = new DaemonClient(proxyPath);
		try {
			await client.connect(3_000);
			await client.waitForHello(3_000);
			const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
				supportsExtensionUi: false,
				snapshotTimeoutMs: 3_000,
			});
			const snapshot = await connection.getInitialSnapshot();
			expect(snapshot.messages).toContainEqual(
				expect.objectContaining({ role: "user", content: "real snapshot retry fixture" }),
			);
			expect(proxy.attachRequests()).toBe(2);
			expect(proxy.transferIds()).toHaveLength(2);
			expect(new Set(proxy.transferIds()).size).toBe(2);
			await connection.dispose();
		} finally {
			client.close();
			await new Promise<void>((resolveClose) => proxy.server.close(() => resolveClose()));
		}

		const shutdownClient = await connectEventually(socketPath);
		await shutdownClient.request({ type: "shutdown" });
		shutdownClient.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("retries a real worker snapshot whose first generation never sends an end frame", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const suffix = `${process.pid}-${randomUUID().slice(0, 8)}`;
		const socketPath = join(tmpdir(), `prime-snapshot-missing-end-${suffix}.sock`);
		const proxyPath = join(tmpdir(), `prime-snapshot-missing-end-proxy-${suffix}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const fixtureLabel = "real missing-end snapshot fixture";
		const sessionFile = createSnapshotSessionFile(agentDir, projectDir, fixtureLabel);

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const creator = await connectEventually(socketPath, supervisor);
		const created = await creator.request({
			type: "create",
			sessionPath: sessionFile,
			config: {
				cwd: projectDir,
				agentDir,
				sessionDir: join(agentDir, "sessions"),
				noTools: true,
				noExtensions: true,
			},
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.activeSessionId || !summary.workerPid) throw new Error("Missing-end worker was incomplete");
		workerPids.add(summary.workerPid);
		creator.close();

		const proxy = await createSnapshotRetryProxy(proxyPath, socketPath, "drop-first-end");
		const client = new DaemonClient(proxyPath);
		try {
			await client.connect(3_000);
			await client.waitForHello(3_000);
			const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
				supportsExtensionUi: false,
				snapshotTimeoutMs: 250,
			});
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				messages: expect.arrayContaining([expect.objectContaining({ role: "user", content: fixtureLabel })]),
			});
			expect(proxy.attachRequests()).toBe(2);
			expect(proxy.transferIds()).toHaveLength(2);
			expect(new Set(proxy.transferIds()).size).toBe(2);
			await connection.dispose();
		} finally {
			client.close();
			await new Promise<void>((resolveClose) => proxy.server.close(() => resolveClose()));
		}

		const shutdownClient = await connectEventually(socketPath);
		await shutdownClient.request({ type: "shutdown" });
		shutdownClient.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("terminates after two corrupted real worker snapshot generations without self-requeue", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const suffix = `${process.pid}-${randomUUID().slice(0, 8)}`;
		const socketPath = join(tmpdir(), `prime-snapshot-terminal-${suffix}.sock`);
		const proxyPath = join(tmpdir(), `prime-snapshot-terminal-proxy-${suffix}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionFile = createSnapshotSessionFile(agentDir, projectDir, "real terminal snapshot fixture");

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const creator = await connectEventually(socketPath, supervisor);
		const created = await creator.request({
			type: "create",
			sessionPath: sessionFile,
			config: {
				cwd: projectDir,
				agentDir,
				sessionDir: join(agentDir, "sessions"),
				noTools: true,
				noExtensions: true,
			},
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.activeSessionId || !summary.workerPid) throw new Error("Terminal worker was incomplete");
		workerPids.add(summary.workerPid);
		creator.close();

		const proxy = await createSnapshotRetryProxy(proxyPath, socketPath, "corrupt-every-first-chunk");
		const client = new DaemonClient(proxyPath);
		try {
			await client.connect(3_000);
			await client.waitForHello(3_000);
			await expect(
				DaemonAgentConnection.attach(client, summary.activeSessionId, {
					supportsExtensionUi: false,
					snapshotTimeoutMs: 1_000,
				}),
			).rejects.toThrow();
			expect(proxy.attachRequests()).toBe(2);
			expect(proxy.transferIds()).toHaveLength(2);
			expect(new Set(proxy.transferIds()).size).toBe(2);
		} finally {
			client.close();
			await new Promise<void>((resolveClose) => proxy.server.close(() => resolveClose()));
		}

		const shutdownClient = await connectEventually(socketPath);
		await shutdownClient.request({ type: "shutdown" });
		shutdownClient.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("releases a real public snapshot drain when the client detaches", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const suffix = `${process.pid}-${randomUUID().slice(0, 8)}`;
		const socketPath = join(tmpdir(), `prime-snapshot-drain-${suffix}.sock`);
		const proxyPath = join(tmpdir(), `prime-snapshot-drain-proxy-${suffix}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const largeFixture = `drain:${"x".repeat(8 * 1024 * 1024)}`;
		const sessionFile = createSnapshotSessionFile(agentDir, projectDir, largeFixture);

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const creator = await connectEventually(socketPath, supervisor);
		const created = await creator.request({
			type: "create",
			sessionPath: sessionFile,
			config: {
				cwd: projectDir,
				agentDir,
				sessionDir: join(agentDir, "sessions"),
				noTools: true,
				noExtensions: true,
			},
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.activeSessionId || !summary.workerPid) throw new Error("Drain worker was incomplete");
		workerPids.add(summary.workerPid);
		creator.close();

		const proxy = await createSnapshotDrainAbortProxy(proxyPath, socketPath);
		const blockedClient = new DaemonClient(proxyPath);
		try {
			await blockedClient.connect(3_000);
			await blockedClient.waitForHello(3_000);
			const failedAttach = DaemonAgentConnection.attach(blockedClient, summary.activeSessionId, {
				supportsExtensionUi: false,
				snapshotTimeoutMs: 3_000,
			});
			const bufferedBytes = await proxy.aborted;
			expect(bufferedBytes).toBeGreaterThan(0);
			await expect(failedAttach).rejects.toThrow();
		} finally {
			blockedClient.close();
			await new Promise<void>((resolveClose) => proxy.server.close(() => resolveClose()));
		}

		const recoveryClient = await connectEventually(socketPath);
		const connection = await DaemonAgentConnection.attach(recoveryClient, summary.activeSessionId, {
			supportsExtensionUi: false,
			snapshotTimeoutMs: 5_000,
		});
		const recovered = await connection.getInitialSnapshot();
		const recoveredUser = recovered.messages.find((message) => message.role === "user");
		expect(recoveredUser?.content).toBe(largeFixture);
		await connection.dispose();
		await recoveryClient.request({ type: "shutdown" });
		recoveryClient.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("keeps client-owned workers hidden and removes them without archiving", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-owned-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "owned worker fixture", timestamp: 1 });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const launchEnvSentinel = `owned-env-${randomUUID()}`;
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			lifecycle: "client_owned",
			launchEnv: { PRIME_AGENT_OWNED_TEST: launchEnvSentinel },
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		expect(created.success).toBe(true);
		const summary = requireSummary(created.success ? created.data : undefined);
		if (!summary.workerPid || !summary.activeSessionId) {
			throw new Error("Client-owned worker did not expose its process identity");
		}
		workerPids.add(summary.workerPid);

		const publicList = await client.request({ type: "list" });
		expect(publicList.success).toBe(true);
		expect(requireSessionList(publicList.success ? publicList.data : undefined)).toEqual([]);
		const internalList = await client.request({ type: "list", includeClientOwned: true });
		expect(internalList.success).toBe(true);
		expect(requireSessionList(internalList.success ? internalList.data : undefined)).toHaveLength(1);
		const otherClient = await connectEventually(socketPath);
		const deniedList = await otherClient.request({ type: "list", includeClientOwned: true });
		expect(deniedList).toMatchObject({
			success: true,
			data: { sessions: [], busyClientOwnedSessionCount: 0 },
		});
		const privateSelector = summary.sessionId.slice(-8);
		const deniedAttach = await otherClient.request({ type: "attach", activeSessionId: privateSelector });
		expect(deniedAttach).toMatchObject({
			success: false,
			error: `Unknown active session: ${privateSelector}`,
		});
		const deniedCron = await otherClient.request({
			type: "cron_add",
			activeSessionId: summary.activeSessionId,
			schedule: "every 1h",
			prompt: "unauthorized",
		});
		expect(deniedCron).toMatchObject({
			success: false,
			error: `Unknown active session: ${summary.activeSessionId}`,
		});
		const activeCleanup = await otherClient.request({
			type: "get_owned_session_cleanup",
			activeSessionId: summary.activeSessionId,
		});
		expect(activeCleanup).toMatchObject({ success: true, data: { status: "active" } });
		const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
			ownedSession: true,
			supportsExtensionUi: false,
		});
		await expect(connection.listHeartbeats()).resolves.toEqual([]);
		await expect(connection.listCronJobs()).resolves.toEqual([]);
		const cronResponse = await client.request({
			type: "cron_add",
			activeSessionId: summary.activeSessionId,
			schedule: "every 1h",
			prompt: "check status",
		});
		if (!cronResponse.success || !cronResponse.data || typeof cronResponse.data !== "object") {
			throw new Error(cronResponse.success ? "Cron response was missing its job" : cronResponse.error);
		}
		const cronJob = (cronResponse.data as { job: { id: string } }).job;
		await expect(connection.listCronJobs()).resolves.toEqual([expect.objectContaining({ id: cronJob.id })]);
		await expect(connection.cancelCronJob(cronJob.id)).resolves.toMatchObject({ id: cronJob.id });

		const descriptor = readWorkerDescriptor(agentDir);
		expect(descriptor.ownerClientId).toEqual(expect.any(String));
		expect(JSON.stringify(descriptor)).not.toContain(launchEnvSentinel);
		expect(descriptor.createCommand).not.toHaveProperty("launchEnv");
		expect(descriptor.createCommand).not.toHaveProperty("lifecycle");

		const completed = await client.request({
			type: "complete_owned_session",
			activeSessionId: summary.activeSessionId,
		});
		expect(completed.success).toBe(true);
		const settledCleanup = await otherClient.request({
			type: "get_owned_session_cleanup",
			activeSessionId: summary.activeSessionId,
		});
		expect(settledCleanup).toEqual(expect.objectContaining({ success: true, data: { status: "settled" } }));
		expect(settledCleanup.success ? Object.keys(settledCleanup.data as object) : []).toEqual(["status"]);
		await connection.dispose();
		otherClient.close();
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForCondition(
			() => countWorkerDescriptors(agentDir) === 0,
			"Client-owned worker descriptor was not removed",
		);
		expect((await readSessionInfo(sessionFile))?.state?.status).not.toBe("archived");

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	}, 60_000);

	it.skipIf(process.platform === "win32")(
		"adopts a recoverable owned worker with an authoritative snapshot and rotated receipt",
		async () => {
			const root = tempDir();
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const sessionDir = join(agentDir, "sessions");
			const socketPath = join(tmpdir(), `prime-recover-owned-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			mkdirSync(projectDir, { recursive: true });
			const sessionManager = SessionManager.create(projectDir, sessionDir);
			sessionManager.appendMessage({ role: "user", content: "recoverable fixture", timestamp: 1 });
			const sessionPath = sessionManager.getSessionFile();
			if (!sessionPath) throw new Error("Recoverable fixture did not persist");
			const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
			const owner = await connectEventually(socketPath, supervisor);
			const correlationId = `recover-correlation-${randomUUID()}`;
			const previousMcpOwnerId = `recover-mcp-${randomUUID()}`;
			const config = {
				cwd: projectDir,
				agentDir,
				sessionDir,
				apiKey: "faux-key",
				extensions: [fauxExtensionPath],
				provider: "faux",
				model: "faux",
				noTools: true,
				noExtensions: false,
			};
			const launchEnv = {
				...collectDaemonLaunchEnv(),
				PRIME_AGENT_RECOVERY_TEST: `canary-${randomUUID()}`,
			};
			const createRequestId = randomUUID();
			const created = await createRecoverableOwnedSession(owner, {
				requestId: createRequestId,
				correlationId,
				mcpOwnerId: previousMcpOwnerId,
				config,
				sessionPath,
				launchEnv,
				connectionOptions: { supportsExtensionUi: false },
			});
			if (!created.state.workerPid || !created.state.activeSessionId)
				throw new Error("Recoverable worker was incomplete");
			workerPids.add(created.state.workerPid);
			const recoverableMcpServers = [
				{
					name: "recoverable-task",
					type: "http" as const,
					url: "https://recoverable.invalid/mcp",
					headers: { Authorization: "Bearer recoverable-mcp-canary" },
				},
			];
			await created.connection.replaceAcpMcpServers(recoverableMcpServers, previousMcpOwnerId);
			await created.connection.submitCorrelatedPrompt("record an adoption lifecycle", {
				correlationId,
				queueIfBusy: true,
			});
			const before = await created.connection.getInitialSnapshot();
			if (!before.lastEventCursor) throw new Error("Recoverable fixture did not expose an event cursor");
			const earlyAdopter = await connectEventually(socketPath, supervisor);
			const requestId = randomUUID();
			const nextMcpOwnerId = `recover-mcp-${randomUUID()}`;
			const adoptionOptions = {
				requestId,
				recoveryHandle: created.recoveryHandle,
				expectedSupervisorGeneration: created.supervisorGeneration,
				activeSessionId: created.state.activeSessionId,
				sessionId: created.state.sessionId,
				correlationId,
				cursor: before.lastEventCursor,
				previousMcpOwnerId,
				mcpOwnerId: nextMcpOwnerId,
				config,
				launchEnv,
				connectionOptions: { supportsExtensionUi: false },
			} as const;
			await expect(adoptRecoverableOwnedSession(earlyAdopter, adoptionOptions)).rejects.toThrow(
				"Recoverable owned session adoption is unavailable",
			);
			earlyAdopter.close();
			await disconnectDaemonClientWithServerCloseBarrier(owner);
			const wrongHandleClient = await connectEventually(socketPath, supervisor);
			await expect(
				adoptRecoverableOwnedSession(wrongHandleClient, {
					...adoptionOptions,
					requestId: randomUUID(),
					recoveryHandle: "W".repeat(43),
				}),
			).rejects.toThrow("Recoverable owned session adoption is unavailable");
			wrongHandleClient.close();

			const proxyPath = join(tmpdir(), `prime-recover-loss-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			const responseLoss = await createCommandResponseLossProxy(
				proxyPath,
				socketPath,
				"commit_recoverable_owned_session_adoption",
			);
			const lostResponseAdopter = await connectEventually(proxyPath, supervisor);
			let lostAdoption: Promise<Awaited<ReturnType<typeof adoptRecoverableOwnedSession>>> | undefined;
			for (let attempt = 0; attempt < 20; attempt++) {
				const candidate = adoptRecoverableOwnedSession(lostResponseAdopter, adoptionOptions);
				const outcome = await Promise.race([
					responseLoss.responseObserved.then(() => "response_lost" as const),
					candidate.then(
						() => "unexpected_success" as const,
						() => "retry" as const,
					),
				]);
				if (outcome === "response_lost") {
					lostAdoption = candidate;
					break;
				}
				if (outcome === "unexpected_success") throw new Error("Response-loss proxy forwarded the commit receipt");
			}
			if (!lostAdoption) throw new Error("Recoverable commit response was not observed by the loss proxy");

			const retryAdopter = await connectEventually(socketPath, supervisor);
			const observedAdoptionMessages: string[] = [];
			const stopObservingAdoptionMessages = retryAdopter.onMessage((message) => {
				observedAdoptionMessages.push(JSON.stringify(message));
			});
			await expect(adoptRecoverableOwnedSession(retryAdopter, adoptionOptions)).rejects.toThrow(
				"Recoverable owned session adoption is unavailable",
			);
			responseLoss.release();
			await expect(lostAdoption).rejects.toThrow("Recoverable owned session adoption is unavailable");
			lostResponseAdopter.close();
			await new Promise<void>((resolveClose) => responseLoss.server.close(() => resolveClose()));

			let commitRetry: Awaited<ReturnType<typeof adoptRecoverableOwnedSession>> | undefined;
			for (let attempt = 0; attempt < 20; attempt++) {
				try {
					commitRetry = await adoptRecoverableOwnedSession(retryAdopter, adoptionOptions);
					break;
				} catch (error) {
					if (!(error instanceof Error) || error.message !== "Recoverable owned session adoption is unavailable") {
						throw error;
					}
				}
			}
			if (!commitRetry) throw new Error("Final recoverable adoption receipt did not rebind");
			expect(commitRetry.recoveryHandle).not.toBe(created.recoveryHandle);
			expect(commitRetry.proof).toMatchObject({
				feature: "recoverable_owned_session_adoption_v1",
				status: "adopted",
				activeSessionId: created.state.activeSessionId,
				sessionId: created.state.sessionId,
				correlationId,
				mcpOwnerId: nextMcpOwnerId,
				ownershipGeneration: 1,
			});
			const after = await commitRetry.connection.getInitialSnapshot();
			expect(after.messages).toEqual(before.messages);
			expect(after.lastEventCursor?.sequence).toBeGreaterThanOrEqual(before.lastEventCursor.sequence);
			const futureCorrelationId = randomUUID();
			let resolveFutureLifecycle!: () => void;
			const futureLifecycle = new Promise<void>((resolve) => {
				resolveFutureLifecycle = resolve;
			});
			const stopObservingFutureLifecycle = commitRetry.connection.subscribe((event) => {
				if (
					event.type === "prompt_lifecycle" &&
					event.lifecycle.correlationId === futureCorrelationId &&
					["completed", "cancelled", "failed"].includes(event.lifecycle.phase)
				) {
					resolveFutureLifecycle();
				}
			});
			await commitRetry.connection.submitCorrelatedPrompt("future event after adoption retry", {
				correlationId: futureCorrelationId,
				queueIfBusy: true,
			});
			await futureLifecycle;
			stopObservingFutureLifecycle();
			await expect(
				commitRetry.connection.replaceAcpMcpServers(recoverableMcpServers, previousMcpOwnerId),
			).rejects.toThrow("owned by another daemon client");
			await commitRetry.connection.releaseAcpMcpServers(nextMcpOwnerId, ["recoverable-task"]);
			const confirmation = {
				requestId,
				recoveryHandle: commitRetry.recoveryHandle,
				proof: commitRetry.proof,
			};
			await confirmRecoverableOwnedSessionAdoption(retryAdopter, confirmation);
			await confirmRecoverableOwnedSessionAdoption(retryAdopter, confirmation);
			await owner.connect();
			await owner.waitForHello();
			const retiredCreateReplay = await owner.request({
				type: "create_recoverable_owned_session",
				requestId: createRequestId,
				expectedSupervisorGeneration: created.supervisorGeneration,
				correlationId,
				mcpOwnerId: previousMcpOwnerId,
				recoveryConfig: config,
				sessionPath,
				config,
				env: collectDaemonClientEnv(),
				launchEnv,
				launchEnvMode: "replace",
			});
			expect(retiredCreateReplay).toMatchObject({
				success: false,
				error: "Recoverable owned session adoption is unavailable",
			});
			expect(JSON.stringify(retiredCreateReplay)).not.toContain(commitRetry.recoveryHandle);
			expect(readWorkerDescriptor(agentDir).ownerClientId).toBe(retryAdopter.clientId);
			const fencedOldOwnerMutation = await owner.request({
				type: "replace_acp_mcp_servers",
				activeSessionId: created.state.activeSessionId,
				ownerId: previousMcpOwnerId,
				servers: recoverableMcpServers,
			});
			expect(fencedOldOwnerMutation).toMatchObject({
				success: false,
				error: `Unknown active session: ${created.state.activeSessionId}`,
			});
			owner.close();
			const descriptor = readWorkerDescriptor(agentDir);
			expect(descriptor.pid).toBe(created.state.workerPid);
			expect(descriptor.ownerClientId).toBe(retryAdopter.clientId);
			const listed = await retryAdopter.request({ type: "list", includeClientOwned: true });
			const privateSurfaces = [
				JSON.stringify(descriptor),
				JSON.stringify(listed),
				observedAdoptionMessages.join("\n"),
				readDaemonLogs(agentDir),
			];
			for (const canary of [
				created.recoveryHandle,
				commitRetry.recoveryHandle,
				requestId,
				previousMcpOwnerId,
				nextMcpOwnerId,
				launchEnv.PRIME_AGENT_RECOVERY_TEST,
			]) {
				for (const surface of privateSurfaces) expect(surface).not.toContain(canary);
			}
			if (!descriptor.processStartId) throw new Error("Recoverable worker did not expose a process identity");
			identityTrackedProcesses.set(descriptor.pid, descriptor.processStartId);
			signalIdentityVerifiedProcess(descriptor, "SIGKILL");
			await waitForProcessGone(descriptor.pid);
			identityTrackedProcesses.delete(descriptor.pid);
			workerPids.delete(descriptor.pid);
			await waitForCondition(
				() => {
					const recovered = readWorkerDescriptor(agentDir);
					return recovered.pid !== descriptor.pid && recovered.lifecycle === "ready";
				},
				"Adopted owner did not recover through a replacement worker incarnation",
				20_000,
			);
			const recoveredDescriptor = readWorkerDescriptor(agentDir);
			if (!recoveredDescriptor.processStartId) {
				throw new Error("Recovered adopted worker did not expose a process identity");
			}
			workerPids.add(recoveredDescriptor.pid);
			identityTrackedProcesses.set(recoveredDescriptor.pid, recoveredDescriptor.processStartId);
			expect(recoveredDescriptor.ownerClientId).toBe(retryAdopter.clientId);
			await expect(confirmRecoverableOwnedSessionAdoption(retryAdopter, confirmation)).rejects.toThrow(
				"Recoverable owned session adoption is unavailable",
			);
			const recoveredCorrelationId = randomUUID();
			const recoveredLifecycle = new Promise<void>((resolveLifecycle) => {
				const unsubscribe = commitRetry.connection.subscribe((event) => {
					if (event.type === "prompt_lifecycle" && event.lifecycle.correlationId === recoveredCorrelationId) {
						unsubscribe();
						resolveLifecycle();
					}
				});
			});
			await commitRetry.connection.submitCorrelatedPrompt("prompt after worker incarnation replacement", {
				correlationId: recoveredCorrelationId,
				queueIfBusy: true,
			});
			await recoveredLifecycle;
			stopObservingAdoptionMessages();
			const cleanupProof = await commitRetry.connection.disposeOwnedSession();
			expect(cleanupProof).toMatchObject({
				feature: "caller_owned_session_environment_cleanup_v1",
				status: "completed",
				started: { status: "attached" },
				observed: { supervisorGeneration: created.supervisorGeneration },
				daemonReplaced: false,
			});
			await waitForProcessGone(recoveredDescriptor.pid);
			workerPids.delete(recoveredDescriptor.pid);
			identityTrackedProcesses.delete(recoveredDescriptor.pid);
			await retryAdopter.request({ type: "shutdown" });
			retryAdopter.close();
			await waitForSocketGone(socketPath);
		},
		60_000,
	);

	it.skipIf(process.platform === "win32")(
		"grants exactly one authority to simultaneous recoverable claimants",
		async () => {
			const root = tempDir();
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const sessionDir = join(agentDir, "sessions");
			const socketPath = join(tmpdir(), `prime-recover-race-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			mkdirSync(projectDir, { recursive: true });
			const sessionManager = SessionManager.create(projectDir, sessionDir);
			sessionManager.appendMessage({ role: "user", content: "recoverable race fixture", timestamp: 1 });
			const sessionPath = sessionManager.getSessionFile();
			if (!sessionPath) throw new Error("Recoverable race fixture did not persist");
			const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
			const owner = await connectEventually(socketPath, supervisor);
			const correlationId = `recover-race-${randomUUID()}`;
			const previousMcpOwnerId = `recover-race-mcp-${randomUUID()}`;
			const config = {
				cwd: projectDir,
				agentDir,
				sessionDir,
				apiKey: "faux-key",
				extensions: [fauxExtensionPath],
				provider: "faux",
				model: "faux",
				noTools: true,
				noExtensions: false,
			};
			const launchEnv = {
				...collectDaemonLaunchEnv(),
				PRIME_AGENT_RECOVERY_RACE: `canary-${randomUUID()}`,
			};
			const created = await createRecoverableOwnedSession(owner, {
				requestId: randomUUID(),
				correlationId,
				mcpOwnerId: previousMcpOwnerId,
				config,
				sessionPath,
				launchEnv,
				connectionOptions: { supportsExtensionUi: false },
			});
			if (!created.state.workerPid || !created.state.activeSessionId) {
				throw new Error("Recoverable race worker was incomplete");
			}
			workerPids.add(created.state.workerPid);
			await created.connection.submitCorrelatedPrompt("record a recoverable race lifecycle", {
				correlationId,
				queueIfBusy: true,
			});
			const snapshot = await created.connection.getInitialSnapshot();
			if (!snapshot.lastEventCursor) throw new Error("Recoverable race fixture did not expose a cursor");
			await disconnectDaemonClientWithServerCloseBarrier(owner);
			const claimantA = await connectEventually(socketPath, supervisor);
			const claimantB = await connectEventually(socketPath, supervisor);
			const requestId = randomUUID();
			const adoptionOptions = {
				requestId,
				recoveryHandle: created.recoveryHandle,
				expectedSupervisorGeneration: created.supervisorGeneration,
				activeSessionId: created.state.activeSessionId,
				sessionId: created.state.sessionId,
				correlationId,
				cursor: snapshot.lastEventCursor,
				previousMcpOwnerId,
				mcpOwnerId: `recover-race-next-${randomUUID()}`,
				config,
				launchEnv,
				connectionOptions: { supportsExtensionUi: false },
			} as const;
			const outcomes = await Promise.allSettled([
				adoptRecoverableOwnedSession(claimantA, adoptionOptions),
				adoptRecoverableOwnedSession(claimantB, adoptionOptions),
			]);
			const fulfilled = outcomes.flatMap((outcome, index) =>
				outcome.status === "fulfilled" ? [{ adoption: outcome.value, index }] : [],
			);
			const rejected = outcomes.filter((outcome) => outcome.status === "rejected");
			expect(fulfilled).toHaveLength(1);
			expect(rejected).toHaveLength(1);
			expect(rejected[0]).toMatchObject({
				status: "rejected",
				reason: { message: "Recoverable owned session adoption is unavailable" },
			});
			const winner = fulfilled[0]!;
			const winnerClient = winner.index === 0 ? claimantA : claimantB;
			const loserClient = winner.index === 0 ? claimantB : claimantA;
			expect(readWorkerDescriptor(agentDir).ownerClientId).toBe(winnerClient.clientId);
			await confirmRecoverableOwnedSessionAdoption(winnerClient, {
				requestId,
				recoveryHandle: winner.adoption.recoveryHandle,
				proof: winner.adoption.proof,
			});
			loserClient.close();
			owner.close();
			await winner.adoption.connection.dispose();
			await waitForProcessGone(created.state.workerPid);
			workerPids.delete(created.state.workerPid);
			await winnerClient.request({ type: "shutdown" });
			winnerClient.close();
			await waitForSocketGone(socketPath);
		},
		60_000,
	);

	it.skipIf(process.platform === "win32")(
		"fails closed when recoverable authority crosses a supervisor generation",
		async () => {
			const root = tempDir();
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const sessionDir = join(agentDir, "sessions");
			const socketPath = join(tmpdir(), `prime-recover-generation-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			mkdirSync(projectDir, { recursive: true });
			const sessionManager = SessionManager.create(projectDir, sessionDir);
			sessionManager.appendMessage({ role: "user", content: "generation fixture", timestamp: 1 });
			const sessionPath = sessionManager.getSessionFile();
			if (!sessionPath) throw new Error("Generation fixture did not persist");
			const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
			const owner = await connectEventually(socketPath, supervisor);
			const correlationId = `recover-generation-${randomUUID()}`;
			const mcpOwnerId = `recover-generation-mcp-${randomUUID()}`;
			const config = {
				cwd: projectDir,
				agentDir,
				sessionDir,
				apiKey: "faux-key",
				extensions: [fauxExtensionPath],
				provider: "faux",
				model: "faux",
				noTools: true,
				noExtensions: false,
			};
			const launchEnv = collectDaemonLaunchEnv();
			const created = await createRecoverableOwnedSession(owner, {
				requestId: randomUUID(),
				correlationId,
				mcpOwnerId,
				config,
				launchEnv,
				sessionPath,
				connectionOptions: {
					supportsExtensionUi: false,
					recoverDaemon: () => new Promise<void>(() => undefined),
				},
			});
			if (!created.state.workerPid || !created.state.activeSessionId) {
				throw new Error("Generation fixture worker was incomplete");
			}
			workerPids.add(created.state.workerPid);
			await created.connection.submitCorrelatedPrompt("record a generation lifecycle", {
				correlationId,
				queueIfBusy: true,
			});
			const snapshot = await created.connection.getInitialSnapshot();
			if (!snapshot.lastEventCursor) throw new Error("Generation fixture did not expose an event cursor");
			const workerIdentity = readWorkerDescriptor(agentDir);
			const supervisorIdentity = {
				pid: owner.hello?.supervisorPid,
				processStartId: owner.hello?.supervisorProcessStartId,
			};
			signalIdentityVerifiedProcess(supervisorIdentity, "SIGKILL");
			await waitForProcessGone(supervisorIdentity.pid!);
			owner.close();
			const claimant = await connectEventually(socketPath);
			expect(await claimant.request({ type: "list" })).toMatchObject({ success: true, command: "list" });
			expect(readWorkerDescriptor(agentDir)).toMatchObject({
				workerId: workerIdentity.workerId,
				pid: workerIdentity.pid,
				processStartId: workerIdentity.processStartId,
			});
			await expect(
				adoptRecoverableOwnedSession(claimant, {
					requestId: randomUUID(),
					recoveryHandle: created.recoveryHandle,
					expectedSupervisorGeneration: created.supervisorGeneration,
					activeSessionId: created.state.activeSessionId,
					sessionId: created.state.sessionId,
					correlationId,
					cursor: snapshot.lastEventCursor,
					previousMcpOwnerId: mcpOwnerId,
					mcpOwnerId: `replacement-mcp-${randomUUID()}`,
					config,
					launchEnv,
					connectionOptions: { supportsExtensionUi: false },
				}),
			).rejects.toThrow("Recoverable owned session adoption is unavailable");
			const shutdownResult = await claimant.request({ type: "shutdown", force: true });
			expect(shutdownResult).toMatchObject({ success: true, command: "shutdown" });
			claimant.close();
			await waitForProcessGone(created.state.workerPid);
			workerPids.delete(created.state.workerPid);
			await waitForSocketGone(socketPath);
		},
		60_000,
	);

	it.skipIf(process.platform === "win32")(
		"isolates two caller-owned environments through worker and supervisor recovery",
		async () => {
			const root = tempDir();
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const socketPath = join(tmpdir(), `prime-issue33-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			const observerModule = join(root, "observe-launch-env.cjs");
			mkdirSync(projectDir, { recursive: true });
			writeFileSync(
				observerModule,
				`const fs = require("node:fs");
const path = process.env.PRIME_AGENT_TEST_ENV_OBSERVATION;
if (path) fs.appendFileSync(path, JSON.stringify({ pid: process.pid, role: process.env.PRIME_AGENT_INTERNAL_DAEMON_WORKER, hasA: process.env.PRIME_AGENT_TEST_ENV_A !== undefined, hasB: process.env.PRIME_AGENT_TEST_ENV_B !== undefined, hasC: process.env.PRIME_AGENT_TEST_SUPERVISOR_C !== undefined }) + "\\n");
`,
			);
			const sessionDir = join(agentDir, "sessions");
			const sessionFiles = {
				A: createSnapshotSessionFile(agentDir, projectDir, "issue #33 owner A"),
				B: createSnapshotSessionFile(agentDir, projectDir, "issue #33 owner B"),
			};
			const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
			let replacementSupervisor: ChildProcess | undefined;
			let daemonRecovery: Promise<void> | undefined;
			const recoverDaemon = () => {
				daemonRecovery ??= (async () => {
					replacementSupervisor = spawnSupervisor(agentDir, socketPath, projectDir, [], {
						PRIME_AGENT_TEST_SUPERVISOR_C: "ambient-c",
					});
					const probe = await connectEventually(socketPath, replacementSupervisor);
					probe.close();
				})();
				return daemonRecovery;
			};
			const safeBaseEnvironment = collectDaemonLaunchEnv(process.env);
			for (const name of Object.keys(safeBaseEnvironment)) {
				if (name.startsWith("RLM_") || /TOKEN|KEY|SECRET|PASSWORD|CREDENTIAL|AUTH|COOKIE/i.test(name)) {
					delete safeBaseEnvironment[name];
				}
			}
			Object.assign(safeBaseEnvironment, {
				HOME: process.env.HOME ?? root,
				PI_OFFLINE: "1",
				TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json"),
				[ENV_AGENT_DIR]: agentDir,
			});
			delete safeBaseEnvironment.PRIME_AGENT_TEST_SUPERVISOR_C;
			delete safeBaseEnvironment.PRIME_AGENT_TEST_ENV_A;
			delete safeBaseEnvironment.PRIME_AGENT_TEST_ENV_B;
			const createOwned = async (label: "A" | "B") => {
				const client = await connectEventually(socketPath, supervisor);
				const observationPath = join(root, `observed-${label}.jsonl`);
				const canary = `issue33-private-${label.toLowerCase()}-${randomUUID()}`;
				const launchEnv = {
					...safeBaseEnvironment,
					NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --require=${observerModule}`.trim(),
					PRIME_AGENT_TEST_ENV_OBSERVATION: observationPath,
					...(label === "A" ? { PRIME_AGENT_TEST_ENV_A: canary } : { PRIME_AGENT_TEST_ENV_B: canary }),
				};
				const created = await client.request({
					type: "create",
					lifecycle: "client_owned",
					sessionPath: sessionFiles[label],
					launchEnv,
					launchEnvMode: "replace",
					config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
				});
				if (!created.success) throw new Error(created.error);
				const summary = requireSummary(created.data);
				if (!summary.workerPid) throw new Error("Owned worker did not expose its pid");
				const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId ?? summary.id, {
					ownedSession: true,
					ownedSessionLaunchEnv: launchEnv,
					ownedSessionRecoveryConfig: {
						cwd: projectDir,
						agentDir,
						sessionDir,
						provider: "faux",
						model: "faux",
						noTools: true,
						noExtensions: true,
					},
					recoverDaemon,
					reconnectTimeoutMs: 30_000,
					supportsExtensionUi: false,
				});
				const proof = connection.getOwnedSessionContractProof();
				if (!proof) throw new Error("Owned attach did not publish the issue #33 contract proof");
				return { client, connection, summary, launchEnv, observationPath, canary, proof };
			};
			const ownerA = await createOwned("A");
			const ownerB = await createOwned("B");
			const readObservations = (path: string) => {
				try {
					return readFileSync(path, "utf8")
						.trim()
						.split("\n")
						.filter(Boolean)
						.map(
							(line) =>
								JSON.parse(line) as {
									pid: number;
									role?: string;
									hasA: boolean;
									hasB: boolean;
									hasC: boolean;
								},
						);
				} catch {
					return [];
				}
			};
			await waitForCondition(
				() => readObservations(ownerA.observationPath).some((entry) => entry.role === "1"),
				"Worker A did not report its launch environment",
			);
			await waitForCondition(
				() => readObservations(ownerB.observationPath).some((entry) => entry.role === "1"),
				"Worker B did not report its launch environment",
			);
			for (const entry of readObservations(ownerA.observationPath).filter(
				(observation) => observation.role === "1",
			)) {
				expect(entry).toMatchObject({ hasA: true, hasB: false, hasC: false });
			}
			for (const entry of readObservations(ownerB.observationPath).filter(
				(observation) => observation.role === "1",
			)) {
				expect(entry).toMatchObject({ hasA: false, hasB: true, hasC: false });
			}

			const activeA = ownerA.summary.activeSessionId ?? ownerA.summary.id;
			const activeB = ownerB.summary.activeSessionId ?? ownerB.summary.id;
			const originalDescriptors = readWorkerDescriptors(agentDir);
			const originalA = originalDescriptors.find((descriptor) => descriptor.rootActiveSessionId === activeA);
			const originalB = originalDescriptors.find((descriptor) => descriptor.rootActiveSessionId === activeB);
			if (!originalA?.pid || !originalA.processStartId || !originalB?.pid || !originalB.processStartId) {
				throw new Error("Owned worker descriptors did not contain complete process identities");
			}
			identityTrackedProcesses.set(originalA.pid, originalA.processStartId);
			identityTrackedProcesses.set(originalB.pid, originalB.processStartId);

			const helloA = ownerA.client.hello;
			const helloB = ownerB.client.hello;
			if (
				!helloA?.supervisorPid ||
				!helloA.supervisorProcessStartId ||
				helloB?.supervisorPid !== helloA.supervisorPid ||
				helloB.supervisorProcessStartId !== helloA.supervisorProcessStartId
			) {
				throw new Error("Owned clients did not agree on the authenticated supervisor process identity");
			}
			signalIdentityVerifiedProcess(
				{ pid: helloA.supervisorPid, processStartId: helloA.supervisorProcessStartId },
				"SIGKILL",
			);
			await waitForProcessGone(helloA.supervisorPid);
			await waitForExit(supervisor);
			children.delete(supervisor);
			await waitForCondition(
				() => {
					const nextA = ownerA.connection.getOwnedSessionContractProof();
					const nextB = ownerB.connection.getOwnedSessionContractProof();
					return (
						nextA !== undefined &&
						nextB !== undefined &&
						nextA.daemon.supervisorGeneration !== ownerA.proof.daemon.supervisorGeneration &&
						nextB.daemon.supervisorGeneration !== ownerB.proof.daemon.supervisorGeneration
					);
				},
				"Owned connections did not republish proof after supervisor replacement",
				30_000,
			);
			if (!replacementSupervisor) throw new Error("Shared daemon recovery did not spawn a replacement supervisor");
			const replacementHelloA = ownerA.client.hello;
			const replacementHelloB = ownerB.client.hello;
			if (
				!replacementHelloA?.supervisorPid ||
				!replacementHelloA.supervisorProcessStartId ||
				replacementHelloB?.supervisorPid !== replacementHelloA.supervisorPid ||
				replacementHelloB.supervisorProcessStartId !== replacementHelloA.supervisorProcessStartId ||
				getProcessStartId(replacementHelloA.supervisorPid) !== replacementHelloA.supervisorProcessStartId
			) {
				throw new Error("Replacement supervisor identity was not current and shared");
			}
			identityTrackedProcesses.set(replacementHelloA.supervisorPid, replacementHelloA.supervisorProcessStartId);

			const previousAmbientC = process.env.PRIME_AGENT_TEST_SUPERVISOR_C;
			process.env.PRIME_AGENT_TEST_SUPERVISOR_C = "mutated-c";
			let recoveredA: DaemonWorkerDescriptor | undefined;
			try {
				signalIdentityVerifiedProcess(originalA, "SIGKILL");
				await waitForProcessGone(originalA.pid);
				identityTrackedProcesses.delete(originalA.pid);
				await waitForCondition(
					() =>
						readWorkerDescriptors(agentDir).some(
							(descriptor) =>
								descriptor.rootActiveSessionId === activeA &&
								descriptor.pid !== originalA.pid &&
								descriptor.lifecycle === "ready",
						),
					"Worker A was not recovered with a new process after supervisor replacement",
					20_000,
				);
				recoveredA = readWorkerDescriptors(agentDir).find(
					(descriptor) =>
						descriptor.rootActiveSessionId === activeA &&
						descriptor.pid !== originalA.pid &&
						descriptor.lifecycle === "ready",
				);
				if (!recoveredA?.pid || !recoveredA.processStartId) {
					throw new Error("Recovered worker A descriptor did not contain a complete process identity");
				}
				identityTrackedProcesses.set(recoveredA.pid, recoveredA.processStartId);
				await waitForCondition(
					() =>
						readObservations(ownerA.observationPath).filter(
							(entry) => entry.role === "1" && entry.hasA && !entry.hasB && !entry.hasC,
						).length >= 2,
					"Recovered worker A did not retain its exact environment",
					20_000,
				);
			} finally {
				if (previousAmbientC === undefined) delete process.env.PRIME_AGENT_TEST_SUPERVISOR_C;
				else process.env.PRIME_AGENT_TEST_SUPERVISOR_C = previousAmbientC;
			}
			if (!recoveredA?.pid) throw new Error("Recovered worker A was unavailable after recovery");
			expect(ownerA.connection.getOwnedSessionContractProof()?.daemon.supervisorGeneration).toBe(
				replacementHelloA.supervisorGeneration,
			);
			expect(ownerB.connection.getOwnedSessionContractProof()?.daemon.supervisorGeneration).toBe(
				replacementHelloB?.supervisorGeneration,
			);

			const denied = await ownerB.client.request({
				type: "complete_owned_session",
				activeSessionId: activeA,
			});
			expect(denied).toMatchObject({
				success: false,
				errorInfo: { code: "owned_session_owner_mismatch" },
			});
			expect(JSON.stringify(denied)).not.toContain(activeA);
			for (const descriptor of readWorkerDescriptors(agentDir)) {
				const serialized = JSON.stringify(descriptor);
				expect(serialized).not.toContain(ownerA.canary);
				expect(serialized).not.toContain(ownerB.canary);
				expect(descriptor.callerOwnedEnvironmentContract).toBe(true);
				expect(descriptor.createCommand).not.toHaveProperty("launchEnv");
				expect(descriptor.createCommand).not.toHaveProperty("launchEnvMode");
			}
			const daemonLogs = readDaemonLogs(agentDir);
			expect(daemonLogs).not.toContain(ownerA.canary);
			expect(daemonLogs).not.toContain(ownerB.canary);
			expect(JSON.stringify(ownerA.connection.getOwnedSessionContractProof())).not.toContain(ownerA.canary);

			const [cleanupA, cleanupB] = await Promise.all([
				ownerA.connection.disposeOwnedSession({ timeoutMs: 30_000 }),
				ownerB.connection.disposeOwnedSession({ timeoutMs: 30_000 }),
			]);
			expect(cleanupA.status).toBe("completed");
			expect(cleanupB.status).toBe("completed");
			expect(JSON.stringify([cleanupA, cleanupB])).not.toContain("issue33-private");
			await waitForProcessGone(recoveredA.pid);
			await waitForProcessGone(originalB.pid);
			identityTrackedProcesses.delete(recoveredA.pid);
			identityTrackedProcesses.delete(originalB.pid);
			const shutdown = await ownerA.client.request({ type: "shutdown" });
			if (!shutdown.success) throw new Error("Replacement supervisor rejected test shutdown");
			ownerA.client.close();
			ownerB.client.close();
			await waitForProcessGone(replacementHelloA.supervisorPid);
			identityTrackedProcesses.delete(replacementHelloA.supervisorPid);
			if (replacementSupervisor.exitCode === null && replacementSupervisor.signalCode === null) {
				replacementSupervisor.kill("SIGTERM");
			}
			await waitForExit(replacementSupervisor);
			children.delete(replacementSupervisor);

			// Detached launchers admitted during the killed-supervisor window can finish
			// after the tracked replacement exits. Drain only supervisors authenticated
			// on this test-owned socket, and require two clients to agree on identity.
			const lateSupervisorHandshakeTimeoutMs = 5000;
			const unavailableProofMs = 5000;
			let unavailableSince = Date.now();
			const drainDeadline = Date.now() + 20_000;
			while (Date.now() < drainDeadline) {
				const first = new DaemonClient(socketPath);
				try {
					await first.connect(100);
				} catch {
					first.close();
					if (Date.now() - unavailableSince >= unavailableProofMs) break;
					await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
					continue;
				}
				unavailableSince = Date.now();
				const second = new DaemonClient(socketPath);
				try {
					await second.connect(lateSupervisorHandshakeTimeoutMs);
					const firstHello = await first.waitForHello(lateSupervisorHandshakeTimeoutMs);
					const secondHello = await second.waitForHello(lateSupervisorHandshakeTimeoutMs);
					if (
						!firstHello.supervisorPid ||
						!firstHello.supervisorProcessStartId ||
						secondHello.supervisorPid !== firstHello.supervisorPid ||
						secondHello.supervisorProcessStartId !== firstHello.supervisorProcessStartId ||
						getProcessStartId(firstHello.supervisorPid) !== firstHello.supervisorProcessStartId
					) {
						throw new Error("Late test supervisor identity was not current and shared");
					}
					identityTrackedProcesses.set(firstHello.supervisorPid, firstHello.supervisorProcessStartId);
					const lateShutdown = await first.request({ type: "shutdown" }, 2000);
					if (!lateShutdown.success) throw new Error("Late test supervisor rejected shutdown");
					await waitForProcessGone(firstHello.supervisorPid);
					identityTrackedProcesses.delete(firstHello.supervisorPid);
				} finally {
					first.close();
					second.close();
				}
			}
			if (Date.now() - unavailableSince < unavailableProofMs) {
				throw new Error("Test-owned supervisor socket did not remain unavailable");
			}
		},
		75_000,
	);

	it.runIf(process.platform === "win32")(
		"proves exact owned cleanup over a Windows named pipe",
		async () => {
			const root = tempDir();
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const sessionDir = join(agentDir, "sessions");
			const sessionFile = createSnapshotSessionFile(agentDir, projectDir, "issue #33 Windows owner");
			const socketPath = String.raw`\\.\pipe\prime-issue33-${process.pid}-${randomUUID().slice(0, 8)}`;
			mkdirSync(projectDir, { recursive: true });
			const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
			const owner = await connectEventually(socketPath, supervisor);
			const launchEnv = collectDaemonLaunchEnv(process.env);
			for (const name of Object.keys(launchEnv)) {
				if (name.startsWith("RLM_") || /TOKEN|KEY|SECRET|PASSWORD|CREDENTIAL|AUTH|COOKIE/i.test(name)) {
					delete launchEnv[name];
				}
			}
			Object.assign(launchEnv, {
				PI_OFFLINE: "1",
				TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json"),
				[ENV_AGENT_DIR]: agentDir,
				PRIME_AGENT_TEST_WINDOWS_ENV_A: "windows-a",
			});
			const created = await owner.request({
				type: "create",
				lifecycle: "client_owned",
				sessionPath: sessionFile,
				launchEnv,
				launchEnvMode: "replace",
				config: {
					cwd: projectDir,
					agentDir,
					sessionDir,
					provider: "faux",
					model: "faux",
					noTools: true,
					noExtensions: true,
				},
			});
			if (!created.success) throw new Error(created.error);
			const summary = requireSummary(created.data);
			if (!summary.workerPid) throw new Error("Windows owned worker did not expose its pid");
			const activeSessionId = summary.activeSessionId ?? summary.id;
			const descriptor = readWorkerDescriptors(agentDir).find(
				(candidate) => candidate.rootActiveSessionId === activeSessionId,
			);
			if (!descriptor?.pid || !descriptor.processStartId) {
				throw new Error("Windows owned worker did not expose a complete process identity");
			}
			identityTrackedProcesses.set(descriptor.pid, descriptor.processStartId);
			const connection = await DaemonAgentConnection.attach(owner, activeSessionId, {
				ownedSession: true,
				ownedSessionLaunchEnv: launchEnv,
				ownedSessionRecoveryConfig: {
					cwd: projectDir,
					agentDir,
					sessionDir,
					provider: "faux",
					model: "faux",
					noTools: true,
					noExtensions: true,
				},
				supportsExtensionUi: false,
			});
			expect(connection.getOwnedSessionContractProof()).toMatchObject({
				feature: "caller_owned_session_environment_cleanup_v1",
				status: "attached",
			});
			const cleanup = await connection.disposeOwnedSession({ timeoutMs: 30_000 });
			expect(cleanup.status).toBe("completed");
			await waitForProcessGone(descriptor.pid);
			identityTrackedProcesses.delete(descriptor.pid);
			await owner.request({ type: "shutdown" });
			owner.close();
			await waitForSocketGone(socketPath);
			await waitForExit(supervisor);
		},
		60_000,
	);

	it("lets a different client prove cleanup after the owner socket disappears", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const socketPath = join(tmpdir(), `prime-supervisor-owned-crash-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const owner = await connectEventually(socketPath, supervisor);
		const created = await owner.request({
			type: "create",
			lifecycle: "client_owned",
			noSession: true,
			launchEnv: { TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json") },
			config: { cwd: projectDir, agentDir, noTools: true, noExtensions: true },
		});
		if (!created.success) throw new Error(created.error);
		const summary = requireSummary(created.data);
		if (!summary.workerPid || !summary.activeSessionId) {
			throw new Error("Client-owned worker did not expose its process identity");
		}
		workerPids.add(summary.workerPid);
		const observer = await connectEventually(socketPath);
		try {
			await expect(
				observer.request({ type: "get_owned_session_cleanup", activeSessionId: summary.activeSessionId }),
			).resolves.toMatchObject({ success: true, data: { status: "active" } });
			process.kill(summary.workerPid, "SIGSTOP");
			owner.close();

			let sawStopping = false;
			let settled = false;
			const deadline = Date.now() + 50_000;
			while (Date.now() < deadline) {
				const response = await observer.request({
					type: "get_owned_session_cleanup",
					activeSessionId: summary.activeSessionId,
				});
				if (!response.success) throw new Error(response.error);
				const status = (response.data as { status?: string } | undefined)?.status;
				if (status === "stopping") sawStopping = true;
				if (status === "settled") {
					settled = true;
					break;
				}
				await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
			}
			expect(sawStopping).toBe(true);
			expect(settled).toBe(true);
			await waitForProcessGone(summary.workerPid);
			workerPids.delete(summary.workerPid);
			expect(countWorkerDescriptors(agentDir)).toBe(0);
			await observer.request({ type: "shutdown" });
			await waitForSocketGone(socketPath);
		} finally {
			owner.close();
			observer.close();
			if (workerPids.has(summary.workerPid)) {
				try {
					process.kill(summary.workerPid, "SIGCONT");
					process.kill(summary.workerPid, "SIGKILL");
				} catch {
					// The authoritative cleanup path already removed it.
				}
				workerPids.delete(summary.workerPid);
			}
		}
	}, 75_000);

	it("cleans up a client-owned create whose owner disconnects before registration", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const socketPath = join(
			tmpdir(),
			`prime-supervisor-owned-opening-${process.pid}-${randomUUID().slice(0, 8)}.sock`,
		);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const readiness = await connectEventually(socketPath, supervisor);
		readiness.close();
		const openingOwner = createConnection(socketPath);
		openingOwner.on("error", () => undefined);
		await new Promise<void>((resolveConnect, rejectConnect) => {
			openingOwner.once("connect", resolveConnect);
			openingOwner.once("error", rejectConnect);
		});
		const commandId = `opening-${randomUUID()}`;
		const wire = `${JSON.stringify(
			createDaemonCommandEnvelope(
				{
					type: "create",
					lifecycle: "client_owned",
					noSession: true,
					launchEnv: { TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json") },
					config: { cwd: projectDir, agentDir, noTools: true, noExtensions: true },
				},
				commandId,
				"opening-owner",
			),
		)}\n`;
		const closed = new Promise<void>((resolveClose) => openingOwner.once("close", resolveClose));
		await new Promise<void>((resolveWrite, rejectWrite) => {
			openingOwner.end(wire, (error?: Error | null) => (error ? rejectWrite(error) : resolveWrite()));
		});
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 5));
		expect(countWorkerDescriptors(agentDir)).toBe(0);
		openingOwner.destroy();
		// The command bytes are committed to the socket, but the client is gone
		// before launchWorker can publish the new resident registration.
		await closed;

		let descriptor: DaemonWorkerDescriptor | undefined;
		const descriptorDeadline = Date.now() + 15_000;
		while (!descriptor && Date.now() < descriptorDeadline) {
			try {
				descriptor = readWorkerDescriptor(agentDir);
			} catch {
				await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
			}
		}
		if (!descriptor) throw new Error(`Opening client-owned create was not registered\n${readDaemonLogs(agentDir)}`);
		expect(descriptor.ownerClientId).toBe("opening-owner");
		workerPids.add(descriptor.pid);

		const observer = await connectEventually(socketPath, supervisor);
		try {
			let settled = false;
			const deadline = Date.now() + 50_000;
			while (Date.now() < deadline) {
				const response = await observer.request({
					type: "get_owned_session_cleanup",
					activeSessionId: descriptor.rootActiveSessionId,
				});
				if (!response.success) throw new Error(response.error);
				const status = (response.data as { status?: string } | undefined)?.status;
				if (status === "settled") {
					settled = true;
					break;
				}
				await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
			}
			// `settled` is the authoritative result; the observer may miss the
			// transient `stopping` phase when registration and cleanup race.
			expect(settled).toBe(true);
			await waitForProcessGone(descriptor.pid);
			workerPids.delete(descriptor.pid);
			expect(countWorkerDescriptors(agentDir)).toBe(0);
			await observer.request({ type: "shutdown" });
			await waitForSocketGone(socketPath);
		} finally {
			observer.close();
		}
	}, 75_000);

	it("releases an adopted client-owned worker when disposal races supervisor replacement", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const socketPath = join(tmpdir(), `prime-supervisor-owned-adopt-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			lifecycle: "client_owned",
			noSession: true,
			launchEnv: { TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json") },
			config: { cwd: projectDir, agentDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid || !summary.activeSessionId) {
			throw new Error("Client-owned worker did not expose its process identity");
		}
		workerPids.add(summary.workerPid);
		const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
			closeClientOnDispose: true,
			ownedSession: true,
			recoverDaemon: async () => {},
			supportsExtensionUi: false,
		});

		supervisor.kill("SIGTERM");
		await waitForExit(supervisor);
		children.delete(supervisor);
		await connection.dispose();

		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForCondition(
			() => countWorkerDescriptors(agentDir) === 0,
			"Adopted client-owned worker descriptor was not removed",
		);
		const replacementClient = await connectEventually(socketPath);
		const replacementCleanup = await replacementClient.request({
			type: "get_owned_session_cleanup",
			activeSessionId: summary.activeSessionId,
		});
		expect(replacementCleanup).toMatchObject({ success: true, data: { status: "settled" } });
		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
	}, 60_000);

	it("delivers agent-origin messages between root siblings on separate workers", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(
			tmpdir(),
			`prime-supervisor-root-message-${process.pid}-${randomUUID().slice(0, 8)}.sock`,
		);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const createRoot = async (name: string) => {
			const response = await client.request({
				type: "create",
				name,
				config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
			});
			expect(response.success).toBe(true);
			return requireSummary(response.success ? response.data : undefined);
		};
		const source = await createRoot("source-root");
		const target = await createRoot("target-root");
		expect(source.workerPid).not.toBe(target.workerPid);
		await startBlockingBash(client, target.activeSessionId ?? target.id, join(root, "target-root-blocker.ready"));

		const response = await client.request({
			type: "send_message",
			fromActiveSessionId: source.activeSessionId ?? source.id,
			targetActiveSessionId: target.activeSessionId ?? target.id,
			message: "hello sibling root",
			agentOrigin: true,
		});
		expect(response.success, JSON.stringify(response)).toBe(true);
		expect(response).toMatchObject({
			success: true,
			data: {
				source: "agent_message",
				target: { activeSessionId: target.activeSessionId ?? target.id },
				message: "hello sibling root",
				deliveryStatus: "queued",
			},
		});
		const shutdown = await client.request({ type: "shutdown" }, 10_000);
		expect(shutdown.success).toBe(true);
		client.close();
		await waitForSocketGone(socketPath);
	}, 30_000);

	it("cancels an archived session heartbeat without spawning a worker", { tags: ["process-stress"] }, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(
			tmpdir(),
			`prime-supervisor-archived-cron-${process.pid}-${randomUUID().slice(0, 8)}.sock`,
		);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "do not revive me", timestamp: 1 });
		sessionManager.appendSessionState({ status: "active" });
		sessionManager.appendSessionState({ status: "archived" });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}
		const cronStore = new AgentCronJobStore(getCronJobsPath(agentDir));
		const heartbeat = cronStore.createHeartbeat({
			activeSessionId: "old-active-session",
			sessionId: sessionManager.getSessionId(),
			sessionFile,
			cwd: projectDir,
			scheduleText: "every 10s",
			prompt: "continue old work",
			now: new Date(Date.now() - 20_000),
		});

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const migratedStore = AgentCronJobStore.forSessionArtifacts();
		migratedStore.registerSessionArtifact(sessionManager.getSessionId(), sessionManager.getSessionArtifactDir()!);
		await waitForCondition(
			() => migratedStore.list().find((job) => job.id === heartbeat.id)?.status === "cancelled",
			"Archived heartbeat was not cancelled",
		);

		expect(countWorkerDescriptors(agentDir)).toBe(0);
		const listed = await client.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(
			requireSessionList(listed.success ? listed.data : undefined).filter(
				(session) => session.activeSessionId || session.workerPid,
			),
		).toEqual([]);
		expect((await readSessionInfo(sessionFile))?.state).toEqual({ status: "archived" });

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	});

	it("cancels an orphan heartbeat instead of recreating a descriptorless active session", {
		tags: ["process-stress"],
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-orphan-cron-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "old scheduled work", timestamp: 1 });
		sessionManager.appendSessionState({ status: "active" });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}
		const cronStore = new AgentCronJobStore(getCronJobsPath(agentDir));
		const heartbeat = cronStore.createHeartbeat({
			activeSessionId: "deleted-worker",
			sessionId: sessionManager.getSessionId(),
			sessionFile,
			cwd: projectDir,
			scheduleText: "every 10s",
			prompt: "continue old work",
			now: new Date(Date.now() - 20_000),
		});

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const migratedStore = AgentCronJobStore.forSessionArtifacts();
		migratedStore.registerSessionArtifact(sessionManager.getSessionId(), sessionManager.getSessionArtifactDir()!);
		await waitForCondition(
			() => migratedStore.list().find((job) => job.id === heartbeat.id)?.status === "cancelled",
			"Orphan heartbeat was not cancelled",
		);

		expect(countWorkerDescriptors(agentDir)).toBe(0);
		const listed = await client.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(
			requireSessionList(listed.success ? listed.data : undefined).filter(
				(session) => session.activeSessionId || session.workerPid,
			),
		).toEqual([]);

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	});

	it("restarts an empty supervisor without requiring a resident worker", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(root, "custom-sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-restart-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir, ["--session-dir", sessionDir, "--no-tools"]);
		const client = await connectEventually(socketPath, supervisor);
		const restarted = await client.request({ type: "restart" });
		expect(restarted.success).toBe(true);
		client.close();
		await waitForExit(supervisor);
		children.delete(supervisor);

		const replacementClient = await connectEventually(socketPath);
		const listed = await replacementClient.request({ type: "list" });
		expect(listed.success).toBe(true);
		const persistedConfig = readSupervisorConfig(agentDir);
		expect(persistedConfig).toMatchObject({ defaultSessionConfig: { sessionDir } });
		expect(persistedConfig.defaultSessionConfig).not.toHaveProperty("noTools");
		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
	});

	it("survives a worker process spawn error", { tags: ["process-stress"] }, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const missingCwd = join(root, "missing-project");
		const socketPath = join(tmpdir(), `prime-supervisor-spawn-error-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const failed = await client.request({
			type: "create",
			config: { cwd: missingCwd, agentDir, noTools: true, noExtensions: true },
		});

		expect(failed).toMatchObject({ success: false });
		expect(supervisor.exitCode).toBeNull();
		const listed = await client.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(requireSessionList(listed.success ? listed.data : undefined)).toEqual([]);

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	});

	it("archives resident roots and cancels their heartbeats on explicit daemon shutdown", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-shutdown-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "stop with daemon", timestamp: 1 });
		sessionManager.appendSessionState({ status: "active" });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(summary.workerPid);
		const heartbeatResponse = await client.request({
			type: "heartbeat_set",
			activeSessionId: summary.activeSessionId ?? summary.id,
			schedule: "every 1h",
			prompt: "continue old work",
		});
		if (!heartbeatResponse.success || !heartbeatResponse.data || typeof heartbeatResponse.data !== "object") {
			throw new Error(heartbeatResponse.success ? "Heartbeat response was missing data" : heartbeatResponse.error);
		}
		const heartbeat = (heartbeatResponse.data as { heartbeat: { id: string } }).heartbeat;
		const cronStore = AgentCronJobStore.forSessionArtifacts();
		cronStore.registerSessionArtifact(summary.sessionId, sessionManager.getSessionArtifactDir()!);
		const observer = await connectEventually(socketPath, supervisor);
		const observerClosed = new Promise<Error>((resolveClose) => observer.onClose(resolveClose));

		expect((await client.request({ type: "shutdown" })).success).toBe(true);
		client.close();
		expect(getDaemonSocketCloseReason(await observerClosed)).toBe("shutdown");
		observer.close();
		await waitForSocketGone(socketPath);
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		expect(countWorkerDescriptors(agentDir)).toBe(0);
		expect((await readSessionInfo(sessionFile))?.state).toEqual({ status: "archived" });
		expect(cronStore.list().find((job) => job.id === heartbeat.id)).toMatchObject({ status: "cancelled" });

		const replacement = spawnSupervisor(agentDir, socketPath, projectDir);
		const replacementClient = await connectEventually(socketPath, replacement);
		const listed = await replacementClient.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(
			requireSessionList(listed.success ? listed.data : undefined).filter(
				(session) => session.activeSessionId || session.workerPid,
			),
		).toEqual([]);
		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
	}, 30_000);

	it("finalizes a timed-out worker stop by force-stopping the process and removing its registration", {
		tags: ["process-stress"],
		timeout: 45_000,
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(
			tmpdir(),
			`prime-supervisor-stop-finalize-${process.pid}-${randomUUID().slice(0, 8)}.sock`,
		);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "finalize me", timestamp: 1 });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			lifecycle: "client_owned",
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(summary.workerPid);
		const activeSessionId = summary.activeSessionId ?? summary.id;

		// A suspended worker cannot exit within the stop deadline, so the stop
		// times out and used to leave a tombstoned registration behind forever.
		process.kill(summary.workerPid, "SIGSTOP");
		const stopResult = await client.request({ type: "complete_owned_session", activeSessionId }, 30_000);
		expect(stopResult).toMatchObject({
			success: false,
			error: expect.stringContaining("did not stop"),
		});
		const tombstone = readWorkerDescriptor(agentDir);
		expect(tombstone.stopRequestedAt).toEqual(expect.any(String));

		// The supervisor finishes the interrupted stop on its own: it escalates
		// to SIGKILL, waits for the process to die, and removes the registration.
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForCondition(
			() => countWorkerDescriptors(agentDir) === 0,
			"Timed-out worker stop was not finalized",
			20_000,
		);

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
	});

	it("resumes a saved session immediately after a worker stop fails and the process dies", {
		tags: ["process-stress"],
		timeout: 45_000,
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-resume-heal-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "resume me", timestamp: 1 });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			lifecycle: "client_owned",
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(summary.workerPid);
		const activeSessionId = summary.activeSessionId ?? summary.id;

		// A suspended worker forces the stop past its deadline, leaving a
		// tombstoned registration for a process that dies moments later.
		process.kill(summary.workerPid, "SIGSTOP");
		const stopResult = await client.request({ type: "complete_owned_session", activeSessionId }, 30_000);
		expect(stopResult).toMatchObject({
			success: false,
			error: expect.stringContaining("did not stop"),
		});
		process.kill(summary.workerPid, "SIGKILL");
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);

		// Resuming the saved transcript must not be blocked by the stale
		// registration. Whichever cleanup wins the race — the background stop
		// finalizer or the resume-time reclaim (each covered deterministically
		// by unit tests) — the user-visible guarantee is the same: the resume
		// below must succeed with a fresh worker.
		const resumed = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		expect(resumed.success).toBe(true);
		const resumedSummary = requireSummary(resumed.success ? resumed.data : undefined);
		expect(resumedSummary.sessionId).toBe(summary.sessionId);
		expect(resumedSummary.workerPid).not.toBe(summary.workerPid);
		expect(resumedSummary.workerState).toBe("ready");
		if (resumedSummary.workerPid) {
			workerPids.add(resumedSummary.workerPid);
		}

		const attached = await client.request({
			type: "attach",
			activeSessionId: resumedSummary.activeSessionId ?? resumedSummary.id,
		});
		expect(attached.success).toBe(true);

		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
		if (resumedSummary.workerPid) {
			await waitForProcessGone(resumedSummary.workerPid);
			workerPids.delete(resumedSummary.workerPid);
		}
	});

	it("does not resurrect an intentionally stopped root when the supervisor dies during kill", {
		tags: ["process-stress"],
		timeout: 30_000,
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-stop-race-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "stop me", timestamp: 1 });
		sessionManager.appendSessionState({ status: "active" });
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const firstSupervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, firstSupervisor);
		const firstSupervisorPid = client.hello?.supervisorPid;
		if (!firstSupervisorPid) {
			throw new Error("Daemon hello did not expose its supervisor pid");
		}
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(summary.workerPid);
		const activeSessionId = summary.activeSessionId ?? summary.id;
		const heartbeatResponse = await client.request({
			type: "heartbeat_set",
			activeSessionId,
			schedule: "every 1h",
			prompt: "continue old work",
		});
		if (!heartbeatResponse.success || !heartbeatResponse.data || typeof heartbeatResponse.data !== "object") {
			throw new Error(heartbeatResponse.success ? "Heartbeat response was missing data" : heartbeatResponse.error);
		}
		const heartbeat = (heartbeatResponse.data as { heartbeat: { id: string } }).heartbeat;
		const cronStore = AgentCronJobStore.forSessionArtifacts();
		cronStore.registerSessionArtifact(summary.sessionId, sessionManager.getSessionArtifactDir()!);
		process.kill(summary.workerPid, "SIGSTOP");
		const killResult = client.request({ type: "kill", activeSessionId }).catch((error: unknown) => error);
		const tombstone = await waitForWorkerStopTombstone(agentDir);
		expect(tombstone.stopRequestedAt).toEqual(expect.any(String));
		expect(tombstone.archiveOnStop).toBe(true);

		process.kill(firstSupervisorPid, "SIGKILL");
		await waitForExit(firstSupervisor);
		children.delete(firstSupervisor);
		client.close();
		await expect(killResult).resolves.toBeInstanceOf(Error);

		const replacementSupervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const replacementClient = await connectEventually(socketPath, replacementSupervisor);
		const listed = await replacementClient.request({ type: "list" });
		expect(listed.success).toBe(true);
		const sessions = requireSessionList(listed.success ? listed.data : undefined);
		expect(sessions.filter((session) => session.activeSessionId || session.workerPid)).toEqual([]);
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
		await waitForCondition(
			() => countWorkerDescriptors(agentDir) === 0,
			"Intentional worker stop descriptor was not removed",
		);
		expect(countWorkerDescriptors(agentDir)).toBe(0);
		expect((await readSessionInfo(sessionFile))?.state).toEqual({ status: "archived" });
		expect(cronStore.list().find((job) => job.id === heartbeat.id)).toMatchObject({ status: "cancelled" });

		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
	});

	it("hosts and adopts isolated worker processes", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-smoke-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionFiles = Array.from({ length: 2 }, (_, index) => {
			const manager = SessionManager.create(projectDir, sessionDir);
			manager.appendMessage({ role: "user", content: `smoke root ${index}`, timestamp: index + 1 });
			const sessionFile = manager.getSessionFile();
			if (!sessionFile) {
				throw new Error("Fixture session did not persist");
			}
			return sessionFile;
		});

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await Promise.all(
			sessionFiles.map((sessionPath) =>
				client.request({
					type: "create",
					sessionPath,
					config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
				}),
			),
		);
		const summaries = created.map((response) => {
			if (!response.success) {
				throw new Error(response.error);
			}
			return requireSummary(response.data);
		});
		const pids = summaries.map((summary) => summary.workerPid);
		expect(new Set(pids).size).toBe(2);
		expect(pids).not.toContain(supervisor.pid);
		for (const pid of pids) {
			if (!pid) {
				throw new Error("Resident root did not expose a worker pid");
			}
			workerPids.add(pid);
		}

		const listed = await client.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(requireSessionList(listed.success ? listed.data : undefined)).toHaveLength(2);
		supervisor.kill("SIGTERM");
		await waitForExit(supervisor);
		children.delete(supervisor);
		client.close();

		const replacementClient = await connectEventually(socketPath);
		const adopted = await replacementClient.request({ type: "list" });
		expect(adopted.success).toBe(true);
		expect(
			new Set(requireSessionList(adopted.success ? adopted.data : undefined).map((summary) => summary.workerPid)),
		).toEqual(new Set(pids));
		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
		await Promise.all(
			pids.map(async (pid) => {
				if (pid) {
					await waitForProcessGone(pid);
					workerPids.delete(pid);
				}
			}),
		);
	}, 60_000);

	it("hosts resident roots in isolated worker processes without a session cap", {
		tags: ["process-stress"],
		timeout: 180_000,
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-many-roots-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionFiles = Array.from({ length: PROCESS_STRESS_WORKERS }, (_, index) => {
			const manager = SessionManager.create(projectDir, sessionDir);
			manager.appendMessage({ role: "user", content: `root ${index}`, timestamp: index + 1 });
			const sessionFile = manager.getSessionFile();
			if (!sessionFile) {
				throw new Error("Fixture session did not persist");
			}
			return sessionFile;
		});

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const externalLease = acquireSessionLease(sessionFiles[0], agentDir, {
			[SESSION_LEASES_ENABLED_ENV]: "1",
			[SESSION_LEASE_OWNER_ID_ENV]: "external-owner",
		});
		const conflict = await client.request({
			type: "create",
			sessionPath: sessionFiles[0],
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		expect(conflict).toMatchObject({
			success: false,
			errorInfo: { code: "session_already_active", activeSessionId: "external-owner" },
		});
		const emptyAfterConflict = await client.request({ type: "list" });
		expect(requireSessionList(emptyAfterConflict.success ? emptyAfterConflict.data : undefined)).toHaveLength(0);
		externalLease?.release();
		const created = await Promise.all(
			sessionFiles.map((sessionPath) =>
				client.request({
					type: "create",
					sessionPath,
					config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
				}),
			),
		);
		const summaries = created.map((response) => {
			if (!response.success) {
				throw new Error(response.error);
			}
			return requireSummary(response.data);
		});
		const pids = summaries.map((summary) => summary.workerPid);
		expect(new Set(pids).size).toBe(PROCESS_STRESS_WORKERS);
		expect(pids).not.toContain(supervisor.pid);
		for (const pid of pids) {
			if (!pid) {
				throw new Error("Resident root did not expose a worker pid");
			}
			workerPids.add(pid);
		}
		const firstActiveSessionId = summaries[0]!.activeSessionId ?? summaries[0]!.id;
		const addedCron = await client.request({
			type: "cron_add",
			activeSessionId: firstActiveSessionId,
			schedule: "every 1h",
			prompt: "check status",
		});
		expect(addedCron.success).toBe(true);
		const cronJob = (addedCron.success ? addedCron.data : undefined) as { job?: { id?: string } } | undefined;
		if (!cronJob?.job?.id) {
			throw new Error("Supervisor did not persist the cron job");
		}
		const listedCron = await client.request({ type: "cron_list", activeSessionId: firstActiveSessionId });
		expect(listedCron).toMatchObject({ success: true, data: { jobs: [{ id: cronJob.job.id }] } });
		const cancelledCron = await client.request({ type: "cron_cancel", jobId: cronJob.job.id });
		expect(cancelledCron.success).toBe(true);
		const activeSessionIds = summaries.map((summary) => summary.activeSessionId ?? summary.id);
		await Promise.all(
			activeSessionIds.map((activeSessionId, index) =>
				startBlockingBash(client, activeSessionId, join(root, `stress-blocker-${index}.ready`)),
			),
		);
		const heartbeats = await Promise.all(
			activeSessionIds.map((activeSessionId, index) =>
				client.request({
					type: "heartbeat_set",
					activeSessionId,
					schedule: "every 10s",
					prompt: `heartbeat ${index}`,
				}),
			),
		);
		expect(heartbeats.every((response) => response.success)).toBe(true);
		await waitForCondition(
			() => {
				const stores = summaries.map((summary, index) => {
					const store = AgentCronJobStore.forSessionArtifacts();
					store.registerSessionArtifact(
						summary.sessionId,
						join(dirname(dirname(sessionFiles[index]!)), "session-artifacts", summary.sessionId),
					);
					return store;
				});
				return stores.every((store) => store.list().some((job) => job.lastSkippedAt !== undefined));
			},
			"Session workers did not advance their heartbeats independently",
			15_000,
		);

		const listed = await client.request({ type: "list" });
		expect(listed.success).toBe(true);
		expect(requireSessionList(listed.success ? listed.data : undefined)).toHaveLength(PROCESS_STRESS_WORKERS);
		supervisor.kill("SIGTERM");
		await waitForExit(supervisor);
		children.delete(supervisor);
		client.close();
		const replacementClient = await connectEventually(socketPath);
		const adopted = await replacementClient.request({ type: "list" });
		expect(adopted.success).toBe(true);
		expect(
			new Set(requireSessionList(adopted.success ? adopted.data : undefined).map((summary) => summary.workerPid)),
		).toEqual(new Set(pids));
		await replacementClient.request({ type: "shutdown" });
		replacementClient.close();
		await waitForSocketGone(socketPath);
		await Promise.all(
			pids.map(async (pid) => {
				if (pid) {
					await waitForProcessGone(pid);
					workerPids.delete(pid);
				}
			}),
		);
	});

	it("isolates a root, streams a chunked snapshot, and adopts the same worker after restart", async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-supervisor-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		const largePrompt = `large:${"x".repeat(600 * 1024)}`;
		sessionManager.appendMessage({ role: "user", content: largePrompt, timestamp: 1 });
		sessionManager.appendMessage({
			role: "assistant",
			content: [{ type: "text", text: "done" }],
			api: "openai-responses",
			provider: "faux",
			model: "faux",
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
		});
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) {
			throw new Error("Fixture session did not persist");
		}

		const firstSupervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, firstSupervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: {
				cwd: projectDir,
				agentDir,
				sessionDir,
				noTools: true,
				noExtensions: true,
			},
		});
		if (!created.success) {
			const diagnostics = childDiagnostics.get(firstSupervisor);
			throw new Error(
				`${created.error}\nsupervisor stdout:\n${diagnostics?.stdout ?? ""}\nsupervisor stderr:\n${diagnostics?.stderr ?? ""}\n${readDaemonLogs(agentDir)}`,
			);
		}
		expect(created.success).toBe(true);
		const createdSummary = requireSummary(created.data);
		expect(createdSummary.workerState).toBe("ready");
		expect(createdSummary.workerPid).not.toBe(firstSupervisor.pid);
		if (!createdSummary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(createdSummary.workerPid);

		const connection = await DaemonAgentConnection.attach(
			client,
			createdSummary.activeSessionId ?? createdSummary.id,
			{ recoverDaemon: async () => {} },
		);
		const connectionEvents: string[] = [];
		const replacementMessageCounts: number[] = [];
		connection.subscribe((event) => {
			connectionEvents.push(event.type === "connection_status" ? `${event.type}:${event.status}` : event.type);
			if (event.type === "session_replaced") {
				replacementMessageCounts.push(event.messages.length);
			}
		});
		const snapshot = await connection.getInitialSnapshot();
		expect(snapshot.messages).toHaveLength(2);
		expect(snapshot.messages[0]).toMatchObject({ role: "user", content: largePrompt });

		const activeSessionId = createdSummary.activeSessionId ?? createdSummary.id;
		const createdNew = await client.request({ type: "new_session", activeSessionId });
		expect(createdNew.success).toBe(true);
		const emptyReplacementDeadline = Date.now() + 5000;
		while (!replacementMessageCounts.includes(0) && Date.now() < emptyReplacementDeadline) {
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
		}
		expect(replacementMessageCounts).toContain(0);
		const switchedBack = await client.request({ type: "switch_session", activeSessionId, sessionPath: sessionFile });
		expect(switchedBack.success).toBe(true);
		const restoredReplacementDeadline = Date.now() + 5000;
		while (replacementMessageCounts.at(-1) !== 2 && Date.now() < restoredReplacementDeadline) {
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
		}
		expect(replacementMessageCounts.at(-1)).toBe(2);

		firstSupervisor.kill("SIGTERM");
		await waitForExit(firstSupervisor);
		children.delete(firstSupervisor);

		const reconnectDeadline = Date.now() + 15_000;
		while (!connectionEvents.includes("connection_status:connected") && Date.now() < reconnectDeadline) {
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
		}
		expect(connectionEvents).toContain("connection_status:reconnecting");
		expect(connectionEvents).toContain("session_resynced");
		expect(connectionEvents).toContain("connection_status:connected");
		expect(connectionEvents).not.toContain("closed");
		await expect(connection.getState()).resolves.toMatchObject({
			activeSessionId: createdSummary.activeSessionId,
			sessionId: createdSummary.sessionId,
		});
		const listed = await client.request({ type: "list", all: true, sessionDir });
		expect(listed.success).toBe(true);
		if (!listed.success) {
			throw new Error(listed.error);
		}
		const adopted = requireSessionList(listed.data).find(
			(summary) => (summary.activeSessionId ?? summary.id) === (createdSummary.activeSessionId ?? createdSummary.id),
		);
		expect(adopted).toMatchObject({
			workerState: "ready",
			workerPid: createdSummary.workerPid,
		});

		const descriptor = readWorkerDescriptor(agentDir);
		await startBlockingBash(client, activeSessionId, join(root, "orphan-blocker.ready"));
		if (!descriptor.orphanProcessJournalPath) {
			throw new Error("Resident worker did not persist its orphan-process journal path");
		}
		let orphanPids: number[] = [];
		const orphanDeadline = Date.now() + 5000;
		while (orphanPids.length === 0 && Date.now() < orphanDeadline) {
			orphanPids = readActiveOrphanProcesses(descriptor.orphanProcessJournalPath, descriptor.pid).map(
				(orphan) => orphan.pid,
			);
			if (orphanPids.length === 0) {
				await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
			}
		}
		expect(orphanPids.length).toBeGreaterThan(0);
		for (const pid of orphanPids) {
			workerPids.add(pid);
		}
		process.kill(createdSummary.workerPid, "SIGKILL");
		await waitForProcessGone(createdSummary.workerPid);
		workerPids.delete(createdSummary.workerPid);
		await Promise.all(orphanPids.map((pid) => waitForProcessGone(pid)));
		for (const pid of orphanPids) {
			workerPids.delete(pid);
		}

		let failed: SessionSummary | undefined;
		const recoveryDeadline = Date.now() + 20_000;
		while (Date.now() < recoveryDeadline) {
			const response = await client.request({ type: "list" });
			if (response.success) {
				failed = requireSessionList(response.data).find(
					(summary) =>
						(summary.activeSessionId ?? summary.id) === (createdSummary.activeSessionId ?? createdSummary.id),
				);
				if (failed?.workerState === "failed") break;
			}
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
		}
		expect(failed).toMatchObject({ workerState: "failed", activeSessionId: createdSummary.activeSessionId });
		await connection.dispose();

		const reopened = await client.request({
			type: "create",
			sessionPath: sessionFile,
			continueRecent: false,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
			launchEnv: { PRIME_AGENT_TEST_FRESH_CONTEXT: "1" },
		});
		if (!reopened.success) throw new Error(reopened.error);
		const recovered = requireSummary(reopened.data);
		if (!recovered.workerPid) throw new Error("Recovered worker did not expose its pid");
		workerPids.add(recovered.workerPid);
		const recoveredConnection = await DaemonAgentConnection.attach(
			client,
			recovered.activeSessionId ?? recovered.id,
			{ recoverDaemon: async () => {} },
		);
		const recoveredSnapshot = await recoveredConnection.getInitialSnapshot();
		expect(recoveredSnapshot.messages).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ role: "user" }),
				expect.objectContaining({ role: "assistant" }),
			]),
		);
		await expect(recoveredConnection.getState()).resolves.toMatchObject({ sessionId: createdSummary.sessionId });

		await recoveredConnection.dispose();
		await client.request({ type: "shutdown" });
		client.close();
		await waitForSocketGone(socketPath);
		await waitForProcessGone(recovered.workerPid);
		workerPids.delete(recovered.workerPid);
	});

	it("runs a session-artifact cron job while the supervisor is being replaced", {
		tags: ["process-stress"],
		timeout: 30_000,
	}, async () => {
		const root = tempDir();
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-worker-cron-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		sessionManager.appendMessage({ role: "user", content: "scheduled work", timestamp: 1 });
		sessionManager.appendSessionState({ status: "active" });
		const sessionFile = sessionManager.getSessionFile();
		const artifactDir = sessionManager.getSessionArtifactDir();
		if (!sessionFile || !artifactDir) {
			throw new Error("Fixture session did not persist");
		}

		const supervisor = spawnSupervisor(agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success) {
			throw new Error(created.error);
		}
		const summary = requireSummary(created.data);
		if (!summary.workerPid) {
			throw new Error("Resident worker did not expose its pid");
		}
		workerPids.add(summary.workerPid);
		await startBlockingBash(client, summary.activeSessionId ?? summary.id, join(root, "heartbeat-blocker.ready"));
		const scheduled = await client.request({
			type: "heartbeat_set",
			activeSessionId: summary.activeSessionId ?? summary.id,
			schedule: "every 10s",
			prompt: "continue without the supervisor",
		});
		if (!scheduled.success || !scheduled.data || typeof scheduled.data !== "object") {
			throw new Error(scheduled.success ? "Heartbeat response was missing its job" : scheduled.error);
		}
		const job = (scheduled.data as { heartbeat: { id: string } }).heartbeat;

		client.close();
		supervisor.kill("SIGKILL");
		await waitForExit(supervisor);
		children.delete(supervisor);

		const store = AgentCronJobStore.forSessionArtifacts();
		store.registerSessionArtifact(sessionManager.getSessionId(), artifactDir);
		await waitForCondition(
			() => store.list().find((candidate) => candidate.id === job.id)?.lastSkippedAt !== undefined,
			"Resident worker did not advance its heartbeat without the supervisor",
			15_000,
		);
		expect(store.list().find((candidate) => candidate.id === job.id)).toBeDefined();

		const replacement = await connectEventually(socketPath);
		await replacement.request({ type: "shutdown" });
		replacement.close();
		await waitForSocketGone(socketPath);
		await waitForProcessGone(summary.workerPid);
		workerPids.delete(summary.workerPid);
	});
});
