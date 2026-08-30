import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ENV_AGENT_DIR } from "../src/config.js";
import { SessionManager } from "../src/core/session-manager.js";
import { DaemonAgentConnection } from "../src/modes/agent-connection/daemon-agent-connection.js";
import { DaemonClient } from "../src/modes/daemon/daemon-client.js";
import type { SessionSummary } from "../src/modes/daemon/daemon-session-list.js";

/**
 * Run with PRIME_AGENT_HISTORICAL_081_CLI=<v0.8.1 dist/bundle/cli.js>.
 * The artifact must be built from the pinned git tag v0.8.1, not a protocol mock.
 */
const historicalCli = process.env.PRIME_AGENT_HISTORICAL_081_CLI;
const currentCli = resolve(__dirname, "../dist/bundle/cli.js");
const compiledCli = process.env.PRIME_AGENT_COMPILED_CLI;
const processTests = historicalCli ? describe : describe.skip;
const children = new Set<ChildProcess>();
const workerPids = new Set<number>();
const roots: string[] = [];

function scrubbedEnvironment(agentDir: string): NodeJS.ProcessEnv {
	const environment = { ...process.env, [ENV_AGENT_DIR]: agentDir, PI_OFFLINE: "1" };
	for (const name of [
		"PRIME_AGENT_INTERNAL_DAEMON_WORKER",
		"PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN",
		"PRIME_AGENT_INTERNAL_DAEMON_WORKER_ACTIVE_SESSION_ID",
		"PRIME_AGENT_INTERNAL_DAEMON_SUPERVISOR_SOCKET",
		"PRIME_AGENT_INTERNAL_DAEMON_WORKER_RECOVERY_JOURNAL",
	]) {
		delete environment[name];
	}
	return environment;
}

function launch(cli: string, agentDir: string, socketPath: string, cwd: string): ChildProcess {
	const child = spawn(process.execPath, [cli, "--mode", "daemon", "--daemon-socket", socketPath, "--offline"], {
		cwd,
		env: scrubbedEnvironment(agentDir),
		stdio: ["ignore", "ignore", "pipe"],
	});
	children.add(child);
	return child;
}

async function connectEventually(socketPath: string, process?: ChildProcess): Promise<DaemonClient> {
	const deadline = Date.now() + 15_000;
	let lastError: unknown;
	while (Date.now() < deadline) {
		if (process && (process.exitCode !== null || process.signalCode !== null)) {
			throw new Error(`Compatibility supervisor exited early (${process.exitCode ?? process.signalCode})`);
		}
		const client = new DaemonClient(socketPath);
		try {
			await client.connect(250);
			await client.waitForHello(1_000);
			return client;
		} catch (error) {
			lastError = error;
			client.close();
			await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
		}
	}
	throw new Error(`Compatibility supervisor did not become ready: ${String(lastError)}`);
}

async function waitForSupervisorExit(child: ChildProcess, timeoutMs: number): Promise<void> {
	if (child.exitCode !== null || child.signalCode !== null) return;
	await new Promise<void>((resolveExit, reject) => {
		const timeout = setTimeout(() => reject(new Error("Compatibility supervisor did not exit")), timeoutMs);
		child.once("exit", () => {
			clearTimeout(timeout);
			resolveExit();
		});
	});
}

async function stopSupervisor(child: ChildProcess): Promise<void> {
	if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
	await waitForSupervisorExit(child, 10_000);
	children.delete(child);
}

function summaries(data: unknown): SessionSummary[] {
	if (!data || typeof data !== "object" || !("sessions" in data)) throw new Error("Missing session list");
	const sessions = (data as { sessions: unknown }).sessions;
	if (!Array.isArray(sessions)) throw new Error("Invalid session list");
	return sessions as SessionSummary[];
}

function workerDescriptorFiles(agentDir: string): string[] {
	const workersRoot = join(agentDir, "daemon-workers");
	if (!existsSync(workersRoot)) return [];
	return readdirSync(workersRoot, { recursive: true })
		.map(String)
		.filter((path) => path.endsWith(".json"));
}

function isProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
		throw error;
	}
}

async function waitForProcessExit(pid: number, timeoutMs: number): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (isProcessAlive(pid)) {
		if (Date.now() >= deadline) throw new Error(`Compatibility worker ${pid} did not exit`);
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
	}
}

async function stopWorkerProcess(pid: number): Promise<void> {
	try {
		await waitForProcessExit(pid, 250);
		return;
	} catch {
		// Test-only fallback after the natural shutdown assertion has already failed or been skipped.
	}
	try {
		process.kill(-pid, "SIGTERM");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
		try {
			process.kill(pid, "SIGTERM");
		} catch (pidError) {
			if ((pidError as NodeJS.ErrnoException).code === "ESRCH") return;
			throw pidError;
		}
	}
	try {
		await waitForProcessExit(pid, 2_000);
		return;
	} catch {
		try {
			process.kill(-pid, "SIGKILL");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
			process.kill(pid, "SIGKILL");
		}
		await waitForProcessExit(pid, 2_000);
	}
}

afterEach(async () => {
	for (const child of [...children]) {
		try {
			await stopSupervisor(child);
		} catch {
			if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
		}
	}
	children.clear();
	for (const pid of workerPids) await stopWorkerProcess(pid);
	workerPids.clear();
	for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

processTests("stock v0.8.1 daemon entrypoint compatibility", () => {
	it.each([
		["new supervisor adopts a stock 0.8.1 worker", historicalCli!, currentCli],
		["stock 0.8.1 supervisor adopts a new worker", currentCli, historicalCli!],
	] as const)(
		"%s",
		async (_name, firstCli, replacementCli) => {
			const root = mkdtempSync(join(tmpdir(), "prime-081-compat-"));
			roots.push(root);
			const agentDir = join(root, "agent");
			const projectDir = join(root, "project");
			const socketPath = join(tmpdir(), `prime-081-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
			mkdirSync(projectDir, { recursive: true });

			const firstSupervisor = launch(firstCli, agentDir, socketPath, projectDir);
			const firstClient = await connectEventually(socketPath, firstSupervisor);
			const created = await firstClient.request({
				type: "create",
				noSession: true,
				config: { cwd: projectDir, agentDir, noTools: true, noExtensions: true },
			});
			if (!created.success || !created.data || typeof created.data !== "object") {
				throw new Error(created.success ? "Compatibility create omitted its summary" : created.error);
			}
			const createdSummary = created.data as SessionSummary;
			if (!createdSummary.activeSessionId || !createdSummary.workerPid)
				throw new Error("Compatibility worker was incomplete");
			workerPids.add(createdSummary.workerPid);
			firstClient.close();
			await stopSupervisor(firstSupervisor);

			const replacement = launch(replacementCli, agentDir, socketPath, projectDir);
			const client = await connectEventually(socketPath, replacement);
			const listed = await client.request({ type: "list" });
			if (!listed.success) throw new Error(listed.error);
			const adopted = summaries(listed.data).find(
				(summary) => summary.activeSessionId === createdSummary.activeSessionId,
			);
			expect(adopted?.workerPid).toBe(createdSummary.workerPid);
			const connection = await DaemonAgentConnection.attach(client, createdSummary.activeSessionId, {
				supportsExtensionUi: false,
			});
			await expect(connection.getInitialSnapshot()).resolves.toMatchObject({
				state: { activeSessionId: createdSummary.activeSessionId },
			});
			await connection.dispose();
			await client.request({ type: "shutdown" });
			client.close();
			await waitForSupervisorExit(replacement, 10_000);
			children.delete(replacement);
			await waitForProcessExit(createdSummary.workerPid, 10_000);
			expect(workerDescriptorFiles(agentDir)).toEqual([]);
			workerPids.delete(createdSummary.workerPid);
		},
		60_000,
	);
});

const performanceTests = compiledCli ? describe : describe.skip;

function residentSetBytes(pid: number): number {
	const sampled = spawnSync("ps", ["-o", "rss=", "-p", String(pid)], { encoding: "utf8" });
	if (sampled.status !== 0) return 0;
	return Number.parseInt(sampled.stdout.trim(), 10) * 1024;
}

performanceTests("compiled snapshot process performance", () => {
	it("bounds a 36 MiB worker-to-supervisor-to-public transfer", async () => {
		const root = mkdtempSync(join(tmpdir(), "prime-compiled-snapshot-"));
		roots.push(root);
		const agentDir = join(root, "agent");
		const projectDir = join(root, "project");
		const sessionDir = join(agentDir, "sessions");
		const socketPath = join(tmpdir(), `prime-compiled-${process.pid}-${randomUUID().slice(0, 8)}.sock`);
		mkdirSync(projectDir, { recursive: true });
		const sessionManager = SessionManager.create(projectDir, sessionDir);
		const largePrompt = `compiled:${"x".repeat(36 * 1024 * 1024)}`;
		sessionManager.appendMessage({ role: "user", content: largePrompt, timestamp: 1 });
		sessionManager.appendMessage({
			role: "assistant",
			content: [{ type: "text", text: "complete" }],
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
		if (!sessionFile) throw new Error("Compiled snapshot fixture did not persist");

		const supervisor = launch(compiledCli!, agentDir, socketPath, projectDir);
		const client = await connectEventually(socketPath, supervisor);
		const created = await client.request({
			type: "create",
			sessionPath: sessionFile,
			config: { cwd: projectDir, agentDir, sessionDir, noTools: true, noExtensions: true },
		});
		if (!created.success || !created.data || typeof created.data !== "object") {
			throw new Error(created.success ? "Compiled create omitted its summary" : created.error);
		}
		const summary = created.data as SessionSummary;
		if (!summary.activeSessionId || !summary.workerPid || !supervisor.pid) {
			throw new Error("Compiled snapshot processes were incomplete");
		}
		const baselineRss = residentSetBytes(supervisor.pid) + residentSetBytes(summary.workerPid);
		let peakRss = baselineRss;
		let maxEventLoopDelayMs = 0;
		let previousTick = performance.now();
		const monitor = setInterval(() => {
			const current = performance.now();
			maxEventLoopDelayMs = Math.max(maxEventLoopDelayMs, current - previousTick - 25);
			previousTick = current;
			peakRss = Math.max(peakRss, residentSetBytes(supervisor.pid!) + residentSetBytes(summary.workerPid!));
		}, 25);
		try {
			const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
				supportsExtensionUi: false,
			});
			const snapshot = await connection.getInitialSnapshot();
			expect(snapshot.messages[0]).toMatchObject({ role: "user", content: largePrompt });
			await connection.dispose();
		} finally {
			clearInterval(monitor);
		}
		expect(peakRss - baselineRss).toBeLessThan(192 * 1024 * 1024);
		expect(maxEventLoopDelayMs).toBeLessThan(500);
		await client.request({ type: "shutdown" });
		client.close();
		await stopSupervisor(supervisor);
	}, 120_000);
});
