#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
	releaseAssetFile,
	npmInvocation,
	PYLON_RELEASE_PACKAGES,
	run,
} from "./lib/pylon-release.mjs";
import { verifyPylonPrimeAgentRelease } from "./verify-pylon-prime-agent-release.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultArtifacts = join(root, ".npm", "pylon-release", "artifacts");
const MAX_DIAGNOSTICS = 64 * 1024;
export const PYLON_RELEASE_EXPECTED_SDK_FEATURES = Object.freeze([
	"bounded_daemon_ingress_v1",
	"negotiated_daemon_session_capabilities_v1",
	"caller_owned_session_environment_cleanup_v1",
]);

function parseArgs(args) {
	if (args.length === 0) return defaultArtifacts;
	if (args.length === 2 && args[0] === "--artifact-dir") return resolve(root, args[1]);
	throw new Error("Usage: node scripts/smoke-pylon-prime-agent-release.mjs [--artifact-dir path]");
}

export function releaseInstallTimeoutMs(platform = process.platform) {
	return platform === "win32" ? 360_000 : 180_000;
}

function runCli(command, args, options = {}) {
	return spawnSync(command, args, {
		cwd: options.cwd,
		env: options.env,
		shell: false,
		encoding: "utf8",
		stdio: "pipe",
		timeout: options.timeoutMs ?? 30_000,
		maxBuffer: options.maxBuffer ?? MAX_DIAGNOSTICS,
	});
}

function diagnostics(child, limit = MAX_DIAGNOSTICS) {
	const state = { stdout: "", stderr: "", overflow: false, error: undefined };
	const append = (key, chunk) => {
		const next = state[key] + chunk.toString("utf8");
		if (Buffer.byteLength(next) > limit) {
			state[key] = next.slice(0, limit);
			state.overflow = true;
			return;
		}
		state[key] = next;
	};
	child.stdout?.on("data", (chunk) => append("stdout", chunk));
	child.stderr?.on("data", (chunk) => append("stderr", chunk));
	child.on("error", (error) => {
		state.error = error;
	});
	return state;
}

function waitForExit(child, timeoutMs) {
	if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true);
	return new Promise((resolveWait) => {
		let settled = false;
		const finish = (result) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			child.off("close", onClose);
			resolveWait(result);
		};
		const onClose = () => finish(true);
		const timer = setTimeout(() => finish(false), timeoutMs);
		child.once("close", onClose);
	});
}

async function terminateCapturedChild(child) {
	if (child.exitCode !== null || child.signalCode !== null) return;
	child.kill("SIGTERM");
	if (await waitForExit(child, 3_000)) return;
	child.kill("SIGKILL");
	if (!(await waitForExit(child, 3_000))) {
		const error = new Error(`Captured child ${child.pid ?? "unknown"} did not exit.`);
		error.preserveTempRoot = true;
		throw error;
	}
}

function isProcessAlive(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error && typeof error === "object" && error.code === "EPERM";
	}
}

function getProcessStartId(pid) {
	if (!Number.isSafeInteger(pid) || pid <= 1) return undefined;
	if (process.platform === "win32") {
		const result = runCli(
			"powershell.exe",
			[
				"-NoLogo",
				"-NoProfile",
				"-NonInteractive",
				"-Command",
				`([System.Diagnostics.Process]::GetProcessById(${pid})).StartTime.ToUniversalTime().Ticks`,
			],
			{ timeoutMs: 2_000, maxBuffer: 4096 },
		);
		const ticks = result.status === 0 && !result.error ? result.stdout.trim() : "";
		return /^\d+$/.test(ticks) ? `win:${ticks}` : undefined;
	}
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const commandEnd = stat.lastIndexOf(")");
		const startTime = stat.slice(commandEnd + 2).split(" ")[19];
		if (startTime) return `proc:${startTime}`;
	} catch {}
	const result = runCli("ps", ["-p", String(pid), "-o", "lstart="], { timeoutMs: 2_000, maxBuffer: 4096 });
	const startTime = result.status === 0 && !result.error ? result.stdout.trim() : "";
	return startTime ? `ps:${startTime}` : undefined;
}

function unsafeCleanupError(message) {
	const error = new Error(message);
	error.preserveTempRoot = true;
	return error;
}

function classifyTrackedIdentity(identity) {
	if (!isProcessAlive(identity.pid)) return "gone";
	const observed = getProcessStartId(identity.pid);
	if (!observed) return "unknown";
	return observed === identity.processStartId ? "current" : "reused";
}

async function waitForTrackedIdentityChange(identity, timeoutMs) {
	const deadline = Date.now() + timeoutMs;
	let state = classifyTrackedIdentity(identity);
	while (state === "current" && Date.now() < deadline) {
		await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
		state = classifyTrackedIdentity(identity);
	}
	return state;
}

async function terminateTrackedIdentity(identity) {
	if (!Number.isSafeInteger(identity.pid) || identity.pid <= 1 || typeof identity.processStartId !== "string") {
		throw unsafeCleanupError("Tracked process identity was incomplete; refusing to signal a bare PID.");
	}
	let state = classifyTrackedIdentity(identity);
	if (state === "gone" || state === "reused") return;
	if (state === "unknown") {
		throw unsafeCleanupError(`Could not prove the start identity of tracked process ${identity.pid}.`);
	}
	try {
		if (getProcessStartId(identity.pid) !== identity.processStartId) return;
		process.kill(identity.pid, "SIGTERM");
	} catch (error) {
		if (error && typeof error === "object" && error.code === "ESRCH") return;
		throw unsafeCleanupError(`Could not stop tracked process ${identity.pid}.`);
	}
	state = await waitForTrackedIdentityChange(identity, 3_000);
	if (state === "gone" || state === "reused") return;
	if (state === "unknown") {
		throw unsafeCleanupError(`Lost the start identity of tracked process ${identity.pid} after SIGTERM.`);
	}
	try {
		if (getProcessStartId(identity.pid) !== identity.processStartId) return;
		process.kill(identity.pid, "SIGKILL");
	} catch (error) {
		if (error && typeof error === "object" && error.code === "ESRCH") return;
		throw unsafeCleanupError(`Could not kill tracked process ${identity.pid}.`);
	}
	state = await waitForTrackedIdentityChange(identity, 3_000);
	if (state === "gone" || state === "reused") return;
	throw unsafeCleanupError(`Tracked process ${identity.pid} did not exit with its original start identity.`);
}

function recordedWorkerIdentities(agentDir) {
	const identities = new Map();
	const unprovedPids = new Set();
	const record = (value) => {
		if (!value || typeof value !== "object" || !Number.isSafeInteger(value.pid) || value.pid <= 1) return;
		if (typeof value.processStartId === "string" && value.processStartId) {
			identities.set(`${value.pid}:${value.processStartId}`, {
				pid: value.pid,
				processStartId: value.processStartId,
			});
		} else {
			unprovedPids.add(value.pid);
		}
	};
	const workersRoot = join(agentDir, "daemon-workers");
	const visit = (directory) => {
		if (!existsSync(directory)) return;
		for (const entry of readdirSync(directory, { withFileTypes: true })) {
			const path = join(directory, entry.name);
			if (entry.isDirectory()) visit(path);
			else if (entry.isFile() && entry.name.endsWith(".json")) {
				try {
					record(JSON.parse(readFileSync(path, "utf8")));
				} catch {}
			}
		}
	};
	visit(workersRoot);
	return { identities: [...identities.values()], unprovedPids: [...unprovedPids] };
}

async function collectChild(child, timeoutMs, limit = MAX_DIAGNOSTICS) {
	const output = diagnostics(child, limit);
	const exited = await waitForExit(child, timeoutMs);
	if (!exited) {
		await terminateCapturedChild(child);
		throw new Error(`Captured child timed out after ${timeoutMs} ms.\n${output.stderr}`);
	}
	if (output.error) throw output.error;
	if (output.overflow) throw new Error("Captured child exceeded the diagnostics limit.");
	return { status: child.exitCode, signal: child.signalCode, stdout: output.stdout, stderr: output.stderr };
}

function cleanRuntimeEnv({ home, agentDir, sessionDir, packageDir }) {
	const env = { ...process.env };
	for (const name of Object.keys(env)) {
		if (
			name.startsWith("PRIME_AGENT_INTERNAL_DAEMON_") ||
			name.startsWith("RLM_") ||
			name === "FORCE_COLOR" ||
			name === "NO_COLOR"
		) {
			delete env[name];
		}
	}
	return {
		...env,
		HOME: home,
		USERPROFILE: home,
		PRIME_AGENT_CODING_AGENT_DIR: agentDir,
		PRIME_AGENT_SESSION_DIR: sessionDir,
		PI_PACKAGE_DIR: packageDir,
		PI_OFFLINE: "1",
		PI_SKIP_VERSION_CHECK: "1",
		PRIME_AGENT_TELEMETRY: "0",
		DO_NOT_TRACK: "1",
		npm_config_offline: "true",
		npm_config_ignore_scripts: "true",
		npm_config_cache: join(home, "npm-cache"),
		npm_config_prefix: join(home, "npm-prefix"),
	};
}

function writeRuntimeFixture(tempRoot, packageDir) {
	const home = join(tempRoot, "home");
	const agentDir = join(tempRoot, "agent");
	const sessionDir = join(tempRoot, "sessions");
	const projectDir = join(tempRoot, "project");
	for (const path of [home, agentDir, sessionDir, projectDir]) mkdirSync(path, { recursive: true });
	const fakeNpmPath = join(tempRoot, "fake-npm.cjs");
	const fakeNpmRecord = join(tempRoot, "unexpected-npm-invocation.json");
	writeFileSync(
		fakeNpmPath,
		`const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
const record = ${JSON.stringify(fakeNpmRecord)};
const calls = fs.existsSync(record) ? JSON.parse(fs.readFileSync(record, "utf8")) : [];
calls.push(args);
fs.writeFileSync(record, JSON.stringify(calls));
if (args.includes("root")) {
  console.log(path.join(${JSON.stringify(tempRoot)}, "fake-npm-root"));
  process.exit(0);
}
process.exit(91);
`,
	);
	writeFileSync(
		join(agentDir, "settings.json"),
		`${JSON.stringify({ npmCommand: [process.execPath, fakeNpmPath, "--prefix", join(tempRoot, "fake-prefix")] }, null, "\t")}\n`,
	);
	writeFileSync(
		join(agentDir, "models.json"),
		`${JSON.stringify({
			providers: {
				"artifact-faux": {
					baseUrl: "http://127.0.0.1:0",
					api: "faux",
					apiKey: "fixture-key",
					models: [
						{
							id: "artifact-faux",
							name: "Artifact Faux",
							reasoning: false,
							input: ["text"],
							cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
							contextWindow: 4096,
							maxTokens: 256,
						},
					],
				},
			},
		}, null, "\t")}\n`,
	);
	return {
		home,
		agentDir,
		sessionDir,
		projectDir,
		fakeNpmRecord,
		env: cleanRuntimeEnv({ home, agentDir, sessionDir, packageDir }),
	};
}

function staticSdkImport() {
	return 'import * as sdk from "prime-agent";\n';
}

async function smokeSdkIdentity({ probesDir, cwd, env }) {
	const probePath = join(probesDir, "sdk-identity.mjs");
	writeFileSync(
		probePath,
		`${staticSdkImport()}
const expected = ${JSON.stringify(PYLON_RELEASE_EXPECTED_SDK_FEATURES)};
if (!Object.isFrozen(sdk.PRIME_AGENT_SDK_FEATURES)) throw new Error("SDK feature tokens are not frozen.");
if (JSON.stringify([...sdk.PRIME_AGENT_SDK_FEATURES]) !== JSON.stringify(expected)) throw new Error("Unexpected SDK feature tokens.");
if (typeof sdk.DaemonClient !== "function" || typeof sdk.DaemonAgentConnection !== "function") throw new Error("Missing daemon SDK constructors.");
console.log(JSON.stringify({ features: [...sdk.PRIME_AGENT_SDK_FEATURES] }));
`,
	);
	const child = spawn(process.execPath, [probePath], { cwd, env, stdio: ["ignore", "pipe", "pipe"], shell: false });
	const result = await collectChild(child, 15_000);
	if (result.status !== 0) throw new Error(`Installed SDK identity smoke failed.\n${result.stderr || result.stdout}`);
}

async function smokePosixDaemon({ tempRoot, probesDir, cliEntry, fixture, expectedBuildId }) {
	const socketPath = join(tempRoot, "daemon.sock");
	const workerReceiptPath = join(tempRoot, "worker-receipt.json");
	const daemon = spawn(process.execPath, [cliEntry, "--mode", "daemon", "--daemon-socket", socketPath, "--offline"], {
		cwd: fixture.projectDir,
		env: fixture.env,
		stdio: ["ignore", "pipe", "pipe"],
		shell: false,
		detached: false,
	});
	const daemonOutput = diagnostics(daemon);
	const probePath = join(probesDir, "daemon-probe.mjs");
	writeFileSync(
		probePath,
		`${staticSdkImport()}import { writeFileSync } from "node:fs";
const [socketPath, projectDir, agentDir, sessionDir, workerReceiptPath, expectedBuildId] = process.argv.slice(2);
const expected = ${JSON.stringify(PYLON_RELEASE_EXPECTED_SDK_FEATURES)};
if (!Object.isFrozen(sdk.PRIME_AGENT_SDK_FEATURES) || JSON.stringify([...sdk.PRIME_AGENT_SDK_FEATURES]) !== JSON.stringify(expected)) throw new Error("Unexpected SDK feature contract.");
function delay(ms) { return new Promise((resolveDelay) => setTimeout(resolveDelay, ms)); }
async function within(promise, timeoutMs, label) {
  let timer;
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label + " timed out.")), timeoutMs); });
  try { return await Promise.race([promise, timeout]); } finally { clearTimeout(timer); }
}
async function connectEventually() {
  const deadline = Date.now() + 20_000;
  let lastError;
  while (Date.now() < deadline) {
    const candidate = new sdk.DaemonClient(socketPath);
    try {
      await candidate.connect(250);
      await candidate.waitForHello(1_000);
      return candidate;
    } catch (error) {
      lastError = error;
      candidate.close();
      await delay(100);
    }
  }
  throw new Error("Daemon readiness timed out: " + String(lastError));
}
let client;
let connection;
let activeSessionId;
let receipt = {};
let failure;
try {
  client = await connectEventually();
  if (client.hello?.runtime?.buildId !== expectedBuildId) throw new Error("Daemon runtime build identity did not match the release manifest.");
  if (!client.hello?.serverCapabilities?.includes("correlated_prompt_lifecycle_v1")) throw new Error("Daemon did not offer correlated lifecycle.");
  if (!client.hello?.serverCapabilities?.includes("caller_owned_session_environment_cleanup_v1")) throw new Error("Daemon did not offer caller-owned environment cleanup.");
  const launchEnv = Object.fromEntries(
    Object.entries(process.env).filter(([name, value]) => {
      const normalizedName = name.toUpperCase();
      return (
        typeof value === "string" &&
        !normalizedName.startsWith("PRIME_AGENT_INTERNAL_") &&
        normalizedName !== "RLM_DEPTH"
      );
    }),
  );
  launchEnv.PRIME_AGENT_CODING_AGENT_DIR = agentDir;
  const sessionConfig = {
    cwd: projectDir,
    agentDir,
    sessionDir,
    provider: "artifact-faux",
    model: "artifact-faux",
    noTools: true,
    noExtensions: true,
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    telemetryDisabled: true
  };
  const created = await client.request({
    type: "create",
    lifecycle: "client_owned",
    noSession: true,
    launchEnv,
    launchEnvMode: "replace",
    config: sessionConfig
  }, 20_000);
  if (!created.success) throw new Error(created.error);
  activeSessionId = created.data?.activeSessionId ?? created.data?.id;
  const workerPid = created.data?.workerPid;
  if (typeof activeSessionId !== "string" || !Number.isSafeInteger(workerPid) || workerPid <= 1) throw new Error("Daemon did not return owned worker identity.");
  writeFileSync(workerReceiptPath, JSON.stringify({ activeSessionId, workerPid }));
  if (created.data?.model?.provider !== "artifact-faux") throw new Error("Daemon created the wrong fixture session.");
  if (process.env.PYLON_RELEASE_SMOKE_TEST_HANG_AFTER_CREATE === "1") {
    await new Promise(() => setInterval(() => {}, 1_000));
  }
  connection = await within(sdk.DaemonAgentConnection.attach(client, activeSessionId, {
    ownedSession: true,
    ownedSessionLaunchEnv: launchEnv,
    ownedSessionRecoveryConfig: sessionConfig,
    closeClientOnDispose: false,
    supportsExtensionUi: false,
    telemetryDisabled: true,
    snapshotTimeoutMs: 10_000
  }), 20_000, "Daemon attach");
  if (!connection.supportsNegotiatedCapability("correlated_prompt_lifecycle_v1")) throw new Error("Post-attach negotiated capability was not proved.");
  const ownedProof = connection.getOwnedSessionContractProof();
  if (ownedProof?.feature !== "caller_owned_session_environment_cleanup_v1" || ownedProof.status !== "attached") {
    throw new Error("Post-attach caller-owned environment contract was not proved.");
  }
  receipt = { negotiated: true, ownedContract: true, activeSessionId, runtimeBuildId: client.hello.runtime.buildId };
} catch (error) {
  failure = error;
} finally {
  try {
    if (connection) {
      const ownedCleanup = await within(
        connection.disposeOwnedSession({ timeoutMs: 15_000 }),
        20_000,
        "Owned session disposal",
      );
      if (ownedCleanup.status !== "completed") {
        throw new Error("Owned session disposal was not authoritatively completed: " + ownedCleanup.status);
      }
      receipt.ownedCleanup = ownedCleanup.status;
    } else if (client && activeSessionId) {
      const complete = await client.request({ type: "complete_owned_session", activeSessionId }, 10_000);
      if (!complete.success) throw new Error(complete.error);
    }
    if (client && activeSessionId) {
      const deadline = Date.now() + 10_000;
      let cleanup;
      while (Date.now() < deadline) {
        cleanup = await client.request({ type: "get_owned_session_cleanup", activeSessionId }, 5_000);
        if (!cleanup.success) throw new Error(cleanup.error);
        if (cleanup.data?.status === "settled") break;
        await delay(50);
      }
      if (cleanup?.data?.status !== "settled") throw new Error("Owned cleanup did not settle.");
      receipt.cleanup = "settled";
    }
    if (client) {
      const shutdown = await client.request({ type: "shutdown", force: true }, 5_000);
      if (!shutdown.success) throw new Error(shutdown.error);
    }
  } catch (cleanupError) {
    failure = failure ?? cleanupError;
  } finally {
    client?.close();
  }
}
if (failure) throw failure;
console.log(JSON.stringify(receipt));
`,
	);
	const forceCleanupPath = join(probesDir, "daemon-force-cleanup.mjs");
	writeFileSync(
		forceCleanupPath,
		`${staticSdkImport()}
const [socketPath, activeSessionId] = process.argv.slice(2);
const client = new sdk.DaemonClient(socketPath);
let connected = false;
try {
  await client.connect(1_000);
  await client.waitForHello(1_000);
  connected = true;
  if (activeSessionId) {
    try { await client.request({ type: "complete_owned_session", activeSessionId }, 5_000); } catch {}
  }
  const shutdown = await client.request({ type: "shutdown", force: true }, 10_000);
  if (!shutdown.success) throw new Error(shutdown.error);
} catch (error) {
  if (connected) throw error;
} finally {
  client.close();
}
console.log(JSON.stringify({ connected }));
`,
	);

	let failure;
	try {
		const probe = spawn(
			process.execPath,
			[
				probePath,
				socketPath,
				fixture.projectDir,
				fixture.agentDir,
				fixture.sessionDir,
				workerReceiptPath,
				expectedBuildId,
			],
			{ cwd: probesDir, env: fixture.env, stdio: ["ignore", "pipe", "pipe"], shell: false, detached: false },
		);
		let probeTimeoutMs = 70_000;
		if (fixture.env.PYLON_RELEASE_SMOKE_TEST_HANG_AFTER_CREATE === "1") {
			const requested = Number.parseInt(fixture.env.PYLON_RELEASE_SMOKE_TEST_TIMEOUT_MS ?? "2000", 10);
			if (Number.isSafeInteger(requested) && requested >= 1_000 && requested <= 10_000) probeTimeoutMs = requested;
		}
		const result = await collectChild(probe, probeTimeoutMs);
		if (result.status !== 0) throw new Error(`Installed daemon attach smoke failed.\n${result.stderr || result.stdout}`);
		const receipt = JSON.parse(result.stdout.trim().split("\n").at(-1));
		if (
			receipt.negotiated !== true ||
			receipt.ownedContract !== true ||
			receipt.ownedCleanup !== "completed" ||
			receipt.cleanup !== "settled" ||
			receipt.runtimeBuildId !== expectedBuildId
		) {
			throw new Error("Installed daemon receipt was incomplete.");
		}
		if (!(await waitForExit(daemon, 10_000))) throw new Error("Installed daemon did not exit after shutdown.");
		if (daemon.exitCode !== 0 || daemonOutput.overflow || daemonOutput.error) {
			throw new Error(`Installed daemon exited unexpectedly.\n${daemonOutput.stderr || daemonOutput.stdout}`);
		}
	} catch (error) {
		failure = error;
	} finally {
		const cleanupWasNeeded = failure !== undefined || (daemon.exitCode === null && daemon.signalCode === null);
		const beforeCleanup = cleanupWasNeeded
			? recordedWorkerIdentities(fixture.agentDir)
			: { identities: [], unprovedPids: [] };
		let receiptWorkerPid;
		try {
			let workerReceipt;
			if (existsSync(workerReceiptPath)) {
				workerReceipt = JSON.parse(readFileSync(workerReceiptPath, "utf8"));
				receiptWorkerPid = workerReceipt.workerPid;
			}
			if (daemon.exitCode === null && daemon.signalCode === null) {
				const cleanup = spawn(
					process.execPath,
					[forceCleanupPath, socketPath, workerReceipt?.activeSessionId ?? ""],
					{
						cwd: probesDir,
						env: fixture.env,
						stdio: ["ignore", "pipe", "pipe"],
						shell: false,
						detached: false,
					},
				);
				const cleanupResult = await collectChild(cleanup, 20_000);
				if (cleanupResult.status !== 0) {
					throw new Error(`Authenticated daemon cleanup failed.\n${cleanupResult.stderr || cleanupResult.stdout}`);
				}
				await waitForExit(daemon, 10_000);
			}
		} catch (cleanupError) {
			failure = failure ?? cleanupError;
		} finally {
			try {
				await terminateCapturedChild(daemon);
				if (cleanupWasNeeded) {
					const afterCleanup = recordedWorkerIdentities(fixture.agentDir);
					const identities = new Map(
						[...beforeCleanup.identities, ...afterCleanup.identities].map((identity) => [
							`${identity.pid}:${identity.processStartId}`,
							identity,
						]),
					);
					for (const identity of identities.values()) await terminateTrackedIdentity(identity);
					for (const pid of [...beforeCleanup.unprovedPids, ...afterCleanup.unprovedPids]) {
						if (isProcessAlive(pid)) {
							throw unsafeCleanupError(`Worker ${pid} remained alive without a start identity; refusing to signal it.`);
						}
					}
					if (
						Number.isSafeInteger(receiptWorkerPid) &&
						receiptWorkerPid > 1 &&
						isProcessAlive(receiptWorkerPid)
					) {
						throw unsafeCleanupError(
							`Worker ${receiptWorkerPid} remained alive without a descriptor start identity; refusing to signal it.`,
						);
					}
				}
			} catch (terminationError) {
				if (terminationError && typeof terminationError === "object" && terminationError.preserveTempRoot === true) {
					if (failure) terminationError.message = `${failure.message}\nCleanup failure: ${terminationError.message}`;
					failure = terminationError;
				} else {
					failure = failure ?? terminationError;
				}
			}
		}
	}
	if (failure) throw failure;
}
async function smokeWindowsAcp({ probesDir, fixture }) {
	const runnerPath = join(probesDir, "acp-runner.mjs");
	writeFileSync(
		runnerPath,
		`import { main } from "prime-agent";
await main(process.argv.slice(2), { extensionFactories: [() => {}] });
`,
	);
	const pipe = `\\\\.\\pipe\\pylon-prime-artifact-${process.pid}-${randomUUID()}`;
	const child = spawn(
		process.execPath,
		[
			runnerPath,
			"--mode",
			"acp",
			"--provider",
			"artifact-faux",
			"--model",
			"artifact-faux",
			"--no-session",
			"--no-tools",
			"--no-extensions",
			"--no-skills",
			"--no-prompt-templates",
			"--no-themes",
			"--no-context-files",
			"--offline",
			"--daemon-socket",
			pipe,
		],
		{ cwd: fixture.projectDir, env: fixture.env, stdio: ["pipe", "pipe", "pipe"], shell: false, detached: false },
	);
	const output = diagnostics(child, 1024 * 1024);
	let buffer = "";
	let response;
	let resolveResponse;
	let rejectResponse;
	const responsePromise = new Promise((resolveValue, rejectValue) => {
		resolveResponse = resolveValue;
		rejectResponse = rejectValue;
	});
	child.stdout.on("data", (chunk) => {
		buffer += chunk.toString("utf8");
		if (Buffer.byteLength(buffer) > 1024 * 1024) {
			rejectResponse(new Error("ACP stdout exceeded 1 MiB."));
			return;
		}
		let newline;
		while ((newline = buffer.indexOf("\n")) >= 0) {
			const line = buffer.slice(0, newline).trim();
			buffer = buffer.slice(newline + 1);
			if (!line) continue;
			try {
				const frame = JSON.parse(line);
				if (frame.id === 1) {
					response = frame;
					resolveResponse(frame);
				}
			} catch {
				rejectResponse(new Error("ACP emitted non-JSON stdout."));
			}
		}
	});
	child.once("close", () => {
		if (!response) rejectResponse(new Error("ACP exited before initialize response."));
	});
	const timer = setTimeout(() => rejectResponse(new Error("ACP initialize timed out.")), 45_000);
	let failure;
	try {
		child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: 1, clientCapabilities: {} } })}\n`);
		const frame = await responsePromise;
		clearTimeout(timer);
		if (
			frame.jsonrpc !== "2.0" ||
			frame.error !== undefined ||
			frame.result?.protocolVersion !== 1 ||
			frame.result?.agentInfo?.name !== "prime-agent"
		) {
			throw new Error(`Unexpected ACP initialize response: ${JSON.stringify(frame)}`);
		}
		child.stdin.end();
		if (!(await waitForExit(child, 10_000))) throw new Error("ACP did not exit after input EOF.");
		if (child.exitCode !== 0 || output.overflow || output.error) {
			throw new Error(`Installed ACP fallback failed.\n${output.stderr || output.stdout}`);
		}
	} catch (error) {
		failure = error;
	} finally {
		clearTimeout(timer);
		try {
			await terminateCapturedChild(child);
		} catch (terminationError) {
			failure = failure ?? terminationError;
		}
	}

	const cleanupReceiptPath = join(probesDir, "unexpected-daemon.json");
	const cleanupPath = join(probesDir, "acp-daemon-cleanup.mjs");
	writeFileSync(
		cleanupPath,
		`${staticSdkImport()}import { writeFileSync } from "node:fs";
const [pipe, receiptPath] = process.argv.slice(2);
const client = new sdk.DaemonClient(pipe);
let connected = false;
try {
  await client.connect(500);
  await client.waitForHello(500);
  connected = true;
  writeFileSync(receiptPath, JSON.stringify({
    supervisorPid: client.hello?.supervisorPid,
    supervisorProcessStartId: client.hello?.supervisorProcessStartId
  }));
  const shutdown = await client.request({ type: "shutdown", force: true }, 10_000);
  if (!shutdown.success) throw new Error(shutdown.error);
} catch (error) {
  if (connected) throw error;
} finally {
  client.close();
}
console.log(JSON.stringify({ connected }));
`,
	);
	const beforeCleanup = recordedWorkerIdentities(fixture.agentDir);
	try {
		const cleanup = spawn(process.execPath, [cleanupPath, pipe, cleanupReceiptPath], {
			cwd: probesDir,
			env: fixture.env,
			stdio: ["ignore", "pipe", "pipe"],
			shell: false,
			detached: false,
		});
		const cleanupResult = await collectChild(cleanup, 15_000);
		if (cleanupResult.status !== 0) {
			throw new Error(`Unexpected ACP daemon cleanup failed.\n${cleanupResult.stderr || cleanupResult.stdout}`);
		}
		const cleanupReceipt = JSON.parse(cleanupResult.stdout.trim().split("\n").at(-1));
		if (cleanupReceipt.connected === true) {
			failure = failure ?? new Error("ACP fallback unexpectedly launched a detached daemon.");
		}
	} catch (cleanupError) {
		failure = failure ?? cleanupError;
	} finally {
		try {
			if (existsSync(cleanupReceiptPath)) {
				const supervisor = JSON.parse(readFileSync(cleanupReceiptPath, "utf8"));
				if (
					Number.isSafeInteger(supervisor.supervisorPid) &&
					supervisor.supervisorPid > 1 &&
					typeof supervisor.supervisorProcessStartId === "string"
				) {
					const identity = {
						pid: supervisor.supervisorPid,
						processStartId: supervisor.supervisorProcessStartId,
					};
					const state = await waitForTrackedIdentityChange(identity, 10_000);
					if (state === "current") await terminateTrackedIdentity(identity);
					else if (state === "unknown") {
						throw unsafeCleanupError(
							`Could not prove the start identity of unexpected ACP daemon ${identity.pid}.`,
						);
					}
				} else if (
					Number.isSafeInteger(supervisor.supervisorPid) &&
					supervisor.supervisorPid > 1 &&
					isProcessAlive(supervisor.supervisorPid)
				) {
					throw unsafeCleanupError(
						`Unexpected ACP daemon ${supervisor.supervisorPid} lacked a start identity; refusing to signal it.`,
					);
				}
			}
			const afterCleanup = recordedWorkerIdentities(fixture.agentDir);
			const identities = new Map(
				[...beforeCleanup.identities, ...afterCleanup.identities].map((identity) => [
					`${identity.pid}:${identity.processStartId}`,
					identity,
				]),
			);
			for (const identity of identities.values()) await terminateTrackedIdentity(identity);
			for (const pid of [...beforeCleanup.unprovedPids, ...afterCleanup.unprovedPids]) {
				if (isProcessAlive(pid)) {
					throw unsafeCleanupError(`Worker ${pid} remained alive without a start identity; refusing to signal it.`);
				}
			}
		} catch (terminationError) {
			if (terminationError && typeof terminationError === "object" && terminationError.preserveTempRoot === true) {
				if (failure) terminationError.message = `${failure.message}\nCleanup failure: ${terminationError.message}`;
				failure = terminationError;
			} else {
				failure = failure ?? terminationError;
			}
		}
	}
	if (failure) throw failure;
}
function createLocalAssetConsumer(prefix, artifactsDir, manifest) {
	const bySourceName = new Map(
		PYLON_RELEASE_PACKAGES.map((releasePackage) => [
			releasePackage.packageName,
			pathToFileURL(join(artifactsDir, releaseAssetFile(releasePackage.assetStem, manifest.package.version))).href,
		]),
	);
	const rootArchive = bySourceName.get("prime-agent");
	if (!rootArchive) throw new Error("Missing root artifact.");
	const overrides = Object.fromEntries([...bySourceName.entries()].filter(([name]) => name !== "prime-agent"));
	writeFileSync(
		join(prefix, "package.json"),
		`${JSON.stringify({ name: "pylon-prime-artifact-smoke", private: true, dependencies: { "prime-agent": rootArchive }, overrides }, null, "\t")}\n`,
	);
}

export async function smokePylonPrimeAgentRelease(artifactsDir) {
	const manifest = verifyPylonPrimeAgentRelease(artifactsDir);
	const tempRoot = mkdtempSync(join(tmpdir(), "pylon-prime-release-"));
	let removeTempRoot = true;
	try {
		const prefix = join(tempRoot, "install");
		mkdirSync(prefix, { recursive: true });
		createLocalAssetConsumer(prefix, artifactsDir, manifest);
		const npm = npmInvocation();
		run(
			npm.command,
			[
				...npm.prefixArgs,
				"install",
				"--ignore-scripts",
				"--no-audit",
				"--no-fund",
				"--package-lock=false",
				"--loglevel=verbose",
			],
			{
				cwd: prefix,
				env: process.env,
				stdio: "inherit",
				timeoutMs: releaseInstallTimeoutMs(),
			},
		);
		const packageDir = join(prefix, "node_modules", "prime-agent");
		const installedPackage = JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8"));
		const cliEntry = join(packageDir, installedPackage.bin["prime-agent"]);
		const probesDir = join(prefix, "runtime-probes");
		mkdirSync(probesDir, { recursive: true });
		const fixture = writeRuntimeFixture(tempRoot, packageDir);
		await smokeSdkIdentity({ probesDir, cwd: fixture.projectDir, env: fixture.env });

		const installedNpm = npmInvocation(fixture.env);
		const version = runCli(
			installedNpm.command,
			[...installedNpm.prefixArgs, "exec", "--offline", "--yes=false", "--", "prime-agent", "--version"],
			{ cwd: prefix, env: fixture.env },
		);
		const versionOutput = `${version.stdout}${version.stderr}`.trim();
		if (version.error || version.status !== 0 || versionOutput !== manifest.package.version) {
			throw new Error(`Installed prime-agent --version failed: ${versionOutput || version.error}`);
		}
		const update = runCli(process.execPath, [cliEntry, "update", "--force"], {
			cwd: fixture.projectDir,
			env: { ...fixture.env, PRIME_AGENT_DOWNLOAD_BASE_URL: "https://upstream-feed.invalid" },
		});
		const updateOutput = `${update.stdout}\n${update.stderr}`;
		if (
			update.error ||
			update.status === 0 ||
			!updateOutput.includes("https://github.com/pylon-code/prime-agent/releases") ||
			updateOutput.includes("upstream-feed.invalid") ||
			existsSync(fixture.fakeNpmRecord)
		) {
			throw new Error("Pylon artifact did not block the stock self-updater with Pylon release guidance.");
		}

		if (process.platform === "win32" || process.env.PYLON_RELEASE_SMOKE_TEST_FORCE_ACP === "1") {
			await smokeWindowsAcp({ probesDir, fixture });
		} else {
			await smokePosixDaemon({ tempRoot, probesDir, cliEntry, fixture, expectedBuildId: manifest.build.id });
		}
		console.log(`Installed and verified ${manifest.build.id} on ${process.platform}.`);
		return manifest;
	} catch (error) {
		if (error && typeof error === "object" && error.preserveTempRoot === true) {
			removeTempRoot = false;
			error.message = `${error.message} Temporary state was preserved at ${tempRoot}.`;
		}
		throw error;
	} finally {
		if (removeTempRoot) rmSync(tempRoot, { recursive: true, force: true });
	}
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		await smokePylonPrimeAgentRelease(parseArgs(process.argv.slice(2)));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
