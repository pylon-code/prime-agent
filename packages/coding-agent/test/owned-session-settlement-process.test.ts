import { spawn } from "node:child_process";
import { once } from "node:events";
import { watch } from "node:fs";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it } from "vitest";
import { ENV_AGENT_DIR } from "../src/config.js";
import { DaemonAgentConnection } from "../src/modes/agent-connection/daemon-agent-connection.js";
import { DaemonClient } from "../src/modes/daemon/daemon-client.js";
import { observeOwnedSessionSettlement } from "../src/modes/daemon/owned-session-settlement.js";

it.runIf(process.platform !== "win32")(
	"matches real supervisor cleanup and remains authoritative after supervisor exit",
	async () => {
		const root = await realpath(await mkdtemp(join(tmpdir(), "prime-settlement-native-")));
		const agentDir = join(root, "agent");
		const socket = join(root, "d.sock");
		await mkdir(agentDir, { mode: 0o700 });
		const watcher = watch(root);
		const ready = new Promise<void>((resolveReady, reject) => {
			watcher.on("change", (_event, name) => {
				if (name?.toString() === "d.sock") resolveReady();
			});
			watcher.on("error", reject);
		});
		const environment = Object.fromEntries(
			Object.entries(process.env).flatMap(([name, value]) =>
				value !== undefined &&
				!name.startsWith("PRIME_AGENT_INTERNAL_") &&
				!name.startsWith("RLM_") &&
				!/TOKEN|KEY|SECRET|PASSWORD|CREDENTIAL|AUTH|COOKIE/i.test(name)
					? [[name, value]]
					: [],
			),
		);
		Object.assign(environment, {
			HOME: root,
			USERPROFILE: root,
			[ENV_AGENT_DIR]: agentDir,
			PI_OFFLINE: "1",
			TSX_TSCONFIG_PATH: resolve(__dirname, "../../../tsconfig.json"),
		});
		const child = spawn(
			process.execPath,
			[
				resolve(__dirname, "../../../node_modules/tsx/dist/cli.mjs"),
				resolve(__dirname, "../src/cli.ts"),
				"--mode",
				"daemon",
				"--daemon-socket",
				socket,
				"--offline",
			],
			{ cwd: root, env: environment, stdio: ["ignore", "pipe", "pipe"] },
		);
		let diagnostics = "";
		child.stdout.on("data", (chunk) => {
			diagnostics += chunk.toString();
		});
		child.stderr.on("data", (chunk) => {
			diagnostics += chunk.toString();
		});
		const exited = once(child, "exit");
		const client = new DaemonClient(socket);
		try {
			await Promise.race([
				ready,
				exited.then(() => {
					throw new Error(`Supervisor exited: ${diagnostics}`);
				}),
			]);
			watcher.close();
			await client.connect(10_000);
			await client.waitForHello(10_000);
			const config = { cwd: root, agentDir, noTools: true, noExtensions: true };
			const created = await client.request(
				{
					type: "create",
					lifecycle: "client_owned",
					noSession: true,
					launchEnv: environment,
					launchEnvMode: "replace",
					config,
				},
				30_000,
			);
			if (!created.success) throw new Error(created.error);
			const summary = created.data as { activeSessionId: string };
			const connection = await DaemonAgentConnection.attach(client, summary.activeSessionId, {
				ownedSession: true,
				ownedSessionLaunchEnv: environment,
				ownedSessionRecoveryConfig: config,
				supportsExtensionUi: false,
			});
			const contractProof = connection.getOwnedSessionContractProof();
			if (!contractProof) throw new Error("Missing actual owned attachment proof");
			const input = { agentDir, activeSessionId: summary.activeSessionId, contractProof };
			expect((await observeOwnedSessionSettlement(input)).status).toBe("registered");
			expect((await connection.disposeOwnedSession({ timeoutMs: 30_000 })).status).toBe("completed");
			// The live supervisor can still update registry metadata; join its exit before the stable scan.
			const observer = new DaemonClient(socket);
			await observer.connect(10_000);
			try {
				await observer.request({ type: "shutdown" }, 10_000);
			} finally {
				observer.close();
			}
			await exited;
			expect((await observeOwnedSessionSettlement(input)).status).toBe("settled");
		} finally {
			watcher.close();
			client.close();
			if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
			await exited;
			await rm(root, { recursive: true, force: true });
		}
	},
	90_000,
);
