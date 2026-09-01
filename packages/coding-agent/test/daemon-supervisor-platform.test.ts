import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DaemonClient } from "../src/modes/daemon/daemon-client.js";
import { DaemonSupervisor } from "../src/modes/daemon/daemon-supervisor.js";

const directories: string[] = [];

afterEach(() => {
	for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe("DaemonSupervisor platform gates", () => {
	it("starts and says hello on injected win32 without constructing the POSIX recovery store", async () => {
		const root = mkdtempSync(join(tmpdir(), "daemon-supervisor-win32-gate-"));
		directories.push(root);
		const descriptorDir = join(root, "workers");
		const socketPath =
			process.platform === "win32"
				? String.raw`\\.\pipe\prime-win32-gate-${process.pid}`
				: join(root, "daemon.sock");
		let recoveryStoreConstructions = 0;
		const supervisor = new DaemonSupervisor(socketPath, {
			defaultSessionConfig: { agentDir: root, cwd: root },
			descriptorDir,
			platform: "win32",
			ownedSessionRecoveryStoreFactory: () => {
				recoveryStoreConstructions++;
				throw new Error("directory fsync is unavailable");
			},
		});
		const internals = supervisor as unknown as {
			catalog: { start: ReturnType<typeof vi.fn>; stop: ReturnType<typeof vi.fn> };
			cleanupSupervisorResources(): Promise<void>;
		};
		internals.catalog.start = vi.fn(async () => undefined);
		internals.catalog.stop = vi.fn(async () => undefined);
		const client = new DaemonClient(socketPath);
		try {
			await supervisor.start();
			await client.connect();
			const hello = await client.waitForHello();
			expect(hello.serverCapabilities).not.toContain("daemon_recoverable_owned_session_adoption_v1");
			expect(recoveryStoreConstructions).toBe(0);
			expect(readdirSync(descriptorDir)).not.toContain("owned-session-recovery");
		} finally {
			client.close();
			await internals.cleanupSupervisorResources();
		}
	});
});
