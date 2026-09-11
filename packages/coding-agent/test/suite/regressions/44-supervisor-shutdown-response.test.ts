import { Socket } from "node:net";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { DaemonSocketClient } from "../../../src/modes/daemon/active-session-state.js";
import { CommandRecoveryJournal } from "../../../src/modes/daemon/command-recovery-journal.js";
import { createDaemonCommandEnvelope, type DaemonOutbound } from "../../../src/modes/daemon/daemon-protocol.js";
import { DaemonSupervisor } from "../../../src/modes/daemon/daemon-supervisor.js";
import { createHarness, type Harness } from "../harness.js";
import { createDeferred } from "../scheduling.js";

const harnesses: Harness[] = [];
afterEach(() => {
	vi.restoreAllMocks();
	for (const harness of harnesses.splice(0)) harness.cleanup();
});

async function shutdownFixture(backpressured = false) {
	const harness = await createHarness();
	harnesses.push(harness);
	const supervisor = new DaemonSupervisor(join(harness.tempDir, "daemon.sock"), {
		defaultSessionConfig: { agentDir: harness.tempDir, cwd: harness.tempDir },
	});
	const internals = supervisor as unknown as {
		ready: Promise<void>;
		commandJournal: CommandRecoveryJournal;
		assertCurrentOwnership(): Promise<void>;
		handleLine(client: DaemonSocketClient, line: string): Promise<void>;
		shutdown(
			exitCode: number,
			stopWorkers: boolean,
			relaunch: boolean,
			force: boolean,
			reason: string,
		): Promise<never>;
		shuttingDown: boolean;
	};
	internals.ready = Promise.resolve();
	internals.commandJournal = new CommandRecoveryJournal(join(harness.tempDir, "commands.jsonl"));
	const frames: DaemonOutbound[] = [];
	const order: string[] = [];
	const socket = new Socket();
	vi.spyOn(socket, "write").mockImplementation((data) => {
		frames.push(JSON.parse(String(data)) as DaemonOutbound);
		order.push("write");
		return !backpressured;
	});
	const client: DaemonSocketClient = {
		id: "owner",
		socket,
		attachedActiveSessionIds: new Set(),
		capabilities: new Set(),
		supportsExtensionUi: false,
		detachInput() {},
	};
	const shutdown = vi.spyOn(internals, "shutdown").mockImplementation(() => {
		internals.shuttingDown = true;
		order.push("close");
		socket.destroy();
		return new Promise<never>(() => {});
	});
	const command = JSON.stringify(
		createDaemonCommandEnvelope({ type: "shutdown", force: true }, "shutdown-1", "owner"),
	);
	return { internals, frames, order, client, shutdown, command };
}

describe("issue #44 supervisor shutdown acknowledgment", () => {
	it.each([false, true])("journals and writes before closing with backpressure=%s", async (backpressured) => {
		const { internals, frames, order, client, shutdown, command } = await shutdownFixture(backpressured);
		const recheckReached = createDeferred();
		const releaseRecheck = createDeferred();
		let checks = 0;
		vi.spyOn(internals, "assertCurrentOwnership").mockImplementation(async () => {
			if (++checks === 2) {
				recheckReached.resolve();
				await releaseRecheck.promise;
			}
		});
		const journal = internals.commandJournal;
		const recordResult = journal.recordResult.bind(journal);
		vi.spyOn(journal, "recordResult").mockImplementation((...args) => {
			order.push("journal");
			recordResult(...args);
		});
		const pending = internals.handleLine(client, command);
		try {
			await recheckReached.promise;
			expect(journal.lookup("owner", "shutdown-1")).toMatchObject({ status: "pending" });
			expect(internals.shuttingDown).toBe(true);
			expect(shutdown).not.toHaveBeenCalled();
			expect(frames).toEqual([]);
		} finally {
			releaseRecheck.resolve();
			await pending;
		}
		expect(order).toEqual(["journal", "write", "close"]);
		expect(frames).toEqual([{ type: "response", id: "shutdown-1", command: "shutdown", success: true }]);
		expect(journal.lookup("owner", "shutdown-1")).toMatchObject({ status: "complete", response: frames[0] });
		expect(shutdown).toHaveBeenCalledExactlyOnceWith(0, true, false, true, "shutdown");
	});

	it.each(["ownership", "journal"] as const)("does not start cleanup when the %s check fails", async (failure) => {
		const { internals, frames, client, shutdown, command } = await shutdownFixture();
		let checks = 0;
		vi.spyOn(internals, "assertCurrentOwnership").mockImplementation(async () => {
			if (++checks > 1 && failure === "ownership") {
				throw Object.assign(new Error("ownership changed"), { code: "supervisor_generation_stale" });
			}
		});
		if (failure === "journal") {
			vi.spyOn(internals.commandJournal, "recordResult").mockImplementation(() => {
				throw new Error("journal unavailable");
			});
		}
		await internals.handleLine(client, command);
		expect(shutdown).not.toHaveBeenCalled();
		expect(frames).toHaveLength(1);
		expect(frames[0]).toMatchObject({ type: "response", command: "shutdown", success: false });
		expect(internals.commandJournal.lookup("owner", "shutdown-1")).toMatchObject({ status: "pending" });
	});
});
