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
		pendingShutdownCommand?: unknown;
		socketLeaseCompromise?: Error;
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
			expect(internals.pendingShutdownCommand).toMatchObject({ id: "shutdown-1", type: "shutdown" });
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

	it.each([false, true])(
		"accepts fresh commands after failed shutdown journaling, failure persisted=%s",
		async (persistFailure) => {
			const { internals, frames, client, shutdown, command } = await shutdownFixture();
			vi.spyOn(internals, "assertCurrentOwnership").mockResolvedValue();
			const journal = internals.commandJournal;
			const writeResult = vi.spyOn(journal, "recordResult");
			const failWrite = () => {
				throw new Error("journal unavailable");
			};
			if (persistFailure) writeResult.mockImplementationOnce(failWrite);
			else writeResult.mockImplementation(failWrite);
			await internals.handleLine(client, command);
			expect(shutdown).not.toHaveBeenCalled();
			expect(frames.at(-1)).toMatchObject({ command: "shutdown", success: false });
			expect(journal.lookup("owner", "shutdown-1")).toMatchObject({
				status: persistFailure ? "complete" : "pending",
			});
			writeResult.mockRestore();

			await internals.handleLine(
				client,
				JSON.stringify(createDaemonCommandEnvelope({ type: "roster_unsubscribe" }, "fresh-command", "owner")),
			);
			expect(frames.at(-1)).toMatchObject({ id: "fresh-command", command: "roster_unsubscribe", success: true });
			await internals.handleLine(
				client,
				JSON.stringify(createDaemonCommandEnvelope({ type: "shutdown" }, "fresh-shutdown", "owner")),
			);
			expect(frames.at(-1)).toMatchObject({ id: "fresh-shutdown", command: "shutdown", success: true });
			expect(journal.lookup("owner", "fresh-shutdown")).toMatchObject({
				status: "complete",
				response: frames.at(-1),
			});
			expect(shutdown).toHaveBeenCalledExactlyOnceWith(0, true, false, false, "shutdown");
		},
	);

	it.each(["ownership", "cleanup", "lease"] as const)(
		"keeps the %s fence after a failed shutdown attempt",
		async (fence) => {
			const { internals, frames, client, shutdown, command } = await shutdownFixture();
			let checks = 0;
			const ownership = vi.spyOn(internals, "assertCurrentOwnership").mockImplementation(async () => {
				if (++checks > 1 && fence === "ownership")
					throw Object.assign(new Error("ownership changed"), { code: "supervisor_generation_stale" });
				if (checks === 3 && fence === "cleanup") internals.shuttingDown = true;
				if (checks === 3 && fence === "lease") internals.socketLeaseCompromise = new Error("lease compromised");
			});
			const writeResult = vi.spyOn(internals.commandJournal, "recordResult").mockImplementation(() => {
				throw new Error("journal unavailable");
			});
			await internals.handleLine(client, command);
			writeResult.mockRestore();
			ownership.mockResolvedValue();
			await internals.handleLine(
				client,
				JSON.stringify(createDaemonCommandEnvelope({ type: "roster_unsubscribe" }, "fresh-command", "owner")),
			);
			expect(frames.at(-1)).toMatchObject({ id: "fresh-command", command: "dispatch", success: false });
			expect(shutdown).not.toHaveBeenCalled();
		},
	);
});
