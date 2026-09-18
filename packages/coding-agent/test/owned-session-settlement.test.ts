import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { DaemonOwnedSessionContractProof } from "../src/modes/agent-connection/daemon-agent-connection.js";
import { observeOwnedSessionSettlement } from "../src/modes/daemon/owned-session-settlement.js";

const roots: string[] = [];
afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
const proof: DaemonOwnedSessionContractProof = {
	feature: "caller_owned_session_environment_cleanup_v1",
	status: "attached",
	daemon: {
		protocolName: "prime-agent.daemon",
		protocolVersion: 7,
		schemaRevision: 31,
		supervisorGeneration: "previous-supervisor",
		transportGeneration: 1,
	},
};
const socket = "/private/prior-temp/daemon.sock";
const scopeKey = (path: string) => createHash("sha256").update(path).digest("hex").slice(0, 12);
const registration = (id: string, supervisorSocketPath = socket) => ({
	version: 2,
	rootActiveSessionId: id,
	supervisorSocketPath,
	workerId: "worker",
	pid: 99999999,
	socketPath: "/private/absent-worker.sock",
	authenticationToken: "fixture",
	createdAt: "2026-09-18",
	updatedAt: "2026-09-18",
	consecutiveFailures: 0,
	createCommand: { type: "create" },
});
async function fixture() {
	const agentDir = await realpath(await mkdtemp(join(tmpdir(), "prime-settlement-")));
	roots.push(agentDir);
	const registry = join(agentDir, "daemon-workers");
	const scope = join(registry, scopeKey(socket));
	await mkdir(scope, { recursive: true, mode: 0o700 });
	const observe = () =>
		observeOwnedSessionSettlement({ agentDir, activeSessionId: "prior-root", contractProof: proof });
	const descriptor = async (id: string, name = "worker.json") =>
		writeFile(join(scope, name), JSON.stringify(registration(id)), { mode: 0o600 });
	return { agentDir, registry, scope, observe, descriptor };
}

describe.runIf(process.platform !== "win32")("previously proved caller-owned root settlement", () => {
	it("proves absent registration across old socket scopes without mutating another worker", async () => {
		const f = await fixture();
		await f.descriptor("another-root");
		expect(await f.observe()).toEqual({ feature: "owned_session_settlement_observation_v1", status: "settled" });
	});
	it("retains a matching durable registration even when its process and socket are absent", async () => {
		const f = await fixture();
		await f.descriptor("prior-root");
		expect((await f.observe()).status).toBe("registered");
	});
	it("observes every socket namespace, including a prior temporary directory", async () => {
		const f = await fixture();
		const other = join(f.registry, scopeKey("/private/older-temp/daemon.sock"));
		await mkdir(other, { mode: 0o700 });
		await writeFile(
			join(other, "old.json"),
			JSON.stringify(registration("prior-root", "/private/older-temp/daemon.sock")),
			{ mode: 0o600 },
		);
		expect((await f.observe()).status).toBe("registered");
	});
	it("does not treat a missing registry as settlement", async () => {
		const f = await fixture();
		await rm(f.registry, { recursive: true });
		expect((await f.observe()).status).toBe("unavailable");
	});
	it.each(["not json", "{}", '{"version":99,"rootActiveSessionId":"another-root"}'])(
		"retains ambiguity for malformed registration %s",
		async (content) => {
			const f = await fixture();
			await writeFile(join(f.scope, "unknown.json"), content, { mode: 0o600 });
			expect((await f.observe()).status).toBe("unavailable");
		},
	);
	it("rejects an aliased home, registry scope, or descriptor", async () => {
		const f = await fixture();
		const alias = `${f.agentDir}-alias`;
		roots.push(alias);
		await symlink(f.agentDir, alias);
		expect(
			(await observeOwnedSessionSettlement({ agentDir: alias, activeSessionId: "prior-root", contractProof: proof }))
				.status,
		).toBe("unavailable");
		await symlink(f.scope, join(f.registry, "abcdef123456"));
		expect((await f.observe()).status).toBe("unavailable");
		await rm(join(f.registry, "abcdef123456"));
		await f.descriptor("another-root", "target");
		await symlink(join(f.scope, "target"), join(f.scope, "worker.json"));
		expect((await f.observe()).status).toBe("unavailable");
	});
	it("rejects public registry and descriptor permissions", async () => {
		const f = await fixture();
		await chmod(f.registry, 0o755);
		expect((await f.observe()).status).toBe("unavailable");
		await chmod(f.registry, 0o700);
		await f.descriptor("another-root");
		await chmod(join(f.scope, "worker.json"), 0o644);
		expect((await f.observe()).status).toBe("unavailable");
	});
	it("rejects a FIFO registration without blocking on open", async () => {
		const f = await fixture();
		execFileSync("mkfifo", [join(f.scope, "worker.json")]);
		expect((await f.observe()).status).toBe("unavailable");
	});
	it("rejects a registration attributed to a different socket namespace", async () => {
		const f = await fixture();
		await writeFile(join(f.scope, "worker.json"), JSON.stringify(registration("another-root", "/different.sock")), {
			mode: 0o600,
		});
		expect((await f.observe()).status).toBe("unavailable");
	});
	it("rejects an unproved attachment", async () => {
		const f = await fixture();
		expect(
			(
				await observeOwnedSessionSettlement({
					agentDir: f.agentDir,
					activeSessionId: "prior-root",
					contractProof: { ...proof, daemon: { ...proof.daemon, supervisorGeneration: "" } },
				})
			).status,
		).toBe("unavailable");
	});
});
