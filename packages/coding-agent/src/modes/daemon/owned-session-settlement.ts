import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open, readdir, realpath } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE } from "../../sdk-features.js";
import type { DaemonOwnedSessionContractProof } from "../agent-connection/daemon-agent-connection.js";
import type { DaemonWorkerDescriptor } from "./daemon-worker-protocol.js";

export const OWNED_SESSION_SETTLEMENT_OBSERVATION_FEATURE = "owned_session_settlement_observation_v1" as const;

export interface OwnedSessionSettlementObservation {
	readonly feature: typeof OWNED_SESSION_SETTLEMENT_OBSERVATION_FEATURE;
	readonly status: "settled" | "registered" | "unavailable";
}

function validRegistration(value: unknown, scope: string): value is DaemonWorkerDescriptor {
	if (!value || typeof value !== "object") return false;
	const d = value as Partial<DaemonWorkerDescriptor>;
	return (
		(d.version === 1 || d.version === 2) &&
		typeof d.supervisorSocketPath === "string" &&
		isAbsolute(d.supervisorSocketPath) &&
		createHash("sha256").update(d.supervisorSocketPath).digest("hex").slice(0, 12) === scope &&
		typeof d.workerId === "string" &&
		d.workerId.length > 0 &&
		Number.isInteger(d.pid) &&
		(d.pid ?? 0) > 0 &&
		(d.processStartId === undefined || typeof d.processStartId === "string") &&
		(d.ownerClientId === undefined || typeof d.ownerClientId === "string") &&
		(d.callerOwnedEnvironmentContract === undefined || d.callerOwnedEnvironmentContract === true) &&
		typeof d.socketPath === "string" &&
		typeof d.authenticationToken === "string" &&
		typeof d.rootActiveSessionId === "string" &&
		d.rootActiveSessionId.length > 0 &&
		typeof d.createdAt === "string" &&
		typeof d.updatedAt === "string" &&
		Number.isInteger(d.consecutiveFailures) &&
		d.createCommand?.type === "create"
	);
}

/**
 * Read-only local-host recovery for a previously proved caller-owned root.
 * Capable supervisors persist registration before releasing the worker startup gate,
 * retain it across recovery, and remove it last after joining all cleanup. Inspect
 * every socket namespace in the exact agent home: a legacy host may have lost its
 * temporary socket path. This never adopts, completes, signals, or launches work.
 * A proof from before the caller-owned contract cannot certify descriptorless work.
 */
export async function observeOwnedSessionSettlement(input: {
	readonly agentDir: string;
	readonly activeSessionId: string;
	readonly contractProof: DaemonOwnedSessionContractProof;
}): Promise<OwnedSessionSettlementObservation> {
	const result = (status: OwnedSessionSettlementObservation["status"]): OwnedSessionSettlementObservation =>
		Object.freeze({ feature: OWNED_SESSION_SETTLEMENT_OBSERVATION_FEATURE, status });
	const proof = input.contractProof;
	if (
		!isAbsolute(input.agentDir) ||
		!input.activeSessionId ||
		input.activeSessionId.length > 256 ||
		proof?.feature !== CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE ||
		proof.status !== "attached" ||
		proof.daemon?.protocolName !== "prime-agent.daemon" ||
		!Number.isSafeInteger(proof.daemon.protocolVersion) ||
		proof.daemon.protocolVersion < 7 ||
		!Number.isSafeInteger(proof.daemon.schemaRevision) ||
		proof.daemon.schemaRevision < 0 ||
		!Number.isSafeInteger(proof.daemon.transportGeneration) ||
		proof.daemon.transportGeneration < 0 ||
		!proof.daemon.supervisorGeneration
	)
		return result("unavailable");
	const uid = process.getuid?.();
	if (uid === undefined || process.platform === "win32") return result("unavailable");
	const deadline = Date.now() + 10_000;
	let inspectedBytes = 0;
	let inspectedFiles = 0;
	try {
		if ((await realpath(input.agentDir)) !== input.agentDir) return result("unavailable");
		const root = join(input.agentDir, "daemon-workers");
		const privateDirectory = async (path: string) => {
			const info = await lstat(path);
			if (!info.isDirectory() || info.isSymbolicLink() || info.uid !== uid || (info.mode & 0o077) !== 0) {
				throw new Error("Unproved registry directory");
			}
			return info;
		};
		const rootIdentity = await privateDirectory(root);
		const scopes = (await readdir(root)).sort();
		if (scopes.length > 2048) return result("unavailable");
		for (const scope of scopes) {
			if (!/^[a-f0-9]{12}$/.test(scope) || Date.now() >= deadline) return result("unavailable");
			const directory = join(root, scope);
			const identity = await privateDirectory(directory);
			const names = (await readdir(directory)).sort();
			for (const name of names) {
				if (!name.endsWith(".json")) continue;
				if (++inspectedFiles > 16384 || Date.now() >= deadline) return result("unavailable");
				const file = await open(
					join(directory, name),
					constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
				);
				try {
					const before = await file.stat();
					inspectedBytes += before.size;
					if (
						!before.isFile() ||
						before.uid !== uid ||
						(before.mode & 0o077) !== 0 ||
						before.size > 1024 * 1024 ||
						inspectedBytes > 64 * 1024 * 1024
					)
						return result("unavailable");
					const descriptor: unknown = JSON.parse(
						await (async () => {
							const buffer = Buffer.alloc(before.size + 1);
							const { bytesRead } = await file.read(buffer, 0, buffer.length, 0);
							if (bytesRead !== before.size) throw new Error("Changing registration");
							return buffer.subarray(0, bytesRead).toString("utf8");
						})(),
					);
					const after = await file.stat();
					const linked = await lstat(join(directory, name));
					if (
						linked.isSymbolicLink() ||
						linked.dev !== before.dev ||
						linked.ino !== before.ino ||
						before.size !== after.size ||
						before.mtimeMs !== after.mtimeMs ||
						!validRegistration(descriptor, scope)
					)
						return result("unavailable");
					if (descriptor.rootActiveSessionId === input.activeSessionId) return result("registered");
				} finally {
					await file.close();
				}
			}
			const after = await privateDirectory(directory);
			if (
				after.dev !== identity.dev ||
				after.ino !== identity.ino ||
				after.mtimeMs !== identity.mtimeMs ||
				JSON.stringify((await readdir(directory)).sort()) !== JSON.stringify(names)
			)
				return result("unavailable");
		}
		const after = await privateDirectory(root);
		if (
			after.dev !== rootIdentity.dev ||
			after.ino !== rootIdentity.ino ||
			after.mtimeMs !== rootIdentity.mtimeMs ||
			JSON.stringify((await readdir(root)).sort()) !== JSON.stringify(scopes)
		)
			return result("unavailable");
		return result(Date.now() >= deadline ? "unavailable" : "settled");
	} catch {
		return result("unavailable");
	}
}
