/**
 * Real-socket bounded DaemonClient ingress receipt.
 *
 * Run from packages/coding-agent:
 *
 *   npx tsx test/daemon-bounded-ingress-bench.ts --generated-session-mib 100
 *   npx tsx test/daemon-bounded-ingress-bench.ts --generated-session-mib 500
 */
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import { unlink } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { DaemonClient } from "../src/modes/daemon/daemon-client.js";

const MEBIBYTE = 1024 * 1024;
const CLIENT_MAX_FRAME_BYTES = 64 * MEBIBYTE;
const TARGET_PAYLOAD_BYTES = 384 * 1024;
const TARGET_MAX_WIRE_FRAME_BYTES = 512 * 1024;

function generatedSessionMib(): number {
	const flag = process.argv.indexOf("--generated-session-mib");
	const value = flag === -1 ? Number.NaN : Number(process.argv[flag + 1]);
	if (!Number.isSafeInteger(value) || value <= 0) {
		throw new Error("--generated-session-mib must be a positive integer");
	}
	return value;
}

async function writeFrame(socket: Socket, value: unknown): Promise<number> {
	const frame = Buffer.from(`${JSON.stringify(value)}\n`);
	const rawFrameBytes = frame.length - 1;
	if (rawFrameBytes > TARGET_MAX_WIRE_FRAME_BYTES || rawFrameBytes > CLIENT_MAX_FRAME_BYTES) {
		throw new Error(`Generated frame exceeded its declared bound: ${rawFrameBytes}`);
	}
	if (!socket.write(frame)) {
		await once(socket, "drain");
	}
	return rawFrameBytes;
}

const sessionMib = generatedSessionMib();
const totalPayloadBytes = sessionMib * MEBIBYTE;
const activeSessionId = "active-bounded-ingress-benchmark";
const snapshotId = `snapshot-${randomUUID()}`;
const socketPath =
	process.platform === "win32"
		? `\\\\.\\pipe\\prime-bounded-ingress-${process.pid}-${randomUUID()}`
		: `/tmp/prime-bounded-ingress-${process.pid}-${randomUUID().slice(0, 8)}.sock`;

let resolveSender!: (result: { chunkCount: number; digest: string; maxFrameBytes: number }) => void;
let rejectSender!: (error: Error) => void;
const sender = new Promise<{ chunkCount: number; digest: string; maxFrameBytes: number }>((resolve, reject) => {
	resolveSender = resolve;
	rejectSender = reject;
});

const server = createServer((socket) => {
	socket.on("error", (error) => rejectSender(error));
	void (async () => {
		let maxFrameBytes = await writeFrame(socket, {
			type: "session_snapshot_begin",
			activeSessionId,
			snapshotId,
			snapshot: { activeSessionId },
			messageCount: Math.ceil(totalPayloadBytes / TARGET_PAYLOAD_BYTES),
			targetChunkBytes: TARGET_MAX_WIRE_FRAME_BYTES,
			purpose: "attach",
		});
		const hash = createHash("sha256");
		let offset = 0;
		let index = 0;
		while (offset < totalPayloadBytes) {
			const length = Math.min(TARGET_PAYLOAD_BYTES, totalPayloadBytes - offset);
			const content = "x".repeat(length);
			hash.update(content);
			maxFrameBytes = Math.max(
				maxFrameBytes,
				await writeFrame(socket, {
					type: "session_snapshot_chunk",
					activeSessionId,
					snapshotId,
					index,
					messages: [{ role: "user", content, timestamp: index }],
				}),
			);
			offset += length;
			index++;
		}
		const digest = hash.digest("hex");
		maxFrameBytes = Math.max(
			maxFrameBytes,
			await writeFrame(socket, {
				type: "session_snapshot_end",
				activeSessionId,
				snapshotId,
				chunkCount: index,
				lastEventSequence: 0,
				digest,
				totalPayloadBytes,
			}),
		);
		resolveSender({ chunkCount: index, digest, maxFrameBytes });
	})().catch((error) => rejectSender(error instanceof Error ? error : new Error(String(error))));
});
await new Promise<void>((resolve, reject) => {
	server.once("error", reject);
	server.listen(socketPath, resolve);
});

const client = new DaemonClient(socketPath, { maxInboundFrameBytes: CLIENT_MAX_FRAME_BYTES });
const receivedHash = createHash("sha256");
let receivedBytes = 0;
let receivedChunks = 0;
let resolveComplete!: (end: { chunkCount: number; digest: string; totalPayloadBytes: number }) => void;
let rejectComplete!: (error: Error) => void;
const complete = new Promise<{ chunkCount: number; digest: string; totalPayloadBytes: number }>((resolve, reject) => {
	resolveComplete = resolve;
	rejectComplete = reject;
});
client.onMessage((message) => {
	const candidate = message as unknown as Record<string, unknown>;
	if (candidate.type === "session_snapshot_chunk") {
		if (candidate.snapshotId !== snapshotId || candidate.index !== receivedChunks) {
			rejectComplete(new Error("Received an out-of-order snapshot chunk"));
			return;
		}
		const messages = candidate.messages;
		const content =
			Array.isArray(messages) && messages.length === 1 && typeof messages[0]?.content === "string"
				? messages[0].content
				: undefined;
		if (content === undefined) {
			rejectComplete(new Error("Received an invalid snapshot chunk payload"));
			return;
		}
		receivedHash.update(content);
		receivedBytes += Buffer.byteLength(content);
		receivedChunks++;
		return;
	}
	if (candidate.type === "session_snapshot_end") {
		resolveComplete(candidate as unknown as { chunkCount: number; digest: string; totalPayloadBytes: number });
	}
});
client.onClose((error) => rejectComplete(error));
let timeout!: ReturnType<typeof setTimeout>;
const timedComplete = Promise.race([
	complete,
	new Promise<never>((_, reject) => {
		timeout = setTimeout(() => reject(new Error("bounded ingress timed out")), 120_000);
		timeout.unref();
	}),
]);

try {
	await client.connect();
	const [sent, end] = await Promise.all([sender, timedComplete]);
	const receivedDigest = receivedHash.digest("hex");
	if (
		receivedBytes !== totalPayloadBytes ||
		receivedChunks !== sent.chunkCount ||
		end.chunkCount !== sent.chunkCount ||
		end.totalPayloadBytes !== totalPayloadBytes ||
		end.digest !== sent.digest ||
		receivedDigest !== sent.digest
	) {
		throw new Error("Bounded ingress reconstruction did not match the generated transcript");
	}
	console.log(
		JSON.stringify({
			sessionMib,
			totalPayloadBytes,
			chunkCount: sent.chunkCount,
			maxFrameBytes: sent.maxFrameBytes,
			clientMaxFrameBytes: CLIENT_MAX_FRAME_BYTES,
			digest: sent.digest,
		}),
	);
} finally {
	clearTimeout(timeout);
	client.close();
	await new Promise<void>((resolve) => server.close(() => resolve()));
	if (process.platform !== "win32") {
		await unlink(socketPath).catch(() => {});
	}
}
