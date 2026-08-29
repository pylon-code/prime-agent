import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer, type Socket } from "node:net";
import { PassThrough } from "node:stream";
import { describe, expect, it } from "vitest";
import {
	encodePrivateFrame,
	encodePrivateFrameParts,
	PrivateFrameDecoder,
	PrivateFramedChannel,
	type PrivateFrameHeaderValidator,
} from "../src/modes/session-worker/private-framing.js";

interface TestHeader {
	type: string;
	requestId?: string;
}

const isTestHeader: PrivateFrameHeaderValidator<TestHeader> = (value: unknown): value is TestHeader => {
	if (!value || typeof value !== "object") {
		return false;
	}
	const candidate = value as { type?: unknown; requestId?: unknown };
	return (
		typeof candidate.type === "string" &&
		(candidate.requestId === undefined || typeof candidate.requestId === "string")
	);
};

describe("private worker framing", () => {
	it("decodes headers and opaque payloads across arbitrary chunk boundaries", () => {
		const first = encodePrivateFrame({ type: "event", requestId: "one" }, Buffer.from([0, 1, 2, 255]));
		const second = encodePrivateFrame({ type: "response", requestId: "two" }, Buffer.from("payload"));
		const combined = Buffer.concat([first, second]);
		const decoder = new PrivateFrameDecoder(isTestHeader);
		const frames = [];

		for (let offset = 0; offset < combined.length; offset += 3) {
			frames.push(...decoder.push(combined.subarray(offset, offset + 3)));
		}
		decoder.finish();

		expect(frames).toEqual([
			{ header: { type: "event", requestId: "one" }, payload: Buffer.from([0, 1, 2, 255]) },
			{ header: { type: "response", requestId: "two" }, payload: Buffer.from("payload") },
		]);
	});

	it("keeps segmented new-worker payloads byte-compatible with the legacy decoder", () => {
		const header = { type: "snapshot", requestId: "compat" };
		const payloadParts = [Buffer.from('{"messages":['), Buffer.from('{"content":"ok"}'), Buffer.from("]}")];
		const segmented = encodePrivateFrameParts(header, payloadParts);
		const legacyWire = encodePrivateFrame(header, Buffer.concat(payloadParts));
		expect(Buffer.concat(segmented)).toEqual(legacyWire);

		const decoder = new PrivateFrameDecoder(isTestHeader);
		const frames = segmented.flatMap((part) => decoder.push(part));
		decoder.finish();
		expect(frames).toEqual([{ header, payload: Buffer.concat(payloadParts) }]);
	});

	it("crosses a real process boundary with new segmented writes and legacy contiguous replies", async () => {
		const server = createServer();
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(0, "127.0.0.1", () => resolve());
		});
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("expected TCP test address");
		const connection = new Promise<Socket>((resolve) => server.once("connection", resolve));
		const childScript = `
const net = require("node:net");
let buffered = Buffer.alloc(0);
const socket = net.createConnection({ host: "127.0.0.1", port: Number(process.argv[1]) });
const encodeLegacy = (header, payload) => {
  const encodedHeader = Buffer.from(JSON.stringify(header));
  const frame = Buffer.allocUnsafe(8 + encodedHeader.length + payload.length);
  frame.writeUInt32BE(encodedHeader.length, 0);
  frame.writeUInt32BE(payload.length, 4);
  encodedHeader.copy(frame, 8);
  payload.copy(frame, 8 + encodedHeader.length);
  return frame;
};
socket.on("data", (chunk) => {
  buffered = buffered.length === 0 ? chunk : Buffer.concat([buffered, chunk]);
  const replies = [];
  while (buffered.length >= 8) {
    const headerLength = buffered.readUInt32BE(0);
    const payloadLength = buffered.readUInt32BE(4);
    const frameLength = 8 + headerLength + payloadLength;
    if (buffered.length < frameLength) break;
    const header = JSON.parse(buffered.toString("utf8", 8, 8 + headerLength));
    const payload = Buffer.from(buffered.subarray(8 + headerLength, frameLength));
    replies.push(encodeLegacy({ type: "ack", requestId: header.requestId }, payload));
    buffered = Buffer.from(buffered.subarray(frameLength));
  }
  if (replies.length > 0) socket.write(Buffer.concat(replies));
  if (replies.some((reply) => JSON.parse(reply.toString("utf8", 8, 8 + reply.readUInt32BE(0))).requestId === "three")) {
    socket.end();
  }
});`;
		const child = spawn(process.execPath, ["--eval", childScript, String(address.port)], {
			stdio: ["ignore", "ignore", "pipe"],
		});
		let socket: Socket | undefined;
		try {
			socket = await Promise.race([
				connection,
				new Promise<never>((_, reject) =>
					setTimeout(() => reject(new Error("legacy framing child did not connect")), 3000),
				),
			]);
			const decoder = new PrivateFrameDecoder(isTestHeader);
			const replies: Array<{ header: TestHeader; payload: Buffer }> = [];
			const completed = new Promise<void>((resolve, reject) => {
				socket!.on("data", (chunk) => replies.push(...decoder.push(chunk)));
				socket!.once("error", reject);
				socket!.once("end", () => {
					try {
						decoder.finish();
						resolve();
					} catch (error) {
						reject(error);
					}
				});
			});
			for (const [requestId, payloadParts] of [
				["one", [Buffer.from("a"), Buffer.from("b")]],
				["two", [Buffer.from("c"), Buffer.from("d")]],
				["three", [Buffer.from("e"), Buffer.from("f")]],
			] as const) {
				for (const part of encodePrivateFrameParts({ type: "snapshot", requestId }, payloadParts)) {
					socket.write(part);
				}
			}
			await Promise.race([
				completed,
				new Promise<never>((_, reject) =>
					setTimeout(() => reject(new Error("legacy framing child did not finish")), 3000),
				),
			]);
			expect(replies).toEqual([
				{ header: { type: "ack", requestId: "one" }, payload: Buffer.from("ab") },
				{ header: { type: "ack", requestId: "two" }, payload: Buffer.from("cd") },
				{ header: { type: "ack", requestId: "three" }, payload: Buffer.from("ef") },
			]);
			const [exitCode] = (await Promise.race([
				once(child, "close"),
				new Promise<never>((_, reject) =>
					setTimeout(() => reject(new Error("legacy framing child did not close")), 3000),
				),
			])) as [number | null];
			expect(exitCode).toBe(0);
		} finally {
			socket?.destroy();
			server.close();
			if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
		}
	});

	it("rejects invalid lengths, JSON, and routing headers", () => {
		const oversized = Buffer.alloc(8);
		oversized.writeUInt32BE(1025, 0);
		expect(() =>
			new PrivateFrameDecoder(isTestHeader, { maxHeaderBytes: 1024, maxPayloadBytes: 1024 }).push(oversized),
		).toThrow("Invalid private frame header length");

		const invalidJson = Buffer.concat([Buffer.from([0, 0, 0, 1, 0, 0, 0, 0]), Buffer.from("{")]);
		expect(() => new PrivateFrameDecoder(isTestHeader).push(invalidJson)).toThrow(
			"Invalid private frame header JSON",
		);

		const invalidHeader = encodePrivateFrame({ missing: "type" }, Buffer.alloc(0));
		expect(() => new PrivateFrameDecoder(isTestHeader).push(invalidHeader)).toThrow(
			"Invalid private frame routing header",
		);
	});

	it("reports an incomplete trailing frame", () => {
		const decoder = new PrivateFrameDecoder(isTestHeader);
		decoder.push(encodePrivateFrame({ type: "event" }, Buffer.from("body")).subarray(0, 9));
		expect(() => decoder.finish()).toThrow("incomplete bytes");
	});

	it("sends frames through a duplex channel without interpreting payload bytes", async () => {
		const stream = new PassThrough();
		const channel = new PrivateFramedChannel(stream, isTestHeader);
		const received = new Promise<{ header: TestHeader; payload: Buffer }>((resolve) => {
			channel.onFrame(resolve);
		});

		await channel.send({ type: "snapshot", requestId: "request" }, Buffer.from([9, 8, 7]));

		await expect(received).resolves.toEqual({
			header: { type: "snapshot", requestId: "request" },
			payload: Buffer.from([9, 8, 7]),
		});
		channel.close();
	});
});
