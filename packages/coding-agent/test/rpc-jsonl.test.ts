import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { unlink } from "node:fs/promises";
import { createServer } from "node:net";
import { performance } from "node:perf_hooks";
import { Readable } from "node:stream";
import { describe, expect, test } from "vitest";
import { DaemonClient, DaemonInboundFrameTooLargeError } from "../src/modes/daemon/daemon-client.js";
import { attachBoundedJsonlByteReader, attachJsonlLineReader, serializeJsonLine } from "../src/modes/rpc/jsonl.js";

/**
 * Drive the reader with precise control over chunk boundaries (Readable.from
 * coalesces chunks at its own discretion, which hides boundary-splitting bugs).
 */
function readChunks(chunks: Array<string | Buffer>): string[] {
	const lines: string[] = [];
	const emitter = new EventEmitter();
	attachJsonlLineReader(emitter as unknown as Readable, (line) => lines.push(line));
	for (const chunk of chunks) {
		emitter.emit("data", chunk);
	}
	emitter.emit("end");
	return lines;
}

describe("RPC JSONL framing", () => {
	test("serializes strict JSONL records without escaping Unicode separators", () => {
		const line = serializeJsonLine({ text: "a\u2028b\u2029c" });

		expect(line).toContain("a\u2028b\u2029c");
		expect(line.endsWith("\n")).toBe(true);
		expect(JSON.parse(line.trim())).toEqual({ text: "a\u2028b\u2029c" });
	});

	test("splits on LF only and preserves U+2028/U+2029 inside payloads", async () => {
		const lines: string[] = [];
		const stream = Readable.from([serializeJsonLine({ text: "a\u2028b\u2029c" })]);

		const done = new Promise<void>((resolve) => {
			stream.on("end", resolve);
		});

		attachJsonlLineReader(stream, (line) => {
			lines.push(line);
		});

		await done;

		expect(lines).toHaveLength(1);
		expect(JSON.parse(lines[0])).toEqual({ text: "a\u2028b\u2029c" });
	});

	test("handles CRLF-delimited input", async () => {
		const lines: string[] = [];
		const stream = Readable.from([Buffer.from('{"a":1}\r\n{"b":2}\r\n')]);

		const done = new Promise<void>((resolve) => {
			stream.on("end", resolve);
		});

		attachJsonlLineReader(stream, (line) => {
			lines.push(line);
		});

		await done;

		expect(lines).toEqual(['{"a":1}', '{"b":2}']);
	});

	test("emits a final line without trailing LF", async () => {
		const lines: string[] = [];
		const stream = Readable.from([Buffer.from('{"a":1}')]);

		const done = new Promise<void>((resolve) => {
			stream.on("end", resolve);
		});

		attachJsonlLineReader(stream, (line) => {
			lines.push(line);
		});

		await done;

		expect(lines).toEqual(['{"a":1}']);
	});

	test("reassembles a record split across many small chunks", () => {
		const record = serializeJsonLine({ text: "x".repeat(5000) });
		const chunks: string[] = [];
		for (let i = 0; i < record.length; i += 7) {
			chunks.push(record.slice(i, i + 7));
		}

		const lines = readChunks(chunks);

		expect(lines).toHaveLength(1);
		expect(JSON.parse(lines[0])).toEqual({ text: "x".repeat(5000) });
	});

	test("trims CR even when the CRLF straddles a chunk boundary", () => {
		expect(readChunks(['{"a":1}\r', '\n{"b":2}\r\n'])).toEqual(['{"a":1}', '{"b":2}']);
	});

	test("emits empty lines for blank records", () => {
		expect(readChunks(['{"a":1}\n', "\n", '{"b":2}\n'])).toEqual(['{"a":1}', "", '{"b":2}']);
	});

	test("reassembles a multibyte codepoint split across Buffer chunks", () => {
		const euro = Buffer.from("€", "utf8"); // E2 82 AC
		const lines = readChunks([
			Buffer.from('{"c":"'),
			euro.subarray(0, 1),
			euro.subarray(1, 2),
			euro.subarray(2, 3),
			Buffer.from('"}\n'),
		]);

		expect(lines).toHaveLength(1);
		expect(JSON.parse(lines[0])).toEqual({ c: "€" });
	});

	test("decodes a large single record split into many chunks in linear time", () => {
		// The previous accumulate-and-rescan reader was O(n^2): an 8MB record in
		// 16KB chunks took multiple seconds. The linear reader stays well under a
		// second; the generous threshold catches a quadratic regression.
		const record = serializeJsonLine({ blob: "y".repeat(8 * 1024 * 1024) });
		const chunkSize = 16 * 1024;
		const chunks: string[] = [];
		for (let i = 0; i < record.length; i += chunkSize) {
			chunks.push(record.slice(i, i + chunkSize));
		}

		const start = performance.now();
		const lines = readChunks(chunks);
		const elapsed = performance.now() - start;

		expect(lines).toHaveLength(1);
		expect(JSON.parse(lines[0]).blob.length).toBe(8 * 1024 * 1024);
		expect(elapsed).toBeLessThan(1000);
	});

	test("bounds oversized lines and resumes at the next record", () => {
		const lines: string[] = [];
		const overflows: string[] = [];
		const emitter = new EventEmitter();
		attachJsonlLineReader(emitter as unknown as Readable, (line) => lines.push(line), {
			maxLineLength: 5,
			onLineOverflow: (prefix) => overflows.push(prefix),
		});

		emitter.emit("data", "abc");
		emitter.emit("data", "defghi");
		emitter.emit("data", "jkl\nok\n");
		emitter.emit("end");

		expect(overflows).toEqual(["abcde"]);
		expect(lines).toEqual(["ok"]);
	});

	test("fails immediately when a no-LF frame exceeds its raw-byte limit", () => {
		const lines: string[] = [];
		let failures = 0;
		const emitter = new EventEmitter();
		attachBoundedJsonlByteReader(emitter as unknown as Readable, (line) => lines.push(line), {
			maxFrameBytes: 5,
			onFrameTooLarge: () => failures++,
		});

		emitter.emit("data", Buffer.from("abc"));
		emitter.emit("data", Buffer.from("de"));
		expect(failures).toBe(0);
		emitter.emit("data", Buffer.from("f"));
		expect(failures).toBe(1);

		emitter.emit("data", Buffer.from("\nok\n"));
		emitter.emit("end");
		expect(lines).toEqual([]);
		expect(failures).toBe(1);
	});

	test("never resumes after a complete oversized frame", () => {
		const lines: string[] = [];
		let failures = 0;
		const emitter = new EventEmitter();
		attachBoundedJsonlByteReader(emitter as unknown as Readable, (line) => lines.push(line), {
			maxFrameBytes: 5,
			onFrameTooLarge: () => failures++,
		});

		emitter.emit("data", Buffer.from("abcdef\nok\n"));
		expect(lines).toEqual([]);
		expect(failures).toBe(1);
	});

	test("counts UTF-8 bytes and counts CR while excluding LF", () => {
		const allowed: string[] = [];
		let allowedFailures = 0;
		const allowedEmitter = new EventEmitter();
		attachBoundedJsonlByteReader(allowedEmitter as unknown as Readable, (line) => allowed.push(line), {
			maxFrameBytes: 4,
			onFrameTooLarge: () => allowedFailures++,
		});
		const euro = Buffer.from("€", "utf8");
		allowedEmitter.emit("data", euro.subarray(0, 1));
		allowedEmitter.emit("data", euro.subarray(1));
		allowedEmitter.emit("data", Buffer.from("\nabc\r"));
		allowedEmitter.emit("data", Buffer.from("\n"));
		allowedEmitter.emit("end");
		expect(allowed).toEqual(["€", "abc"]);
		expect(allowedFailures).toBe(0);

		let byteFailures = 0;
		const byteEmitter = new EventEmitter();
		attachBoundedJsonlByteReader(
			byteEmitter as unknown as Readable,
			() => {
				throw new Error("oversized UTF-8 was decoded");
			},
			{
				maxFrameBytes: 2,
				onFrameTooLarge: () => byteFailures++,
			},
		);
		byteEmitter.emit("data", euro);
		expect(byteFailures).toBe(1);

		let crFailures = 0;
		const crEmitter = new EventEmitter();
		attachBoundedJsonlByteReader(
			crEmitter as unknown as Readable,
			() => {
				throw new Error("oversized CRLF was decoded");
			},
			{
				maxFrameBytes: 3,
				onFrameTooLarge: () => crFailures++,
			},
		);
		crEmitter.emit("data", Buffer.from("abc\r\n"));
		expect(crFailures).toBe(1);
	});

	test("preserves bounded EOF lines and handles many tiny chunks", () => {
		const lines: string[] = [];
		let failures = 0;
		const emitter = new EventEmitter();
		attachBoundedJsonlByteReader(emitter as unknown as Readable, (line) => lines.push(line), {
			maxFrameBytes: 70_000,
			onFrameTooLarge: () => failures++,
		});
		for (let i = 0; i < 65_537; i++) {
			emitter.emit("data", Buffer.from("x"));
		}
		emitter.emit("data", Buffer.from("\nfinal"));
		emitter.emit("end");

		expect(lines).toEqual(["x".repeat(65_537), "final"]);
		expect(failures).toBe(0);
	});

	test("fails a real socket that overflows immediately on accept", async () => {
		const socketPath = `/tmp/prime-ingress-${process.pid}-${randomUUID()}.sock`;
		const server = createServer((socket) => {
			socket.on("error", () => {});
			socket.write(Buffer.alloc(65, 0x78));
		});
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(socketPath, resolve);
		});

		const client = new DaemonClient(socketPath, { maxInboundFrameBytes: 64 });
		const closed: Error[] = [];
		client.onClose((error) => closed.push(error));
		try {
			const outcome = client.connect().then(() => client.waitForHello(1000));
			const error = await outcome.then(
				() => {
					throw new Error("Expected immediate socket overflow");
				},
				(reason: unknown) => reason,
			);
			expect(error).toBeInstanceOf(DaemonInboundFrameTooLargeError);
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(closed).toEqual([error]);
			expect(client.isConnected).toBe(false);
		} finally {
			client.close();
			await new Promise<void>((resolve, reject) => {
				server.close((error) => (error ? reject(error) : resolve()));
			});
			await unlink(socketPath).catch(() => {});
		}
	});
});
