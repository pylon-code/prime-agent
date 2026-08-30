import type { Readable } from "node:stream";
import { StringDecoder } from "node:string_decoder";

/**
 * Serialize a single strict JSONL record.
 *
 * Framing is LF-only. Payload strings may contain other Unicode separators such as
 * U+2028 and U+2029. Clients must split records on `\n` only.
 */
export function serializeJsonLine(value: unknown): string {
	return `${JSON.stringify(value)}\n`;
}

export interface JsonlLineReaderOptions {
	maxLineLength?: number;
	onLineOverflow?: (prefix: string) => void;
}

/**
 * Attach an LF-only JSONL reader to a stream.
 *
 * This intentionally does not use Node readline. Readline splits on additional
 * Unicode separators that are valid inside JSON strings and therefore does not
 * implement strict JSONL framing.
 */
export function attachJsonlLineReader(
	stream: Readable,
	onLine: (line: string) => void,
	options: JsonlLineReaderOptions = {},
): () => void {
	const decoder = new StringDecoder("utf8");
	// Segments of the current, not-yet-terminated line. We never concatenate into a
	// single growing buffer: appending to one accumulator and rescanning it with
	// indexOf("\n") on every chunk is O(n^2) when a single record is split across
	// many socket reads (e.g. a multi-MB attach snapshot arriving in 64KB chunks).
	// Instead each chunk is scanned once (offset-advancing indexOf) and the segments
	// are joined exactly once, when the terminating newline arrives.
	let pending: string[] = [];
	let pendingLength = 0;
	let discardingOverflow = false;

	const emitLine = (line: string) => {
		onLine(line.endsWith("\r") ? line.slice(0, -1) : line);
	};

	const resetPending = () => {
		pending = [];
		pendingLength = 0;
	};

	const appendPending = (segment: string) => {
		if (discardingOverflow || segment.length === 0) return;
		const maxLineLength = options.maxLineLength;
		if (maxLineLength !== undefined && pendingLength + segment.length > maxLineLength) {
			const remaining = Math.max(0, maxLineLength - pendingLength);
			if (remaining > 0) {
				pending.push(segment.slice(0, remaining));
			}
			options.onLineOverflow?.(pending.join(""));
			resetPending();
			discardingOverflow = true;
			return;
		}
		pending.push(segment);
		pendingLength += segment.length;
	};

	const emitFrom = (segment: string) => {
		if (discardingOverflow) {
			discardingOverflow = false;
			resetPending();
			return;
		}
		appendPending(segment);
		if (discardingOverflow) {
			discardingOverflow = false;
			return;
		}
		emitLine(pending.join(""));
		resetPending();
	};

	const onData = (chunk: string | Buffer) => {
		const text = typeof chunk === "string" ? chunk : decoder.write(chunk);

		let start = 0;
		let newlineIndex = text.indexOf("\n");
		while (newlineIndex !== -1) {
			emitFrom(text.slice(start, newlineIndex));
			start = newlineIndex + 1;
			newlineIndex = text.indexOf("\n", start);
		}
		if (start < text.length) {
			appendPending(text.slice(start));
		}
	};

	const onEnd = () => {
		const tail = decoder.end();
		if (tail.length > 0) {
			appendPending(tail);
		}
		if (!discardingOverflow && pending.length > 0) {
			emitLine(pending.join(""));
		}
		resetPending();
		discardingOverflow = false;
	};

	stream.on("data", onData);
	stream.on("end", onEnd);

	return () => {
		stream.off("data", onData);
		stream.off("end", onEnd);
	};
}

export interface BoundedJsonlByteReaderOptions {
	/** Maximum bytes before LF in one frame. LF is excluded; a preceding CR counts. */
	maxFrameBytes: number;
	/** Called once after the reader becomes permanently terminal. */
	onFrameTooLarge: () => void;
}

const BOUNDED_JSONL_PAGE_BYTES = 64 * 1024;

/**
 * Attach a terminal, raw-byte-bounded JSONL reader.
 *
 * The stream must remain in byte mode. Bytes are copied into fixed-size owned
 * pages before UTF-8 decoding so pending storage is bounded without retaining
 * arbitrary caller buffers. Unlike `maxLineLength`, overflow never resumes at
 * a later record: the caller must permanently close this transport epoch.
 */
export function attachBoundedJsonlByteReader(
	stream: Readable,
	onLine: (line: string) => void,
	options: BoundedJsonlByteReaderOptions,
): () => void {
	if (!Number.isSafeInteger(options.maxFrameBytes) || options.maxFrameBytes <= 0) {
		throw new RangeError("maxFrameBytes must be a positive safe integer");
	}

	let pages: Buffer[] = [];
	let pageLengths: number[] = [];
	let allocatedBytes = 0;
	let pendingBytes = 0;
	let terminal = false;
	let attached = true;

	const resetPending = () => {
		pages = [];
		pageLengths = [];
		allocatedBytes = 0;
		pendingBytes = 0;
	};

	const detach = () => {
		if (!attached) return;
		attached = false;
		stream.off("data", onData);
		stream.off("end", onEnd);
	};

	const failTooLarge = () => {
		if (terminal) return;
		terminal = true;
		detach();
		resetPending();
		options.onFrameTooLarge();
	};

	const appendPending = (segment: Buffer): boolean => {
		if (segment.length === 0) return true;
		if (segment.length > options.maxFrameBytes - pendingBytes) {
			failTooLarge();
			return false;
		}

		let offset = 0;
		while (offset < segment.length) {
			let page = pages.at(-1);
			let pageLength = pageLengths.at(-1) ?? 0;
			if (!page || pageLength === page.length) {
				const capacity = Math.min(BOUNDED_JSONL_PAGE_BYTES, options.maxFrameBytes - allocatedBytes);
				page = Buffer.allocUnsafe(capacity);
				pages.push(page);
				pageLengths.push(0);
				allocatedBytes += capacity;
				pageLength = 0;
			}
			const copied = Math.min(page.length - pageLength, segment.length - offset);
			segment.copy(page, pageLength, offset, offset + copied);
			pageLengths[pageLengths.length - 1] = pageLength + copied;
			offset += copied;
			pendingBytes += copied;
		}
		return true;
	};

	const emitPending = () => {
		const decoder = new StringDecoder("utf8");
		const decoded: string[] = [];
		for (let i = 0; i < pages.length; i++) {
			const length = pageLengths[i] ?? 0;
			if (length > 0) {
				decoded.push(decoder.write(pages[i]!.subarray(0, length)));
			}
		}
		decoded.push(decoder.end());
		let line = decoded.join("");
		if (line.endsWith("\r")) {
			line = line.slice(0, -1);
		}
		resetPending();
		onLine(line);
	};

	function onData(chunk: string | Buffer): void {
		if (terminal) return;
		const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
		let start = 0;
		while (start <= bytes.length && !terminal) {
			const newline = bytes.indexOf(0x0a, start);
			if (newline === -1) {
				appendPending(bytes.subarray(start));
				return;
			}
			if (!appendPending(bytes.subarray(start, newline))) {
				return;
			}
			emitPending();
			start = newline + 1;
		}
	}

	function onEnd(): void {
		if (terminal) return;
		detach();
		if (pendingBytes > 0) {
			emitPending();
		} else {
			resetPending();
		}
	}

	stream.on("data", onData);
	stream.on("end", onEnd);

	return () => {
		terminal = true;
		detach();
		resetPending();
	};
}
