import type { Duplex } from "node:stream";

const FRAME_PREFIX_BYTES = 8;
const FRAME_BUFFER_COMPACTION_MIN_HEAD = 1024;

export interface PrivateFrameLimits {
	maxHeaderBytes: number;
	maxPayloadBytes: number;
}

export const DEFAULT_PRIVATE_FRAME_LIMITS: PrivateFrameLimits = {
	maxHeaderBytes: 1024 * 1024,
	maxPayloadBytes: 1024 * 1024 * 1024,
};

export interface PrivateFrame<THeader extends object> {
	header: THeader;
	payload: Buffer;
}

export type PrivateFrameHeaderValidator<THeader extends object> = (value: unknown) => value is THeader;

function assertFrameLength(name: string, value: number, maximum: number): void {
	if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
		throw new Error(`Invalid private frame ${name}: ${value}`);
	}
}

function isObjectHeader(value: unknown): value is object {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function encodePrivateFrameParts<THeader extends object>(
	header: THeader,
	payloadParts: readonly Uint8Array[],
	limits: PrivateFrameLimits = DEFAULT_PRIVATE_FRAME_LIMITS,
): Buffer[] {
	const headerBuffer = Buffer.from(JSON.stringify(header), "utf8");
	const payloadLength = payloadParts.reduce((total, part) => total + part.length, 0);
	assertFrameLength("header length", headerBuffer.length, limits.maxHeaderBytes);
	assertFrameLength("payload length", payloadLength, limits.maxPayloadBytes);
	if (headerBuffer.length === 0) throw new Error("Private frame header cannot be empty");
	const prefix = Buffer.allocUnsafe(FRAME_PREFIX_BYTES + headerBuffer.length);
	prefix.writeUInt32BE(headerBuffer.length, 0);
	prefix.writeUInt32BE(payloadLength, 4);
	headerBuffer.copy(prefix, FRAME_PREFIX_BYTES);
	return [prefix, ...payloadParts.map((part) => (Buffer.isBuffer(part) ? part : Buffer.from(part)))];
}

export function encodePrivateFrame<THeader extends object>(
	header: THeader,
	payload: Uint8Array = Buffer.alloc(0),
	limits: PrivateFrameLimits = DEFAULT_PRIVATE_FRAME_LIMITS,
): Buffer {
	const headerBuffer = Buffer.from(JSON.stringify(header), "utf8");
	const payloadBuffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
	assertFrameLength("header length", headerBuffer.length, limits.maxHeaderBytes);
	assertFrameLength("payload length", payloadBuffer.length, limits.maxPayloadBytes);
	if (headerBuffer.length === 0) {
		throw new Error("Private frame header cannot be empty");
	}

	const frame = Buffer.allocUnsafe(FRAME_PREFIX_BYTES + headerBuffer.length + payloadBuffer.length);
	frame.writeUInt32BE(headerBuffer.length, 0);
	frame.writeUInt32BE(payloadBuffer.length, 4);
	headerBuffer.copy(frame, FRAME_PREFIX_BYTES);
	payloadBuffer.copy(frame, FRAME_PREFIX_BYTES + headerBuffer.length);
	return frame;
}

export class PrivateFrameDecoder<THeader extends object> {
	private readonly buffers: Buffer[] = [];
	private headIndex = 0;
	private headOffset = 0;
	private totalBufferedBytes = 0;
	private totalCoalescedBytes = 0;
	private pendingLengths?: { header: number; payload: number };

	constructor(
		private readonly validateHeader: PrivateFrameHeaderValidator<THeader>,
		private readonly limits: PrivateFrameLimits = DEFAULT_PRIVATE_FRAME_LIMITS,
	) {}

	get bufferedBytes(): number {
		return this.totalBufferedBytes;
	}

	get coalescedBytes(): number {
		return this.totalCoalescedBytes;
	}

	push(chunk: Uint8Array): PrivateFrame<THeader>[] {
		if (chunk.length > 0) {
			this.buffers.push(
				Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength),
			);
			this.totalBufferedBytes += chunk.length;
		}

		const frames: PrivateFrame<THeader>[] = [];
		while (true) {
			if (!this.pendingLengths) {
				if (this.totalBufferedBytes < FRAME_PREFIX_BYTES) break;
				const prefix = this.consume(FRAME_PREFIX_BYTES);
				const headerLength = prefix.readUInt32BE(0);
				const payloadLength = prefix.readUInt32BE(4);
				assertFrameLength("header length", headerLength, this.limits.maxHeaderBytes);
				assertFrameLength("payload length", payloadLength, this.limits.maxPayloadBytes);
				if (headerLength === 0) throw new Error("Private frame header cannot be empty");
				this.pendingLengths = { header: headerLength, payload: payloadLength };
			}

			const { header: headerLength, payload: payloadLength } = this.pendingLengths;
			if (this.totalBufferedBytes < headerLength + payloadLength) break;
			const header = this.consume(headerLength);
			const payload = this.consume(payloadLength);
			this.pendingLengths = undefined;
			let decoded: unknown;
			try {
				decoded = JSON.parse(header.toString("utf8"));
			} catch (error) {
				throw new Error(
					`Invalid private frame header JSON: ${error instanceof Error ? error.message : String(error)}`,
				);
			}
			if (!isObjectHeader(decoded) || !this.validateHeader(decoded)) {
				throw new Error("Invalid private frame routing header");
			}
			frames.push({ header: decoded, payload });
		}
		return frames;
	}

	finish(): void {
		if (this.totalBufferedBytes !== 0 || this.pendingLengths) {
			throw new Error(
				`Private frame channel ended with ${this.totalBufferedBytes + (this.pendingLengths ? FRAME_PREFIX_BYTES : 0)} incomplete bytes`,
			);
		}
	}

	private consume(length: number): Buffer {
		if (length === 0) return Buffer.alloc(0);
		const first = this.buffers[this.headIndex];
		if (!first) throw new Error("Private frame decoder buffer underflow");
		const firstAvailable = first.length - this.headOffset;
		if (firstAvailable >= length) {
			const value = first.subarray(this.headOffset, this.headOffset + length);
			this.headOffset += length;
			this.totalBufferedBytes -= length;
			if (this.headOffset === first.length) this.releaseHeadBuffer();
			return value;
		}
		const value = Buffer.allocUnsafe(length);
		this.totalCoalescedBytes += length;
		let written = 0;
		while (written < length) {
			const current = this.buffers[this.headIndex];
			if (!current) throw new Error("Private frame decoder buffer underflow");
			const available = current.length - this.headOffset;
			const selected = Math.min(available, length - written);
			current.copy(value, written, this.headOffset, this.headOffset + selected);
			written += selected;
			this.headOffset += selected;
			this.totalBufferedBytes -= selected;
			if (this.headOffset === current.length) this.releaseHeadBuffer();
		}
		return value;
	}

	private releaseHeadBuffer(): void {
		this.headIndex++;
		this.headOffset = 0;
		if (this.headIndex === this.buffers.length) {
			this.buffers.length = 0;
			this.headIndex = 0;
			return;
		}
		if (this.headIndex >= FRAME_BUFFER_COMPACTION_MIN_HEAD && this.headIndex * 2 >= this.buffers.length) {
			this.buffers.splice(0, this.headIndex);
			this.headIndex = 0;
		}
	}
}
export type PrivateFrameListener<THeader extends object> = (frame: PrivateFrame<THeader>) => void;

export class PrivateFramedChannel<THeader extends object> {
	private readonly decoder: PrivateFrameDecoder<THeader>;
	private readonly listeners = new Set<PrivateFrameListener<THeader>>();
	private closed = false;

	constructor(
		private readonly stream: Duplex,
		validateHeader: PrivateFrameHeaderValidator<THeader>,
		private readonly limits: PrivateFrameLimits = DEFAULT_PRIVATE_FRAME_LIMITS,
	) {
		this.decoder = new PrivateFrameDecoder(validateHeader, limits);
		stream.on("data", this.handleData);
		stream.on("end", this.handleEnd);
		stream.on("close", this.handleClose);
	}

	onFrame(listener: PrivateFrameListener<THeader>): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async send(header: THeader, payload?: Uint8Array): Promise<void> {
		if (this.closed || this.stream.destroyed) {
			throw new Error("Private frame channel is closed");
		}
		const frame = encodePrivateFrame(header, payload, this.limits);
		await new Promise<void>((resolve, reject) => {
			this.stream.write(frame, (error?: Error | null) => {
				if (error) {
					reject(error);
				} else {
					resolve();
				}
			});
		});
	}

	close(): void {
		if (this.closed) {
			return;
		}
		this.closed = true;
		this.detach();
		this.stream.end();
	}

	private readonly handleData = (chunk: Buffer): void => {
		try {
			for (const frame of this.decoder.push(chunk)) {
				for (const listener of this.listeners) {
					listener(frame);
				}
			}
		} catch (error) {
			this.stream.destroy(error instanceof Error ? error : new Error(String(error)));
		}
	};

	private readonly handleEnd = (): void => {
		try {
			this.decoder.finish();
		} catch (error) {
			this.stream.destroy(error instanceof Error ? error : new Error(String(error)));
		}
	};

	private readonly handleClose = (): void => {
		this.closed = true;
		this.detach();
	};

	private detach(): void {
		this.stream.off("data", this.handleData);
		this.stream.off("end", this.handleEnd);
		this.stream.off("close", this.handleClose);
		this.listeners.clear();
	}
}
