import { serializeJsonLine } from "../rpc/jsonl.js";
import { OwnedSessionAdoptionUnavailableError } from "./owned-session-recovery-store.js";

export const RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES = 4096;
export const RECOVERABLE_OWNED_MAX_BUFFERED_BYTES = 8 * 1024 * 1024;

export interface RecoverableOwnedSequenceCursor {
	generation: string;
	sequence: number;
}

export function serializeRecoverableOwnedFrame(value: unknown): Buffer {
	try {
		return Buffer.from(serializeJsonLine(value));
	} catch {
		throw new OwnedSessionAdoptionUnavailableError();
	}
}

export interface RecoverableOwnedSequenceFrame {
	payload: Buffer;
	cursor?: RecoverableOwnedSequenceCursor;
}

export function appendRecoverableOwnedFrame<TFrame extends RecoverableOwnedSequenceFrame>(
	frames: TFrame[],
	bufferedBytes: number,
	frame: TFrame,
): number {
	if (
		!frame.cursor ||
		!frame.cursor.generation ||
		!Number.isSafeInteger(frame.cursor.sequence) ||
		frame.cursor.sequence < 0
	) {
		throw new OwnedSessionAdoptionUnavailableError();
	}
	const duplicate = frames.find(
		(existing) =>
			existing.cursor?.generation === frame.cursor!.generation &&
			existing.cursor.sequence === frame.cursor!.sequence,
	);
	if (duplicate) {
		if (!duplicate.payload.equals(frame.payload)) throw new OwnedSessionAdoptionUnavailableError();
		return bufferedBytes;
	}
	if (
		frames.length >= RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES ||
		frame.payload.length > RECOVERABLE_OWNED_MAX_BUFFERED_BYTES - bufferedBytes
	) {
		throw new OwnedSessionAdoptionUnavailableError();
	}
	frames.push(frame);
	return bufferedBytes + frame.payload.length;
}

export function reconcileRecoverableOwnedFrames<TFrame extends RecoverableOwnedSequenceFrame>(
	frames: readonly TFrame[],
	snapshotCursor: RecoverableOwnedSequenceCursor,
): { frames: TFrame[]; bufferedBytes: number } {
	const retained: TFrame[] = [];
	for (const frame of frames) {
		if (!frame.cursor || frame.cursor.generation !== snapshotCursor.generation) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		if (frame.cursor.sequence > snapshotCursor.sequence) retained.push(frame);
	}
	let expected = snapshotCursor.sequence + 1;
	let bufferedBytes = 0;
	for (const frame of retained) {
		if (
			frame.cursor!.sequence !== expected ||
			retained.length > RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES ||
			frame.payload.length > RECOVERABLE_OWNED_MAX_BUFFERED_BYTES - bufferedBytes
		) {
			throw new OwnedSessionAdoptionUnavailableError();
		}
		expected++;
		bufferedBytes += frame.payload.length;
	}
	return { frames: retained, bufferedBytes };
}
