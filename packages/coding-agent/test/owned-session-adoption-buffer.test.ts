import { describe, expect, it } from "vitest";
import {
	appendRecoverableOwnedFrame,
	RECOVERABLE_OWNED_MAX_BUFFERED_BYTES,
	RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES,
	reconcileRecoverableOwnedFrames,
} from "../src/modes/daemon/owned-session-adoption-buffer.js";
import { OWNED_SESSION_ADOPTION_UNAVAILABLE } from "../src/modes/daemon/owned-session-recovery-store.js";

function frame(sequence: number, generation = "generation-a", bytes = 1) {
	return { payload: Buffer.alloc(bytes), cursor: { generation, sequence }, message: `event-${sequence}` };
}

describe("recoverable owned-session adoption buffers", () => {
	it("drops snapshot-covered frames and retains strict post-snapshot order exactly once", () => {
		const reconciled = reconcileRecoverableOwnedFrames([frame(4), frame(5), frame(6), frame(7)], {
			generation: "generation-a",
			sequence: 5,
		});
		expect(reconciled.frames.map((entry) => entry.message)).toEqual(["event-6", "event-7"]);
		expect(reconciled.bufferedBytes).toBe(2);
	});

	it("deduplicates exact cursor and canonical payload retries but rejects conflicting duplicates", () => {
		const frames = [frame(6), frame(7)];
		let bufferedBytes = 2;
		bufferedBytes = appendRecoverableOwnedFrame(frames, bufferedBytes, frame(6));
		bufferedBytes = appendRecoverableOwnedFrame(frames, bufferedBytes, frame(7));
		expect(frames.map((entry) => entry.message)).toEqual(["event-6", "event-7"]);
		expect(bufferedBytes).toBe(2);
		expect(() =>
			appendRecoverableOwnedFrame(frames, bufferedBytes, {
				...frame(6),
				payload: Buffer.from("conflict"),
			}),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
	});

	it("fails closed on malformed, cross-generation, gapped, count-overflow, and byte-overflow frames", () => {
		for (const frames of [
			[{ payload: Buffer.alloc(1), message: "missing" }],
			[frame(6, "generation-b")],
			[frame(7)],
		]) {
			expect(() => reconcileRecoverableOwnedFrames(frames, { generation: "generation-a", sequence: 5 })).toThrow(
				OWNED_SESSION_ADOPTION_UNAVAILABLE,
			);
		}
		expect(() =>
			appendRecoverableOwnedFrame(
				Array.from({ length: RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES }, (_, index) => frame(index)),
				RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES,
				frame(RECOVERABLE_OWNED_MAX_BUFFERED_FRAMES),
			),
		).toThrow(OWNED_SESSION_ADOPTION_UNAVAILABLE);
		expect(() => appendRecoverableOwnedFrame([], RECOVERABLE_OWNED_MAX_BUFFERED_BYTES, frame(1))).toThrow(
			OWNED_SESSION_ADOPTION_UNAVAILABLE,
		);
	});
});
