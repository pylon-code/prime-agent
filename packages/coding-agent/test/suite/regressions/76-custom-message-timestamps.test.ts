import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, expect, it, vi } from "vitest";
import { buildSessionContext, SessionManager } from "../../../src/core/session-manager.js";
import { createHarness, type Harness } from "../harness.js";

const harnesses: Harness[] = [];
afterEach(() => {
	while (harnesses.length) harnesses.pop()!.cleanup();
	vi.restoreAllMocks();
	vi.useRealTimers();
});

it("preserves live custom identities through delayed persistence, compaction and reload", async () => {
	vi.useFakeTimers({ toFake: ["Date"] });
	const harness = await createHarness({
		persistSession: true,
		settings: { compaction: { enabled: false, keepRecentTokens: 1 }, autoRefine: { enabled: false } },
	});
	harnesses.push(harness);
	const append = harness.sessionManager.appendCustomMessageEntry.bind(harness.sessionManager);
	vi.spyOn(harness.sessionManager, "appendCustomMessageEntry").mockImplementation((...args) => {
		vi.setSystemTime(Date.now() + 50);
		return append(...args);
	});
	harness.setResponses(["one", "two", "summary", "turn summary"].map((text) => fauxAssistantMessage(text)));
	await harness.session.prompt("one");
	await harness.session.sendCustomMessage({
		customType: "async_bash_completion",
		content: "done",
		display: true,
		details: { pid: 1, command: "fixture", exitCode: 0 },
	});
	await harness.session.sendCustomMessage(
		{ customType: "ipython_state_restored", content: "restored", display: false, details: { restored: true } },
		{ deliverAs: "nextTurn" },
	);
	await harness.session.prompt("two");
	const before = structuredClone(harness.session.messages);
	expect(harness.sessionManager.buildSessionContext().messages).toEqual(before);
	await harness.session.compact();
	const compaction = harness.sessionManager
		.getEntries()
		.slice()
		.reverse()
		.find((entry) => entry.type === "compaction");
	expect(compaction).toBeDefined();
	expect(buildSessionContext(harness.sessionManager.getEntries(), compaction!.parentId).messages).toEqual(before);
	expect(harness.sessionManager.buildSessionContext().messages).toEqual(harness.session.messages);
	const reopened = SessionManager.open(harness.sessionManager.getSessionFile()!);
	expect(reopened.buildSessionContext().messages).toEqual(harness.session.messages);
});

it("preserves an explicit timestamp through rollback-backed writes and rejects lossy timestamps", async () => {
	const harness = await createHarness({ persistSession: true });
	harnesses.push(harness);
	const manager = harness.sessionManager;
	manager.appendCustomMessageEntryWithRollback("refinement_notice", "notice", false, { source: "auto" }, 1234);
	expect(manager.buildSessionContext().messages.at(-1)?.timestamp).toBe(1234);
	const before = manager.getEntries();
	for (const timestamp of [NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER]) {
		expect(() =>
			manager.appendCustomMessageEntryWithRollback("refinement_notice", "invalid", false, {}, timestamp),
		).toThrow();
		expect(manager.getEntries()).toEqual(before);
	}
	manager.appendCustomMessageEntry("default-time", "notice", false);
	expect(manager.buildSessionContext().messages.at(-1)?.timestamp).toBeGreaterThan(1234);
});
