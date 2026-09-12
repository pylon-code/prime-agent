import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { describe, expect, it } from "vitest";
import { DaemonAgentConnection } from "../../../src/modes/agent-connection/daemon-agent-connection.js";
import type { AgentConnectionEvent } from "../../../src/modes/agent-connection/types.js";
import type { DaemonClientMessageListener, DaemonTransportClient } from "../../../src/modes/daemon/daemon-client.js";
import {
	DAEMON_PROTOCOL_INFO,
	type DaemonCommand,
	type DaemonOutbound,
} from "../../../src/modes/daemon/daemon-protocol.js";
import { createHarness } from "../harness.js";

describe("issue #62 SDK recovery event cursors", () => {
	it("preserves each accepted event cursor without exposing rejected frames or synthesizing legacy cursors", async () => {
		const harness = await createHarness();
		harness.setResponses([fauxAssistantMessage("completed answer")]);
		let receive: DaemonClientMessageListener = () => {};
		const client = {
			onMessage(listener: DaemonClientMessageListener) {
				receive = listener;
				return () => {};
			},
			onClose: () => () => {},
			getTransportGeneration: () => 1,
			supportsServerCapability: () => true,
			async request(command: DaemonCommand) {
				if (command.type !== "attach") return { type: "response", command: command.type, success: true };
				const state = { activeSessionId: "active-1", sessionId: "session-1" };
				return {
					type: "response",
					command: "attach",
					success: true,
					data: {
						activeSessionId: "active-1",
						client: { id: command.clientId, capabilities: command.capabilities },
						snapshot: {
							activeSessionId: "active-1",
							state,
							summary: { id: "active-1", sessionId: "session-1" },
							messages: [],
							promptLifecycles: { records: [], expired: [] },
							lastEventSequence: 0,
							lastEventCursor: { generation: "generation-1", sequence: 0 },
						},
					},
				};
			},
		} as unknown as DaemonTransportClient;
		const connection = new DaemonAgentConnection(client, "active-1");
		try {
			await harness.session.prompt("answer", { promptCorrelationId: "prompt-1" });
			const messageEnd = harness.eventsOfType("message_end").find((event) => event.message.role === "assistant");
			if (!messageEnd) throw new Error("Faux turn did not produce an assistant message");
			await connection.attach();
			const accepted: AgentConnectionEvent[] = [];
			connection.subscribe((event) => {
				accepted.push(event);
			});
			const metadata = { id: "event-id", protocol: DAEMON_PROTOCOL_INFO, emittedAt: "2026-09-12T00:00:00.000Z" };
			const sessionEvent = (sequence: number, generation = "generation-1") =>
				({
					type: "session_event",
					activeSessionId: "active-1",
					event: messageEnd,
					attribution: { scope: "prompt", correlationId: "prompt-1" },
					meta: { ...metadata, sequence, cursor: { generation, sequence } },
				}) satisfies DaemonOutbound;
			const first = sessionEvent(1);
			receive(first);
			receive(first);
			receive({ ...sessionEvent(2), activeSessionId: "other-session" });
			receive({ ...sessionEvent(2), attribution: { scope: "session" } });
			const lifecycle = {
				type: "prompt_lifecycle",
				activeSessionId: "active-1",
				lifecycle: {
					correlationId: "prompt-1",
					kind: "model_prompt",
					phase: "owned",
					revision: 1,
					deliveryCrossed: false,
				},
				meta: { ...metadata, sequence: 2, cursor: { generation: "generation-1", sequence: 2 } },
			} satisfies DaemonOutbound;
			receive(lifecycle);
			receive({
				...lifecycle,
				meta: { ...metadata, sequence: 3, cursor: { generation: "generation-1", sequence: 3 } },
			});
			receive(sessionEvent(1, "generation-2"));
			receive(sessionEvent(100));
			const { meta: _meta, ...legacy } = sessionEvent(2, "generation-2");
			receive({ ...legacy, meta: { ...metadata, sequence: 2 } });

			const sessionEvents = accepted.filter((event) => event.type === "session_event");
			expect(sessionEvents).toHaveLength(3);
			expect(sessionEvents.map((event) => event.meta)).toEqual([
				{ cursor: { generation: "generation-1", sequence: 1 } },
				{ cursor: { generation: "generation-2", sequence: 1 } },
				undefined,
			]);
			expect(accepted.filter((event) => event.type === "prompt_lifecycle")).toEqual([
				{ type: "prompt_lifecycle", lifecycle: lifecycle.lifecycle, meta: { cursor: lifecycle.meta.cursor } },
			]);
			expect(accepted.filter((event) => event.type === "correlated_prompt_protocol_violation")).toHaveLength(1);
			expect(sessionEvents[0]?.meta?.cursor).toEqual(first.meta.cursor);
			expect(sessionEvents[0]?.meta?.cursor).not.toBe(first.meta.cursor);
		} finally {
			await connection.dispose();
			harness.cleanup();
		}
	});
});
