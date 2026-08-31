import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import { PRIME_AGENT_SDK_FEATURES } from "../../../src/index.js";
import { cloneCallerOwnedSessionLaunchEnv } from "../../../src/modes/daemon/daemon-protocol.js";
import { createHarness, getAssistantTexts, type Harness } from "../harness.js";

describe("issue #33 caller-owned session contract", () => {
	let harness: Harness | undefined;

	afterEach(() => {
		harness?.cleanup();
		harness = undefined;
	});

	it("freezes the launch snapshot without leaking it into an ordinary faux-provider turn", async () => {
		const source = { PROVIDER_TOKEN: "issue-33-private-a", PATH: "/caller/a" };
		const snapshot = cloneCallerOwnedSessionLaunchEnv(source);
		source.PROVIDER_TOKEN = "issue-33-private-c";
		source.PATH = "/ambient/c";

		harness = await createHarness();
		harness.setResponses([fauxAssistantMessage("contract remains transport-only")]);
		await harness.session.prompt("run a faux-provider turn");

		expect(snapshot).toEqual({ PROVIDER_TOKEN: "issue-33-private-a", PATH: "/caller/a" });
		expect(Object.isFrozen(snapshot)).toBe(true);
		expect(PRIME_AGENT_SDK_FEATURES).toContain("caller_owned_session_environment_cleanup_v1");
		expect(getAssistantTexts(harness)).toContain("contract remains transport-only");
		const sessionSurface = JSON.stringify(harness.events);
		expect(sessionSurface).not.toContain("issue-33-private");
		expect(sessionSurface).not.toContain("/caller/a");
		expect(sessionSurface).not.toContain("/ambient/c");
	});
});
