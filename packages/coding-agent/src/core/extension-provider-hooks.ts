/**
 * Provider-request hooks bound to one session's extension runner.
 *
 * Anthropic-protocol providers carry session identity only in the payload
 * (`metadata.user_id`), which extensions stamp from `before_provider_request`.
 * The hook therefore has to resolve the runner of the session that owns the
 * request; sharing a parent's hook makes every child request claim the
 * parent's provider session.
 */

import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { SimpleStreamOptions } from "@earendil-works/pi-ai";
import type { ExtensionRunner } from "./extensions/index.js";

export type ProviderPayloadHook = NonNullable<SimpleStreamOptions["onPayload"]>;
export type ProviderResponseHook = NonNullable<SimpleStreamOptions["onResponse"]>;

export interface ExtensionProviderHooks {
	onPayload: ProviderPayloadHook;
	onResponse: ProviderResponseHook;
	transformContext: (messages: AgentMessage[]) => Promise<AgentMessage[]>;
}

export interface ExtensionProviderHookOptions {
	/**
	 * Derives a child identity from the owning session's id for requests that
	 * carry a history the session itself never sent (side questions,
	 * summarization). Without it those requests would key the provider's
	 * session cache to the owner while replaying a divergent conversation.
	 */
	sessionIdScope?: string;
}

/**
 * Build provider hooks that resolve their runner at call time.
 *
 * Late resolution is required in both directions: an `Agent` is constructed
 * before the `AgentSession` that owns its runner, and `/reload` replaces the
 * runner on a live session.
 */
export function createExtensionProviderHooks(
	getRunner: () => ExtensionRunner | undefined,
	options: ExtensionProviderHookOptions = {},
): ExtensionProviderHooks {
	const { sessionIdScope } = options;
	return {
		onPayload: async (payload) => {
			const runner = getRunner();
			if (!runner?.hasHandlers("before_provider_request")) {
				return payload;
			}
			return runner.emitBeforeProviderRequest(payload, { sessionIdScope });
		},
		onResponse: async (response) => {
			const runner = getRunner();
			if (!runner?.hasHandlers("after_provider_response")) {
				return;
			}
			await runner.emit({
				type: "after_provider_response",
				status: response.status,
				headers: response.headers,
			});
		},
		transformContext: async (messages) => {
			const runner = getRunner();
			if (!runner) return messages;
			return runner.emitContext(messages, { sessionIdScope });
		},
	};
}
