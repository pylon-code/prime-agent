import type { Context, Message, UserMessage } from "../types.js";

/** Volatile content for this request with surrounding whitespace removed, or undefined when empty. */
export function resolveVolatileContext(context: Context): string | undefined {
	const text = context.volatileContext?.trim();
	return text ? text : undefined;
}

/**
 * Move volatile content to the end of the message list.
 *
 * Used for providers that do not place explicit cache breakpoints: the content
 * still reaches the model, and because it is last it cannot shift the bytes of
 * an automatically cached prefix.
 */
export function foldVolatileContext(context: Context): Context {
	const text = resolveVolatileContext(context);
	if (!text) {
		return context.volatileContext === undefined ? context : { ...context, volatileContext: undefined };
	}

	const trailing: UserMessage = {
		role: "user",
		content: [{ type: "text", text }],
		timestamp: Date.now(),
	};
	const messages: Message[] = [...context.messages, trailing];
	return { ...context, messages, volatileContext: undefined };
}
