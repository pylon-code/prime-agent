import { basename, join } from "node:path";
import { buildConsumerGeneration, prepareConsumerGeneration, publishConsumerGeneration, recoverConsumerGenerationBuilder, rotateConsumerGeneration, withConsumerGenerationLock } from "../../lib/pylon-consumer-lock.mjs";

const [root, mode, cut = "", suppliedBuilder = ""] = process.argv.slice(2);
const authority = { genesis: { statePath: join(root, "..", "state.json"), stateBytes: null } };
let stopped = false;
const boundary = async ({ phase, operation, path }) => {
	const projection = basename(path).startsWith(".projection-");
	const hit = cut === "projection-created" && phase === "after" && operation === "create-projection" ||
		cut === "projection-synced" && projection && phase === "after" && operation === "file-sync" ||
		cut === "projection-before-rename" && phase === "before" && operation === "projection-rename" ||
		cut === "projection-after-rename" && phase === "after" && operation === "projection-rename" ||
		cut === "builder-before-publish" && phase === "before" && operation === "rename" && basename(path).startsWith("generation-") ||
		cut === "retire-before-rename" && phase === "before" && operation === "rename" && basename(path).startsWith(".retired-") ||
		cut === "delete-before-rename" && phase === "before" && operation === "rename" && basename(path).startsWith(".deleting-") ||
		cut === "retired" && phase === "after" && operation === "rename" && basename(path).startsWith(".retired-") ||
		cut === "deleting" && phase === "after" && operation === "rename" && basename(path).startsWith(".deleting-");
	if (!stopped && hit) {
		stopped = true;
		process.send?.({ type: "cut", phase, operation, path, pid: process.pid });
		await new Promise((resolve) => process.once("message", resolve));
	}
};
const options = { stateMaxBytes: 1024, startHeartbeat: () => async () => {}, hooks: { generationBoundary: boundary } };
try {
	let result;
	if (mode === "commit") result = await withConsumerGenerationLock(root, authority, async (_path, tx) => tx.commitState(Buffer.from("worker-state")), options);
	else if (mode === "rotate") result = await rotateConsumerGeneration(root, authority, options);
	else if (mode === "prepare") result = await prepareConsumerGeneration(root, authority, options);
	else if (mode === "builder") {
		const builder = suppliedBuilder ? await recoverConsumerGenerationBuilder(suppliedBuilder, authority, options) : await buildConsumerGeneration(root, authority, options);
		result = await publishConsumerGeneration(builder, options);
	} else throw new Error("Unknown generation worker mode.");
	process.send?.({ type: "done", epoch: result?.checkpoint?.epoch ?? result?.epoch });
	process.disconnect?.();
} catch (error) {
	process.send?.({ type: "error", message: error.message, code: error.code });
	process.disconnect?.();
	process.exitCode = 1;
}
