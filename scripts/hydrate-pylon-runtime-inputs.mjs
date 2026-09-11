#!/usr/bin/env node
import { resolve } from "node:path";
import { hydrateRuntimeDependencies } from "./lib/pylon-runtime-dependencies.mjs";

try {
	const count = await hydrateRuntimeDependencies(resolve(import.meta.dirname, ".."));
	console.log(`Verified inputs for ${count} locked runtime packages, including all platforms.`);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
