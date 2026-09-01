#!/usr/bin/env node

import { resolve } from "node:path";

import { rotateConsumerStateJournal } from "./lib/pylon-consumer-lock.mjs";

function parseArgs(args) {
	if (args.length !== 2 || args[0] !== "--state" || !args[1] || args[1].startsWith("--")) {
		throw new Error("Usage: rotate-pylon-consumer-journal --state <path>");
	}
	return resolve(args[1]);
}

try {
	const result = await rotateConsumerStateJournal(parseArgs(process.argv.slice(2)));
	console.log(JSON.stringify({ journalEpoch: result.epoch, tipSha256: result.tipSha256 }));
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
