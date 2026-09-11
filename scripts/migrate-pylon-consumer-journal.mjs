#!/usr/bin/env node

import { resolve } from "node:path";

import { migrateConsumerStateJournal } from "./lib/pylon-consumer-lock.mjs";

function parseArgs(args) {
 const remaining = [...args];
 const acknowledgement = remaining.indexOf("--acknowledge-legacy-processes-stopped");
 if (acknowledgement !== -1) remaining.splice(acknowledgement, 1);
 if (acknowledgement === -1 || remaining.length !== 2 || remaining[0] !== "--state" || !remaining[1] || remaining[1].startsWith("--")) {
  throw new Error("Usage: migrate-pylon-consumer-journal --state <path> --acknowledge-legacy-processes-stopped");
 }
 return resolve(remaining[1]);
}

try {
	const result = await migrateConsumerStateJournal(parseArgs(process.argv.slice(2)), { acknowledgeLegacyProcessesStopped: true });
	console.log(JSON.stringify({
		journalEpoch: result.epoch,
		tipSha256: result.tipSha256,
		sourceAuthoritySha256: result.sourceAuthoritySha256,
	}));
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
