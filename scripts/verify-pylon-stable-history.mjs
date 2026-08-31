#!/usr/bin/env node

import { lstatSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { canonicalJson, validateStableHistory, validateStableManifest } from "./lib/pylon-publication.mjs";

export function verifyStableManifestFiles(paths) {
	if (!Array.isArray(paths) || paths.length === 0) throw new Error("Provide every stable manifest path in sequence order.");
	const manifests = paths.map((input) => {
		const path = resolve(input);
		if (!lstatSync(path).isFile()) throw new Error(`Stable manifest is not a regular file: ${path}`);
		const bytes = readFileSync(path);
		const manifest = validateStableManifest(JSON.parse(bytes));
		if (bytes.toString("utf8") !== canonicalJson(manifest)) throw new Error(`Stable manifest is not canonical: ${path}`);
		return manifest;
	});
	return validateStableHistory(manifests);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const history = verifyStableManifestFiles(process.argv.slice(2));
		console.log(JSON.stringify({ sequences: history.length, highWater: history.at(-1).tag }));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
