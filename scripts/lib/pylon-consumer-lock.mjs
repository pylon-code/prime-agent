import { randomUUID } from "node:crypto";
import lockfile from "proper-lockfile";
import { closeSync, fsyncSync, linkSync, lstatSync, mkdirSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
const anchorContents = "pylon-consumer-state-lock-v1\n";

function ensureAnchor(anchorPath) {
	const temporary = `${anchorPath}.${process.pid}.${randomUUID()}.tmp`;
	let descriptor;
	try {
		descriptor = openSync(temporary, "wx", 0o600);
		writeFileSync(descriptor, anchorContents);
		fsyncSync(descriptor);
		closeSync(descriptor);
		descriptor = undefined;
		try {
			linkSync(temporary, anchorPath);
		} catch (error) {
			if (error?.code !== "EEXIST") throw error;
		}
	} finally {
		if (descriptor !== undefined) closeSync(descriptor);
		rmSync(temporary, { force: true });
	}
	const entry = lstatSync(anchorPath);
	if (!entry.isFile() || readFileSync(anchorPath, "utf8") !== anchorContents) {
		throw new Error("Consumer high-water lock anchor is not one exact regular file.");
	}
}

export function withConsumerStateLock(statePath, action) {
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	mkdirSync(directory, { recursive: true, mode: 0o700 });
	if (!lstatSync(directory).isDirectory()) {
		throw new Error("Consumer high-water state directory must be one canonical real directory.");
	}
	const anchorPath = `${absoluteStatePath}.lock-anchor`;
	ensureAnchor(anchorPath);
	let release;
	try {
		release = lockfile.lockSync(anchorPath, {
			realpath: true,
			lockfilePath: `${absoluteStatePath}.lock`,
			stale: PYLON_CONSUMER_LOCK_STALE_MS,
			update: PYLON_CONSUMER_LOCK_UPDATE_MS,
			retries: 0,
		});
	} catch (error) {
		if (error?.code === "ELOCKED") throw new Error(`Consumer high-water state is actively locked: ${absoluteStatePath}.lock`);
		throw error;
	}
	try {
		return action(absoluteStatePath);
	} finally {
		release();
	}
}
