import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
	link,
	lstat,
	mkdir,
	open,
	readFile,
	rm,
} from "node:fs/promises";
import { dirname, resolve } from "node:path";

import lockfile from "proper-lockfile";

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
const anchorContents = "pylon-consumer-state-lock-v1\n";

async function ensureAnchor(anchorPath) {
	const temporary = `${anchorPath}.${process.pid}.${randomUUID()}.tmp`;
	let handle;
	try {
		handle = await open(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
		await handle.writeFile(anchorContents);
		await handle.sync();
		await handle.close();
		handle = undefined;
		try {
			await link(temporary, anchorPath);
		} catch (error) {
			if (error?.code !== "EEXIST") throw error;
		}
	} finally {
		if (handle !== undefined) await handle.close();
		await rm(temporary, { force: true });
	}
	const entry = await lstat(anchorPath);
	if (!entry.isFile() || await readFile(anchorPath, "utf8") !== anchorContents) {
		throw new Error("Consumer high-water lock anchor is not one exact regular file.");
	}
}

export async function withConsumerStateLock(
	statePath,
	action,
	{
		stale = PYLON_CONSUMER_LOCK_STALE_MS,
		update = PYLON_CONSUMER_LOCK_UPDATE_MS,
	} = {},
) {
	const absoluteStatePath = resolve(statePath);
	const directory = dirname(absoluteStatePath);
	await mkdir(directory, { recursive: true, mode: 0o700 });
	if (!(await lstat(directory)).isDirectory()) {
		throw new Error("Consumer high-water state directory must be one canonical real directory.");
	}
	const anchorPath = `${absoluteStatePath}.lock-anchor`;
	await ensureAnchor(anchorPath);
	let release;
	try {
		release = await lockfile.lock(anchorPath, {
			realpath: true,
			lockfilePath: `${absoluteStatePath}.lock`,
			stale,
			update,
			retries: 0,
		});
	} catch (error) {
		if (error?.code === "ELOCKED") throw new Error(`Consumer high-water state is actively locked: ${absoluteStatePath}.lock`);
		throw error;
	}
	try {
		return await action(absoluteStatePath);
	} finally {
		await release();
	}
}
