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
import { dirname, join, parse, relative, resolve, sep } from "node:path";

import lockfile from "proper-lockfile";

export const PYLON_CONSUMER_LOCK_STALE_MS = 30_000;
export const PYLON_CONSUMER_LOCK_UPDATE_MS = 10_000;
const anchorContents = "pylon-consumer-state-lock-v1\n";

export async function syncConsumerStateDirectory(path, { openDirectory = open } = {}) {
	let handle;
	try {
		handle = await openDirectory(path, "r");
		await handle.sync();
	} catch (error) {
		if (!["EINVAL", "EPERM", "EISDIR"].includes(error?.code)) throw error;
	} finally {
		if (handle !== undefined) await handle.close();
	}
}

export async function ensureDurableConsumerStateDirectory(
	directory,
	{
		lstatEntry = lstat,
		makeDirectory = mkdir,
		syncDirectory = syncConsumerStateDirectory,
	} = {},
) {
	const absolute = resolve(directory);
	const root = parse(absolute).root;
	let parent = root;
	const rootEntry = await lstatEntry(root);
	if (!rootEntry.isDirectory()) {
		throw new Error("Consumer high-water state directory must be one canonical real directory.");
	}
	const remainder = relative(root, absolute);
	for (const component of remainder ? remainder.split(sep) : []) {
		const current = join(parent, component);
		let entry;
		try {
			entry = await lstatEntry(current);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			try {
				await makeDirectory(current, { mode: 0o700 });
			} catch (mkdirError) {
				if (mkdirError?.code !== "EEXIST") throw mkdirError;
			}
			await syncDirectory(parent);
			entry = await lstatEntry(current);
		}
		if (!entry.isDirectory()) {
			throw new Error("Consumer high-water state directory must be one canonical real directory.");
		}
		parent = current;
	}
	return absolute;
}

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
	await ensureDurableConsumerStateDirectory(directory);
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
