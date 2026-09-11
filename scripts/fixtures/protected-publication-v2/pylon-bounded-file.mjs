import { createHash } from "node:crypto";
import {
	closeSync,
	constants,
	fstatSync,
	lstatSync,
	openSync,
	readSync,
} from "node:fs";
import { lstat, open } from "node:fs/promises";
export const PYLON_PUBLICATION_MANIFEST_MAX_BYTES = 64 * 1024;
export const PYLON_STABLE_HISTORY_MAX_MANIFESTS = 4096;
export const PYLON_STABLE_HISTORY_MAX_BYTES = 32 * 1024 * 1024;

function statEvidence(stat) {
	return Object.freeze({
		dev: stat.dev,
		ino: stat.ino,
		size: stat.size,
		mtimeMs: stat.mtimeMs,
		ctimeMs: stat.ctimeMs,
		nlink: stat.nlink,
	});
}

export class BoundedFileUnlinkedDuringReadError extends Error {
	constructor(path, description, bytes, expectedSha256, pathEntry, before, after, confirmedHandle = null) {
		super(`${description} changed while it was read because the same opened inode was removed.`);
		this.name = "BoundedFileUnlinkedDuringReadError";
		this.path = path;
		this.description = description;
		this.bytes = Buffer.from(bytes);
		this.expectedSha256 = expectedSha256;
		this.sha256 = createHash("sha256").update(this.bytes).digest("hex");
		this.statTransition = Object.freeze({
			pathEntry: statEvidence(pathEntry),
			before: statEvidence(before),
			after: statEvidence(after),
			confirmedHandle: confirmedHandle === null ? null : statEvidence(confirmedHandle),
		});
	}
}

export class BoundedFileLinkRetiredBeforeReadError extends Error {
	constructor(path, description, bytes, expectedSha256, pathEntry, openedHandle) {
		super(`${description} changed while it was read because one publication hardlink was retired before the file was opened.`);
		this.name = "BoundedFileLinkRetiredBeforeReadError";
		this.path = path;
		this.description = description;
		this.bytes = Buffer.from(bytes);
		this.expectedSha256 = expectedSha256;
		this.sha256 = createHash("sha256").update(this.bytes).digest("hex");
		this.statTransition = Object.freeze({
			pathEntry: statEvidence(pathEntry),
			openedHandle: statEvidence(openedHandle),
		});
	}
}

export class BoundedFileLinkRetiredDuringReadError extends Error {
	constructor(path, description, bytes, expectedSha256, pathEntry, before, after, finalPathEntry) {
		super(`${description} changed while it was read because one publication hardlink was retired during the bounded read.`);
		this.name = "BoundedFileLinkRetiredDuringReadError";
		this.path = path;
		this.description = description;
		this.bytes = Buffer.from(bytes);
		this.expectedSha256 = expectedSha256;
		this.sha256 = createHash("sha256").update(this.bytes).digest("hex");
		this.statTransition = Object.freeze({
			pathEntry: statEvidence(pathEntry),
			before: statEvidence(before),
			after: statEvidence(after),
			finalPathEntry: statEvidence(finalPathEntry),
		});
	}
}

function sameInodeReadBounds(left, right) {
	return left.dev === right.dev && left.ino === right.ino && left.size === right.size &&
		left.mtimeMs === right.mtimeMs;
}

function sameStat(left, right) {
	return sameInodeReadBounds(left, right) && left.ctimeMs === right.ctimeMs && left.nlink === right.nlink;
}

function exactMonotoneStatCut(observations, fromLinks, toLinks) {
	if (
		observations.length < 2 || observations[0].nlink !== fromLinks ||
		observations.at(-1).nlink !== toLinks ||
		observations.some((stat) => !sameInodeReadBounds(observations[0], stat))
	) return null;
	let cut = null;
	for (let index = 1; index < observations.length; index += 1) {
		const previous = observations[index - 1];
		const current = observations[index];
		if (previous.nlink === current.nlink) {
			if (previous.ctimeMs !== current.ctimeMs) return null;
			continue;
		}
		if (
			cut !== null || previous.nlink !== fromLinks || current.nlink !== toLinks ||
			previous.ctimeMs === current.ctimeMs
		) return null;
		cut = index;
	}
	return cut;
}

function permitsInitialStatTransition(pathEntry, before, expectedSha256) {
	return sameStat(pathEntry, before) || (
		expectedSha256 !== null && (
			exactMonotoneStatCut([pathEntry, before], 2, 1) === 1 ||
			exactMonotoneStatCut([pathEntry, before], 1, 0) === 1
		)
	);
}

function isRegularPathEntry(pathEntry) {
	return !pathEntry.isSymbolicLink?.() && pathEntry.isFile();
}

function exactSha256(bytes, expectedSha256) {
	return expectedSha256 !== null && createHash("sha256").update(bytes).digest("hex") === expectedSha256;
}

export async function readBoundedRegularFile(
	path,
	{
		maxBytes,
		minBytes = 1,
		description = "Input",
		openFile = open,
		lstatEntry = lstat,
		validateHandle,
		hooks,
		expectedSha256 = null,
	} = {},
) {
	if (
		!Number.isSafeInteger(maxBytes) || maxBytes < 1 || !Number.isSafeInteger(minBytes) || minBytes < 0 || minBytes > maxBytes ||
		!(expectedSha256 === null || /^[0-9a-f]{64}$/.test(expectedSha256))
	) {
		throw new Error("Bounded file limits are invalid.");
	}
	let pathEntry;
	try {
		pathEntry = await lstatEntry(path);
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		throw error;
	}
	if (pathEntry.isSymbolicLink?.() || !pathEntry.isFile()) {
		throw new Error(`${description} is not one regular non-symlink file.`);
	}
	let handle;
	try {
		handle = await openFile(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		if (["ELOOP", "EISDIR"].includes(error?.code)) {
			throw new Error(`${description} is not one regular non-symlink file.`);
		}
		throw error;
	}
	try {
		let before = await handle.stat();
		if (!before.isFile()) throw new Error(`${description} is not one regular non-symlink file.`);
		if (validateHandle) before = await validateHandle(handle, before, description);
		if (!permitsInitialStatTransition(pathEntry, before, expectedSha256)) {
			throw new Error(`${description} changed while it was read.`);
		}
		if (before.size < minBytes || before.size > maxBytes) throw new Error(`${description} exceeds its format byte limit or is malformed.`);
		await hooks?.afterInitialStat?.({ path, handle, stat: before });
		const bytes = Buffer.alloc(before.size);
		let offset = 0;
		while (offset < bytes.length) {
			const { bytesRead } = await handle.read(bytes, offset, bytes.length - offset, offset);
			if (bytesRead === 0) throw new Error(`${description} changed while it was read.`);
			offset += bytesRead;
		}
		const extra = Buffer.alloc(1);
		const { bytesRead: extraBytes } = await handle.read(extra, 0, 1, bytes.length);
		await hooks?.beforeFinalStat?.({ path, handle, bytes });
		const after = await handle.stat();
		await hooks?.afterFinalStat?.({ path, handle, stat: after, bytes });
		let finalPathEntry;
		let finalPathMissing = false;
		try {
			finalPathEntry = await lstatEntry(path);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			finalPathMissing = true;
		}
		let confirmedHandle = null;
		if (finalPathMissing && after.nlink === 1 && sameStat(pathEntry, before) && sameStat(before, after)) {
			confirmedHandle = await handle.stat();
		}
		if (extraBytes === 0 && exactSha256(bytes, expectedSha256)) {
			if (!finalPathMissing && isRegularPathEntry(finalPathEntry)) {
				const retirementCut = exactMonotoneStatCut([pathEntry, before, after, finalPathEntry], 2, 1);
				if (retirementCut === 1) {
					throw new BoundedFileLinkRetiredBeforeReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
					);
				}
				if (retirementCut !== null) {
					throw new BoundedFileLinkRetiredDuringReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
						after,
						finalPathEntry,
					);
				}
			}
			if (finalPathMissing) {
				const unlinkStats = [pathEntry, before, after];
				if (confirmedHandle !== null) unlinkStats.push(confirmedHandle);
				if (exactMonotoneStatCut(unlinkStats, 1, 0) !== null) {
					throw new BoundedFileUnlinkedDuringReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
						after,
						confirmedHandle,
					);
				}
			}
		}
		if (
			extraBytes !== 0 || finalPathMissing || !isRegularPathEntry(finalPathEntry) ||
			!sameStat(pathEntry, before) || !sameStat(before, after) || !sameStat(after, finalPathEntry)
		) throw new Error(`${description} changed while it was read.`);
		return bytes;
	} finally {
		await handle.close();
	}
}


export function readBoundedRegularFileSync(
	path,
	{
		maxBytes,
		minBytes = 1,
		description = "Input",
		openFile = openSync,
		lstatEntry = lstatSync,
		statFile = fstatSync,
		readFile = readSync,
		closeFile = closeSync,
		hooks,
		expectedSha256 = null,
	} = {},
) {
	if (
		!Number.isSafeInteger(maxBytes) || maxBytes < 1 || !Number.isSafeInteger(minBytes) || minBytes < 0 || minBytes > maxBytes ||
		!(expectedSha256 === null || /^[0-9a-f]{64}$/.test(expectedSha256))
	) {
		throw new Error("Bounded file limits are invalid.");
	}
	let pathEntry;
	try {
		pathEntry = lstatEntry(path);
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		throw error;
	}
	if (pathEntry.isSymbolicLink?.() || !pathEntry.isFile()) {
		throw new Error(`${description} is not one regular non-symlink file.`);
	}
	let descriptor;
	try {
		descriptor = openFile(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
	} catch (error) {
		if (error?.code === "ENOENT") return null;
		if (["ELOOP", "EISDIR"].includes(error?.code)) throw new Error(`${description} is not one regular non-symlink file.`);
		throw error;
	}
	try {
		const before = statFile(descriptor);
		if (!before.isFile()) throw new Error(`${description} is not one regular non-symlink file.`);
		if (!permitsInitialStatTransition(pathEntry, before, expectedSha256)) {
			throw new Error(`${description} changed while it was read.`);
		}
		if (before.size < minBytes || before.size > maxBytes) throw new Error(`${description} exceeds its format byte limit or is malformed.`);
		hooks?.afterInitialStat?.({ path, descriptor, stat: before });
		const bytes = Buffer.alloc(before.size);
		let offset = 0;
		while (offset < bytes.length) {
			const bytesRead = readFile(descriptor, bytes, offset, bytes.length - offset, offset);
			if (bytesRead === 0) throw new Error(`${description} changed while it was read.`);
			offset += bytesRead;
		}
		const extra = Buffer.alloc(1);
		const extraBytes = readFile(descriptor, extra, 0, 1, bytes.length);
		hooks?.beforeFinalStat?.({ path, descriptor, bytes });
		const after = statFile(descriptor);
		hooks?.afterFinalStat?.({ path, descriptor, stat: after, bytes });
		let finalPathEntry;
		let finalPathMissing = false;
		try {
			finalPathEntry = lstatEntry(path);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			finalPathMissing = true;
		}
		let confirmedHandle = null;
		if (finalPathMissing && after.nlink === 1 && sameStat(pathEntry, before) && sameStat(before, after)) {
			confirmedHandle = statFile(descriptor);
		}
		if (extraBytes === 0 && exactSha256(bytes, expectedSha256)) {
			if (!finalPathMissing && isRegularPathEntry(finalPathEntry)) {
				const retirementCut = exactMonotoneStatCut([pathEntry, before, after, finalPathEntry], 2, 1);
				if (retirementCut === 1) {
					throw new BoundedFileLinkRetiredBeforeReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
					);
				}
				if (retirementCut !== null) {
					throw new BoundedFileLinkRetiredDuringReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
						after,
						finalPathEntry,
					);
				}
			}
			if (finalPathMissing) {
				const unlinkStats = [pathEntry, before, after];
				if (confirmedHandle !== null) unlinkStats.push(confirmedHandle);
				if (exactMonotoneStatCut(unlinkStats, 1, 0) !== null) {
					throw new BoundedFileUnlinkedDuringReadError(
						path,
						description,
						bytes,
						expectedSha256,
						pathEntry,
						before,
						after,
						confirmedHandle,
					);
				}
			}
		}
		if (
			extraBytes !== 0 || finalPathMissing || !isRegularPathEntry(finalPathEntry) ||
			!sameStat(pathEntry, before) || !sameStat(before, after) || !sameStat(after, finalPathEntry)
		) throw new Error(`${description} changed while it was read.`);
		return bytes;
	} finally {
		closeFile(descriptor);
	}
}
