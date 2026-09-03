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

export class BoundedFileUnlinkedDuringReadError extends Error {
	constructor(path, description, bytes, expectedSha256) {
		super(`${description} changed while it was read because the same opened inode was removed.`);
		this.name = "BoundedFileUnlinkedDuringReadError";
		this.path = path;
		this.description = description;
		this.bytes = Buffer.from(bytes);
		this.expectedSha256 = expectedSha256;
	}
}

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

function sameInodeReadBounds(left, right) {
	return left.dev === right.dev && left.ino === right.ino && left.size === right.size &&
		left.mtimeMs === right.mtimeMs;
}

function sameStat(left, right) {
	return sameInodeReadBounds(left, right) && left.ctimeMs === right.ctimeMs && left.nlink === right.nlink;
}

function isPinnedHandleRemoval(pathEntry, before, after, extraBytes, finalPathMissing) {
	return finalPathMissing && extraBytes === 0 && sameStat(pathEntry, before) &&
		sameInodeReadBounds(before, after) && before.nlink > 0 && after.nlink === 0;
}

function initialRetirementKind(pathEntry, openedHandle, expectedSha256) {
	if (
		expectedSha256 === null || !sameInodeReadBounds(pathEntry, openedHandle) ||
		pathEntry.ctimeMs === openedHandle.ctimeMs
	) return null;
	if (pathEntry.nlink === 2 && openedHandle.nlink === 1) return "link-retired";
	if (pathEntry.nlink === 1 && openedHandle.nlink === 0) return "unlinked";
	return null;
}

function isStableLinkRetirementBeforeRead(before, after, extraBytes, finalPathEntry, finalPathMissing) {
	return !finalPathMissing && extraBytes === 0 && !finalPathEntry.isSymbolicLink?.() && finalPathEntry.isFile() &&
		sameStat(before, after) && sameStat(after, finalPathEntry);
}

function isStableHandleRemovalBeforeRead(before, after, extraBytes, finalPathMissing) {
	return finalPathMissing && extraBytes === 0 && sameStat(before, after);
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
		let retirementBeforeRead = null;
		if (!sameStat(pathEntry, before)) {
			retirementBeforeRead = initialRetirementKind(pathEntry, before, expectedSha256);
			if (retirementBeforeRead === null) throw new Error(`${description} changed while it was read.`);
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
		let finalPathEntry;
		let finalPathMissing = false;
		try {
			finalPathEntry = await lstatEntry(path);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			finalPathMissing = true;
		}
		if (exactSha256(bytes, expectedSha256)) {
			if (
				retirementBeforeRead === "link-retired" &&
				isStableLinkRetirementBeforeRead(before, after, extraBytes, finalPathEntry, finalPathMissing)
			) {
				throw new BoundedFileLinkRetiredBeforeReadError(
					path,
					description,
					bytes,
					expectedSha256,
					pathEntry,
					before,
				);
			}
			if (
				retirementBeforeRead === "unlinked" &&
				isStableHandleRemovalBeforeRead(before, after, extraBytes, finalPathMissing)
			) {
				throw new BoundedFileUnlinkedDuringReadError(path, description, bytes, expectedSha256);
			}
			if (
				retirementBeforeRead === null &&
				isPinnedHandleRemoval(pathEntry, before, after, extraBytes, finalPathMissing)
			) {
				throw new BoundedFileUnlinkedDuringReadError(path, description, bytes, expectedSha256);
			}
		}
		if (
			retirementBeforeRead !== null || extraBytes !== 0 || finalPathMissing ||
			finalPathEntry.isSymbolicLink?.() || !finalPathEntry.isFile() ||
			!sameStat(before, after) || !sameStat(after, finalPathEntry)
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
		let retirementBeforeRead = null;
		if (!sameStat(pathEntry, before)) {
			retirementBeforeRead = initialRetirementKind(pathEntry, before, expectedSha256);
			if (retirementBeforeRead === null) throw new Error(`${description} changed while it was read.`);
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
		let finalPathEntry;
		let finalPathMissing = false;
		try {
			finalPathEntry = lstatEntry(path);
		} catch (error) {
			if (error?.code !== "ENOENT") throw error;
			finalPathMissing = true;
		}
		if (exactSha256(bytes, expectedSha256)) {
			if (
				retirementBeforeRead === "link-retired" &&
				isStableLinkRetirementBeforeRead(before, after, extraBytes, finalPathEntry, finalPathMissing)
			) {
				throw new BoundedFileLinkRetiredBeforeReadError(
					path,
					description,
					bytes,
					expectedSha256,
					pathEntry,
					before,
				);
			}
			if (
				retirementBeforeRead === "unlinked" &&
				isStableHandleRemovalBeforeRead(before, after, extraBytes, finalPathMissing)
			) {
				throw new BoundedFileUnlinkedDuringReadError(path, description, bytes, expectedSha256);
			}
			if (
				retirementBeforeRead === null &&
				isPinnedHandleRemoval(pathEntry, before, after, extraBytes, finalPathMissing)
			) {
				throw new BoundedFileUnlinkedDuringReadError(path, description, bytes, expectedSha256);
			}
		}
		if (
			retirementBeforeRead !== null || extraBytes !== 0 || finalPathMissing ||
			finalPathEntry.isSymbolicLink?.() || !finalPathEntry.isFile() ||
			!sameStat(before, after) || !sameStat(after, finalPathEntry)
		) throw new Error(`${description} changed while it was read.`);
		return bytes;
	} finally {
		closeFile(descriptor);
	}
}
