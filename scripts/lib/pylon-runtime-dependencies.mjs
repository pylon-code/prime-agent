import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join, posix, resolve } from "node:path";
import { PYLON_RELEASE_PACKAGES, assertPinnedToolchain, isCanonicalSha512Integrity, readCompletePackageLock } from "./pylon-release.mjs";

const MAX_TARBALL_BYTES = 128 * 1024 * 1024;
const internalPaths = new Map(PYLON_RELEASE_PACKAGES.map((entry) => [`packages/${entry.packageDir}`, entry]));

/** Follow the committed installation layout, including installed peers and every platform optional. */
export function runtimeDependencyClosure(lock) {
	const selected = new Map();
	function dependencyPath(from, name, optional) {
		if (!/^(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+$/i.test(name)) throw new Error(`Invalid dependency name: ${name}`);
		let directory = from;
		for (;;) {
			const candidate = posix.join(directory, "node_modules", name);
			if (lock.packages[candidate]) return candidate;
			if (!directory) break;
			directory = posix.dirname(directory);
			if (directory === ".") directory = "";
		}
		if (!optional) throw new Error(`Missing locked runtime dependency ${name} from ${from}`);
	}
	function visit(location) {
		let entry = lock.packages[location];
		if (entry?.link) {
			location = entry.resolved;
			entry = lock.packages[location];
		}
		if (!entry || (!location.startsWith("node_modules/") && !internalPaths.has(location))) {
			throw new Error(`Unsupported runtime lock location: ${location}`);
		}
		if (selected.has(location)) return;
		if (!internalPaths.has(location) && (!isCanonicalSha512Integrity(entry.integrity) ||
			!entry.resolved?.startsWith("https://registry.npmjs.org/"))) {
			throw new Error(`Runtime dependency lacks locked registry integrity: ${location}`);
		}
		selected.set(location, entry);
		for (const name of Object.keys({ ...entry.dependencies, ...entry.optionalDependencies, ...entry.peerDependencies }).sort()) {
			const optional = name in (entry.optionalDependencies ?? {}) || entry.peerDependenciesMeta?.[name]?.optional === true;
			const target = dependencyPath(location, name, optional);
			if (target) visit(target);
		}
	}
	visit("packages/coding-agent");
	return new Map([...selected].sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0));
}

function archivePath(root, entry) {
	const digest = Buffer.from(entry.integrity.slice("sha512-".length), "base64").toString("hex");
	return join(root, ".npm", "pylon-runtime-inputs", `${digest}.tgz`);
}

export function verifyRuntimeArchive(bytes, entry) {
	if (bytes.byteLength < 1 || bytes.byteLength > MAX_TARBALL_BYTES ||
		`sha512-${createHash("sha512").update(bytes).digest("base64")}` !== entry.integrity) {
		throw new Error(`Runtime tarball failed its locked size/integrity check: ${entry.resolved}`);
	}
}

/** Network is permitted only while acquiring build inputs; packaging rechecks every byte offline. */
export async function hydrateRuntimeDependencies(root) {
	assertPinnedToolchain(root);
	const closure = runtimeDependencyClosure(readCompletePackageLock(root));
	mkdirSync(join(root, ".npm", "pylon-runtime-inputs"), { recursive: true });
	for (const [location, entry] of closure) {
		if (internalPaths.has(location)) continue;
		const destination = archivePath(root, entry);
		if (existsSync(destination)) {
			verifyRuntimeArchive(readFileSync(destination), entry);
			continue;
		}
		const response = await fetch(entry.resolved, { redirect: "error", signal: AbortSignal.timeout(60_000) });
		if (!response.ok || !response.body) throw new Error(`Cannot acquire locked runtime input: ${entry.resolved}`);
		const chunks = [];
		let size = 0;
		for await (const chunk of response.body) {
			size += chunk.byteLength;
			if (size > MAX_TARBALL_BYTES) throw new Error(`Runtime input exceeds bounded size: ${entry.resolved}`);
			chunks.push(chunk);
		}
		const bytes = Buffer.concat(chunks);
		verifyRuntimeArchive(bytes, entry);
		writeFileSync(destination, bytes, { flag: "wx", mode: 0o600 });
	}
	return closure.size;
}

function pinnedTar(environment) {
	const cli = environment.PYLON_RELEASE_NPM_CLI || environment.npm_execpath;
	if (!cli?.endsWith("npm-cli.js")) throw new Error("Runtime packaging must run through the pinned npm CLI.");
	return createRequire(resolve(cli))("tar");
}

/** Materialize only regular package files; never invoke a package manager or lifecycle script. */
export async function extractRuntimeArchive(archive, destination, environment = process.env) {
	const tar = pinnedTar(environment);
	const seen = new Map();
	let archiveRoot;
	let expandedBytes = 0;
	tar.t({ file: archive, strict: true, sync: true, onReadEntry(entry) {
		const name = entry.path.replace(/\/$/, "");
		const normalized = posix.normalize(name);
		const first = name.split("/")[0];
		archiveRoot ??= first;
		expandedBytes += entry.size;
		if (name.includes("\\") || name.split("/").some((part) => part === ".." || part === "") ||
			first !== archiveRoot || /^[A-Za-z]:/.test(name) ||
			!["File", "Directory"].includes(entry.type) || (entry.mode & ~0o777) !== 0 ||
			expandedBytes > 768 * 1024 * 1024 || entry.size > MAX_TARBALL_BYTES || seen.size >= 30_000) {
			throw new Error(`Unsafe runtime archive entry: ${name} in ${archive}`);
		}
		const digest = createHash("sha256");
		entry.on("data", (bytes) => digest.update(bytes));
		entry.on("end", () => {
			const identity = `${entry.type}:${entry.mode}:${entry.size}:${digest.digest("hex")}`;
			if (seen.has(normalized) && seen.get(normalized) !== identity) throw new Error(`Conflicting duplicate runtime entry: ${name}`);
			seen.set(normalized, identity);
		});
	} });
	mkdirSync(destination, { recursive: true });
	const extracted = new Set();
	await tar.x({ file: archive, cwd: destination, strip: 1, strict: true, noMtime: true, preserveOwner: false, filter(name) {
		const normalized = posix.normalize(name.replace(/\/$/, ""));
		if (extracted.has(normalized)) return false;
		extracted.add(normalized);
		return true;
	} });
}

export async function materializeRuntimeDependencies({ root, stagingDir, internalStaging, environment }) {
	assertPinnedToolchain(root, environment);
	const closure = runtimeDependencyClosure(readCompletePackageLock(root));
	for (const [location, entry] of closure) {
		if (internalPaths.has(location)) continue;
		const archive = archivePath(root, entry);
		verifyRuntimeArchive(readFileSync(archive), entry);
		await extractRuntimeArchive(archive, join(stagingDir, location), environment);
		const installed = JSON.parse(readFileSync(join(stagingDir, location, "package.json"), "utf8"));
		if (installed.version !== entry.version) throw new Error(`Runtime package version differs from lock: ${location}`);
	}
	for (const [location, packageInfo] of internalPaths) {
		if (!closure.has(location) || packageInfo.publicPackage) continue;
		const source = internalStaging.get(packageInfo.packageName);
		if (!source) throw new Error(`Missing built runtime workspace: ${location}`);
		// Copy through the same package archive used by external consumers, retaining its exact provenance.
		await extractRuntimeArchive(source, join(stagingDir, "node_modules", packageInfo.packageName), environment);
	}
	return [...closure.keys()];
}

/** Check npm's final bundle, including optional packages that the pack host cannot load. */
export function verifyBundledRuntimeDependencies(root, archive, environment = process.env) {
	const expected = new Map();
	for (const [location, entry] of runtimeDependencyClosure(readCompletePackageLock(root))) {
		const internal = internalPaths.get(location);
		if (internal?.publicPackage) continue;
		const installed = internal ? `node_modules/${internal.packageName}` : location;
		expected.set(`package/${installed}/package.json`, entry.version);
	}
	const tar = pinnedTar(environment);
	tar.t({ file: archive, sync: true, strict: true, onReadEntry(entry) {
		const version = expected.get(entry.path);
		if (version === undefined) return;
		if (entry.type !== "File" || entry.size > 256 * 1024) throw new Error(`Invalid bundled manifest: ${entry.path}`);
		const chunks = [];
		entry.on("data", (bytes) => chunks.push(bytes));
		entry.on("end", () => {
			const manifest = JSON.parse(Buffer.concat(chunks).toString("utf8"));
			if (manifest.version !== version) throw new Error(`Bundled runtime version differs from lock: ${entry.path}`);
			expected.delete(entry.path);
		});
	} });
	if (expected.size) throw new Error(`Root archive omitted locked runtime packages: ${[...expected.keys()].join(", ")}`);
}
