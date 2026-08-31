import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	cpSync,
	existsSync,
	lstatSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RELEASE_RECIPE = readJson(fileURLToPath(new URL("../pylon-prime-release-v1.json", import.meta.url)));
const releaseRecipeKeys =
	"buildTagPattern,minimumNodeVersion,nodeVersion,npmVersion,recipeRevision,schemaVersion,sourceRepository";
if (
	Object.keys(RELEASE_RECIPE).sort().join(",") !== releaseRecipeKeys ||
	RELEASE_RECIPE.schemaVersion !== 1 ||
	!Number.isSafeInteger(RELEASE_RECIPE.recipeRevision) ||
	RELEASE_RECIPE.recipeRevision < 1 ||
	RELEASE_RECIPE.sourceRepository !== "https://github.com/pylon-code/prime-agent" ||
	!/^\d+\.\d+\.\d+$/.test(RELEASE_RECIPE.nodeVersion) ||
	!/^\d+\.\d+\.\d+$/.test(RELEASE_RECIPE.npmVersion) ||
	!/^\d+\.\d+\.\d+$/.test(RELEASE_RECIPE.minimumNodeVersion) ||
	RELEASE_RECIPE.buildTagPattern !== "pylon-build-g{commit12}-r{recipeRevision}"
) {
	throw new Error("Invalid Pylon Prime release recipe.");
}

export const PYLON_RELEASE_SCHEMA_VERSION = RELEASE_RECIPE.schemaVersion;
export const PYLON_RELEASE_RECIPE_REVISION = RELEASE_RECIPE.recipeRevision;
export const PYLON_RELEASE_NODE_VERSION = RELEASE_RECIPE.nodeVersion;
export const PYLON_RELEASE_NPM_VERSION = RELEASE_RECIPE.npmVersion;
export const PYLON_RELEASE_REPOSITORY = RELEASE_RECIPE.sourceRepository;
export const PYLON_RELEASE_REPOSITORY_GIT = "git+https://github.com/pylon-code/prime-agent.git";
export const PYLON_RELEASE_MINIMUM_NODE = RELEASE_RECIPE.minimumNodeVersion;
export const PYLON_RELEASE_MANIFEST = "pylon-prime-agent-release-v1.json";
export const PYLON_RELEASE_BUILD_RECEIPT = ".npm/pylon-release-build-v1.json";

export const PYLON_RELEASE_PACKAGES = Object.freeze([
	{ packageDir: "ai", packageName: "@earendil-works/pi-ai", assetStem: "pylon-prime-agent-ai" },
	{ packageDir: "tui", packageName: "@earendil-works/pi-tui", assetStem: "pylon-prime-agent-tui" },
	{ packageDir: "agent", packageName: "@earendil-works/pi-agent-core", assetStem: "pylon-prime-agent-core" },
	{ packageDir: "coding-agent", packageName: "prime-agent", assetStem: "pylon-prime-agent", publicPackage: true },
]);

const INTERNAL_PACKAGE_NAMES = new Set(PYLON_RELEASE_PACKAGES.filter((entry) => !entry.publicPackage).map((entry) => entry.packageName));

const CONTENT_ENTRIES = Object.freeze([
	"dist",
	"docs",
	"examples",
	"skills",
	"postinstall.cjs",
	"README.md",
	"CHANGELOG.md",
]);

function readJson(path) {
	return JSON.parse(readFileSync(path, "utf8"));
}

export function isCanonicalSha512Integrity(value) {
	if (typeof value !== "string") return false;
	const match = /^sha512-([A-Za-z0-9+/]+={0,2})$/.exec(value);
	if (!match) return false;
	const decoded = Buffer.from(match[1], "base64");
	return decoded.byteLength === 64 && decoded.toString("base64") === match[1];
}

export function readCompletePackageLock(root) {
	const lock = readJson(join(root, "package-lock.json"));
	if (lock.lockfileVersion !== 3 || !lock.packages || typeof lock.packages !== "object") {
		throw new Error("Pylon release requires a package-lock v3 packages graph.");
	}
	const incomplete = [];
	for (const [path, entry] of Object.entries(lock.packages)) {
		if (!path.startsWith("node_modules/") || entry.link || typeof entry.version !== "string") continue;
		if (
			typeof entry.resolved !== "string" ||
			!entry.resolved.startsWith("https://registry.npmjs.org/") ||
			!isCanonicalSha512Integrity(entry.integrity)
		) {
			incomplete.push(path);
		}
	}
	if (incomplete.length > 0) {
		throw new Error(`Package lock lacks immutable registry integrity for: ${incomplete.join(", ")}`);
	}
	return lock;
}

export function writeJson(path, value) {
	writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

export function run(command, args, options = {}) {
	const result = spawnSync(command, args, {
		cwd: options.cwd,
		env: options.env,
		stdio: options.stdio ?? "pipe",
		encoding: "utf8",
		timeout: options.timeoutMs ?? 120_000,
		maxBuffer: options.maxBuffer ?? 16 * 1024 * 1024,
	});
	if (result.error) throw result.error;
	if (result.status !== 0) {
		if (result.stdout) process.stdout.write(result.stdout);
		if (result.stderr) process.stderr.write(result.stderr);
		throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}`);
	}
	if (options.forwardStderr && result.stderr) process.stderr.write(result.stderr);
	return result.stdout?.trim() ?? "";
}

export function npmInvocation(environment = process.env) {
	const configuredCli = environment.PYLON_RELEASE_NPM_CLI?.trim();
	const inheritedCli = environment.npm_execpath?.trim();
	const cli = configuredCli || (inheritedCli?.endsWith("npm-cli.js") ? inheritedCli : undefined);
	if (cli) return { command: process.execPath, prefixArgs: [cli] };
	if (process.platform === "win32") {
		throw new Error("Windows release checks require the pinned npm-cli.js path.");
	}
	return { command: "npm", prefixArgs: [] };
}

export function runNpm(args, options = {}) {
	const invocation = npmInvocation(options.env);
	return run(invocation.command, [...invocation.prefixArgs, ...args], options);
}

export function hashBytes(value, algorithm, encoding = "hex") {
	return createHash(algorithm).update(value).digest(encoding);
}

export function hashFile(path, algorithm, encoding = "hex") {
	return hashBytes(readFileSync(path), algorithm, encoding);
}

export function hashReleasePackageContent(path) {
	const digest = createHash("sha256");
	const walk = (absolutePath, relativePath) => {
		const metadata = lstatSync(absolutePath);
		const mode = metadata.mode & 0o777;
		if (metadata.isDirectory()) {
			digest.update(`directory\0${relativePath}\0${mode.toString(8)}\0`);
			for (const entry of readdirSync(absolutePath, { withFileTypes: true }).sort((left, right) =>
				left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
			)) {
				walk(join(absolutePath, entry.name), `${relativePath}/${entry.name}`);
			}
		} else if (metadata.isFile()) {
			const bytes = readFileSync(absolutePath);
			digest.update(`file\0${relativePath}\0${mode.toString(8)}\0${bytes.byteLength}\0`);
			digest.update(bytes);
		} else {
			throw new Error(`Release content contains unsupported entry ${relativePath}.`);
		}
	};
	for (const entry of CONTENT_ENTRIES) {
		const absolutePath = join(path, entry);
		if (existsSync(absolutePath)) walk(absolutePath, entry);
		else digest.update(`missing\0${entry}\0`);
	}
	return digest.digest("hex");
}

function releaseBuildReceipt(root, source, toolchain, lockfileSha256) {
	return {
		schemaVersion: PYLON_RELEASE_SCHEMA_VERSION,
		source,
		buildId: releaseBuildId(source.commit),
		toolchain,
		lockfileSha256,
		packages: PYLON_RELEASE_PACKAGES.map((releasePackage) => ({
			package: releasePackage.packageName,
			directory: releasePackage.packageDir,
			contentSha256: hashReleasePackageContent(packageDirectory(root, releasePackage.packageDir)),
		})),
	};
}

export function writeReleaseBuildReceipt(root, source, toolchain, lockfileSha256) {
	const path = join(root, PYLON_RELEASE_BUILD_RECEIPT);
	mkdirSync(dirname(path), { recursive: true });
	writeJson(path, releaseBuildReceipt(root, source, toolchain, lockfileSha256));
}

export function validateReleaseBuildReceipt(root, source, toolchain, lockfileSha256) {
	const path = join(root, PYLON_RELEASE_BUILD_RECEIPT);
	if (!existsSync(path)) throw new Error("Missing Pylon release build receipt. Run npm run release:pylon:build first.");
	const actual = readJson(path);
	const expected = releaseBuildReceipt(root, source, toolchain, lockfileSha256);
	if (JSON.stringify(actual) !== JSON.stringify(expected)) {
		throw new Error("Pylon release dist does not match the exact source build receipt.");
	}
}

export function normalizeNpmVersion(version) {
	if (typeof version !== "string" || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
		throw new Error(`Invalid npm package version: ${String(version)}`);
	}
	return version;
}

export function releaseBuildId(sourceCommit) {
	if (!/^[0-9a-f]{40}$/.test(sourceCommit)) throw new Error("Source commit must be a full lowercase Git SHA.");
	return RELEASE_RECIPE.buildTagPattern
		.replace("{commit12}", sourceCommit.slice(0, 12))
		.replace("{recipeRevision}", String(PYLON_RELEASE_RECIPE_REVISION));
}

export function releaseAssetBaseUrl(sourceCommit) {
	return `${PYLON_RELEASE_REPOSITORY}/releases/download/${releaseBuildId(sourceCommit)}`;
}

export function releaseAssetFile(assetStem, version) {
	return `${assetStem}-${normalizeNpmVersion(version)}.tgz`;
}

export function releaseAssetUrl(sourceCommit, file) {
	if (!/^pylon-prime-agent(?:-(?:ai|core|tui))?-[-0-9A-Za-z.]+\.tgz$/.test(file)) {
		throw new Error(`Invalid Pylon release asset name: ${file}`);
	}
	return `${releaseAssetBaseUrl(sourceCommit)}/${file}`;
}

export function assertImmutableReleaseUrl(value, sourceCommit, file) {
	if (value !== releaseAssetUrl(sourceCommit, file)) {
		throw new Error(`Internal package URL must be the immutable Pylon build asset URL for ${file}.`);
	}
	const url = new URL(value);
	if (url.protocol !== "https:" || url.search || url.hash) {
		throw new Error(`Internal package URL is not immutable: ${value}`);
	}
}

export function pylonDistribution({ source, toolchain, lockfileSha256 }) {
	if (toolchain.node !== PYLON_RELEASE_NODE_VERSION || toolchain.npm !== PYLON_RELEASE_NPM_VERSION) {
		throw new Error("Pylon distribution metadata requires the pinned release toolchain.");
	}
	if (!/^[0-9a-f]{64}$/.test(lockfileSha256)) throw new Error("Package lock digest must be SHA-256.");
	return Object.freeze({
		schemaVersion: PYLON_RELEASE_SCHEMA_VERSION,
		repository: PYLON_RELEASE_REPOSITORY,
		sourceCommit: source.commit,
		sourceTree: source.tree,
		buildId: releaseBuildId(source.commit),
		recipeRevision: PYLON_RELEASE_RECIPE_REVISION,
		node: toolchain.node,
		npm: toolchain.npm,
		packageLockSha256: lockfileSha256,
	});
}

function rewriteDependencies(dependencies, internalArtifacts) {
	if (!dependencies) return undefined;
	return Object.fromEntries(
		Object.entries(dependencies).map(([name, range]) => {
			if (!INTERNAL_PACKAGE_NAMES.has(name)) return [name, range];
			const artifact = internalArtifacts.get(name);
			if (!artifact || artifact.packageName !== name) {
				throw new Error(`Missing matching Pylon release artifact for internal dependency ${name}.`);
			}
			return [name, artifact.url];
		}),
	);
}

function releaseScripts(sourceScripts) {
	return sourceScripts?.postinstall ? { postinstall: sourceScripts.postinstall } : undefined;
}

export function createReleasePackageJson({
	sourcePackage,
	releasePackage,
	version,
	source,
	toolchain,
	lockfileSha256,
	internalArtifacts,
}) {
	if (sourcePackage.name !== (releasePackage.publicPackage ? "@earendil-works/pi-coding-agent" : releasePackage.packageName)) {
		throw new Error(`Unexpected source package identity for ${releasePackage.packageDir}.`);
	}
	const packageName = releasePackage.packageName;
	const packageJson = {
		...sourcePackage,
		name: packageName,
		version,
		repository: {
			type: "git",
			url: PYLON_RELEASE_REPOSITORY_GIT,
			directory: `packages/${releasePackage.packageDir}`,
		},
		pylonDistribution: pylonDistribution({ source, toolchain, lockfileSha256 }),
		dependencies: rewriteDependencies(sourcePackage.dependencies, internalArtifacts),
		optionalDependencies: rewriteDependencies(sourcePackage.optionalDependencies, internalArtifacts),
		scripts: releaseScripts(sourcePackage.scripts),
		files: [...(sourcePackage.files ?? []), "npm-shrinkwrap.json"],
	};

	delete packageJson.devDependencies;
	delete packageJson.overrides;
	delete packageJson.private;

	if (releasePackage.publicPackage) {
		packageJson.bin = { "prime-agent": "dist/bundle/cli.js" };
		packageJson.piConfig = { ...(packageJson.piConfig || {}), name: "prime-agent", configDir: ".prime/agent" };
	}

	return packageJson;
}

function shrinkwrapRoot(packageJson) {
	const root = {
		name: packageJson.name,
		version: packageJson.version,
		license: packageJson.license,
		dependencies: packageJson.dependencies,
		optionalDependencies: packageJson.optionalDependencies,
		engines: packageJson.engines,
		bin: packageJson.bin,
	};
	if (packageJson.scripts?.postinstall) root.hasInstallScript = true;
	return Object.fromEntries(Object.entries(root).filter(([, value]) => value !== undefined));
}

export function createInternalShrinkwrap(packageJson, internalArtifacts, lockedPackages = {}) {
	const packages = {};
	for (const [path, entry] of Object.entries(lockedPackages)) {
		if (path.startsWith("node_modules/") && !entry.link && typeof entry.version === "string") {
			packages[path] = structuredClone(entry);
		}
	}
	packages[""] = shrinkwrapRoot(packageJson);
	for (const [dependencyName, artifact] of [...internalArtifacts.entries()].sort(([left], [right]) =>
		left.localeCompare(right),
	)) {
		if (packageJson.dependencies?.[dependencyName] !== artifact.url && packageJson.optionalDependencies?.[dependencyName] !== artifact.url) {
			continue;
		}
		packages[`node_modules/${dependencyName}`] = {
			name: dependencyName,
			version: artifact.version,
			resolved: artifact.url,
			integrity: artifact.integrity,
		};
	}
	return {
		name: packageJson.name,
		version: packageJson.version,
		lockfileVersion: 3,
		requires: true,
		packages,
	};
}

export function validateLockedRegistryGraph(shrinkwrap, lockedPackages, internalArtifacts) {
	const internalPaths = new Set([...internalArtifacts.keys()].map((name) => `node_modules/${name}`));
	const expected = new Map(
		Object.entries(lockedPackages).filter(
			([path, entry]) => path.startsWith("node_modules/") && !entry.link && typeof entry.version === "string",
		),
	);
	for (const [path, entry] of Object.entries(shrinkwrap.packages ?? {})) {
		if (path === "" || internalPaths.has(path)) continue;
		const locked = expected.get(path);
		if (!locked || JSON.stringify(entry) !== JSON.stringify(locked)) {
			throw new Error(`Release shrinkwrap changed locked registry metadata for ${path}.`);
		}
		expected.delete(path);
	}
	if (expected.size > 0) {
		throw new Error(`Release shrinkwrap omitted locked registry packages: ${[...expected.keys()].join(", ")}`);
	}
}

export function validateInternalPackageGraph(packageJson, shrinkwrap, internalArtifacts, sourceCommit) {
	if (shrinkwrap.lockfileVersion !== 3 || shrinkwrap.name !== packageJson.name || shrinkwrap.version !== packageJson.version) {
		throw new Error(`Invalid shrinkwrap root for ${packageJson.name}.`);
	}
	for (const [dependencyName, dependency] of Object.entries({
		...packageJson.dependencies,
		...packageJson.optionalDependencies,
	})) {
		if (!INTERNAL_PACKAGE_NAMES.has(dependencyName)) continue;
		const artifact = internalArtifacts.get(dependencyName);
		if (!artifact || artifact.packageName !== dependencyName) {
			throw new Error(`Missing matching Pylon release artifact for internal dependency ${dependencyName}.`);
		}
		assertImmutableReleaseUrl(dependency, sourceCommit, artifact.file);
		const entry = shrinkwrap.packages?.[`node_modules/${dependencyName}`];
		if (!entry || entry.resolved !== dependency || entry.integrity !== artifact.integrity || entry.version !== artifact.version) {
			throw new Error(`Missing integrity-locked shrinkwrap entry for ${dependencyName}.`);
		}
	}
}

export function copyReleasePackage(sourceDir, targetDir, packageJson, shrinkwrap) {
	mkdirSync(targetDir, { recursive: true });
	writeJson(join(targetDir, "package.json"), packageJson);
	writeJson(join(targetDir, "npm-shrinkwrap.json"), shrinkwrap);
	for (const entry of CONTENT_ENTRIES) {
		const source = join(sourceDir, entry);
		if (existsSync(source)) cpSync(source, join(targetDir, entry), { recursive: true });
	}
}

export function assertPylonRepository(root) {
	const origin = run("git", ["remote", "get-url", "origin"], { cwd: root });
	if (
		origin !== "https://github.com/pylon-code/prime-agent.git" &&
		origin !== "https://github.com/pylon-code/prime-agent" &&
		origin !== "git@github.com:pylon-code/prime-agent.git" &&
		origin !== "ssh://git@github.com/pylon-code/prime-agent.git"
	) {
		throw new Error(`Pylon release requires the canonical pylon-code/prime-agent origin; found ${origin}.`);
	}
}

export function gitSource(root) {
	const commit = run("git", ["rev-parse", "HEAD"], { cwd: root });
	const tree = run("git", ["rev-parse", "HEAD^{tree}"], { cwd: root });
	if (!/^[0-9a-f]{40}$/.test(commit) || !/^[0-9a-f]{40}$/.test(tree)) {
		throw new Error("Release source must resolve to full lowercase Git commit and tree SHAs.");
	}
	return { repository: PYLON_RELEASE_REPOSITORY, commit, tree };
}

export function assertCleanSource(root, options = {}) {
	const tracked = run("git", ["status", "--porcelain=v1", "--untracked-files=all"], { cwd: root });
	if (tracked) throw new Error(`Release source tree is dirty:
${tracked}`);
	if (!options.rejectIgnoredInputs) return;
	const includedIgnored = run(
		"git",
		[
			"ls-files",
			"--others",
			"--ignored",
			"--exclude-standard",
			"--",
			"prime-agent-runtime",
			"packages/coding-agent/docs",
			"packages/coding-agent/examples",
			"packages/coding-agent/skills",
		],
		{ cwd: root },
	);
	if (includedIgnored) throw new Error(`Ignored files would contaminate release inputs:
${includedIgnored}`);
}

export function assertPinnedToolchain(root, environment = process.env) {
	if (process.versions.node !== PYLON_RELEASE_NODE_VERSION) {
		throw new Error(`Pylon release requires Node ${PYLON_RELEASE_NODE_VERSION}; found ${process.versions.node}.`);
	}
	const npmVersion = runNpm(["--version"], { cwd: root, env: environment });
	if (npmVersion !== PYLON_RELEASE_NPM_VERSION) {
		throw new Error(`Pylon release requires npm ${PYLON_RELEASE_NPM_VERSION}; found ${npmVersion}.`);
	}
	return { node: process.versions.node, npm: npmVersion };
}

export function prepareOutputDirectory(outDir) {
	const resolved = resolve(outDir);
	if (existsSync(resolved) && readdirSync(resolved).length > 0) {
		throw new Error(`Release output directory must be empty: ${resolved}`);
	}
	rmSync(resolved, { recursive: true, force: true });
	mkdirSync(join(resolved, "staging"), { recursive: true });
	mkdirSync(join(resolved, "artifacts"), { recursive: true });
	return resolved;
}

export function packStagingPackage({ root, stagingDir, artifactsDir, assetFile, environment }) {
	const output = runNpm(["pack", stagingDir, "--pack-destination", artifactsDir, "--silent"], {
		cwd: root,
		env: environment,
		forwardStderr: true,
	});
	const reported = output.split("\n").at(-1);
	if (!reported) throw new Error(`npm pack did not report an archive for ${stagingDir}.`);
	const sourcePath = join(artifactsDir, basename(reported));
	const assetPath = join(artifactsDir, assetFile);
	if (!existsSync(sourcePath) || !statSync(sourcePath).isFile()) {
		throw new Error(`npm pack did not create ${sourcePath}.`);
	}
	if (sourcePath !== assetPath) renameSync(sourcePath, assetPath);
	const bytes = readFileSync(assetPath);
	return {
		path: assetPath,
		size: bytes.byteLength,
		sha256: hashBytes(bytes, "sha256"),
		sha512: hashBytes(bytes, "sha512"),
		integrity: `sha512-${hashBytes(bytes, "sha512", "base64")}`,
	};
}

export function createReleaseManifest({ source, version, toolchain, lockfileSha256, artifacts }) {
	const buildId = releaseBuildId(source.commit);
	const assets = artifacts
		.map((artifact) => ({
			package: artifact.package,
			file: artifact.file,
			size: artifact.size,
			sha256: artifact.sha256,
			sha512: artifact.sha512,
		}))
		.sort((left, right) => left.file.localeCompare(right.file));
	return {
		schemaVersion: PYLON_RELEASE_SCHEMA_VERSION,
		source,
		build: {
			id: buildId,
			recipeRevision: PYLON_RELEASE_RECIPE_REVISION,
			node: toolchain.node,
			npm: toolchain.npm,
			lockfile: { file: "package-lock.json", sha256: lockfileSha256 },
			assetBaseUrl: releaseAssetBaseUrl(source.commit),
		},
		package: { name: "prime-agent", command: "prime-agent", version, minimumNode: PYLON_RELEASE_MINIMUM_NODE },
		assets,
		attestationSubjects: assets.map((asset) => ({
			name: asset.file,
			digest: { sha256: asset.sha256, sha512: asset.sha512 },
		})),
	};
}

export function validateReleaseManifest(manifest) {
	const exactKeys = (value, keys) =>
		value && typeof value === "object" && Object.keys(value).sort().join(",") === [...keys].sort().join(",");
	if (
		!exactKeys(manifest, ["schemaVersion", "source", "build", "package", "assets", "attestationSubjects"]) ||
		manifest.schemaVersion !== PYLON_RELEASE_SCHEMA_VERSION
	) {
		throw new Error("Unsupported Pylon release manifest.");
	}
	const { source, build, package: publicPackage, assets, attestationSubjects } = manifest;
	if (
		!exactKeys(source, ["repository", "commit", "tree"]) ||
		source.repository !== PYLON_RELEASE_REPOSITORY ||
		!/^[0-9a-f]{40}$/.test(source.commit) ||
		!/^[0-9a-f]{40}$/.test(source.tree) ||
		!exactKeys(build, ["id", "recipeRevision", "node", "npm", "lockfile", "assetBaseUrl"]) ||
		build.id !== releaseBuildId(source.commit) ||
		build.recipeRevision !== PYLON_RELEASE_RECIPE_REVISION ||
		build.node !== PYLON_RELEASE_NODE_VERSION ||
		build.npm !== PYLON_RELEASE_NPM_VERSION ||
		!exactKeys(build.lockfile, ["file", "sha256"]) ||
		build.lockfile.file !== "package-lock.json" ||
		!/^[0-9a-f]{64}$/.test(build.lockfile.sha256) ||
		build.assetBaseUrl !== releaseAssetBaseUrl(source.commit) ||
		!exactKeys(publicPackage, ["name", "command", "version", "minimumNode"]) ||
		publicPackage.name !== "prime-agent" ||
		publicPackage.command !== "prime-agent" ||
		normalizeNpmVersion(publicPackage.version) !== publicPackage.version ||
		publicPackage.minimumNode !== PYLON_RELEASE_MINIMUM_NODE ||
		!Array.isArray(assets) ||
		assets.length !== PYLON_RELEASE_PACKAGES.length ||
		!Array.isArray(attestationSubjects) ||
		attestationSubjects.length !== assets.length
	) {
		throw new Error("Malformed Pylon release manifest.");
	}
	const expectedAssets = new Map(
		PYLON_RELEASE_PACKAGES.map((entry) => [
			releaseAssetFile(entry.assetStem, publicPackage.version),
			entry.packageName,
		]),
	);
	const sortedFiles = assets.map((asset) => asset.file).toSorted((left, right) => left.localeCompare(right));
	if (assets.some((asset, index) => asset.file !== sortedFiles[index])) {
		throw new Error("Pylon release assets must be sorted by immutable filename.");
	}
	for (let index = 0; index < assets.length; index += 1) {
		const asset = assets[index];
		const expectedPackage = expectedAssets.get(asset.file);
		if (
			!exactKeys(asset, ["package", "file", "size", "sha256", "sha512"]) ||
			asset.package !== expectedPackage ||
			!Number.isSafeInteger(asset.size) ||
			asset.size <= 0
		) {
			throw new Error(`Unexpected Pylon release asset: ${String(asset.file)}`);
		}
		expectedAssets.delete(asset.file);
		if (!/^[0-9a-f]{64}$/.test(asset.sha256) || !/^[0-9a-f]{128}$/.test(asset.sha512)) {
			throw new Error(`Invalid digest for ${asset.file}.`);
		}
		const subject = attestationSubjects[index];
		if (
			!exactKeys(subject, ["name", "digest"]) ||
			!exactKeys(subject.digest, ["sha256", "sha512"]) ||
			subject.name !== asset.file ||
			subject.digest.sha256 !== asset.sha256 ||
			subject.digest.sha512 !== asset.sha512
		) {
			throw new Error(`Attestation subject mismatch for ${asset.file}.`);
		}
	}
	if (expectedAssets.size > 0) throw new Error(`Missing Pylon release assets: ${[...expectedAssets.keys()].join(", ")}`);
	return manifest;
}

export function readTarJson(archive, entry, options = {}) {
	const output = run("tar", ["-xOf", archive, entry], options);
	return JSON.parse(output);
}

export function packageDirectory(root, packageDir) {
	return join(root, "packages", packageDir);
}

export function relativeOutput(root, outDir) {
	return relative(root, outDir);
}

export function readPackageJson(root, packageDir) {
	return readJson(join(packageDirectory(root, packageDir), "package.json"));
}
