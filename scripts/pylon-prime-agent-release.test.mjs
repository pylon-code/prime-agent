import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "node:test";
import {
	assertImmutableReleaseUrl,
	createInternalShrinkwrap,
	createReleaseManifest,
	createReleasePackageJson,
	isCanonicalSha512Integrity,
	npmInvocation,
	pylonDistribution,
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_NPM_VERSION,
	PYLON_RELEASE_NODE_VERSION,
	PYLON_RELEASE_PACKAGES,
	PYLON_RELEASE_REPOSITORY_GIT,
	readCompletePackageLock,
	releaseAssetFile,
	releaseAssetUrl,
	releaseBuildId,
	validateInternalPackageGraph,
	validateLockedRegistryGraph,
	validateReleaseBuildReceipt,
	validateReleaseManifest,
	writeReleaseBuildReceipt,
} from "./lib/pylon-release.mjs";
import {
	PYLON_RELEASE_EXPECTED_SDK_FEATURES,
	releaseInstallTimeoutMs,
} from "./smoke-pylon-prime-agent-release.mjs";

const root = resolve(import.meta.dirname, "..");
const source = {
	repository: "https://github.com/pylon-code/prime-agent",
	commit: "0123456789abcdef0123456789abcdef01234567",
	tree: "89abcdef0123456789abcdef0123456789abcdef",
};
const version = "0.8.1";
const toolchain = { node: PYLON_RELEASE_NODE_VERSION, npm: PYLON_RELEASE_NPM_VERSION };
const lockfileSha256 = "a".repeat(64);

function fakeArtifacts() {
	return PYLON_RELEASE_PACKAGES.map((releasePackage, index) => ({
		package: releasePackage.packageName,
		file: releaseAssetFile(releasePackage.assetStem, version),
		size: index + 1,
		sha256: String(index + 1).repeat(64),
		sha512: String(index + 1).repeat(128),
	}));
}

test("uses channel-neutral immutable Pylon asset identity", () => {
	assert.equal(releaseBuildId(source.commit), "pylon-build-g0123456789ab-r1");
	assert.equal(
		releaseAssetUrl(source.commit, "pylon-prime-agent-0.8.1.tgz"),
		"https://github.com/pylon-code/prime-agent/releases/download/pylon-build-g0123456789ab-r1/pylon-prime-agent-0.8.1.tgz",
	);
	assert.throws(
		() =>
			assertImmutableReleaseUrl(
				"https://github.com/pylon-code/prime-agent/releases/latest/download/pylon-prime-agent-0.8.1.tgz",
				source.commit,
				"pylon-prime-agent-0.8.1.tgz",
			),
		/immutable Pylon build asset URL/,
	);
});

test("rewrites package provenance while preserving public package and command identity", () => {
	const releasePackage = PYLON_RELEASE_PACKAGES.at(-1);
	const packageJson = createReleasePackageJson({
		sourcePackage: {
			name: "@earendil-works/pi-coding-agent",
			version,
			private: true,
			devDependencies: { vitest: "1" },
			dependencies: { typebox: "^1.3.9" },
			scripts: { build: "ignored", postinstall: "node postinstall.cjs" },
			bin: { pi: "dist/cli.js" },
			piConfig: { name: "pi" },
		},
		releasePackage,
		version,
		source,
		toolchain,
		lockfileSha256,
		internalArtifacts: new Map(),
	});
	assert.equal(packageJson.name, "prime-agent");
	assert.deepEqual(packageJson.bin, { "prime-agent": "dist/bundle/cli.js" });
	assert.equal(packageJson.repository.url, PYLON_RELEASE_REPOSITORY_GIT);
	assert.deepEqual(packageJson.pylonDistribution, pylonDistribution({ source, toolchain, lockfileSha256 }));
	assert.deepEqual(packageJson.scripts, { postinstall: "node postinstall.cjs" });
	assert.deepEqual(packageJson.files, ["npm-shrinkwrap.json"]);
	assert.equal(packageJson.private, undefined);
	assert.equal(packageJson.devDependencies, undefined);
});

test("locks every internal workspace dependency to its immutable tarball digest", () => {
	const aiFile = "pylon-prime-agent-ai-0.8.1.tgz";
	const ai = {
		packageName: "@earendil-works/pi-ai",
		file: aiFile,
		version,
		url: releaseAssetUrl(source.commit, aiFile),
		integrity: "sha512-YWJj",
	};
	const internal = new Map([["@earendil-works/pi-ai", ai]]);
	const packageJson = createReleasePackageJson({
		sourcePackage: {
			name: "@earendil-works/pi-agent-core",
			version,
			dependencies: { "@earendil-works/pi-ai": "^0.8.1", typebox: "^1.3.9" },
		},
		releasePackage: PYLON_RELEASE_PACKAGES[2],
		version,
		source,
		toolchain,
		lockfileSha256,
		internalArtifacts: internal,
	});
	const lockedPackages = {
		"node_modules/typebox": {
			version: "1.3.9",
			resolved: "https://registry.npmjs.org/typebox/-/typebox-1.3.9.tgz",
			integrity: "sha512-dHlwZWJveA==",
		},
	};
	assert.throws(
		() =>
			createReleasePackageJson({
				sourcePackage: {
					name: "@earendil-works/pi-agent-core",
					version,
					dependencies: { "@earendil-works/pi-ai": "^0.8.1" },
				},
				releasePackage: PYLON_RELEASE_PACKAGES[2],
				version,
				source,
				toolchain,
				lockfileSha256,
				internalArtifacts: new Map(),
			}),
		/Missing matching Pylon release artifact/,
	);
	const shrinkwrap = createInternalShrinkwrap(packageJson, internal, lockedPackages);
	assert.equal(packageJson.dependencies["@earendil-works/pi-ai"], ai.url);
	assert.equal(packageJson.dependencies.typebox, "^1.3.9");
	assert.deepEqual(shrinkwrap.packages["node_modules/@earendil-works/pi-ai"], {
		name: "@earendil-works/pi-ai",
		version,
		resolved: ai.url,
		integrity: ai.integrity,
	});
	assert.deepEqual(shrinkwrap.packages["node_modules/typebox"], lockedPackages["node_modules/typebox"]);
	validateInternalPackageGraph(packageJson, shrinkwrap, internal, source.commit);
	const mutablePackageJson = structuredClone(packageJson);
	mutablePackageJson.dependencies["@earendil-works/pi-ai"] = "^0.8.1";
	assert.throws(
		() => validateInternalPackageGraph(mutablePackageJson, shrinkwrap, internal, source.commit),
		/immutable Pylon build asset URL/,
	);
	const crossWiredPackageJson = structuredClone(packageJson);
	crossWiredPackageJson.dependencies["@earendil-works/pi-ai"] = releaseAssetUrl(
		source.commit,
		releaseAssetFile("pylon-prime-agent-tui", version),
	);
	assert.throws(
		() => validateInternalPackageGraph(crossWiredPackageJson, shrinkwrap, internal, source.commit),
		/immutable Pylon build asset URL/,
	);
	validateLockedRegistryGraph(shrinkwrap, lockedPackages, internal);
	shrinkwrap.packages["node_modules/typebox"].integrity = "sha512-wrong";
	assert.throws(
		() => validateLockedRegistryGraph(shrinkwrap, lockedPackages, internal),
		/changed locked registry metadata/,
	);
	shrinkwrap.packages["node_modules/typebox"] = structuredClone(lockedPackages["node_modules/typebox"]);
	shrinkwrap.packages["node_modules/@earendil-works/pi-ai"].integrity = "sha512-wrong";
	assert.throws(
		() => validateInternalPackageGraph(packageJson, shrinkwrap, internal, source.commit),
		/integrity-locked shrinkwrap/,
	);
});

test("builds the exact manifest and attestation subject contract", () => {
	const manifest = createReleaseManifest({
		source,
		version,
		toolchain: { node: PYLON_RELEASE_NODE_VERSION, npm: PYLON_RELEASE_NPM_VERSION },
		lockfileSha256: "a".repeat(64),
		artifacts: fakeArtifacts(),
	});
	assert.equal(PYLON_RELEASE_MANIFEST, "pylon-prime-agent-release-v1.json");
	assert.equal(manifest.package.name, "prime-agent");
	assert.equal(manifest.package.command, "prime-agent");
	assert.deepEqual(
		manifest.attestationSubjects.map((subject) => subject.name),
		manifest.assets.map((asset) => asset.file),
	);
	assert.equal(validateReleaseManifest(manifest), manifest);
	assert.throws(() => validateReleaseManifest({ ...manifest, build: { ...manifest.build, npm: "latest" } }), /Malformed/);
});

test("binds every copied input to the exact release build receipt", () => {
	const fixture = mkdtempSync(join(tmpdir(), "pylon-release-receipt-"));
	try {
		for (const releasePackage of PYLON_RELEASE_PACKAGES) {
			const dist = join(fixture, "packages", releasePackage.packageDir, "dist");
			mkdirSync(dist, { recursive: true });
			writeFileSync(join(dist, "index.js"), `export const name = ${JSON.stringify(releasePackage.packageName)};\n`);
		}
		writeReleaseBuildReceipt(fixture, source, toolchain, lockfileSha256);
		assert.doesNotThrow(() => validateReleaseBuildReceipt(fixture, source, toolchain, lockfileSha256));
		const ignoredDirectory = join(fixture, "packages", "coding-agent", "docs");
		const ignoredInput = join(ignoredDirectory, "ignored.txt");
		mkdirSync(ignoredDirectory, { recursive: true });
		writeFileSync(ignoredInput, "not in the source tree\n");
		assert.throws(
			() => validateReleaseBuildReceipt(fixture, source, toolchain, lockfileSha256),
			/does not match the exact source build receipt/,
		);
		rmSync(ignoredDirectory, { recursive: true });
		assert.doesNotThrow(() => validateReleaseBuildReceipt(fixture, source, toolchain, lockfileSha256));
		writeFileSync(join(fixture, "packages", "ai", "dist", "index.js"), "stale output\n");
		assert.throws(
			() => validateReleaseBuildReceipt(fixture, source, toolchain, lockfileSha256),
			/does not match the exact source build receipt/,
		);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("requires canonical SHA-512 registry integrity for every locked package", () => {
	assert.equal(isCanonicalSha512Integrity(`sha512-${Buffer.alloc(64, 1).toString("base64")}`), true);
	assert.equal(isCanonicalSha512Integrity("sha512-a"), false);
	assert.doesNotThrow(() => readCompletePackageLock(root));
	const fixture = mkdtempSync(join(tmpdir(), "pylon-release-lock-"));
	try {
		writeFileSync(
			join(fixture, "package-lock.json"),
			JSON.stringify({
				lockfileVersion: 3,
				packages: {
					"node_modules/example": {
						version: "1.0.0",
						resolved: "https://registry.npmjs.org/example/-/example-1.0.0.tgz",
						integrity: "sha512-a",
					},
				},
			}),
		);
		assert.throws(() => readCompletePackageLock(fixture), /lacks immutable registry integrity/);
	} finally {
		rmSync(fixture, { recursive: true, force: true });
	}
});

test("smokes the exact caller-owned session SDK contract", () => {
	assert.deepEqual([...PYLON_RELEASE_EXPECTED_SDK_FEATURES], [
		"bounded_daemon_ingress_v1",
		"negotiated_daemon_session_capabilities_v1",
		"caller_owned_session_environment_cleanup_v1",
	]);
	assert.equal(Object.isFrozen(PYLON_RELEASE_EXPECTED_SDK_FEATURES), true);
	const smokeSource = readFileSync(join(root, "scripts", "smoke-pylon-prime-agent-release.mjs"), "utf8");
	assert.match(smokeSource, /ownedSessionLaunchEnv: launchEnv/);
	assert.match(smokeSource, /getOwnedSessionContractProof\(\)/);
	assert.match(smokeSource, /disposeOwnedSession\(\{ timeoutMs: 15_000 \}\)/);
});

test("bounds hosted Windows artifact installation without weakening POSIX checks", () => {
	assert.equal(releaseInstallTimeoutMs("win32"), 360_000);
	assert.equal(releaseInstallTimeoutMs("linux"), 180_000);
	assert.equal(releaseInstallTimeoutMs("darwin"), 180_000);
});

test("uses the pinned npm CLI path for cross-platform release subprocesses", () => {
	assert.deepEqual(npmInvocation({ npm_execpath: "C:\\npm\\bin\\npm-cli.js" }), {
		command: process.execPath,
		prefixArgs: ["C:\\npm\\bin\\npm-cli.js"],
	});
});

test("offline release scripts cannot invoke live model generation", () => {
	const packageJson = JSON.parse(readFileSync(join(root, "packages", "ai", "package.json"), "utf8"));
	assert.equal(packageJson.scripts["build:offline"], "tsgo -p tsconfig.build.json");
	assert.doesNotMatch(packageJson.scripts["build:offline"], /generate-models/);
	assert.match(
		readFileSync(join(root, "packages", "ai", "scripts", "generate-models.ts"), "utf8"),
		/PYLON_RELEASE_OFFLINE/,
	);
});
