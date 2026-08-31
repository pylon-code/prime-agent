#!/usr/bin/env node

import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	assertImmutableReleaseIdentity,
	canonicalJson,
	createStableManifest,
	nextStableSequence,
	parseStableTag,
	publicationReleaseBody,
	PYLON_PREVIEW_MANIFEST,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_MANIFEST,
	sha256Bytes,
	validateStableHistory,
	validateStableManifest,
} from "./lib/pylon-publication.mjs";
import { verifyPreviewPublication } from "./verify-pylon-preview-publication.mjs";
import { verifyStableAttestation } from "./verify-pylon-stable-attestation.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(args) {
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) {
		const name = args[index];
		const value = args[index + 1];
		if (!name?.startsWith("--") || value === undefined) throw new Error("Stable preparation arguments must be name/value pairs.");
		values.set(name, value);
	}
	const artifactDir = resolve(root, values.get("--artifact-dir") ?? ".npm/pylon-stable/preview");
	const outDir = resolve(root, values.get("--out-dir") ?? ".npm/pylon-stable/output");
	const operation = values.get("--operation") ?? "promote";
	const policySha = values.get("--policy-sha") ?? "";
	const policyTree = values.get("--policy-tree") ?? "";
	const revokeTag = values.get("--revoke-tag") ?? "";
	const reason = values.get("--reason") ?? "withdrawn";
	if (!["promote", "withdraw"].includes(operation)) throw new Error("Stable operation must be promote or withdraw.");
	if (!/^[0-9a-f]{40}$/.test(policySha)) throw new Error("Stable preparation requires an exact --policy-sha.");
	if (!/^[0-9a-f]{40}$/.test(policyTree)) throw new Error("Stable preparation requires an exact --policy-tree.");
	if (operation === "promote" && (revokeTag || values.has("--reason"))) {
		throw new Error("A normal promotion cannot carry withdrawal metadata.");
	}
	if (operation === "withdraw" && !revokeTag) throw new Error("Withdrawal requires --revoke-tag.");
	return { artifactDir, outDir, operation, policySha, policyTree, revokeTag, reason };
}

function apiHeaders(accept = "application/vnd.github+json") {
	const token = process.env.GITHUB_TOKEN;
	if (!token) throw new Error("GITHUB_TOKEN is required to read stable publication history.");
	return {
		Accept: accept,
		Authorization: `Bearer ${token}`,
		"X-GitHub-Api-Version": "2022-11-28",
		"User-Agent": "pylon-prime-stable-publication",
	};
}

async function api(path, options = {}) {
	const response = await fetch(`https://api.github.com${path}`, {
		method: options.method ?? "GET",
		headers: apiHeaders(options.accept),
		redirect: "follow",
		signal: AbortSignal.timeout(30_000),
	});
	if (!response.ok) throw new Error(`GitHub API ${path} failed with ${response.status}.`);
	return options.bytes ? Buffer.from(await response.arrayBuffer()) : response.json();
}

async function paginate(path) {
	const values = [];
	for (let page = 1; ; page += 1) {
		const separator = path.includes("?") ? "&" : "?";
		const batch = await api(`${path}${separator}per_page=100&page=${page}`);
		if (!Array.isArray(batch)) throw new Error(`GitHub API ${path} did not return a list.`);
		values.push(...batch);
		if (batch.length < 100) return values;
	}
}

async function stableTagNames() {
	try {
		const refs = await paginate(`/repos/${PYLON_PUBLICATION_REPOSITORY}/git/matching-refs/tags/pylon-stable-`);
		return refs
			.map((entry) => entry.ref.replace(/^refs\/tags\//, ""))
			.filter((tag) => /^pylon-stable-[0-9]{6}-g[0-9a-f]{12}-r[1-9][0-9]*$/.test(tag))
			.sort();
	} catch (error) {
		if (String(error).includes("failed with 409") || String(error).includes("failed with 404")) return [];
		throw error;
	}
}

export async function readStableHistory({ verifyAllAttestations = false } = {}) {
	const releases = (await paginate(`/repos/${PYLON_PUBLICATION_REPOSITORY}/releases`)).filter((release) =>
		release.tag_name?.startsWith("pylon-stable-"),
	);
	const manifests = [];
	const manifestBytes = new Map();
	for (const release of releases) {
		parseStableTag(release.tag_name);
		if (release.draft || release.immutable !== true || release.assets?.length !== 1) {
			throw new Error(`Stable release ${release.tag_name} is draft, mutable, or has an unexpected asset set.`);
		}
		const asset = release.assets[0];
		if (asset.name !== PYLON_STABLE_MANIFEST || !asset.url) {
			throw new Error(`Stable release ${release.tag_name} lacks its only channel manifest.`);
		}
		const bytes = await api(new URL(asset.url).pathname, { accept: "application/octet-stream", bytes: true });
		if (sha256Bytes(bytes) !== asset.digest?.replace(/^sha256:/, "")) {
			throw new Error(`Stable release asset digest mismatch for ${release.tag_name}.`);
		}
		const manifest = validateStableManifest(JSON.parse(bytes));
		if (canonicalJson(manifest) !== bytes.toString("utf8") || manifest.tag !== release.tag_name) {
			throw new Error(`Stable release ${release.tag_name} has noncanonical or mismatched metadata.`);
		}
		assertImmutableReleaseIdentity(release, {
			tag: manifest.tag,
			name: `Pylon Prime stable ${manifest.tag}`,
			body: publicationReleaseBody({
				channel: "stable",
				tag: manifest.tag,
				source: manifest.build.source.commit,
				tree: manifest.build.source.tree,
				recipeRevision: manifest.build.recipeRevision,
				policyCommit: manifest.promotion.policyCommit,
				policyTree: manifest.promotion.policyTree,
			}),
			prerelease: false,
			sourceSha: manifest.promotion.policyCommit,
			assets: [{ name: PYLON_STABLE_MANIFEST, size: bytes.byteLength, sha256: sha256Bytes(bytes) }],
		});
		manifests.push(manifest);
		manifestBytes.set(manifest.tag, bytes);
	}
	const ordered = validateStableHistory(manifests);
	const tags = await stableTagNames();
	const releaseTags = ordered.map((manifest) => manifest.tag).sort();
	if (canonicalJson(tags) !== canonicalJson(releaseTags)) {
		throw new Error("Stable tags and immutable release history differ.");
	}
	const authorizationSet = verifyAllAttestations ? ordered : ordered.slice(-1);
	if (authorizationSet.length > 0) {
		const verificationDir = mkdtempSync(join(tmpdir(), "pylon-stable-attestation-"));
		const verificationPath = join(verificationDir, PYLON_STABLE_MANIFEST);
		try {
			for (const manifest of authorizationSet) {
				writeFileSync(verificationPath, manifestBytes.get(manifest.tag), { mode: 0o600 });
				verifyStableAttestation(
					verificationPath,
					manifest.promotion.policyCommit,
					manifest.promotion.policyTree,
				);
			}
		} finally {
			rmSync(verificationDir, { recursive: true, force: true });
		}
	}
	return ordered;
}

function writeOutputs(values) {
	if (!process.env.GITHUB_OUTPUT) return;
	for (const [name, value] of Object.entries(values)) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const verified = verifyPreviewPublication(args.artifactDir, { historical: true });
		const previewBytes = readFileSync(join(args.artifactDir, PYLON_PREVIEW_MANIFEST));
		const history = await readStableHistory();
		const latest = history.at(-1);
		let publish = true;
		let stableManifest;
		if (
			args.operation === "promote" &&
			latest?.build.previewTag === verified.previewManifest.build.tag &&
			latest.promotion.kind === "promote"
		) {
			publish = false;
			stableManifest = latest;
		} else if (
			args.operation === "withdraw" &&
			latest?.build.previewTag === verified.previewManifest.build.tag &&
			latest.revocations.some((entry) => entry.stableTag === args.revokeTag)
		) {
			publish = false;
			stableManifest = latest;
		} else {
			const sequence = nextStableSequence(history);
			const revocations = latest ? structuredClone(latest.revocations) : [];
			let promotion = { kind: "promote", policyCommit: args.policySha, policyTree: args.policyTree };
			if (args.operation === "withdraw") {
				const revoked = history.find((manifest) => manifest.tag === args.revokeTag);
				if (!revoked) throw new Error("Withdrawal can revoke only an existing stable sequence.");
				if (revocations.some((entry) => entry.stableTag === args.revokeTag)) {
					throw new Error("Stable sequence is already withdrawn.");
				}
				const revocation = {
					stableTag: revoked.tag,
					buildTag: revoked.build.previewTag,
					reason: args.reason,
					revokedBySequence: sequence,
				};
				revocations.push(revocation);
				promotion = { kind: "withdraw", policyCommit: args.policySha, policyTree: args.policyTree, revocation };
			}
			stableManifest = createStableManifest({
				previewManifest: verified.previewManifest,
				previewManifestBytes: previewBytes,
				sequence,
				previous: latest
					? { tag: latest.tag, sha256: sha256Bytes(Buffer.from(canonicalJson(latest))) }
					: null,
				revocations,
				promotion,
			});
		}
		mkdirSync(args.outDir, { recursive: true });
		const outputBytes = canonicalJson(stableManifest);
		writeFileSync(join(args.outDir, PYLON_STABLE_MANIFEST), outputBytes);
		writeOutputs({
			publish: String(publish),
			tag: stableManifest.tag,
			source_sha: stableManifest.build.source.commit,
			source_tree: stableManifest.build.source.tree,
			sequence: String(stableManifest.sequence),
		});
		console.log(JSON.stringify({ publish, tag: stableManifest.tag, sequence: stableManifest.sequence }));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
