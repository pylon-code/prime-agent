#!/usr/bin/env node

import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
	canonicalJson,
	publicationReleaseBody,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_MANIFEST,
	sha256Bytes,
	validateStableHistory,
	validateStableManifest,
} from "./lib/pylon-publication.mjs";
import { readStableHistory } from "./prepare-pylon-stable-manifest.mjs";
import { verifyStableAttestation } from "./verify-pylon-stable-attestation.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(args) {
	const values = new Map();
	for (let index = 0; index < args.length; index += 2) {
		if (!args[index]?.startsWith("--") || args[index + 1] === undefined) throw new Error("Recovery arguments must be name/value pairs.");
		values.set(args[index], args[index + 1]);
	}
	const draftId = values.get("--draft-id") ?? "";
	const reservationTag = values.get("--reservation-tag") ?? "";
	const previewTag = values.get("--preview-tag") ?? "";
	const operation = values.get("--operation") ?? "";
	if (!/^[0-9]+$/.test(draftId)) throw new Error("Recovery needs an exact numeric draft id.");
	if (reservationTag && !/^pylon-stable-sequence-[0-9]{6}$/.test(reservationTag)) throw new Error("Recovery reservation tag is malformed.");
	if (!/^pylon-build-g[0-9a-f]{12}-r[1-9][0-9]*$/.test(previewTag) || !["promote", "withdraw"].includes(operation)) {
		throw new Error("Recovery preview or operation is malformed.");
	}
	return {
		draftId: Number(draftId), reservationTag, previewTag, operation,
		revokeTag: values.get("--revoke-tag") ?? "", reason: values.get("--reason") ?? "",
		outDir: resolve(root, values.get("--out-dir") ?? ".npm/pylon-stable/output"),
	};
}

function headers(accept = "application/vnd.github+json") {
	const token = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN;
	if (!token) throw new Error("GITHUB_TOKEN is required for stable recovery.");
	return { Accept: accept, Authorization: `Bearer ${token}`, "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "pylon-stable-recovery" };
}

async function api(path, { bytes = false, accept } = {}) {
	const response = await fetch(`https://api.github.com${path}`, {
		headers: headers(accept), redirect: "follow", signal: AbortSignal.timeout(30_000),
	});
	if (!response.ok) {
		const error = new Error(`GitHub API ${path} failed with ${response.status}.`);
		error.status = response.status;
		throw error;
	}
	return bytes ? Buffer.from(await response.arrayBuffer()) : response.json();
}

function reservationMessage(manifest, digest, draftId) {
	const withdrawal = manifest.promotion.kind === "withdraw"
		? [
			`Withdraw stable tag: ${manifest.promotion.revocation.stableTag}`,
			`Withdraw build tag: ${manifest.promotion.revocation.buildTag}`,
			`Withdraw reason: ${manifest.promotion.revocation.reason}`,
		]
		: [];
	return [
		"Pylon stable sequence reservation", `Sequence: ${String(manifest.sequence).padStart(6, "0")}`,
		`Policy: ${manifest.promotion.policyCommit}`, `Policy tree: ${manifest.promotion.policyTree}`,
		`Operation: ${manifest.promotion.kind}`, ...withdrawal, `Stable tag: ${manifest.tag}`, `Preview: ${manifest.build.previewTag}`,
		`Manifest: sha256:${digest}`, `Draft release: ${draftId}`, "",
	].join("\n");
}

function outputs(values) {
	if (!process.env.GITHUB_OUTPUT) return;
	for (const [key, value] of Object.entries(values)) appendFileSync(process.env.GITHUB_OUTPUT, `${key}=${value}\n`);
}

export async function recoverStableDraft(args) {
	const release = await api(`/repos/${PYLON_PUBLICATION_REPOSITORY}/releases/${args.draftId}`);
	if (!release.draft || release.immutable === true || release.assets?.length !== 1 || release.assets[0].name !== PYLON_STABLE_MANIFEST) {
		throw new Error("Recovery release is not one exact mutable stable draft.");
	}
	const bytes = await api(new URL(release.assets[0].url).pathname, { bytes: true, accept: "application/octet-stream" });
	const digest = sha256Bytes(bytes);
	const manifest = validateStableManifest(JSON.parse(bytes));
	if (bytes.toString("utf8") !== canonicalJson(manifest)) throw new Error("Recovery stable manifest is not canonical.");
	const name = `Pylon Prime stable ${manifest.tag}`;
	const body = publicationReleaseBody({
		channel: "stable", tag: manifest.tag, source: manifest.build.source.commit, tree: manifest.build.source.tree,
		recipeRevision: manifest.build.recipeRevision, policyCommit: manifest.promotion.policyCommit, policyTree: manifest.promotion.policyTree,
	});
	if (
		release.tag_name !== manifest.tag || release.name !== name || release.body !== body || release.prerelease ||
		release.target_commitish !== manifest.promotion.policyCommit || release.assets[0].size !== bytes.length ||
		release.assets[0].digest !== `sha256:${digest}` || manifest.build.previewTag !== args.previewTag ||
		manifest.promotion.kind !== args.operation
	) throw new Error("Recovery draft metadata, bytes, preview, or operation differs.");
	if (args.operation === "withdraw") {
		if (manifest.promotion.revocation?.stableTag !== args.revokeTag || manifest.promotion.revocation?.reason !== args.reason) {
			throw new Error("Recovery withdrawal inputs differ from the approved draft.");
		}
	} else if (args.revokeTag || args.reason) throw new Error("Promote recovery cannot carry withdrawal inputs.");
	const history = await readStableHistory({ verifyAllAttestations: true });
	const combined = validateStableHistory([...history, manifest]);
	if (combined.length !== manifest.sequence || combined.at(-1).tag !== manifest.tag) {
		throw new Error("Recovery draft is not the exact next signed-history sequence.");
	}
	const expectedReservation = `pylon-stable-sequence-${String(manifest.sequence).padStart(6, "0")}`;
	if (args.reservationTag) {
		if (args.reservationTag !== expectedReservation) throw new Error("Recovery reservation sequence differs from the draft.");
		const ref = await api(`/repos/${PYLON_PUBLICATION_REPOSITORY}/git/ref/tags/${args.reservationTag}`);
		if (ref.object?.type !== "tag") throw new Error("Recovery reservation is not annotated.");
		const annotation = await api(`/repos/${PYLON_PUBLICATION_REPOSITORY}/git/tags/${ref.object.sha}`);
		if (
			annotation.tag !== args.reservationTag || annotation.message !== reservationMessage(manifest, digest, release.id) ||
			annotation.object?.type !== "commit" || annotation.object.sha !== manifest.promotion.policyCommit
		) throw new Error("Recovery reservation differs from the exact approved draft.");
	} else {
		try {
			await api(`/repos/${PYLON_PUBLICATION_REPOSITORY}/git/ref/tags/${expectedReservation}`);
			throw new Error("Draft-only recovery cannot replace an existing reservation.");
		} catch (error) {
			if (error.status !== 404) throw error;
		}
	}
	mkdirSync(args.outDir, { recursive: true });
	const manifestPath = join(args.outDir, PYLON_STABLE_MANIFEST);
	writeFileSync(manifestPath, bytes, { mode: 0o600 });
	verifyStableAttestation(manifestPath, manifest.promotion.policyCommit, manifest.promotion.policyTree);
	return { release, manifest, bytes, digest, reservationTag: expectedReservation };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	try {
		const args = parseArgs(process.argv.slice(2));
		const recovered = await recoverStableDraft(args);
		outputs({
			publish: "true", tag: recovered.manifest.tag, source_sha: recovered.manifest.build.source.commit,
			source_tree: recovered.manifest.build.source.tree, sequence: String(recovered.manifest.sequence),
			draft_id: String(recovered.release.id), reservation_tag: recovered.reservationTag,
		});
		console.log(JSON.stringify({ recovered: recovered.manifest.tag, draftId: recovered.release.id }));
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
