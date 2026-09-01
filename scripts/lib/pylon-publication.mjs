import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
	normalizeNpmVersion,
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_PACKAGES,
	PYLON_RELEASE_RECIPE_REVISION,
	PYLON_RELEASE_REPOSITORY,
	releaseAssetFile,
	releaseBuildId,
	validateReleaseManifest,
} from "./pylon-release.mjs";

export const PYLON_PREVIEW_MANIFEST = "pylon-preview-channel-v1.json";
export const PYLON_STABLE_MANIFEST = "pylon-stable-channel-v1.json";
export const PYLON_PUBLICATION_SCHEMA_VERSION = 1;
export const PYLON_PUBLICATION_REPOSITORY = "pylon-code/prime-agent";
export const PYLON_PUBLICATION_REF = "refs/heads/pylon";
export const PYLON_PREVIEW_WORKFLOW = ".github/workflows/pylon-preview-release.yml";
export const PYLON_STABLE_WORKFLOW = ".github/workflows/pylon-stable-release.yml";
export const GITHUB_ACTIONS_APP_ID = 15368;
export const PYLON_REQUIRED_CHECKS = Object.freeze([
	Object.freeze({ context: "Check changelog fragment", appId: GITHUB_ACTIONS_APP_ID, workflowPath: ".github/workflows/changelog-merged-proof.yml" }),
	Object.freeze({ context: "build-check-test", appId: GITHUB_ACTIONS_APP_ID, workflowPath: ".github/workflows/ci.yml" }),
]);

function compactJsonSource(text) {
	let result = "";
	let quoted = false;
	let escaped = false;
	for (const character of text) {
		if (quoted) {
			result += character;
			if (escaped) escaped = false;
			else if (character === "\\") escaped = true;
			else if (character === '"') quoted = false;
		} else if (character === '"') {
			quoted = true;
			result += character;
		} else if (!/\s/.test(character)) result += character;
	}
	if (quoted || escaped) throw new Error("Pylon historical release recipe/publication policy registry has truncated JSON.");
	return result;
}

export function parseSupportedReleaseRecipeRegistry(text) {
	if (typeof text !== "string") throw new Error("Pylon historical release recipe/publication policy registry must be JSON text.");
	const registry = JSON.parse(text);
	if (compactJsonSource(text) !== JSON.stringify(registry)) {
		throw new Error("Pylon historical release recipe/publication policy registry has duplicate keys or noncanonical JSON tokens.");
	}
	const recipeKeys = [
		"recipeRevision", "manifestSchemaVersion", "nodeVersion", "npmVersion", "minimumNodeVersion",
	];
	const publicationPolicyKeys = [
		"publicationPolicyRevision", "previewWorkflowPath", "previewWorkflowSha256", "stableWorkflowPath", "stableWorkflowSha256",
	];
	if (
		!registry || Object.keys(registry).sort().join(",") !== "publicationPolicies,recipes,schemaVersion" ||
		registry.schemaVersion !== 1 || !Array.isArray(registry.recipes) || registry.recipes.length === 0 ||
		!Array.isArray(registry.publicationPolicies) || registry.publicationPolicies.length === 0 ||
		registry.recipes.some((recipe) =>
			!recipe || Object.keys(recipe).sort().join(",") !== recipeKeys.toSorted().join(",") ||
			!Number.isSafeInteger(recipe.recipeRevision) || recipe.recipeRevision < 1 || recipe.manifestSchemaVersion !== 1 ||
			![recipe.nodeVersion, recipe.npmVersion, recipe.minimumNodeVersion].every((value) => /^\d+\.\d+\.\d+$/.test(value))
		) ||
		registry.publicationPolicies.some((policy) =>
			!policy || Object.keys(policy).sort().join(",") !== publicationPolicyKeys.toSorted().join(",") ||
			!Number.isSafeInteger(policy.publicationPolicyRevision) || policy.publicationPolicyRevision < 1 ||
			policy.previewWorkflowPath !== PYLON_PREVIEW_WORKFLOW || policy.stableWorkflowPath !== PYLON_STABLE_WORKFLOW ||
			![policy.previewWorkflowSha256, policy.stableWorkflowSha256].every((value) => /^[0-9a-f]{64}$/.test(value))
		) ||
		new Set(registry.recipes.map((recipe) => recipe.recipeRevision)).size !== registry.recipes.length ||
		new Set(registry.publicationPolicies.map((policy) => policy.publicationPolicyRevision)).size !== registry.publicationPolicies.length
	) throw new Error("Pylon historical release recipe/publication policy registry is malformed.");
	return registry;
}

const supportedRecipeRegistry = parseSupportedReleaseRecipeRegistry(
	readFileSync(fileURLToPath(new URL("../pylon-prime-supported-release-recipes-v1.json", import.meta.url)), "utf8"),
);
export const PYLON_SUPPORTED_RELEASE_RECIPES = Object.freeze(
	supportedRecipeRegistry.recipes.map((recipe) => Object.freeze({ ...recipe })),
);
export const PYLON_SUPPORTED_PUBLICATION_POLICIES = Object.freeze(
	supportedRecipeRegistry.publicationPolicies.map((policy) => Object.freeze({ ...policy })),
);

const previewTagPattern = /^pylon-build-g([0-9a-f]{12})-r([1-9][0-9]*)$/;
const stableTagPattern = /^pylon-stable-([0-9]{6})-g([0-9a-f]{12})-r([1-9][0-9]*)$/;
const stableReservationTagPattern = /^pylon-stable-sequence-([0-9]{6})$/;

function isPlainObject(value) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
	const prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
}

function compareText(left, right) {
	return left < right ? -1 : left > right ? 1 : 0;
}

function canonicalValue(value) {
	if (value === null || typeof value === "string" || typeof value === "boolean") return value;
	if (typeof value === "number" && Number.isFinite(value)) return value;
	if (Array.isArray(value)) return value.map(canonicalValue);
	if (!isPlainObject(value)) throw new Error("Publication JSON contains an unsupported value.");
	return Object.fromEntries(
		Object.keys(value)
			.sort(compareText)
			.map((key) => {
				if (value[key] === undefined) throw new Error("Publication JSON contains undefined.");
				return [key, canonicalValue(value[key])];
			}),
	);
}

export function canonicalJson(value) {
	return `${JSON.stringify(canonicalValue(value), null, 2)}\n`;
}

export function sha256Bytes(value) {
	return createHash("sha256").update(value).digest("hex");
}

export function parsePreviewTag(tag) {
	const match = previewTagPattern.exec(tag);
	if (!match) throw new Error(`Invalid Pylon preview tag: ${String(tag)}`);
	return { commit12: match[1], recipeRevision: Number.parseInt(match[2], 10) };
}

export function stableSequenceReservationTag(sequence) {
	if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > 999_999) {
		throw new Error("Stable reservation sequence must be an integer from 000001 through 999999.");
	}
	return `pylon-stable-sequence-${String(sequence).padStart(6, "0")}`;
}

export function stableReservationMessage(manifest, digest, draftId) {
	if (!/^[0-9a-f]{64}$/.test(digest ?? "") || !Number.isSafeInteger(Number(draftId)) || Number(draftId) < 1) {
		throw new Error("Stable reservation needs an exact manifest digest and draft id.");
	}
	const withdrawal = manifest?.promotion?.kind === "withdraw" ? [
		`Withdraw stable tag: ${manifest.promotion.revocation?.stableTag}`,
		`Withdraw build tag: ${manifest.promotion.revocation?.buildTag}`,
		`Withdraw reason: ${manifest.promotion.revocation?.reason}`,
	] : [];
	return [
		"Pylon stable sequence reservation", `Sequence: ${String(manifest?.sequence).padStart(6, "0")}`,
		`Policy: ${manifest?.promotion?.policyCommit}`, `Policy tree: ${manifest?.promotion?.policyTree}`,
		`Operation: ${manifest?.promotion?.kind}`, ...withdrawal, `Stable tag: ${manifest?.tag}`,
		`Preview: ${manifest?.build?.previewTag}`, `Manifest: sha256:${digest}`, `Draft release: ${Number(draftId)}`, "",
	].join("\n");
}

export function parseStableSequenceReservationTag(tag) {
	const match = stableReservationTagPattern.exec(tag);
	if (!match || Number.parseInt(match[1], 10) < 1) throw new Error(`Invalid Pylon stable reservation tag: ${String(tag)}`);
	return { sequence: Number.parseInt(match[1], 10) };
}

export function stableTag({ sequence, sourceCommit, recipeRevision }) {
	if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > 999_999) {
		throw new Error("Stable sequence must be an integer from 000001 through 999999.");
	}
	if (!/^[0-9a-f]{40}$/.test(sourceCommit)) throw new Error("Stable source must be a full lowercase Git SHA.");
	if (!Number.isSafeInteger(recipeRevision) || recipeRevision < 1) throw new Error("Invalid stable recipe revision.");
	return `pylon-stable-${String(sequence).padStart(6, "0")}-g${sourceCommit.slice(0, 12)}-r${recipeRevision}`;
}

export function parseStableTag(tag) {
	const match = stableTagPattern.exec(tag);
	if (!match) throw new Error(`Invalid Pylon stable tag: ${String(tag)}`);
	const sequence = Number.parseInt(match[1], 10);
	if (sequence < 1) throw new Error(`Invalid Pylon stable sequence: ${tag}`);
	return {
		sequence,
		commit12: match[2],
		recipeRevision: Number.parseInt(match[3], 10),
	};
}

function exactKeys(value, keys) {
	return isPlainObject(value) && Object.keys(value).sort().join(",") === [...keys].sort().join(",");
}

export function validatePublishedReleaseManifest(manifest, supportedRecipes = PYLON_SUPPORTED_RELEASE_RECIPES) {
	if (!exactKeys(manifest, ["schemaVersion", "source", "build", "package", "assets", "attestationSubjects"])) {
		throw new Error("Unsupported published Pylon release manifest.");
	}
	const { source, build, package: publicPackage, assets, attestationSubjects } = manifest;
	const recipe = supportedRecipes.find((candidate) => candidate.recipeRevision === build?.recipeRevision);
	if (
		!recipe || !exactKeys(recipe, [
			"recipeRevision", "manifestSchemaVersion", "nodeVersion", "npmVersion", "minimumNodeVersion",
		]) ||
		manifest.schemaVersion !== recipe.manifestSchemaVersion ||
		!exactKeys(source, ["repository", "commit", "tree"]) || source.repository !== PYLON_RELEASE_REPOSITORY ||
		!/^[0-9a-f]{40}$/.test(source.commit ?? "") || !/^[0-9a-f]{40}$/.test(source.tree ?? "") ||
		!exactKeys(build, ["id", "recipeRevision", "node", "npm", "lockfile", "assetBaseUrl"]) ||
		build.id !== `pylon-build-g${source.commit.slice(0, 12)}-r${recipe.recipeRevision}` ||
		build.node !== recipe.nodeVersion || build.npm !== recipe.npmVersion ||
		!exactKeys(build.lockfile, ["file", "sha256"]) || build.lockfile.file !== "package-lock.json" ||
		!/^[0-9a-f]{64}$/.test(build.lockfile.sha256 ?? "") ||
		build.assetBaseUrl !== `${PYLON_RELEASE_REPOSITORY}/releases/download/${build.id}` ||
		!exactKeys(publicPackage, ["name", "command", "version", "minimumNode"]) ||
		publicPackage.name !== "prime-agent" || publicPackage.command !== "prime-agent" ||
		normalizeNpmVersion(publicPackage.version) !== publicPackage.version || publicPackage.minimumNode !== recipe.minimumNodeVersion ||
		!Array.isArray(assets) || assets.length !== PYLON_RELEASE_PACKAGES.length ||
		!Array.isArray(attestationSubjects) || attestationSubjects.length !== assets.length
	) throw new Error("Malformed historical Pylon release manifest.");
	const expectedAssets = new Map(PYLON_RELEASE_PACKAGES.map((entry) => [
		releaseAssetFile(entry.assetStem, publicPackage.version), entry.packageName,
	]));
	const sortedFiles = assets.map((asset) => asset.file).toSorted();
	if (assets.some((asset, index) => asset.file !== sortedFiles[index])) {
		throw new Error("Published Pylon release assets are not sorted.");
	}
	for (let index = 0; index < assets.length; index += 1) {
		const asset = assets[index];
		const subject = attestationSubjects[index];
		if (
			!exactKeys(asset, ["package", "file", "size", "sha256", "sha512"]) ||
			asset.package !== expectedAssets.get(asset.file) || !Number.isSafeInteger(asset.size) || asset.size < 1 ||
			!/^[0-9a-f]{64}$/.test(asset.sha256 ?? "") || !/^[0-9a-f]{128}$/.test(asset.sha512 ?? "") ||
			!exactKeys(subject, ["name", "digest"]) || !exactKeys(subject.digest, ["sha256", "sha512"]) ||
			subject.name !== asset.file || subject.digest.sha256 !== asset.sha256 || subject.digest.sha512 !== asset.sha512
		) throw new Error(`Malformed historical Pylon release asset: ${String(asset?.file)}`);
		expectedAssets.delete(asset.file);
	}
	if (expectedAssets.size !== 0) throw new Error("Historical Pylon release asset set is incomplete.");
	return manifest;
}

function publicationAssets(releaseManifest) {
	return releaseManifest.assets.map(({ file, size, sha256, sha512 }) => ({ file, size, sha256, sha512 }));
}

function validatePreviewSequence({ sequenceEpoch, sequence, workflowRunId }) {
	if (
		sequenceEpoch !== 1 || !Number.isSafeInteger(sequence) || sequence < 1 ||
		!isCanonicalPositiveDecimal(workflowRunId)
	) throw new Error("Preview sequence identity is malformed.");
	return { sequenceEpoch, sequence, workflowRunId };
}

function isCanonicalPositiveDecimal(value) {
	return typeof value === "string" && /^[1-9][0-9]*$/.test(value);
}

function publicationPolicyFor(revision, supportedPublicationPolicies) {
	if (!Number.isSafeInteger(revision) || revision < 1) {
		throw new Error("Publication policy revision must be an exact positive integer.");
	}
	const policy = supportedPublicationPolicies.find((candidate) => candidate.publicationPolicyRevision === revision);
	if (!policy) throw new Error(`Unsupported publication policy revision: ${revision}`);
	return policy;
}

function previewManifestFor(releaseManifest, releaseManifestBytes, invocation, supportedPublicationPolicies) {
	if (!Buffer.isBuffer(releaseManifestBytes) || releaseManifestBytes.byteLength === 0) {
		throw new Error("Build manifest bytes are required.");
	}
	const tag = releaseManifest.build.id;
	const sequence = validatePreviewSequence(invocation);
	publicationPolicyFor(invocation.publicationPolicyRevision, supportedPublicationPolicies);
	return {
		schemaVersion: PYLON_PUBLICATION_SCHEMA_VERSION,
		channel: "preview",
		repository: PYLON_RELEASE_REPOSITORY,
		publicationPolicyRevision: invocation.publicationPolicyRevision,
		...sequence,
		build: {
			tag,
			id: releaseManifest.build.id,
			recipeRevision: releaseManifest.build.recipeRevision,
			source: releaseManifest.source,
			releaseManifest: {
				file: PYLON_RELEASE_MANIFEST,
				sha256: sha256Bytes(releaseManifestBytes),
			},
		},
		assets: publicationAssets(releaseManifest),
	};
}

export function createPreviewManifest(
	releaseManifest,
	releaseManifestBytes,
	invocation,
	{ supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES } = {},
) {
	validateReleaseManifest(releaseManifest);
	if (releaseManifest.build.id !== releaseBuildId(releaseManifest.source.commit)) throw new Error("Current preview build id is malformed.");
	return previewManifestFor(releaseManifest, releaseManifestBytes, invocation, supportedPublicationPolicies);
}

export function validatePreviewManifest(
	previewManifest,
	releaseManifest,
	releaseManifestBytes,
	{
		historical = false,
		supportedRecipes = PYLON_SUPPORTED_RELEASE_RECIPES,
		supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES,
	} = {},
) {
	if (historical) validatePublishedReleaseManifest(releaseManifest, supportedRecipes);
	else validateReleaseManifest(releaseManifest);
	const expected = previewManifestFor(releaseManifest, releaseManifestBytes, previewManifest, supportedPublicationPolicies);
	if (canonicalJson(previewManifest) !== canonicalJson(expected)) {
		throw new Error("Preview manifest does not match the exact deterministic build manifest.");
	}
	const parsedTag = parsePreviewTag(previewManifest.build.tag);
	if (
		parsedTag.commit12 !== previewManifest.build.source.commit.slice(0, 12) ||
		parsedTag.recipeRevision !== previewManifest.build.recipeRevision
	) {
		throw new Error("Preview tag is not bound to the full source and recipe.");
	}
	return previewManifest;
}

function validateRevocation(value) {
	if (
		!exactKeys(value, ["stableTag", "buildTag", "reason", "revokedBySequence"]) ||
		parseStableTag(value.stableTag).sequence >= value.revokedBySequence ||
		!previewTagPattern.test(value.buildTag) ||
		typeof value.reason !== "string" ||
		!/^[a-z0-9][a-z0-9-]{2,63}$/.test(value.reason) ||
		!Number.isSafeInteger(value.revokedBySequence)
	) {
		throw new Error("Malformed stable revocation entry.");
	}
	return value;
}

export function createStableManifest({
	previewManifest,
	previewManifestBytes,
	sequence,
	previous = null,
	revocations = [],
	promotion,
	supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES,
}) {
	if (canonicalJson(previewManifest) !== previewManifestBytes.toString("utf8")) {
		throw new Error("Preview manifest is not canonical publication JSON.");
	}
	const previewTag = parsePreviewTag(previewManifest.build?.tag);
	publicationPolicyFor(previewManifest.publicationPolicyRevision, supportedPublicationPolicies);
	if (
		previewManifest.schemaVersion !== PYLON_PUBLICATION_SCHEMA_VERSION ||
		previewManifest.channel !== "preview" ||
		previewManifest.repository !== PYLON_RELEASE_REPOSITORY ||
		previewTag.commit12 !== previewManifest.build.source.commit.slice(0, 12) ||
		previewTag.recipeRevision !== previewManifest.build.recipeRevision
	) {
		throw new Error("Malformed preview manifest for stable promotion.");
	}
	const tag = stableTag({
		sequence,
		sourceCommit: previewManifest.build.source.commit,
		recipeRevision: previewManifest.build.recipeRevision,
	});
	const normalizedRevocations = revocations
		.map((entry) => ({ ...validateRevocation(entry) }))
		.sort((left, right) => parseStableTag(left.stableTag).sequence - parseStableTag(right.stableTag).sequence);
	if (new Set(normalizedRevocations.map((entry) => entry.stableTag)).size !== normalizedRevocations.length) {
		throw new Error("A stable tag cannot be revoked more than once.");
	}
	if (sequence === 1) {
		if (previous !== null) throw new Error("The first stable sequence cannot claim previous publication state.");
	} else if (
		!exactKeys(previous, ["tag", "sha256"]) ||
		parseStableTag(previous.tag).sequence !== sequence - 1 ||
		!/^[0-9a-f]{64}$/.test(previous.sha256)
	) {
		throw new Error("Stable publication must bind the exact previous sequence digest.");
	}
	if (
		!promotion || !["promote", "withdraw"].includes(promotion.kind) ||
		!/^[0-9a-f]{40}$/.test(promotion.policyCommit ?? "") || !/^[0-9a-f]{40}$/.test(promotion.policyTree ?? "")
	) {
		throw new Error("Stable promotion must bind its protected policy commit/tree and operation.");
	}
	publicationPolicyFor(promotion.publicationPolicyRevision, supportedPublicationPolicies);
	const expectedPromotionKeys = promotion.kind === "promote"
		? ["kind", "policyCommit", "policyTree", "publicationPolicyRevision"]
		: ["kind", "policyCommit", "policyTree", "publicationPolicyRevision", "revocation"];
	if (!exactKeys(promotion, expectedPromotionKeys)) throw new Error("Malformed stable promotion metadata.");
	if (promotion.kind === "withdraw") {
		const revocation = validateRevocation(promotion.revocation);
		if (
			revocation.revokedBySequence !== sequence ||
			!normalizedRevocations.some((entry) => canonicalJson(entry) === canonicalJson(revocation))
		) {
			throw new Error("Withdrawal must append its exact revocation at the new sequence.");
		}
	}
	return {
		schemaVersion: PYLON_PUBLICATION_SCHEMA_VERSION,
		channel: "stable",
		repository: PYLON_RELEASE_REPOSITORY,
		sequence,
		tag,
		history: {
			highWater: sequence - 1,
			previous,
		},
		build: {
			previewSequence: validatePreviewSequence(previewManifest),
			previewTag: previewManifest.build.tag,
			id: previewManifest.build.id,
			recipeRevision: previewManifest.build.recipeRevision,
			publicationPolicyRevision: previewManifest.publicationPolicyRevision,
			source: previewManifest.build.source,
			releaseManifest: previewManifest.build.releaseManifest,
			previewManifest: {
				file: PYLON_PREVIEW_MANIFEST,
				sha256: sha256Bytes(previewManifestBytes),
			},
			assets: previewManifest.assets,
		},
		promotion,
		revocations: normalizedRevocations,
	};
}

export function validateStableManifest(
	stableManifest,
	supportedRecipes = PYLON_SUPPORTED_RELEASE_RECIPES,
	supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES,
) {
	if (
		!exactKeys(stableManifest, [
			"schemaVersion",
			"channel",
			"repository",
			"sequence",
			"tag",
			"history",
			"build",
			"promotion",
			"revocations",
		]) ||
		stableManifest.schemaVersion !== PYLON_PUBLICATION_SCHEMA_VERSION ||
		stableManifest.channel !== "stable" ||
		stableManifest.repository !== PYLON_RELEASE_REPOSITORY ||
		!Array.isArray(stableManifest.revocations)
	) {
		throw new Error("Malformed Pylon stable manifest.");
	}
	const build = stableManifest.build;
	const recipe = supportedRecipes.find((candidate) => candidate.recipeRevision === build?.recipeRevision);
	publicationPolicyFor(build?.publicationPolicyRevision, supportedPublicationPolicies);
	publicationPolicyFor(stableManifest.promotion?.publicationPolicyRevision, supportedPublicationPolicies);
	if (
		!recipe ||
		!exactKeys(build, [
			"previewSequence", "previewTag", "id", "recipeRevision", "publicationPolicyRevision", "source",
			"releaseManifest", "previewManifest", "assets",
		]) ||
		canonicalJson(validatePreviewSequence(build.previewSequence ?? {})) !== canonicalJson(build.previewSequence) ||
		build.previewTag !== build.id || !Number.isSafeInteger(build.recipeRevision) || build.recipeRevision < 1 ||
		!exactKeys(build.source, ["repository", "commit", "tree"]) ||
		build.source.repository !== PYLON_RELEASE_REPOSITORY ||
		!/^[0-9a-f]{40}$/.test(build.source.commit ?? "") || !/^[0-9a-f]{40}$/.test(build.source.tree ?? "") ||
		!exactKeys(build.releaseManifest, ["file", "sha256"]) || build.releaseManifest.file !== PYLON_RELEASE_MANIFEST ||
		!/^[0-9a-f]{64}$/.test(build.releaseManifest.sha256 ?? "") ||
		!exactKeys(build.previewManifest, ["file", "sha256"]) || build.previewManifest.file !== PYLON_PREVIEW_MANIFEST ||
		!/^[0-9a-f]{64}$/.test(build.previewManifest.sha256 ?? "") ||
		!Array.isArray(build.assets) || build.assets.length !== 4
	) throw new Error("Stable build receipt is not an exact closed Pylon preview schema.");
	const safeAsset = /^pylon-prime-agent(?:-(ai|core|tui))?-([0-9]+\.[0-9]+\.[0-9]+)\.tgz$/;
	const roles = [];
	const versions = [];
	for (const asset of build.assets) {
		const assetMatch = safeAsset.exec(asset.file ?? "");
		if (
			!exactKeys(asset, ["file", "size", "sha256", "sha512"]) || !assetMatch ||
			!Number.isSafeInteger(asset.size) || asset.size < 1 ||
			!/^[0-9a-f]{64}$/.test(asset.sha256 ?? "") || !/^[0-9a-f]{128}$/.test(asset.sha512 ?? "")
		) throw new Error("Stable build contains an unsafe or malformed preview asset.");
		roles.push(assetMatch[1] ?? "root");
		versions.push(assetMatch[2]);
	}
	if (
		roles.toSorted().join(",") !== "ai,core,root,tui" || new Set(versions).size !== 1 ||
		new Set(build.assets.map((asset) => asset.file)).size !== build.assets.length ||
		canonicalJson(build.assets.toSorted((left, right) => compareText(left.file, right.file))) !== canonicalJson(build.assets)
	) throw new Error("Stable preview assets must be unique and sorted by exact file name.");
	const tag = parseStableTag(stableManifest.tag);
	if (
		!exactKeys(stableManifest.history, ["highWater", "previous"]) ||
		stableManifest.history.highWater !== stableManifest.sequence - 1 ||
		(stableManifest.sequence === 1
			? stableManifest.history.previous !== null
			: !exactKeys(stableManifest.history.previous, ["tag", "sha256"]) ||
				parseStableTag(stableManifest.history.previous.tag).sequence !== stableManifest.sequence - 1 ||
				!/^[0-9a-f]{64}$/.test(stableManifest.history.previous.sha256)) ||
		tag.sequence !== stableManifest.sequence ||
		tag.commit12 !== stableManifest.build?.source?.commit?.slice(0, 12) ||
		tag.recipeRevision !== stableManifest.build?.recipeRevision ||
		stableManifest.build.previewTag !== stableManifest.build.id ||
		parsePreviewTag(stableManifest.build.previewTag).commit12 !== tag.commit12 ||
		parsePreviewTag(stableManifest.build.previewTag).recipeRevision !== stableManifest.build.recipeRevision
	) {
		throw new Error("Stable tag does not match its exact preview build.");
	}
	for (const revocation of stableManifest.revocations) validateRevocation(revocation);
	const sortedRevocations = stableManifest.revocations.toSorted(
		(left, right) => parseStableTag(left.stableTag).sequence - parseStableTag(right.stableTag).sequence,
	);
	if (canonicalJson(sortedRevocations) !== canonicalJson(stableManifest.revocations)) {
		throw new Error("Stable revocations are not sorted by sequence.");
	}
	if (new Set(stableManifest.revocations.map((entry) => entry.stableTag)).size !== stableManifest.revocations.length) {
		throw new Error("Stable revocation history contains a duplicate.");
	}
	if (stableManifest.promotion?.kind === "withdraw") {
		if (
			!exactKeys(stableManifest.promotion, ["kind", "policyCommit", "policyTree", "publicationPolicyRevision", "revocation"]) ||
			!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyCommit) ||
			!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyTree) ||
			!stableManifest.revocations.some(
				(entry) => canonicalJson(entry) === canonicalJson(stableManifest.promotion.revocation),
			)
		) {
			throw new Error("Stable withdrawal does not append one exact revocation.");
		}
	} else if (
		!exactKeys(stableManifest.promotion, ["kind", "policyCommit", "policyTree", "publicationPolicyRevision"]) ||
		stableManifest.promotion.kind !== "promote" ||
		!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyCommit) ||
		!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyTree)
	) {
		throw new Error("Malformed stable promotion record.");
	}
	return stableManifest;
}

function containsHistory(previous, next) {
	return previous.every((entry) => next.some((candidate) => canonicalJson(entry) === canonicalJson(candidate)));
}

export function validateStableHistory(
	manifests,
	supportedRecipes = PYLON_SUPPORTED_RELEASE_RECIPES,
	supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES,
) {
	const ordered = manifests.toSorted((left, right) => left.sequence - right.sequence);
	let previous;
	for (let index = 0; index < ordered.length; index += 1) {
		const current = validateStableManifest(ordered[index], supportedRecipes, supportedPublicationPolicies);
		if (current.sequence !== index + 1) throw new Error("Stable publication history has a skipped or duplicate sequence.");
		if (previous) {
			if (
				current.history.previous.tag !== previous.tag ||
				current.history.previous.sha256 !== sha256Bytes(Buffer.from(canonicalJson(previous)))
			) {
				throw new Error("Stable sequence does not bind the exact previous manifest digest.");
			}
			if (!containsHistory(previous.revocations, current.revocations)) {
				throw new Error("Stable revocation history is not append-only.");
			}
			const delta = current.revocations.length - previous.revocations.length;
			if (delta < 0 || delta > 1 || (current.promotion.kind === "withdraw") !== (delta === 1)) {
				throw new Error("Stable sequence changed revocations outside one declared withdrawal.");
			}
			if (delta === 1) {
				const added = current.revocations.find((entry) => !previous.revocations.some((prior) => prior.stableTag === entry.stableTag));
				if (!added || added.revokedBySequence !== current.sequence || canonicalJson(added) !== canonicalJson(current.promotion.revocation)) {
					throw new Error("Stable withdrawal was not introduced by its declared sequence.");
				}
			}
		}
		previous = current;
	}
	const byTag = new Map(ordered.map((manifest) => [manifest.tag, manifest]));
	for (const manifest of ordered) {
		for (const revocation of manifest.revocations) {
			const revoked = byTag.get(revocation.stableTag);
			if (!revoked || revoked.sequence >= manifest.sequence || revoked.build.previewTag !== revocation.buildTag) {
				throw new Error("Stable revocation does not bind the exact previously published build.");
			}
		}
	}
	return ordered;
}

export function nextStableSequence(manifests) {
	return validateStableHistory(manifests).length + 1;
}

export function assertCanonicalInvocation({ repository, ref, eventName, sha, expectedEvent }) {
	if (
		repository !== PYLON_PUBLICATION_REPOSITORY ||
		ref !== PYLON_PUBLICATION_REF ||
		eventName !== expectedEvent ||
		!/^[0-9a-f]{40}$/.test(sha)
	) {
		throw new Error("Publication requires the canonical repository and protected pylon ref.");
	}
}

export function validateRequiredChecks({ sourceSha, requiredChecks, checkRuns }) {
	if (!/^[0-9a-f]{40}$/.test(sourceSha)) throw new Error("Required checks need an exact source SHA.");
	if (!Array.isArray(requiredChecks) || !Array.isArray(checkRuns)) {
		throw new Error("Protected pylon required-check policy is unavailable.");
	}
	const actualPolicy = requiredChecks
		.map((required) => ({ context: required?.context, appId: required?.appId }))
		.sort((left, right) => compareText(String(left.context), String(right.context)));
	const expectedPolicy = PYLON_REQUIRED_CHECKS.map(({ context, appId }) => ({ context, appId }));
	if (canonicalJson(actualPolicy) !== canonicalJson(expectedPolicy)) {
		throw new Error("Protected pylon required-check policy differs from the exact two GitHub Actions checks.");
	}
	for (const required of PYLON_REQUIRED_CHECKS) {
		const matches = checkRuns.filter(
			(candidate) => candidate.name === required.context && candidate.head_sha === sourceSha && candidate.app?.id === required.appId &&
				candidate.status === "completed" && candidate.conclusion === "success" && candidate.workflowPath === required.workflowPath,
		);
		if (matches.length === 0) {
			throw new Error(`Required check ${required.context} is not green on ${sourceSha} from app ${required.appId}.`);
		}
	}
	return true;
}

export function requiredCheckWorkflowPath(context) {
	const required = PYLON_REQUIRED_CHECKS.find((candidate) => candidate.context === context);
	if (!required) throw new Error(`Unknown required check context: ${String(context)}`);
	return required.workflowPath;
}

export function validateMergedChangelogProof({ repository, ref, eventName, mergeSha, pullRequests, headChecks, workflowRuns }) {
	assertCanonicalInvocation({ repository, ref, eventName, sha: mergeSha, expectedEvent: "push" });
	const matches = pullRequests.filter(
		(pr) =>
			pr.merged_at &&
			pr.merge_commit_sha === mergeSha &&
			pr.base?.ref === "pylon" &&
			pr.base?.repo?.full_name === PYLON_PUBLICATION_REPOSITORY &&
			pr.head?.repo?.full_name === PYLON_PUBLICATION_REPOSITORY &&
			/^[0-9a-f]{40}$/.test(pr.head?.sha ?? ""),
	);
	if (matches.length !== 1) throw new Error("Merge SHA does not resolve to exactly one merged pylon pull request.");
	const pullRequest = matches[0];
	const checks = headChecks.filter(
		(check) =>
			check.name === "Check changelog fragment" &&
			check.head_sha === pullRequest.head.sha &&
			check.app?.id === GITHUB_ACTIONS_APP_ID &&
			check.status === "completed" &&
			check.conclusion === "success",
	);
	const valid = checks.filter((check) => {
		const run = workflowRuns.find((candidate) => candidate.checkRunId === check.id);
		return (
			run?.event === "pull_request" &&
			run.head_sha === pullRequest.head.sha &&
			run.path === ".github/workflows/changelog-fragment.yml" &&
			run.repository === PYLON_PUBLICATION_REPOSITORY &&
			run.headRepository === PYLON_PUBLICATION_REPOSITORY &&
			run.pullRequests?.includes(pullRequest.number)
		);
	});
	if (valid.length < 1) throw new Error("Merged PR has no successful GitHub Actions changelog head check with exact provenance.");
	return pullRequest;
}

export function validateAttestationEvidence(evidence, expected) {
	if (
		evidence.repository !== expected.repository ||
		evidence.workflow !== expected.workflow ||
		evidence.ref !== expected.ref ||
		evidence.sourceSha !== expected.sourceSha ||
		evidence.issuer !== "https://token.actions.githubusercontent.com" ||
		evidence.rekorIncluded !== true ||
		evidence.subjectName !== expected.subjectName ||
		evidence.subjectSha256 !== expected.subjectSha256
	) {
		throw new Error("Attestation evidence does not satisfy the exact Pylon publication policy.");
	}
	return true;
}

export function validateWorkflowArtifactProvenance(actual, expected) {
	if (
		actual.runId !== expected.runId ||
		actual.repositoryId !== 1_349_002_285 ||
		actual.workflowPath !== expected.workflowPath ||
		actual.event !== expected.event ||
		actual.ref !== PYLON_PUBLICATION_REF ||
		actual.headSha !== expected.headSha ||
		actual.conclusion !== "success" ||
		actual.checkSuiteHeadSha !== expected.headSha ||
		actual.checkSuiteConclusion !== "success" ||
		!Array.isArray(actual.artifacts) ||
		actual.artifacts.length !== 1
	) {
		throw new Error("Workflow artifact is not bound to the exact canonical run.");
	}
	const artifact = actual.artifacts[0];
	if (
		artifact.name !== expected.artifactName ||
		artifact.expired !== false ||
		!/^sha256:[0-9a-f]{64}$/.test(artifact.digest ?? "")
	) {
		throw new Error("Workflow artifact transport identity is missing, expired, or ambiguous.");
	}
	return artifact;
}

export function assertImmutableReleaseIdentity(actual, expected) {
	if (
		actual.immutable !== true ||
		actual.draft !== false ||
		actual.tag_name !== expected.tag ||
		actual.name !== expected.name ||
		actual.body !== expected.body ||
		actual.prerelease !== expected.prerelease ||
		actual.target_commitish !== expected.sourceSha
	) {
		throw new Error("Existing release metadata differs or is not immutable.");
	}
	const expectedAssets = new Map(expected.assets.map((asset) => [asset.name, asset]));
	if (!Array.isArray(actual.assets) || actual.assets.length !== expectedAssets.size) {
		throw new Error("Existing release asset set differs.");
	}
	for (const asset of actual.assets) {
		const wanted = expectedAssets.get(asset.name);
		if (!wanted || asset.size !== wanted.size || asset.digest !== `sha256:${wanted.sha256}`) {
			throw new Error(`Existing release asset differs: ${String(asset.name)}`);
		}
		expectedAssets.delete(asset.name);
	}
	if (expectedAssets.size > 0) throw new Error("Existing release is missing assets.");
	return true;
}

export const PYLON_STABLE_MANIFEST_MAX_BYTES = 48 * 1024;
export const PYLON_STABLE_RELEASE_BODY_MAX_BYTES = 80 * 1024;

export function publicationReleaseBody({
	channel, tag, source, tree, recipeRevision, policyCommit, policyTree, stableManifestBytes,
}) {
	if (!["preview", "stable"].includes(channel)) throw new Error("Invalid publication channel.");
	const policy = channel === "stable"
		? [`Policy: ${policyCommit}`, `Policy tree: ${policyTree}`]
		: [];
	if (channel === "stable" && (!/^[0-9a-f]{40}$/.test(policyCommit ?? "") || !/^[0-9a-f]{40}$/.test(policyTree ?? ""))) {
		throw new Error("Stable release body needs the protected promotion policy commit and tree.");
	}
	let recovery = [];
	if (channel === "stable") {
		if (!Buffer.isBuffer(stableManifestBytes) || stableManifestBytes.length < 1 || stableManifestBytes.length > PYLON_STABLE_MANIFEST_MAX_BYTES) {
			throw new Error(`Stable recovery manifest must be from 1 through ${PYLON_STABLE_MANIFEST_MAX_BYTES} bytes.`);
		}
		recovery = [
			"Stable recovery manifest: base64-v1",
			`Manifest bytes: ${stableManifestBytes.length}`,
			`Manifest sha256: ${sha256Bytes(stableManifestBytes)}`,
			`Manifest base64: ${stableManifestBytes.toString("base64")}`,
		];
	}
	const body = [
		`Pylon Prime ${channel} publication.`,
		"",
		`Tag: ${tag}`,
		`Source: ${source}`,
		`Tree: ${tree}`,
		...policy,
		`Recipe: r${recipeRevision}`,
		...recovery,
		"",
		"Verify the immutable release and artifact attestations before use.",
	].join("\n");
	if (Buffer.byteLength(body, "utf8") > PYLON_STABLE_RELEASE_BODY_MAX_BYTES) {
		throw new Error(`Stable recovery release body exceeds ${PYLON_STABLE_RELEASE_BODY_MAX_BYTES} bytes.`);
	}
	return body;
}

export function stableManifestBytesFromReleaseBody(body) {
	if (typeof body !== "string" || Buffer.byteLength(body, "utf8") > PYLON_STABLE_RELEASE_BODY_MAX_BYTES) {
		throw new Error("Stable recovery release body is missing or exceeds its safe byte bound.");
	}
	const size = /^Manifest bytes: ([1-9][0-9]*)$/m.exec(body)?.[1];
	const digest = /^Manifest sha256: ([0-9a-f]{64})$/m.exec(body)?.[1];
	const encoded = /^Manifest base64: ([A-Za-z0-9+/]+={0,2})$/m.exec(body)?.[1];
	if (!size || !digest || !encoded || (body.match(/^Stable recovery manifest: base64-v1$/gm) ?? []).length !== 1) {
		throw new Error("Stable recovery release body has no exact manifest envelope.");
	}
	const bytes = Buffer.from(encoded, "base64");
	if (
		bytes.length !== Number(size) || bytes.length > PYLON_STABLE_MANIFEST_MAX_BYTES || bytes.toString("base64") !== encoded ||
		sha256Bytes(bytes) !== digest
	) throw new Error("Stable recovery manifest envelope is truncated, altered, or oversized.");
	const manifest = validateStableManifest(JSON.parse(bytes));
	if (bytes.toString("utf8") !== canonicalJson(manifest)) throw new Error("Stable recovery manifest is not canonical JSON.");
	const expectedBody = publicationReleaseBody({
		channel: "stable", tag: manifest.tag, source: manifest.build.source.commit, tree: manifest.build.source.tree,
		recipeRevision: manifest.build.recipeRevision, policyCommit: manifest.promotion.policyCommit,
		policyTree: manifest.promotion.policyTree, stableManifestBytes: bytes,
	});
	if (body !== expectedBody) throw new Error("Stable recovery release body metadata differs from its exact manifest.");
	return { bytes, manifest, digest };
}
