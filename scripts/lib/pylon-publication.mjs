import { createHash } from "node:crypto";

import {
	PYLON_RELEASE_MANIFEST,
	PYLON_RELEASE_RECIPE_REVISION,
	PYLON_RELEASE_REPOSITORY,
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

function publicationAssets(releaseManifest) {
	return releaseManifest.assets.map(({ file, size, sha256, sha512 }) => ({ file, size, sha256, sha512 }));
}

export function createPreviewManifest(releaseManifest, releaseManifestBytes) {
	validateReleaseManifest(releaseManifest);
	if (!Buffer.isBuffer(releaseManifestBytes) || releaseManifestBytes.byteLength === 0) {
		throw new Error("Build manifest bytes are required.");
	}
	const tag = releaseBuildId(releaseManifest.source.commit);
	return {
		schemaVersion: PYLON_PUBLICATION_SCHEMA_VERSION,
		channel: "preview",
		repository: PYLON_RELEASE_REPOSITORY,
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

export function validatePreviewManifest(previewManifest, releaseManifest, releaseManifestBytes) {
	const expected = createPreviewManifest(releaseManifest, releaseManifestBytes);
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

export function createStableManifest({ previewManifest, previewManifestBytes, sequence, previous = null, revocations = [], promotion }) {
	if (canonicalJson(previewManifest) !== previewManifestBytes.toString("utf8")) {
		throw new Error("Preview manifest is not canonical publication JSON.");
	}
	const previewTag = parsePreviewTag(previewManifest.build?.tag);
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
	const expectedPromotionKeys = promotion.kind === "promote"
		? ["kind", "policyCommit", "policyTree"]
		: ["kind", "policyCommit", "policyTree", "revocation"];
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
			previewTag: previewManifest.build.tag,
			id: previewManifest.build.id,
			recipeRevision: previewManifest.build.recipeRevision,
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

export function validateStableManifest(stableManifest) {
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
	if (
		!exactKeys(build, ["previewTag", "id", "recipeRevision", "source", "releaseManifest", "previewManifest", "assets"]) ||
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
		parsePreviewTag(stableManifest.build.previewTag).commit12 !== tag.commit12
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
			!exactKeys(stableManifest.promotion, ["kind", "policyCommit", "policyTree", "revocation"]) ||
			!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyCommit) ||
			!/^[0-9a-f]{40}$/.test(stableManifest.promotion.policyTree) ||
			!stableManifest.revocations.some(
				(entry) => canonicalJson(entry) === canonicalJson(stableManifest.promotion.revocation),
			)
		) {
			throw new Error("Stable withdrawal does not append one exact revocation.");
		}
	} else if (
		!exactKeys(stableManifest.promotion, ["kind", "policyCommit", "policyTree"]) ||
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

export function validateStableHistory(manifests) {
	const ordered = manifests.toSorted((left, right) => left.sequence - right.sequence);
	let previous;
	for (let index = 0; index < ordered.length; index += 1) {
		const current = validateStableManifest(ordered[index]);
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

export function validateRequiredChecks({ sourceSha, requiredChecks, checkRuns, statuses = [] }) {
	if (!/^[0-9a-f]{40}$/.test(sourceSha)) throw new Error("Required checks need an exact source SHA.");
	if (!Array.isArray(requiredChecks) || requiredChecks.length === 0) {
		throw new Error("Protected pylon has no readable required exact-SHA checks.");
	}
	for (const required of requiredChecks) {
		if (typeof required.context !== "string" || !required.context) throw new Error("Malformed required check context.");
		if (required.appId !== null && !Number.isSafeInteger(required.appId)) throw new Error("Malformed required check app.");
		if (required.appId === null) {
			const status = statuses.find((candidate) => candidate.context === required.context && candidate.sha === sourceSha);
			if (!status || status.state !== "success") throw new Error(`Required status ${required.context} is not green on ${sourceSha}.`);
			continue;
		}
		const check = checkRuns.find(
			(candidate) =>
				candidate.name === required.context &&
				candidate.head_sha === sourceSha &&
				candidate.app?.id === required.appId,
		);
		if (!check || check.status !== "completed" || check.conclusion !== "success") {
			throw new Error(`Required check ${required.context} is not green on ${sourceSha} from app ${required.appId}.`);
		}
	}
	return true;
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

export function publicationReleaseBody({ channel, tag, source, tree, recipeRevision, policyCommit, policyTree }) {
	if (!["preview", "stable"].includes(channel)) throw new Error("Invalid publication channel.");
	const policy = channel === "stable"
		? [`Policy: ${policyCommit}`, `Policy tree: ${policyTree}`]
		: [];
	if (channel === "stable" && (!/^[0-9a-f]{40}$/.test(policyCommit ?? "") || !/^[0-9a-f]{40}$/.test(policyTree ?? ""))) {
		throw new Error("Stable release body needs the protected promotion policy commit and tree.");
	}
	return [
		`Pylon Prime ${channel} publication.`,
		"",
		`Tag: ${tag}`,
		`Source: ${source}`,
		`Tree: ${tree}`,
		...policy,
		`Recipe: r${recipeRevision}`,
		"",
		"Verify the immutable release and artifact attestations before use.",
	].join("\n");
}
