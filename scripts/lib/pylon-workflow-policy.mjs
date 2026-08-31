import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";

import {
	PYLON_PREVIEW_WORKFLOW,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_WORKFLOW,
	PYLON_SUPPORTED_RELEASE_RECIPES,
} from "./pylon-publication.mjs";

export const ATTEST_BUILD_PROVENANCE_ACTION = "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8";
// The pinned composite action above immutably delegates to this reviewed signer implementation.
export const ATTEST_ACTION_CHAIN = "actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d";

function jobNames(workflow) {
	const lines = workflow.replaceAll("\r\n", "\n").split("\n");
	const jobs = lines.findIndex((line) => line === "jobs:");
	if (jobs === -1) throw new Error("Approved workflow has no top-level jobs mapping.");
	return lines.slice(jobs + 1).flatMap((line) => /^  ([A-Za-z0-9_-]+):\s*$/.exec(line)?.[1] ?? []);
}

function jobBlock(workflow, jobName) {
	const lines = workflow.replaceAll("\r\n", "\n").split("\n");
	const jobs = lines.findIndex((line) => line === "jobs:");
	if (jobs === -1) throw new Error("Approved workflow has no top-level jobs mapping.");
	const start = lines.findIndex((line, index) => index > jobs && line === `  ${jobName}:`);
	if (start === -1) throw new Error(`Approved workflow lacks the exact ${jobName} job.`);
	let end = lines.length;
	for (let index = start + 1; index < lines.length; index += 1) {
		if (/^  [A-Za-z0-9_-]+:\s*$/.test(lines[index])) {
			end = index;
			break;
		}
	}
	return lines.slice(start, end).join("\n");
}

function scalar(block, name) {
	const matches = [...block.matchAll(new RegExp(`^    ${name}:\\s*([^\\n]+)\\s*$`, "gm"))];
	if (matches.length !== 1) throw new Error(`Approved job needs one exact ${name} value.`);
	return matches[0][1].trim();
}

function mapping(block, name) {
	const lines = block.split("\n");
	const start = lines.findIndex((line) => line === `    ${name}:`);
	if (start === -1) throw new Error(`Approved job needs an exact ${name} mapping.`);
	const values = {};
	for (let index = start + 1; index < lines.length; index += 1) {
		const line = lines[index];
		if (/^    \S/.test(line)) break;
		const match = /^      ([a-z-]+):\s*(\S+)\s*$/.exec(line);
		if (match) values[match[1]] = match[2];
		else if (line.trim()) throw new Error(`Approved ${name} mapping is not closed.`);
	}
	return values;
}

function exactObject(actual, expected, description) {
	if (JSON.stringify(Object.entries(actual).sort()) !== JSON.stringify(Object.entries(expected).sort())) {
		throw new Error(`Approved workflow ${description} differs from the closed policy.`);
	}
}

function needs(block) {
	const value = scalar(block, "needs");
	if (value.startsWith("[") && value.endsWith("]")) {
		return value.slice(1, -1).split(",").map((entry) => entry.trim()).filter(Boolean);
	}
	return [value];
}

function assertNoDownloadedOrRepositoryExecution(block, description) {
	if (
		/actions\/checkout@|actions\/setup-node@|\bnpm\s+(?:ci|install|run)\b|\bnode\s+scripts\/|\btar\s|\.tgz\b[^\n]*(?:exec|run)|node:child_process|\b(?:execFile|spawn|fork|eval)\s*\(|new Function|require\(["']\.|import\s*\(|\bchmod\b|(?:^|\s)\.\//im.test(block)
	) throw new Error(`${description} may not checkout or execute repository/downloaded code.`);
}

function canonicalList(values) {
	return JSON.stringify([...values].sort());
}

export function validateApprovedAttestationWorkflow(workflow, channel) {
	if (typeof workflow !== "string" || !workflow.endsWith("\n") || workflow.includes("\r")) {
		throw new Error("Approved workflow bytes must be normalized text.");
	}
	const policy = channel === "preview"
		? {
			environment: "pylon-preview",
			workflow: PYLON_PREVIEW_WORKFLOW,
			subject: ".npm/pylon-release/artifacts/*",
			attestNeeds: ["pack", "reproducibility", "install"],
			publisher: "publish",
		}
		: channel === "stable"
			? {
				environment: "pylon-stable",
				workflow: PYLON_STABLE_WORKFLOW,
				subject: "publication/pylon-stable-channel-v1.json",
				attestNeeds: ["prepare"],
				publisher: "stage-draft",
			}
			: null;
	if (!policy) throw new Error("Unknown approved attestation channel.");
	if (!/^permissions:\s*\{\}\s*$/m.test(workflow)) throw new Error("Approved publication workflow needs deny-by-default permissions.");
	const attest = jobBlock(workflow, "attest");
	if (scalar(attest, "environment") !== policy.environment) throw new Error("Attester lacks its exact approval environment.");
	exactObject(mapping(attest, "permissions"), {
		actions: "read",
		attestations: "write",
		"id-token": "write",
	}, "attester permissions");
	if (JSON.stringify(needs(attest).sort()) !== JSON.stringify(policy.attestNeeds.sort())) {
		throw new Error("Attester dependency path differs from the approved graph.");
	}
	const attestUses = [...attest.matchAll(/^        uses:\s*([^\s#]+)(?:\s+#.*)?$/gm)].map((match) => match[1]);
	if (attestUses.filter((value) => value === ATTEST_BUILD_PROVENANCE_ACTION).length !== 1) {
		throw new Error("Attester must use the one exact pinned provenance action.");
	}
	for (const action of attestUses) {
		const revision = action.split("@").at(-1);
		if (!/^[0-9a-f]{40}$/.test(revision)) throw new Error("Attester action is not pinned to a full SHA.");
	}
	const allowedAttesterActions = new Set([
		"actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea",
		"actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
		ATTEST_BUILD_PROVENANCE_ACTION,
	]);
	if (attestUses.some((action) => !allowedAttesterActions.has(action))) throw new Error("Attester uses an action outside the exact closed allowlist.");
	const expectedAttesterActions = channel === "preview"
		? ["actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", ATTEST_BUILD_PROVENANCE_ACTION]
		: [
			"actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea",
			"actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
			ATTEST_BUILD_PROVENANCE_ACTION,
		];
	if (canonicalList(attestUses) !== canonicalList(expectedAttesterActions)) throw new Error("Attester action graph differs from the exact allowlist.");
	if (
		!attest.includes("      - name: Validate exact subjects before signing\n") &&
		!attest.includes("      - name: Validate exact stable subject before signing\n") ||
		(attest.match(/          node <<'NODE'/g) ?? []).length !== 1
	) throw new Error("Attester lacks its one exact inline subject validator.");
	const subjects = [...attest.matchAll(/^          subject-path:\s*(.+?)\s*$/gm)].map((match) => match[1]);
	if (subjects.length !== 1 || subjects[0] !== policy.subject) throw new Error("Attester subject path differs from the exact approved subject.");
	assertNoDownloadedOrRepositoryExecution(attest, "Attester");

	const verifier = jobBlock(workflow, "verify-attestation");
	if (!needs(verifier).includes("attest")) throw new Error("Read-only verifier is not downstream of the approved attester.");
	const verifierPermissions = mapping(verifier, "permissions");
	if (Object.values(verifierPermissions).some((value) => value === "write")) throw new Error("Attestation verifier is not read-only.");

	const blocks = new Map(jobNames(workflow).map((name) => [name, jobBlock(workflow, name)]));
	const writers = [...blocks].filter(([, block]) => /^      contents: write$/m.test(block)).map(([name]) => name).sort();
	if (canonicalList(writers) !== canonicalList(["publish", "stage-draft"])) {
		throw new Error("Approved publication workflow has an extra or missing contents writer.");
	}
	const stage = blocks.get("stage-draft");
	const publisher = blocks.get("publish");
	for (const [name, block] of [["stage-draft", stage], ["publish", publisher]]) {
		if (mapping(block, "permissions").contents !== "write" || /id-token:\s*write|attestations:\s*write|^    environment:/m.test(block)) {
			throw new Error(`${name} does not isolate contents write from approval and OIDC.`);
		}
		assertNoDownloadedOrRepositoryExecution(block, `${name} contents publisher`);
	}
	if (!needs(stage).includes("verify-attestation")) throw new Error("Draft staging is not directly downstream of verified approval evidence.");
	if (!needs(publisher).includes("stage-draft") || !needs(publisher).includes("verify-attestation") && channel === "preview") {
		throw new Error("Final publisher dependency path differs from the approved graph.");
	}
	if (channel === "stable") {
		const recovery = blocks.get("authorize-stable-resume");
		if (
			!recovery || scalar(recovery, "environment") !== "pylon-stable" || !/^    permissions: \{\}$/m.test(recovery) ||
			!/mode == 'resume'/.test(recovery) || !needs(publisher).includes("authorize-stable-resume") ||
			!/mode == 'normal'/.test(attest) || !/needs\.verify-attestation\.result == 'success'/.test(publisher) ||
			!/needs\.authorize-stable-resume\.result == 'success'/.test(publisher)
		) throw new Error("Stable normal and recovery approvals are not exact mutually exclusive paths.");
	}
	return { workflow: policy.workflow, environment: policy.environment };
}

export function readWorkflowAtSignerDigest(workflowPath, signerDigest) {
	if (![PYLON_PREVIEW_WORKFLOW, PYLON_STABLE_WORKFLOW].includes(workflowPath)) throw new Error("Unsupported attestation workflow path.");
	if (!/^[0-9a-f]{40}$/.test(signerDigest)) throw new Error("Workflow signer digest must be a full lowercase Git SHA.");
	const token = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN;
	if (!token) throw new Error("GH_TOKEN or GITHUB_TOKEN is required to authorize the historical signer workflow.");
	const result = spawnSync(
		"gh",
		["api", `repos/${PYLON_PUBLICATION_REPOSITORY}/contents/${workflowPath}?ref=${signerDigest}`],
		{ encoding: "utf8", timeout: 120_000, maxBuffer: 16 * 1024 * 1024, env: { ...process.env, GH_TOKEN: token } },
	);
	if (result.error) throw result.error;
	if (result.status !== 0) throw new Error(`Unable to read approved workflow at signer digest: ${result.stderr}`);
	const response = JSON.parse(result.stdout.replace(/\u001b\[[0-9;]*m/g, ""));
	if (response.type !== "file" || response.path !== workflowPath || response.encoding !== "base64" || typeof response.content !== "string") {
		throw new Error("Historical signer workflow response is not one exact file.");
	}
	return Buffer.from(response.content.replaceAll("\n", ""), "base64");
}

export function validateApprovedWorkflowBytes(workflowPath, workflow, channel, recipeRevision) {
	const workflowBytes = Buffer.isBuffer(workflow) ? workflow : Buffer.from(workflow, "utf8");
	const workflowText = workflowBytes.toString("utf8");
	if (!Buffer.from(workflowText, "utf8").equals(workflowBytes)) throw new Error("Signer workflow is not exact UTF-8 bytes.");
	if (!Number.isSafeInteger(recipeRevision) || recipeRevision < 1) throw new Error("Workflow policy needs an exact positive recipe revision.");
	const recipe = PYLON_SUPPORTED_RELEASE_RECIPES.find((candidate) => candidate.recipeRevision === recipeRevision);
	if (!recipe) throw new Error(`Unsupported historical release recipe revision: ${recipeRevision}`);
	const expectedPath = channel === "preview" ? recipe.previewWorkflowPath : channel === "stable" ? recipe.stableWorkflowPath : "";
	const expectedDigest = channel === "preview" ? recipe.previewWorkflowSha256 : channel === "stable" ? recipe.stableWorkflowSha256 : "";
	if (workflowPath !== expectedPath || !expectedPath) throw new Error("Signer workflow path differs from the exact recipe channel.");
	const actualDigest = createHash("sha256").update(workflowBytes).digest("hex");
	if (actualDigest !== expectedDigest) {
		throw new Error(`Signer workflow bytes differ from recipe r${recipeRevision} for ${channel}.`);
	}
	return validateApprovedAttestationWorkflow(workflowText, channel);
}

export function verifyApprovedWorkflowAtSignerDigest(workflowPath, signerDigest, channel, recipeRevision) {
	const workflow = readWorkflowAtSignerDigest(workflowPath, signerDigest);
	return validateApprovedWorkflowBytes(workflowPath, workflow, channel, recipeRevision);
}
