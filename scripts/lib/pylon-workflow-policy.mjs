import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";

import {
	PYLON_PREVIEW_WORKFLOW,
	PYLON_PUBLICATION_REPOSITORY,
	PYLON_STABLE_WORKFLOW,
	PYLON_SUPPORTED_PUBLICATION_POLICIES,
} from "./pylon-publication.mjs";

export const ATTEST_BUILD_PROVENANCE_ACTION = "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8";
// The pinned composite action above immutably delegates to this reviewed signer implementation.
export const ATTEST_ACTION_CHAIN = "actions/attest@508db95dd578ae2727ebd6217d5ba78e4fbda05d";
export const CREATE_GITHUB_APP_TOKEN_ACTION = "actions/create-github-app-token@fee1f7d63c2ff003460e3d139729b119787bc349";

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

function stepBlocks(block) {
	const matches = [...block.matchAll(/^      - name: (.+)$/gm)];
	return matches.map((match, index) => ({
		name: match[1],
		block: block.slice(match.index, matches[index + 1]?.index ?? block.length),
	}));
}

function parseStepShape(step, description) {
	const lines = step.block.split("\n");
	const root = {};
	const nested = {};
	const first = /^      - ([a-z-]+):\s*(.+)$/.exec(lines[0]);
	if (!first) throw new Error(`${description} is not one closed named YAML step.`);
	root[first[1]] = first[2];
	for (let index = 1; index < lines.length; index += 1) {
		const line = lines[index];
		if (!line) continue;
		const entry = /^        ([a-z-]+):(?:\s*(.*))?$/.exec(line);
		if (!entry) continue;
		if (Object.hasOwn(root, entry[1])) throw new Error(`${description} has a duplicate step key.`);
		root[entry[1]] = entry[2] ?? "";
		if (entry[2]) continue;
		const values = {};
		for (index += 1; index < lines.length; index += 1) {
			const child = /^          ([a-z-]+):\s*(.*)$/.exec(lines[index]);
			if (!child) {
				index -= 1;
				break;
			}
			if (Object.hasOwn(values, child[1])) throw new Error(`${description} has a duplicate ${entry[1]} key.`);
			values[child[1]] = child[2];
			if (child[2] === "|") {
				while (index + 1 < lines.length && (!lines[index + 1] || lines[index + 1].startsWith("            "))) index += 1;
			}
		}
		nested[entry[1]] = values;
	}
	return { root, nested };
}

function assertExactRulesetAuditorMint(step, description, expectedIf) {
	if (/[#&]|(?:^|\s)\*[^/]|<<:/m.test(step.block)) {
		throw new Error(`${description} may not use YAML comments, anchors, aliases, or merge keys.`);
	}
	const parsed = parseStepShape(step, description);
	const expectedRoot = {
		name: "Mint repository-scoped ruleset auditor token",
		id: "ruleset-auditor",
		uses: CREATE_GITHUB_APP_TOKEN_ACTION,
		with: "",
	};
	if (expectedIf !== null) expectedRoot.if = expectedIf;
	exactObject(parsed.root, expectedRoot, `${description} step mapping`);
	exactObject(parsed.nested.with ?? {}, {
		"app-id": "${{ vars.PYLON_RULESET_AUDITOR_APP_ID }}",
		"private-key": "${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}",
		owner: "pylon-code",
		repositories: "prime-agent",
		"permission-administration": "read",
	}, `${description} with mapping`);
	if (Object.keys(parsed.nested).length !== 1) throw new Error(`${description} has an unexpected nested mapping.`);
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

function assertNoBranchProtectionAdministrationRead(block, description) {
	if (
		/branchProtectionRule|requiredStatusChecks|github\.rest\.repos\.(?:getBranchProtection|getAdminBranchProtection)|\/branches\/[^\s"'`]+\/protection/.test(block)
	) throw new Error(`${description} may not make an Administration-gated branch-protection read.`);
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
	assertNoBranchProtectionAdministrationRead(workflow, "Default-token publication workflow");
	const admission = jobBlock(workflow, "admission");
	if (/^    environment:|PYLON_RULESET_AUDITOR|permission-administration|github-token:/m.test(admission)) {
		throw new Error("Source admission may not receive the protected ruleset-auditor environment or credential.");
	}
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
	const protectedMutationJobs = channel === "preview" ? new Set(["stage-draft", "publish"]) : new Set(["publish"]);
	for (const [name, block] of [["stage-draft", stage], ["publish", publisher]]) {
		if (mapping(block, "permissions").contents !== "write" || /id-token:\s*write|attestations:\s*write/.test(block)) {
			throw new Error(`${name} does not isolate contents write from OIDC.`);
		}
		const environmentMatches = [...block.matchAll(/^    environment:\s*(\S+)\s*$/gm)].map((match) => match[1]);
		if (protectedMutationJobs.has(name)) {
			if (environmentMatches.length !== 1 || environmentMatches[0] !== policy.environment) {
				throw new Error(`${name} lacks its direct protected mutation environment.`);
			}
		} else if (environmentMatches.length !== 0) {
			throw new Error(`${name} unexpectedly carries a protected environment.`);
		}
		assertNoDownloadedOrRepositoryExecution(block, `${name} contents publisher`);
	}

	const expectedAudits = channel === "preview" ? { "stage-draft": 1, publish: 1 } : { publish: 3 };
	const authoritativeValidators = new Set();
	const protectedMutations = [];
	let mintCount = 0;
	let auditCount = 0;
	for (const [name, expectedAuditCount] of Object.entries(expectedAudits)) {
		const block = blocks.get(name);
		const steps = stepBlocks(block);
		const mintSteps = steps.filter((step) => /^        uses: actions\/create-github-app-token@/m.test(step.block));
		if (mintSteps.length !== 1) throw new Error(`${name} needs one exact ruleset-auditor token mint.`);
		const expectedMintIf = channel === "preview" && name === "publish" ? "steps.finalize.outputs.release_id != ''" : null;
		assertExactRulesetAuditorMint(mintSteps[0], `${name} ruleset-auditor mint`, expectedMintIf);
		mintCount += 1;
		const audits = steps.filter((step) => step.block.includes("github-token: ${{ steps.ruleset-auditor.outputs.token }}"));
		if (audits.length !== expectedAuditCount) throw new Error(`${name} lacks one fresh authoritative audit per protected mutation.`);
		auditCount += audits.length;
		for (const audit of audits) {
			const parsedAudit = parseStepShape(audit, `${name} ruleset audit`);
			const expectedAuditRoot = {
				name: audit.name,
				uses: "actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1",
				with: "",
			};
			if (Object.hasOwn(parsedAudit.root, "if")) expectedAuditRoot.if = parsedAudit.root.if;
			exactObject(parsedAudit.root, expectedAuditRoot, `${name} ruleset audit step mapping`);
			const githubMemberReferences = [...audit.block.matchAll(/\bgithub\.([A-Za-z]+)/g)].map((match) => match[1]).sort();
			if (
				parsedAudit.nested.with?.["github-token"] !== "${{ steps.ruleset-auditor.outputs.token }}" ||
				parsedAudit.nested.with?.script !== "|" || Object.keys(parsedAudit.nested.with).length !== 2 ||
				Object.keys(parsedAudit.nested).length !== 1 ||
				canonicalList(githubMemberReferences) !== canonicalList(["graphql", "request"]) ||
				/\b(?:arguments|console|context|core|fetch|globalThis|process|require)\b/.test(audit.block) ||
				!/GET \/repos\/\{owner\}\/\{repo\}\/rulesets\/\{ruleset_id\}/.test(audit.block) ||
				!/ruleset_id: 21950766, includes_parents: false/.test(audit.block) ||
				!/restRuleset\.node_id !== "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4"/.test(audit.block) ||
				!/Object\.hasOwn\(restRuleset, "bypass_actors"\)/.test(audit.block) ||
				!/Object\.hasOwn\(restRuleset, "current_user_can_bypass"\)/.test(audit.block) ||
				!/ruleset\(databaseId: \$rulesetDatabaseId, includeParents: false\)/.test(audit.block) ||
				!/const authoritative = await github\.graphql\(query/.test(audit.block) ||
				!/bypassActors\.totalCount !== 0/.test(audit.block) ||
				!/updateAllowsFetchAndMerge !== false/.test(audit.block) ||
				/github\.rest\.git\.createRef|github\.rest\.repos\.updateRelease|github\.rest\.git\.createTag|repos\.createRelease/.test(audit.block)
			) throw new Error(`${name} ruleset audit is not the exact read-only combined REST and GraphQL proof.`);
			const script = audit.block.slice(audit.block.indexOf("          script: |"));
			authoritativeValidators.add(script);
		}
		for (let index = 0; index < steps.length; index += 1) {
			const mutation = steps[index];
			if (!/github\.rest\.git\.createRef|github\.rest\.repos\.updateRelease/.test(mutation.block)) continue;
			protectedMutations.push(`${name}:${mutation.name}`);
			const audit = steps[index - 1];
			if (!audit?.block.includes("github-token: ${{ steps.ruleset-auditor.outputs.token }}")) {
				throw new Error(`${name} protected mutation lacks an adjacent fresh authoritative audit.`);
			}
			const auditIf = parseStepShape(audit, `${name} adjacent ruleset audit`).root.if ?? null;
			const mutationIf = parseStepShape(mutation, `${name} protected mutation`).root.if ?? null;
			if (auditIf !== mutationIf) throw new Error(`${name} protected mutation and adjacent audit conditions differ.`);
			if (mutation.block.includes("steps.ruleset-auditor.outputs.token") || mutation.block.includes("PYLON_RULESET_AUDITOR")) {
				throw new Error(`${name} passes the ruleset-auditor credential to a contents mutation.`);
			}
		}
	}
	const occurrenceCount = (value) => workflow.split(value).length - 1;
	const allExpressions = [...workflow.matchAll(/\$\{\{[\s\S]*?\}\}/g)].map((match) => match[0]);
	const sensitiveExpressions = allExpressions
		.filter((expression) => /PYLON_RULESET_AUDITOR|ruleset-auditor/.test(expression));
	const expectedSensitiveExpressions = [
		...Array.from({ length: mintCount }, () => "${{ vars.PYLON_RULESET_AUDITOR_APP_ID }}"),
		...Array.from({ length: mintCount }, () => "${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}"),
		...Array.from({ length: auditCount }, () => "${{ steps.ruleset-auditor.outputs.token }}"),
	];
	if (
		canonicalList(sensitiveExpressions) !== canonicalList(expectedSensitiveExpressions) ||
		canonicalList(allExpressions.filter((expression) => /\bsecrets\b/.test(expression))) !==
			canonicalList(Array.from({ length: mintCount }, () => "${{ secrets.PYLON_RULESET_AUDITOR_PRIVATE_KEY }}")) ||
		canonicalList(allExpressions.filter((expression) => /\bvars\b/.test(expression))) !==
			canonicalList(Array.from({ length: mintCount }, () => "${{ vars.PYLON_RULESET_AUDITOR_APP_ID }}")) ||
		occurrenceCount(CREATE_GITHUB_APP_TOKEN_ACTION) !== mintCount || occurrenceCount("private-key:") !== mintCount ||
		occurrenceCount("id: ruleset-auditor") !== mintCount || occurrenceCount("PYLON_RULESET_AUDITOR_APP_ID") !== mintCount ||
		occurrenceCount("PYLON_RULESET_AUDITOR_PRIVATE_KEY") !== mintCount ||
		occurrenceCount("steps.ruleset-auditor.outputs.token") !== auditCount || occurrenceCount("github-token:") !== auditCount ||
		/\$\{\{[\s\S]*?(?:toJSON|toJson)\s*\(\s*(?:steps|secrets|vars)\b/.test(workflow) ||
		/\$\{\{\s*steps\s*\}\}/.test(workflow) || /\bsteps\s*\[/.test(workflow)
	) throw new Error("Ruleset-auditor credentials or token outputs escape the exact mint and audit roles.");
	if (authoritativeValidators.size !== 1) throw new Error("Protected mutations do not share one frozen authoritative ruleset validator.");
	const expectedMutations = channel === "preview"
		? ["publish:Publish the exact approved preview draft", "stage-draft:Create or refetch the exact protected preview tag"]
		: [
			"publish:Create or refetch the exact protected stable tag",
			"publish:Create the exact protected stable reservation ref",
			"publish:Publish only the exact protected stable draft",
		];
	if (canonicalList(protectedMutations) !== canonicalList(expectedMutations)) {
		throw new Error("Protected mutation inventory differs from the closed ruleset-auditor policy.");
	}
	const nonAuditRulesetReads = [...workflow.matchAll(/GET \/repos\/\{owner\}\/\{repo\}\/rulesets\/\{ruleset_id\}/g)].length -
		Object.values(expectedAudits).reduce((total, count) => total + count, 0);
	if (nonAuditRulesetReads !== 0) throw new Error("Normal GITHUB_TOKEN ruleset admission is incorrectly treated as authoritative.");
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

export function validateApprovedWorkflowBytes(
	workflowPath,
	workflow,
	channel,
	publicationPolicyRevision,
	supportedPublicationPolicies = PYLON_SUPPORTED_PUBLICATION_POLICIES,
) {
	const workflowBytes = Buffer.isBuffer(workflow) ? workflow : Buffer.from(workflow, "utf8");
	const workflowText = workflowBytes.toString("utf8");
	if (!Buffer.from(workflowText, "utf8").equals(workflowBytes)) throw new Error("Signer workflow is not exact UTF-8 bytes.");
	if (!Number.isSafeInteger(publicationPolicyRevision) || publicationPolicyRevision < 1) {
		throw new Error("Workflow policy needs an exact positive publication policy revision.");
	}
	const policy = supportedPublicationPolicies.find(
		(candidate) => candidate.publicationPolicyRevision === publicationPolicyRevision,
	);
	if (!policy) throw new Error(`Unsupported publication policy revision: ${publicationPolicyRevision}`);
	const expectedPath = channel === "preview" ? policy.previewWorkflowPath : channel === "stable" ? policy.stableWorkflowPath : "";
	const expectedDigest = channel === "preview" ? policy.previewWorkflowSha256 : channel === "stable" ? policy.stableWorkflowSha256 : "";
	if (workflowPath !== expectedPath || !expectedPath) throw new Error("Signer workflow path differs from the exact publication policy channel.");
	const actualDigest = createHash("sha256").update(workflowBytes).digest("hex");
	if (actualDigest !== expectedDigest) {
		throw new Error(`Signer workflow bytes differ from publication policy p${publicationPolicyRevision} for ${channel}.`);
	}
	return validateApprovedAttestationWorkflow(workflowText, channel);
}

export function verifyApprovedWorkflowAtSignerDigest(workflowPath, signerDigest, channel, publicationPolicyRevision) {
	const workflow = readWorkflowAtSignerDigest(workflowPath, signerDigest);
	return validateApprovedWorkflowBytes(workflowPath, workflow, channel, publicationPolicyRevision);
}
