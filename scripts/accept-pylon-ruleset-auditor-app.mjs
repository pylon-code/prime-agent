#!/usr/bin/env node

import { createPrivateKey, sign } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

import {
	auditPublicationRuleset,
	PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY,
	PYLON_REPOSITORY_ID,
	PYLON_REPOSITORY_NAME,
	PYLON_REPOSITORY_NAME_WITH_OWNER,
	PYLON_REPOSITORY_OWNER,
	PYLON_RULESET_BYPASS_CANARY_GRAPHQL_QUERY,
	PYLON_RULESET_ID,
	validateRulesetBypassCanary,
} from "./lib/pylon-ruleset-auditor.mjs";

const API_VERSION = "2022-11-28";
const MAX_PRIVATE_KEY_BYTES = 64 * 1024;
const PYLON_ORGANIZATION_ID = 314_006_107;

function base64Url(value) {
	return Buffer.from(value).toString("base64url");
}

function exactObject(actual, expected) {
	return actual !== null && typeof actual === "object" && !Array.isArray(actual) &&
		JSON.stringify(Object.entries(actual).sort()) === JSON.stringify(Object.entries(expected).sort());
}

function exactRepository(repository) {
	return repository?.id === PYLON_REPOSITORY_ID && repository?.name === PYLON_REPOSITORY_NAME &&
		repository?.full_name === PYLON_REPOSITORY_NAME_WITH_OWNER && repository?.owner?.login === PYLON_REPOSITORY_OWNER;
}

function assertAppPermissions(permissions, description) {
	if (!exactObject(permissions, { administration: "read", metadata: "read" })) {
		throw new Error(`${description} permissions are not exact Administration: read plus unavoidable Metadata: read.`);
	}
}

function assertTokenPermissions(permissions, description) {
	if (
		!exactObject(permissions, { administration: "read" }) &&
		!exactObject(permissions, { administration: "read", metadata: "read" })
	) throw new Error(`${description} response permissions exceed or omit the exact read-only scope.`);
}

function readPrivateKey(privateKeyPath) {
	if (typeof privateKeyPath !== "string" || !privateKeyPath) throw new Error("--private-key-path is required.");
	const metadata = lstatSync(privateKeyPath);
	if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size < 1 || metadata.size > MAX_PRIVATE_KEY_BYTES) {
		throw new Error("The App private-key path must be one bounded regular file.");
	}
	return readFileSync(privateKeyPath);
}

export function createAppJwt({ appId, privateKeyPath, now = Date.now() }) {
	if (typeof appId !== "string" || !/^[1-9][0-9]*$/.test(appId)) throw new Error("--app-id must be a positive decimal GitHub App id.");
	if (!Number.isSafeInteger(now) || now < 0) throw new Error("JWT clock is invalid.");
	const seconds = Math.floor(now / 1000);
	const encodedHeader = base64Url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
	const encodedPayload = base64Url(JSON.stringify({ iat: seconds - 60, exp: seconds + 540, iss: appId }));
	const signingInput = `${encodedHeader}.${encodedPayload}`;
	const privateKeyBytes = readPrivateKey(privateKeyPath);
	try {
		const key = createPrivateKey(privateKeyBytes);
		return `${signingInput}.${sign("RSA-SHA256", Buffer.from(signingInput), key).toString("base64url")}`;
	} finally {
		privateKeyBytes.fill(0);
	}
}

async function defaultRequest({ method, path, token, body }) {
	const response = await fetch(`https://api.github.com${path}`, {
		method,
		headers: {
			accept: "application/vnd.github+json",
			authorization: `Bearer ${token}`,
			"content-type": "application/json",
			"user-agent": "pylon-ruleset-auditor-acceptance",
			"x-github-api-version": API_VERSION,
		},
		body: body === undefined ? undefined : JSON.stringify(body),
		signal: AbortSignal.timeout(30_000),
	});
	const bytes = Buffer.from(await response.arrayBuffer());
	if (bytes.length > 4 * 1024 * 1024) throw new Error("GitHub acceptance response exceeds the bounded body limit.");
	let data = null;
	if (bytes.length > 0) {
		try {
			data = JSON.parse(bytes);
		} catch {
			throw new Error(`GitHub acceptance endpoint returned non-JSON status ${response.status}.`);
		}
	}
	return { status: response.status, data };
}

function requireStatus(response, status, description) {
	if (response?.status !== status) throw new Error(`${description} returned status ${response?.status ?? "missing"}.`);
	return response.data;
}

async function graphql(request, token, query, variables, description) {
	const response = await request({ method: "POST", path: "/graphql", token, body: { query, variables } });
	const envelope = requireStatus(response, 200, description);
	if (
		envelope === null || typeof envelope !== "object" || Array.isArray(envelope) ||
		Object.hasOwn(envelope, "errors") || envelope.data === null || typeof envelope.data !== "object" || Array.isArray(envelope.data)
	) throw new Error(`${description} returned GraphQL errors or a null/partial envelope.`);
	return envelope.data;
}

function mintedToken(response, description) {
	const data = requireStatus(response, 201, description);
	if (typeof data?.token !== "string" || !data.token) throw new Error(`${description} returned no revocable token.`);
	return { data, token: data.token };
}

function validateTokenResponse(data, description, now, requireRepositoryResponse) {
	const expiration = Date.parse(data.expires_at);
	const repositoriesAreExact = Array.isArray(data.repositories) && data.repositories.length === 1 && exactRepository(data.repositories[0]);
	if (
		typeof data.expires_at !== "string" || !Number.isFinite(expiration) || expiration <= now || expiration > now + 65 * 60 * 1000 ||
		data.repository_selection !== "selected" || requireRepositoryResponse && !repositoriesAreExact ||
		!requireRepositoryResponse && data.repositories !== undefined && !repositoriesAreExact
	) throw new Error(`${description} response does not carry the exact selected prime-agent repository scope.`);
	assertTokenPermissions(data.permissions, description);
}

async function revokeToken(request, token) {
	const response = await request({ method: "DELETE", path: "/installation/token", token });
	requireStatus(response, 204, "Installation-token revocation");
}

async function withRevokedToken(request, token, use) {
	try {
		return await use(token);
	} finally {
		await revokeToken(request, token);
	}
}

function validateCanary(canary) {
	if (
		canary === null || typeof canary !== "object" || !/^[A-Za-z0-9_.-]+$/.test(canary.owner ?? "") ||
		!/^[A-Za-z0-9_.-]+$/.test(canary.repo ?? "") || !Number.isSafeInteger(canary.rulesetDatabaseId) ||
		canary.rulesetDatabaseId < 1 ||
		`${canary.owner}/${canary.repo}:${canary.rulesetDatabaseId}` === `${PYLON_REPOSITORY_NAME_WITH_OWNER}:${PYLON_RULESET_ID}`
	) throw new Error("A different known-public positive-bypass ruleset canary is required.");
	return canary;
}

export async function acceptRulesetAuditorApp({
	appId,
	privateKeyPath,
	appSlug,
	canary,
	request = defaultRequest,
	now = Date.now(),
}) {
	if (typeof appSlug !== "string" || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(appSlug)) {
		throw new Error("--app-slug must be the exact expected GitHub App slug.");
	}
	validateCanary(canary);
	const jwt = createAppJwt({ appId, privateKeyPath, now });
	const app = requireStatus(await request({ method: "GET", path: "/app", token: jwt }), 200, "Authenticated App readback");
	if (String(app?.id) !== appId || app?.slug !== appSlug) throw new Error("Authenticated App id or slug differs from the expected identity.");
	assertAppPermissions(app.permissions, "Authenticated App");

	const installation = requireStatus(
		await request({ method: "GET", path: `/orgs/${PYLON_REPOSITORY_OWNER}/installation`, token: jwt }),
		200,
		"pylon-code App installation readback",
	);
	if (
		!Number.isSafeInteger(installation?.id) || installation.id < 1 || String(installation.app_id) !== appId ||
		installation.app_slug !== appSlug || installation.account?.login !== PYLON_REPOSITORY_OWNER ||
		installation.account?.id !== PYLON_ORGANIZATION_ID || installation.account?.type !== "Organization" ||
		installation.target_id !== PYLON_ORGANIZATION_ID || installation.target_type !== "Organization" ||
		installation.repository_selection !== "selected" || installation.suspended_at !== null || installation.suspended_by !== null
	) throw new Error("pylon-code App installation identity, selection, or suspension state differs from the exact policy.");
	assertAppPermissions(installation.permissions, "pylon-code App installation");

	const tokenPath = `/app/installations/${installation.id}/access_tokens`;
	const fullMint = mintedToken(await request({
		method: "POST",
		path: tokenPath,
		token: jwt,
		body: { permissions: { administration: "read" } },
	}), "Full-installation token mint");
	await withRevokedToken(request, fullMint.token, async (token) => {
		validateTokenResponse(fullMint.data, "Full-installation token mint", now, false);
		const repositories = [];
		let expectedTotal = null;
		for (let page = 1; page <= 500; page += 1) {
			const data = requireStatus(await request({
				method: "GET",
				path: `/installation/repositories?per_page=100&page=${page}`,
				token,
			}), 200, "Installation repository pagination");
			if (!Number.isSafeInteger(data?.total_count) || data.total_count < 0 || !Array.isArray(data.repositories)) {
				throw new Error("Installation repository page is malformed.");
			}
			expectedTotal ??= data.total_count;
			if (data.total_count !== expectedTotal || data.repositories.length > 100) {
				throw new Error("Installation repository pagination changed or exceeded its page bound.");
			}
			repositories.push(...data.repositories);
			if (repositories.length >= expectedTotal) break;
			if (data.repositories.length === 0) throw new Error("Installation repository pagination stopped before total_count.");
		}
		if (expectedTotal !== 1 || repositories.length !== 1 || !exactRepository(repositories[0])) {
			throw new Error("App installation repository selection is not the exact pylon-code/prime-agent singleton.");
		}
	});

	const runtimeMint = mintedToken(await request({
		method: "POST",
		path: tokenPath,
		token: jwt,
		body: { repositories: [PYLON_REPOSITORY_NAME], permissions: { administration: "read" } },
	}), "Runtime token mint");
	await withRevokedToken(request, runtimeMint.token, async (token) => {
		validateTokenResponse(runtimeMint.data, "Runtime token mint", now, true);
		await auditPublicationRuleset({
			requestRest: () => request({
				method: "GET",
				path: `/repos/${PYLON_REPOSITORY_NAME_WITH_OWNER}/rulesets/${PYLON_RULESET_ID}`,
				token,
			}),
			requestGraphql: (query, variables) => graphql(request, token, query, variables, "Target ruleset GraphQL audit"),
		});
		const canaryResponse = await graphql(
			request,
			token,
			PYLON_RULESET_BYPASS_CANARY_GRAPHQL_QUERY,
			canary,
			"Bypass-count canary GraphQL audit",
		);
		validateRulesetBypassCanary(canaryResponse, canary);
	});

	return {
		appId,
		appSlug,
		installationId: installation.id,
		repository: PYLON_REPOSITORY_NAME_WITH_OWNER,
		rulesetId: PYLON_RULESET_ID,
		accepted: true,
	};
}

function parseArguments(argv) {
	const values = {};
	const allowed = new Set(["app-id", "private-key-path", "app-slug", "canary-owner", "canary-repo", "canary-ruleset-id"]);
	for (let index = 0; index < argv.length; index += 2) {
		const option = argv[index];
		const value = argv[index + 1];
		if (!option?.startsWith("--") || !allowed.has(option.slice(2)) || value === undefined || value.startsWith("--")) {
			throw new Error("Usage: accept-pylon-ruleset-auditor-app --app-id ID --private-key-path PATH --app-slug SLUG --canary-owner OWNER --canary-repo REPO --canary-ruleset-id ID");
		}
		if (Object.hasOwn(values, option)) throw new Error(`Duplicate option: ${option}`);
		values[option] = value;
	}
	if (values["--canary-ruleset-id"] === undefined || !/^[1-9][0-9]*$/.test(values["--canary-ruleset-id"])) {
		throw new Error("--canary-ruleset-id must be a positive decimal ruleset id.");
	}
	return {
		appId: values["--app-id"],
		privateKeyPath: values["--private-key-path"],
		appSlug: values["--app-slug"],
		canary: {
			owner: values["--canary-owner"],
			repo: values["--canary-repo"],
			rulesetDatabaseId: Number(values["--canary-ruleset-id"]),
		},
	};
}

async function main() {
	const result = await acceptRulesetAuditorApp(parseArguments(process.argv.slice(2)));
	process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
	main().catch((error) => {
		process.stderr.write(`Ruleset-auditor App acceptance failed: ${error instanceof Error ? error.message : "unknown error"}\n`);
		process.exitCode = 1;
	});
}
