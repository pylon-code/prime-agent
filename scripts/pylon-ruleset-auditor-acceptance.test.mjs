import assert from "node:assert/strict";
import { generateKeyPairSync, verify } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

import {
	acceptRulesetAuditorApp,
	createAppJwt,
} from "./accept-pylon-ruleset-auditor-app.mjs";
import {
	PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY,
	PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES,
	PYLON_RULESET_BYPASS_CANARY_GRAPHQL_QUERY,
} from "./lib/pylon-ruleset-auditor.mjs";

const fixture = mkdtempSync(join(tmpdir(), "pylon-ruleset-auditor-acceptance-"));
const keyPath = join(fixture, "app.pem");
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
writeFileSync(keyPath, privateKey.export({ type: "pkcs8", format: "pem" }), { mode: 0o600 });
after(() => rmSync(fixture, { recursive: true, force: true }));

const appId = "123456";
const appSlug = "pylon-publication-ruleset-auditor";
const canary = { owner: "public-canary", repo: "ruleset-canary", rulesetDatabaseId: 98_765 };
const repository = {
	id: 1_349_002_285,
	name: "prime-agent",
	full_name: "pylon-code/prime-agent",
	owner: { login: "pylon-code" },
};

function restRuleset() {
	return {
		id: 21_950_766,
		node_id: "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4",
		name: "Pylon immutable publication tags",
		target: "tag",
		source_type: "Repository",
		source: "pylon-code/prime-agent",
		enforcement: "active",
		conditions: {
			ref_name: {
				exclude: [],
				include: ["refs/tags/pylon-build-*", "refs/tags/pylon-stable-*"],
			},
		},
		rules: [
			{ type: "update", parameters: { update_allows_fetch_and_merge: false } },
			{ type: "deletion" },
		],
	};
}

function targetGraphql() {
	return {
		repository: {
			id: "R_kgDOUGgkLQ",
			databaseId: 1_349_002_285,
			nameWithOwner: "pylon-code/prime-agent",
			ruleset: {
				id: "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4",
				databaseId: 21_950_766,
				name: "Pylon immutable publication tags",
				enforcement: "ACTIVE",
				target: "TAG",
				source: {
					__typename: "Repository",
					id: "R_kgDOUGgkLQ",
					databaseId: 1_349_002_285,
					nameWithOwner: "pylon-code/prime-agent",
				},
				bypassActors: { totalCount: 0 },
				conditions: {
					refName: { include: ["refs/tags/pylon-build-*", "refs/tags/pylon-stable-*"], exclude: [] },
					organizationProperty: null,
					repositoryId: null,
					repositoryName: null,
					repositoryProperty: null,
				},
				rules: {
					totalCount: 2,
					nodes: [
						{ type: "UPDATE", parameters: { __typename: "UpdateParameters", updateAllowsFetchAndMerge: false } },
						{ type: "DELETION", parameters: null },
					],
				},
			},
		},
	};
}

function exactApp() {
	return { id: Number(appId), slug: appSlug, permissions: { administration: "read", metadata: "read" } };
}

function exactInstallation() {
	return {
		id: 654321,
		app_id: Number(appId),
		app_slug: appSlug,
		account: { login: "pylon-code", id: 314_006_107, type: "Organization" },
		target_id: 314_006_107,
		target_type: "Organization",
		repository_selection: "selected",
		suspended_at: null,
		suspended_by: null,
		permissions: { administration: "read", metadata: "read" },
	};
}

function tokenResponse(token) {
	return {
		token,
		expires_at: new Date(1_800_000_000_000 + 60 * 60 * 1000).toISOString(),
		permissions: { administration: "read" },
		repository_selection: "selected",
		repositories: [structuredClone(repository)],
	};
}

function mockAcceptance(overrides = {}) {
	const calls = [];
	let mint = 0;
	const request = async (call) => {
		calls.push(structuredClone(call));
		const { method, path, token, body } = call;
		if (method === "GET" && path === "/app") {
			assert.match(token, /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
			return { status: 200, data: structuredClone(overrides.app ?? exactApp()) };
		}
		if (method === "GET" && path === "/orgs/pylon-code/installation") {
			assert.match(token, /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
			return { status: 200, data: structuredClone(overrides.installation ?? exactInstallation()) };
		}
		if (method === "POST" && path === "/app/installations/654321/access_tokens") {
			assert.match(token, /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/);
			mint += 1;
			if (mint === 1) {
				assert.deepEqual(body, { permissions: { administration: "read" } });
				return { status: 201, data: structuredClone(overrides.fullToken ?? tokenResponse("full-token")) };
			}
			assert.deepEqual(body, { repositories: ["prime-agent"], permissions: { administration: "read" } });
			return { status: 201, data: structuredClone(overrides.runtimeToken ?? tokenResponse("runtime-token")) };
		}
		if (method === "GET" && path === "/installation/repositories?per_page=100&page=1") {
			assert.equal(token, "full-token");
			return {
				status: 200,
				data: structuredClone(overrides.repositoryPage ?? { total_count: 1, repositories: [repository] }),
			};
		}
		if (method === "DELETE" && path === "/installation/token") {
			assert.ok(["full-token", "runtime-token"].includes(token));
			const status = overrides.revokeStatus?.[token] ?? 204;
			return { status, data: null };
		}
		if (method === "GET" && path === "/repos/pylon-code/prime-agent/rulesets/21950766") {
			assert.equal(token, "runtime-token");
			return { status: 200, data: structuredClone(overrides.rest ?? restRuleset()) };
		}
		if (method === "POST" && path === "/graphql") {
			assert.equal(token, "runtime-token");
			if (body.query === PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY) {
				assert.deepEqual(body.variables, PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES);
				return { status: 200, data: structuredClone(overrides.targetEnvelope ?? { data: targetGraphql() }) };
			}
			assert.equal(body.query, PYLON_RULESET_BYPASS_CANARY_GRAPHQL_QUERY);
			assert.deepEqual(body.variables, canary);
			return {
				status: 200,
				data: structuredClone(overrides.canaryEnvelope ?? {
					data: {
						repository: {
							nameWithOwner: `${canary.owner}/${canary.repo}`,
							ruleset: { databaseId: canary.rulesetDatabaseId, bypassActors: { totalCount: 3 } },
						},
					},
				}),
			};
		}
		throw new Error(`Unexpected mocked request: ${method} ${path}`);
	};
	return { request, calls };
}

async function acceptWithMock(mock) {
	return acceptRulesetAuditorApp({ appId, privateKeyPath: keyPath, appSlug, canary, request: mock.request, now: 1_800_000_000_000 });
}

function revocations(mock) {
	return mock.calls.filter((call) => call.method === "DELETE").map((call) => call.token);
}

test("locally signs one bounded GitHub App JWT without exposing private-key bytes", () => {
	const now = 1_800_000_000_000;
	const jwt = createAppJwt({ appId, privateKeyPath: keyPath, now });
	const [header, payload, signature] = jwt.split(".");
	assert.deepEqual(JSON.parse(Buffer.from(header, "base64url")), { alg: "RS256", typ: "JWT" });
	assert.deepEqual(JSON.parse(Buffer.from(payload, "base64url")), {
		iat: Math.floor(now / 1000) - 60,
		exp: Math.floor(now / 1000) + 540,
		iss: appId,
	});
	assert.equal(verify("RSA-SHA256", Buffer.from(`${header}.${payload}`), publicKey, Buffer.from(signature, "base64url")), true);
	assert.ok(jwt.length < 4096);
});

test("live acceptance mocks every read-only endpoint, exact scope, canary, and revocation", async () => {
	const mock = mockAcceptance();
	assert.deepEqual(await acceptWithMock(mock), {
		appId,
		appSlug,
		installationId: 654321,
		repository: "pylon-code/prime-agent",
		rulesetId: 21_950_766,
		accepted: true,
	});
	assert.deepEqual(mock.calls.map(({ method, path }) => `${method} ${path}`), [
		"GET /app",
		"GET /orgs/pylon-code/installation",
		"POST /app/installations/654321/access_tokens",
		"GET /installation/repositories?per_page=100&page=1",
		"DELETE /installation/token",
		"POST /app/installations/654321/access_tokens",
		"GET /repos/pylon-code/prime-agent/rulesets/21950766",
		"POST /graphql",
		"POST /graphql",
		"DELETE /installation/token",
	]);
	assert.deepEqual(revocations(mock), ["full-token", "runtime-token"]);
});

test("live acceptance rejects App and installation identity, permission, selection, and suspension drift", async () => {
	const cases = [
		{ app: { ...exactApp(), id: 1 } },
		{ app: { ...exactApp(), slug: "other-app" } },
		{ app: { ...exactApp(), permissions: { administration: "write", metadata: "read" } } },
		{ installation: { ...exactInstallation(), repository_selection: "all" } },
		{ installation: { ...exactInstallation(), suspended_at: "2030-01-01T00:00:00Z" } },
		{ installation: { ...exactInstallation(), permissions: { metadata: "read" } } },
	];
	for (const drift of cases) {
		const mock = mockAcceptance(drift);
		await assert.rejects(() => acceptWithMock(mock));
		assert.deepEqual(revocations(mock), []);
	}
});

test("live acceptance rejects token scope and exact repository-selection drift and still revokes minted tokens", async () => {
	for (const fullToken of [
		{ ...tokenResponse("full-token"), expires_at: "2030-01-01T00:00:00Z" },
		{ ...tokenResponse("full-token"), permissions: { administration: "write" } },
		{ ...tokenResponse("full-token"), repository_selection: "all" },
		{ ...tokenResponse("full-token"), repositories: [{ ...repository, id: 1 }] },
	]) {
		const mock = mockAcceptance({ fullToken });
		await assert.rejects(() => acceptWithMock(mock));
		assert.deepEqual(revocations(mock), ["full-token"]);
	}
	const mockPage = mockAcceptance({ repositoryPage: { total_count: 2, repositories: [repository, { ...repository, id: 1 }] } });
	await assert.rejects(() => acceptWithMock(mockPage));
	assert.deepEqual(revocations(mockPage), ["full-token"]);
	for (const runtimeToken of [
		{ ...tokenResponse("runtime-token"), permissions: { administration: "read", contents: "read" } },
		{ ...tokenResponse("runtime-token"), repositories: [{ ...repository, full_name: "pylon-code/other" }] },
	]) {
		const mock = mockAcceptance({ runtimeToken });
		await assert.rejects(() => acceptWithMock(mock));
		assert.deepEqual(revocations(mock), ["full-token", "runtime-token"]);
	}
});

test("live acceptance rejects target GraphQL null, partial, errors, redaction, and nonzero bypass while revoking", async () => {
	const envelopes = [
		{ data: null },
		{ data: { repository: null } },
		{ errors: [{ message: "Resource not accessible by integration" }], data: targetGraphql() },
		{ data: { ...targetGraphql(), repository: { ...targetGraphql().repository, ruleset: null } } },
		(() => {
			const value = targetGraphql();
			value.repository.ruleset.bypassActors = null;
			return { data: value };
		})(),
		(() => {
			const value = targetGraphql();
			value.repository.ruleset.bypassActors.totalCount = 1;
			return { data: value };
		})(),
	];
	for (const targetEnvelope of envelopes) {
		const mock = mockAcceptance({ targetEnvelope });
		await assert.rejects(() => acceptWithMock(mock));
		assert.deepEqual(revocations(mock), ["full-token", "runtime-token"]);
	}
});

test("live acceptance rejects a null, errored, redacted, or zero bypass-count canary and revocation failure", async () => {
	for (const canaryEnvelope of [
		{ data: null },
		{ errors: [{ message: "redacted" }] },
		{ data: { repository: null } },
		{ data: { repository: { nameWithOwner: `${canary.owner}/${canary.repo}`, ruleset: null } } },
		{
			data: {
				repository: {
					nameWithOwner: `${canary.owner}/${canary.repo}`,
					ruleset: { databaseId: canary.rulesetDatabaseId, bypassActors: { totalCount: 0 } },
				},
			},
		},
	]) {
		const mock = mockAcceptance({ canaryEnvelope });
		await assert.rejects(() => acceptWithMock(mock));
		assert.deepEqual(revocations(mock), ["full-token", "runtime-token"]);
	}
	const revokeFailure = mockAcceptance({ revokeStatus: { "runtime-token": 500 } });
	await assert.rejects(() => acceptWithMock(revokeFailure), /revocation/);
	assert.deepEqual(revocations(revokeFailure), ["full-token", "runtime-token"]);
});
