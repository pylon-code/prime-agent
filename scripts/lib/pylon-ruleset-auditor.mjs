export const PYLON_RULESET_ID = 21_950_766;
export const PYLON_RULESET_NODE_ID = "RRS_lACqUmVwb3NpdG9yec5QaCQtzgFO8S4";
export const PYLON_REPOSITORY_ID = 1_349_002_285;
export const PYLON_REPOSITORY_NODE_ID = "R_kgDOUGgkLQ";
export const PYLON_REPOSITORY_OWNER = "pylon-code";
export const PYLON_REPOSITORY_NAME = "prime-agent";
export const PYLON_REPOSITORY_NAME_WITH_OWNER = `${PYLON_REPOSITORY_OWNER}/${PYLON_REPOSITORY_NAME}`;
export const PYLON_RULESET_NAME = "Pylon immutable publication tags";
export const PYLON_RULESET_REF_INCLUDES = ["refs/tags/pylon-build-*", "refs/tags/pylon-stable-*"];

export const PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY = `query PylonPublicationRulesetAudit($owner: String!, $repo: String!, $rulesetDatabaseId: Int!) {
  repository(owner: $owner, name: $repo) {
    id
    databaseId
    nameWithOwner
    ruleset(databaseId: $rulesetDatabaseId, includeParents: false) {
      id
      databaseId
      name
      enforcement
      target
      source {
        __typename
        ... on Repository {
          id
          databaseId
          nameWithOwner
        }
      }
      bypassActors { totalCount }
      conditions {
        refName { include exclude }
        organizationProperty { __typename }
        repositoryId { __typename }
        repositoryName { __typename }
        repositoryProperty { __typename }
      }
      rules(first: 100) {
        totalCount
        nodes {
          type
          parameters {
            __typename
            ... on UpdateParameters { updateAllowsFetchAndMerge }
          }
        }
      }
    }
  }
}`;

export const PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES = {
	owner: PYLON_REPOSITORY_OWNER,
	repo: PYLON_REPOSITORY_NAME,
	rulesetDatabaseId: PYLON_RULESET_ID,
};

function exactKeys(value, keys) {
	return value !== null && typeof value === "object" && !Array.isArray(value) &&
		JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort());
}

function exactSortedStrings(value, expected) {
	return Array.isArray(value) && value.every((entry) => typeof entry === "string") &&
		JSON.stringify([...value].sort()) === JSON.stringify([...expected].sort());
}

function invalidRest() {
	throw new Error("REST publication ruleset response differs from the exact target contract.");
}

function invalidGraphql() {
	throw new Error("GraphQL publication ruleset response is null, partial, redacted, or differs from the exact target contract.");
}

export function validatePublicationRulesetRestResponse(response) {
	const ruleset = response?.data;
	const conditions = ruleset?.conditions;
	const refName = conditions?.ref_name;
	const rules = ruleset?.rules;
	const updateRules = Array.isArray(rules) ? rules.filter((rule) => rule?.type === "update") : [];
	const deletionRules = Array.isArray(rules) ? rules.filter((rule) => rule?.type === "deletion") : [];
	if (
		response?.status !== 200 || !ruleset || ruleset.id !== PYLON_RULESET_ID || ruleset.node_id !== PYLON_RULESET_NODE_ID ||
		ruleset.name !== PYLON_RULESET_NAME || ruleset.source_type !== "Repository" ||
		ruleset.source !== PYLON_REPOSITORY_NAME_WITH_OWNER || ruleset.target !== "tag" || ruleset.enforcement !== "active" ||
		Object.hasOwn(ruleset, "bypass_actors") && (!Array.isArray(ruleset.bypass_actors) || ruleset.bypass_actors.length !== 0) ||
		Object.hasOwn(ruleset, "current_user_can_bypass") && ruleset.current_user_can_bypass !== "never" ||
		!exactKeys(conditions, ["ref_name"]) || !exactKeys(refName, ["exclude", "include"]) ||
		!exactSortedStrings(refName.exclude, []) || !exactSortedStrings(refName.include, PYLON_RULESET_REF_INCLUDES) ||
		!Array.isArray(rules) || rules.length !== 2 || updateRules.length !== 1 || deletionRules.length !== 1 ||
		!exactKeys(updateRules[0], ["parameters", "type"]) ||
		!exactKeys(updateRules[0]?.parameters, ["update_allows_fetch_and_merge"]) ||
		updateRules[0]?.parameters?.update_allows_fetch_and_merge !== false ||
		!exactKeys(deletionRules[0], ["type"])
	) invalidRest();
	return ruleset;
}

export function validatePublicationRulesetGraphqlResponse(response, restRuleset) {
	if (Object.hasOwn(response ?? {}, "errors")) invalidGraphql();
	const repository = response?.repository;
	const ruleset = repository?.ruleset;
	const source = ruleset?.source;
	const bypassActors = ruleset?.bypassActors;
	const conditions = ruleset?.conditions;
	const refName = conditions?.refName;
	const rules = ruleset?.rules;
	const nodes = rules?.nodes;
	const updateRules = Array.isArray(nodes) ? nodes.filter((rule) => rule?.type === "UPDATE") : [];
	const deletionRules = Array.isArray(nodes) ? nodes.filter((rule) => rule?.type === "DELETION") : [];
	if (
		!restRuleset || !repository || repository.id !== PYLON_REPOSITORY_NODE_ID || repository.databaseId !== PYLON_REPOSITORY_ID ||
		repository.nameWithOwner !== PYLON_REPOSITORY_NAME_WITH_OWNER || !ruleset || ruleset.id !== restRuleset.node_id ||
		ruleset.id !== PYLON_RULESET_NODE_ID || ruleset.databaseId !== PYLON_RULESET_ID || ruleset.name !== PYLON_RULESET_NAME ||
		ruleset.enforcement !== "ACTIVE" || ruleset.target !== "TAG" || !exactKeys(source, ["__typename", "databaseId", "id", "nameWithOwner"]) ||
		source.__typename !== "Repository" || source.id !== repository.id || source.databaseId !== repository.databaseId ||
		source.nameWithOwner !== repository.nameWithOwner || !exactKeys(bypassActors, ["totalCount"]) ||
		!Number.isInteger(bypassActors.totalCount) || bypassActors.totalCount !== 0 ||
		!exactKeys(conditions, ["organizationProperty", "refName", "repositoryId", "repositoryName", "repositoryProperty"]) ||
		conditions.organizationProperty !== null || conditions.repositoryId !== null || conditions.repositoryName !== null ||
		conditions.repositoryProperty !== null || !exactKeys(refName, ["exclude", "include"]) ||
		!exactSortedStrings(refName.exclude, []) || !exactSortedStrings(refName.include, PYLON_RULESET_REF_INCLUDES) ||
		!exactKeys(rules, ["nodes", "totalCount"]) || !Number.isInteger(rules.totalCount) || rules.totalCount !== 2 ||
		!Array.isArray(nodes) || nodes.length !== 2 || nodes.some((node) => node === null) ||
		updateRules.length !== 1 || deletionRules.length !== 1 || !exactKeys(updateRules[0], ["parameters", "type"]) ||
		!exactKeys(updateRules[0].parameters, ["__typename", "updateAllowsFetchAndMerge"]) ||
		updateRules[0].parameters.__typename !== "UpdateParameters" || updateRules[0].parameters.updateAllowsFetchAndMerge !== false ||
		!exactKeys(deletionRules[0], ["parameters", "type"]) || deletionRules[0].parameters !== null
	) invalidGraphql();
	return ruleset;
}

export async function auditPublicationRuleset({ requestRest, requestGraphql }) {
	if (typeof requestRest !== "function" || typeof requestGraphql !== "function") {
		throw new Error("Publication ruleset audit needs exact REST and GraphQL request functions.");
	}
	const restRuleset = validatePublicationRulesetRestResponse(await requestRest());
	const graphqlResponse = await requestGraphql(
		PYLON_PUBLICATION_RULESET_GRAPHQL_QUERY,
		PYLON_PUBLICATION_RULESET_GRAPHQL_VARIABLES,
	);
	return validatePublicationRulesetGraphqlResponse(graphqlResponse, restRuleset);
}

export const PYLON_RULESET_BYPASS_CANARY_GRAPHQL_QUERY = `query PylonRulesetBypassCanary($owner: String!, $repo: String!, $rulesetDatabaseId: Int!) {
  repository(owner: $owner, name: $repo) {
    nameWithOwner
    ruleset(databaseId: $rulesetDatabaseId, includeParents: false) {
      databaseId
      bypassActors { totalCount }
    }
  }
}`;

export function validateRulesetBypassCanary(response, expected) {
	if (Object.hasOwn(response ?? {}, "errors")) {
		throw new Error("GraphQL bypass-count canary returned errors.");
	}
	const repository = response?.repository;
	const ruleset = repository?.ruleset;
	const totalCount = ruleset?.bypassActors?.totalCount;
	if (
		repository?.nameWithOwner !== `${expected.owner}/${expected.repo}` || ruleset?.databaseId !== expected.rulesetDatabaseId ||
		!Number.isInteger(totalCount) || totalCount <= 0
	) throw new Error("GraphQL bypass-count canary is null, redacted, zero, or differs from its configured identity.");
	return totalCount;
}
