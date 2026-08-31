/**
 * Client-artifact features exposed by this SDK build.
 *
 * These tokens describe local SDK behavior. They are not daemon capabilities,
 * protocol versions, schema revisions, or proof about a remote peer.
 */
export const CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE = "caller_owned_session_environment_cleanup_v1" as const;

export const PRIME_AGENT_SDK_FEATURES = Object.freeze([
	"bounded_daemon_ingress_v1",
	"negotiated_daemon_session_capabilities_v1",
	CALLER_OWNED_SESSION_ENVIRONMENT_CLEANUP_FEATURE,
] as const);

export type PrimeAgentSdkFeature = (typeof PRIME_AGENT_SDK_FEATURES)[number];
