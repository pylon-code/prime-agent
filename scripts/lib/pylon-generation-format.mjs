import { createHash } from "node:crypto";
import { resolve } from "node:path";

export const GENERATION_STATE_MAX_BYTES = 16 * 1024 * 1024;
export const GENERATION_ZERO = "0".repeat(64);
export const GENERATION_ROOT_MAX_ENTRIES = 16;
export const GENERATION_EPOCH_MAX_ENTRIES = 65_537 * 5 + 4096 + 32;
export const GENERATION_RECEIPT_MAX_ENTRIES = GENERATION_EPOCH_MAX_ENTRIES * 2 + 2;
export const GENERATION_JOURNAL_MAX_BYTES = 512 * 1024 * 1024;
const hex = /^[0-9a-f]{64}$/;
const fields = [
	"schemaVersion", "epoch", "epochId", "statePathSha256", "previousCheckpointSha256", "previousGeneration", "previousGenerationIdentity", "retirementAuthoritySha256",
	"previousTipSha256", "historySha256", "anchorDigest", "anchorBase64", "sourceKind", "sourceAuthoritySha256",
	"sourceTipDigest", "sourceTipBase64", "migrationKind", "migrationAuthoritySha256", "migrationTipDigest", "migrationTipBase64",
];
export const generationBytes = (value) => Buffer.from(`${JSON.stringify(value)}\n`);
export const generationDigest = (value) => createHash("sha256").update(value).digest("hex");
export function generationCheckpointMaxBytes(stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	if (!Number.isSafeInteger(stateMaxBytes) || stateMaxBytes < 1 || stateMaxBytes > GENERATION_STATE_MAX_BYTES) throw new Error("Generation state bound is invalid.");
	return 3 * 4 * Math.ceil(stateMaxBytes / 3) + checkpointEnvelopeBytes;
}
const checkpointEnvelopeBytes = generationBytes({
	schemaVersion: 3, epoch: Number.MAX_SAFE_INTEGER, epochId: GENERATION_ZERO, statePathSha256: GENERATION_ZERO,
	previousCheckpointSha256: GENERATION_ZERO, previousGeneration: `generation-9007199254740991-${GENERATION_ZERO}`,
	previousGenerationIdentity: { dev: Number.MAX_SAFE_INTEGER, ino: Number.MAX_SAFE_INTEGER }, retirementAuthoritySha256: GENERATION_ZERO,
	previousTipSha256: GENERATION_ZERO, historySha256: GENERATION_ZERO, anchorDigest: GENERATION_ZERO, anchorBase64: "",
	sourceKind: "v2", sourceAuthoritySha256: GENERATION_ZERO, sourceTipDigest: GENERATION_ZERO, sourceTipBase64: "",
	migrationKind: "v1", migrationAuthoritySha256: GENERATION_ZERO, migrationTipDigest: GENERATION_ZERO, migrationTipBase64: "",
}).length;
// Rotation claims wrap the checkpoint; normal terminal records contain at most two state fields.
export function generationRecordMaxBytes(stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	const rotationEnvelopeBytes = generationBytes({
		schemaVersion: 3, generation: 65_537, token: GENERATION_ZERO, type: "rotation", intent: {
			schemaVersion: 3, predecessorGeneration: `generation-9007199254740991-${GENERATION_ZERO}`,
			checkpointSha256: GENERATION_ZERO, tipSha256: GENERATION_ZERO, checkpoint: null,
		},
	}).length - 5;
	return generationCheckpointMaxBytes(stateMaxBytes) + rotationEnvelopeBytes;
}
function commitment(domain, values) {
	const hash = createHash("sha256").update(`${domain}\0`);
	for (const value of values) {
		const data = Buffer.from(value);
		const length = Buffer.alloc(8);
		length.writeBigUInt64BE(BigInt(data.length));
		hash.update(length).update(data);
	}
	return hash.digest("hex");
}
function stateFields(data, stateMaxBytes) {
	if (data === null) return { digest: GENERATION_ZERO, base64: null };
	if (!Buffer.isBuffer(data) || data.length < 1 || data.length > stateMaxBytes) throw new Error("Generation state exceeds its byte bound.");
	return { digest: generationDigest(data), base64: data.toString("base64") };
}
function decodeState(encoded, digest, stateMaxBytes) {
	if (!hex.test(digest)) throw new Error("Generation state digest is malformed.");
	if (encoded === null) {
		if (digest !== GENERATION_ZERO) throw new Error("Generation null state digest is malformed.");
		return null;
	}
	if (typeof encoded !== "string" || encoded.length < 4 || encoded.length > 4 * Math.ceil(stateMaxBytes / 3)) throw new Error("Generation state exceeds its byte bound.");
	const data = Buffer.from(encoded, "base64");
	if (data.length < 1 || data.length > stateMaxBytes || data.toString("base64") !== encoded || generationDigest(data) !== digest) throw new Error("Generation state bytes are malformed.");
	return data;
}
function identity(value) {
	const { epochId: _epochId, ...payload } = value;
	return commitment("pylon-generation-identity-v3", [generationBytes(payload)]);
}
export function consumerGenerationName(value) {
	if (!Number.isSafeInteger(value.epoch) || value.epoch < 1 || typeof value.epochId !== "string" || !hex.test(value.epochId)) throw new Error("Generation name is malformed.");
	return `generation-${String(value.epoch).padStart(16, "0")}-${value.epochId}`;
}
function provenance(value, stateMaxBytes, migration = false) {
	if (value === null) return { kind: null, authoritySha256: GENERATION_ZERO, digest: GENERATION_ZERO, base64: null };
	if (!value || Object.keys(value).sort().join() !== "authoritySha256,kind,tipBytes" ||
		!(migration ? value.kind === "v1" : ["v1", "v2"].includes(value.kind)) || typeof value.authoritySha256 !== "string" || !hex.test(value.authoritySha256) || value.authoritySha256 === GENERATION_ZERO) throw new Error("Generation provenance is malformed.");
	return { kind: value.kind, authoritySha256: value.authoritySha256, ...stateFields(value.tipBytes, stateMaxBytes) };
}
export function consumerGenerationGenesisCheckpoint({ statePath, stateBytes = null, source = null, migration = null }, stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	generationCheckpointMaxBytes(stateMaxBytes);
	if (typeof statePath !== "string" || statePath.length === 0) throw new Error("Generation state path is required.");
	const anchor = stateFields(stateBytes, stateMaxBytes);
	const src = provenance(source, stateMaxBytes);
	const old = provenance(migration, stateMaxBytes, true);
	if (old.kind !== null && src.kind !== "v2") throw new Error("Generation migration provenance requires a v2 source.");
	if (src.kind !== null && (src.digest !== anchor.digest || src.base64 !== anchor.base64)) throw new Error("Generation source must bind the exact genesis state.");
	const value = {
		schemaVersion: 3, epoch: 1, epochId: "", statePathSha256: generationDigest(Buffer.from(resolve(statePath))),
		previousCheckpointSha256: GENERATION_ZERO, previousGeneration: null, previousGenerationIdentity: null, retirementAuthoritySha256: GENERATION_ZERO, previousTipSha256: GENERATION_ZERO,
		historySha256: "", anchorDigest: anchor.digest, anchorBase64: anchor.base64,
		sourceKind: src.kind, sourceAuthoritySha256: src.authoritySha256, sourceTipDigest: src.digest, sourceTipBase64: src.base64,
		migrationKind: old.kind, migrationAuthoritySha256: old.authoritySha256, migrationTipDigest: old.digest, migrationTipBase64: old.base64,
	};
	value.historySha256 = genesisHistory(value);
	value.epochId = identity(value);
	return value;
}
function genesisHistory(value) {
	return commitment("pylon-generation-genesis-history-v3", [
		value.statePathSha256, value.anchorDigest, value.sourceKind ?? "none", value.sourceAuthoritySha256,
		value.sourceTipDigest, value.migrationKind ?? "none", value.migrationAuthoritySha256, value.migrationTipDigest,
	]);
}
export function validateGenerationCheckpoint(input, stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	generationCheckpointMaxBytes(stateMaxBytes);
	if (!input || Object.keys(input).sort().join() !== [...fields].sort().join()) throw new Error("Generation checkpoint closed format is malformed.");
	const value = Object.fromEntries(fields.map((key) => [key, input[key]]));
	if (value.schemaVersion !== 3 || !Number.isSafeInteger(value.epoch) || value.epoch < 1 ||
		![value.epochId, value.statePathSha256, value.previousCheckpointSha256, value.previousTipSha256, value.historySha256,
			value.sourceAuthoritySha256, value.migrationAuthoritySha256, value.retirementAuthoritySha256].every((v) => typeof v === "string" && hex.test(v))) throw new Error("Generation checkpoint is malformed.");
	const anchorBytes = decodeState(value.anchorBase64, value.anchorDigest, stateMaxBytes);
	decodeState(value.sourceTipBase64, value.sourceTipDigest, stateMaxBytes);
	decodeState(value.migrationTipBase64, value.migrationTipDigest, stateMaxBytes);
	for (const prefix of ["source", "migration"]) {
		if (value[`${prefix}Kind`] === null) {
			if (value[`${prefix}AuthoritySha256`] !== GENERATION_ZERO || value[`${prefix}TipBase64`] !== null) throw new Error("Generation absent provenance is malformed.");
		} else if (!(prefix === "source" ? ["v1", "v2"] : ["v1"]).includes(value[`${prefix}Kind`]) || value[`${prefix}AuthoritySha256`] === GENERATION_ZERO) throw new Error("Generation provenance is malformed.");
	}
	if (value.migrationKind !== null && value.sourceKind !== "v2") throw new Error("Generation migration provenance is malformed.");
	if (value.previousGenerationIdentity !== null && (!value.previousGenerationIdentity || Object.keys(value.previousGenerationIdentity).sort().join() !== "dev,ino" || !Object.values(value.previousGenerationIdentity).every((v) => Number.isSafeInteger(v) && v >= 0))) throw new Error("Generation predecessor inode is malformed.");
	if ((value.previousGenerationIdentity === null) !== (value.retirementAuthoritySha256 === GENERATION_ZERO)) throw new Error("Generation retirement commitment and inode must be paired.");
	if (value.epoch === 1) {
		if (value.previousGenerationIdentity !== null || value.previousCheckpointSha256 !== GENERATION_ZERO || value.previousTipSha256 !== GENERATION_ZERO || value.previousGeneration !== null || value.historySha256 !== genesisHistory(value) ||
			(value.sourceKind !== null && (value.anchorBase64 !== value.sourceTipBase64 || value.anchorDigest !== value.sourceTipDigest))) throw new Error("Generation genesis is not exact.");
	} else if (typeof value.previousGeneration !== "string" || !/^generation-[0-9]{16}-[0-9a-f]{64}$/.test(value.previousGeneration) || value.previousTipSha256 !== value.anchorDigest) throw new Error("Generation successor is malformed.");
	if (generationBytes(value).length > generationCheckpointMaxBytes(stateMaxBytes)) throw new Error("Generation checkpoint exceeds its exact envelope byte bound.");
	if (value.epochId !== identity(value)) throw new Error("Generation deterministic identity is not exact.");
	return { checkpoint: value, anchorBytes };
}
export function consumerGenerationSuccessorCheckpoint(predecessor, tip, stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	const { checkpoint } = validateGenerationCheckpoint(predecessor, stateMaxBytes);
	const anchor = stateFields(tip.tipBytes, stateMaxBytes);
	if (anchor.digest !== tip.tipDigest || checkpoint.epoch === Number.MAX_SAFE_INTEGER) throw new Error("Generation immutable tip or epoch is malformed.");
	const previousDigest = generationDigest(generationBytes(checkpoint));
	const next = { ...checkpoint, epoch: checkpoint.epoch + 1, epochId: "", previousCheckpointSha256: previousDigest,
		previousGeneration: consumerGenerationName(checkpoint), previousGenerationIdentity: tip.previousGenerationIdentity ?? null,
		retirementAuthoritySha256: tip.retirementAuthoritySha256 ?? GENERATION_ZERO, previousTipSha256: anchor.digest,
		historySha256: commitment("pylon-generation-rotation-history-v3", [checkpoint.historySha256, previousDigest, anchor.digest]),
		anchorDigest: anchor.digest, anchorBase64: anchor.base64 };
	next.epochId = identity(next);
	return next;
}
export function consumerGenerationRotationClaim(checkpoint, generation, tip, stateMaxBytes = GENERATION_STATE_MAX_BYTES) {
	if (!Number.isSafeInteger(generation) || generation < 1 || generation > 65_537) throw new Error("Generation rotation slot is malformed.");
	const successor = consumerGenerationSuccessorCheckpoint(checkpoint, tip, stateMaxBytes);
	return { schemaVersion: 3, generation, token: successor.epochId, type: "rotation", intent: {
		schemaVersion: 3, predecessorGeneration: consumerGenerationName(checkpoint), checkpointSha256: generationDigest(generationBytes(checkpoint)),
		tipSha256: tip.tipDigest, checkpoint: successor,
	} };
}
