import { describe, expectTypeOf, it } from "vitest";
import type {
	DaemonRecoverableOwnedSessionAdoptionOptions as RootAdoptionOptions,
	DaemonRecoverableOwnedSessionAdoptionProof as RootAdoptionProof,
	DaemonRecoverableOwnedSessionAdoptionResult as RootAdoptionResult,
	DaemonRecoverableOwnedSessionConfirmResult as RootConfirmResult,
	DaemonRecoverableOwnedSessionCreateResult as RootCreateResult,
	DaemonEventCursor as RootCursor,
	DaemonRecoverableOwnedSessionPrepareResult as RootPrepareResult,
} from "../src/index.js";
import type {
	DaemonRecoverableOwnedSessionAdoptionOptions as AgentBarrelAdoptionOptions,
	DaemonRecoverableOwnedSessionAdoptionProof as AgentBarrelAdoptionProof,
	DaemonRecoverableOwnedSessionAdoptionResult as AgentBarrelAdoptionResult,
	DaemonRecoverableOwnedSessionConfirmResult as AgentBarrelConfirmResult,
	DaemonRecoverableOwnedSessionCreateResult as AgentBarrelCreateResult,
	DaemonEventCursor as AgentBarrelCursor,
	DaemonRecoverableOwnedSessionPrepareResult as AgentBarrelPrepareResult,
} from "../src/modes/agent-connection/index.js";
import type {
	DaemonRecoverableOwnedSessionAdoptionOptions as ModesAdoptionOptions,
	DaemonRecoverableOwnedSessionAdoptionProof as ModesAdoptionProof,
	DaemonRecoverableOwnedSessionAdoptionResult as ModesAdoptionResult,
	DaemonRecoverableOwnedSessionConfirmResult as ModesConfirmResult,
	DaemonRecoverableOwnedSessionCreateResult as ModesCreateResult,
	DaemonEventCursor as ModesCursor,
	DaemonRecoverableOwnedSessionPrepareResult as ModesPrepareResult,
} from "../src/modes/index.js";

describe("recoverable owned-session public type exports", () => {
	it("compile through the root and public barrels", () => {
		expectTypeOf<RootCursor>().toEqualTypeOf<ModesCursor>();
		expectTypeOf<RootCursor>().toEqualTypeOf<AgentBarrelCursor>();
		expectTypeOf<RootAdoptionProof>().toEqualTypeOf<ModesAdoptionProof>();
		expectTypeOf<RootAdoptionProof>().toEqualTypeOf<AgentBarrelAdoptionProof>();
		expectTypeOf<RootAdoptionOptions>().toEqualTypeOf<ModesAdoptionOptions>();
		expectTypeOf<RootAdoptionOptions>().toEqualTypeOf<AgentBarrelAdoptionOptions>();
		expectTypeOf<RootAdoptionResult>().toEqualTypeOf<ModesAdoptionResult>();
		expectTypeOf<RootAdoptionResult>().toEqualTypeOf<AgentBarrelAdoptionResult>();
		expectTypeOf<RootCreateResult>().toEqualTypeOf<ModesCreateResult>();
		expectTypeOf<RootCreateResult>().toEqualTypeOf<AgentBarrelCreateResult>();
		expectTypeOf<RootPrepareResult>().toEqualTypeOf<ModesPrepareResult>();
		expectTypeOf<RootPrepareResult>().toEqualTypeOf<AgentBarrelPrepareResult>();
		expectTypeOf<RootConfirmResult>().toEqualTypeOf<ModesConfirmResult>();
		expectTypeOf<RootConfirmResult>().toEqualTypeOf<AgentBarrelConfirmResult>();
	});
});
