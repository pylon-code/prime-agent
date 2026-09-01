#!/usr/bin/env node
// The Node 22+ module graph fails at link time on older Node, so it must load
// behind the dynamic import, after the dependency-free guard runs.
import { assertNodeVersion } from "./cli/node-version-check.js";

const supported = assertNodeVersion({
	version: process.versions.node,
	log: console.error,
	exit: (code) => process.exit(code),
});

if (supported) {
	const workerProtocol = await import("./modes/daemon/daemon-worker-protocol.js");
	const authenticatedWorkerLaunch =
		workerProtocol.isDaemonWorkerProcess() &&
		process.env[workerProtocol.DAEMON_WORKER_TOKEN_ENV] !== undefined &&
		process.env[workerProtocol.DAEMON_WORKER_STARTUP_GATE_FD_ENV] !== undefined;
	let daemonWorkerBootstrap: ReturnType<typeof workerProtocol.readDaemonWorkerBootstrapEnvironment> | undefined;
	try {
		if (authenticatedWorkerLaunch) {
			workerProtocol.waitForDaemonWorkerStartupGate();
			daemonWorkerBootstrap = workerProtocol.readDaemonWorkerBootstrapEnvironment();
		}
	} finally {
		workerProtocol.sanitizeDaemonWorkerBootstrapEnvironment(process.env);
	}
	const { runCli } = await import("./cli-main.js");
	await runCli(daemonWorkerBootstrap);
}
