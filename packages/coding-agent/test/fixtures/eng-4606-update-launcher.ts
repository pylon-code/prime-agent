import { writeFileSync } from "node:fs";
import { launchDaemonUpdateRestartCoordinator } from "../../src/cli/daemon-update-restart.js";

function requireEnvironment(name: string): string {
	const value = process.env[name];
	if (!value) {
		throw new Error(`Missing ${name}`);
	}
	return value;
}

const agentDir = requireEnvironment("ENG_4606_AGENT_DIR");
const cliPath = requireEnvironment("ENG_4606_CLI_PATH");
const completionPath = requireEnvironment("ENG_4606_COMPLETION_PATH");
const pidPath = requireEnvironment("ENG_4606_PID_PATH");
const socketPath = requireEnvironment("ENG_4606_SOCKET_PATH");
const tsxPath = requireEnvironment("ENG_4606_TSX_PATH");
if (process.env.PRIME_AGENT_INTERNAL_DAEMON_WORKER_ACTIVE_SESSION_ID !== undefined) {
	throw new Error("Private worker bootstrap environment leaked into the updater");
}
const originActiveSessionId = process.argv[2];
if (!originActiveSessionId) throw new Error("Missing updater origin active session id argument");

process.argv[1] = cliPath;
process.execArgv.splice(0, process.execArgv.length, tsxPath);
writeFileSync(pidPath, `${process.pid}\n`);

await launchDaemonUpdateRestartCoordinator({
	socketPath,
	agentDir,
	cwd: agentDir,
	originActiveSessionId,
});

writeFileSync(completionPath, "launcher survived\n");
