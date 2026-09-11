function sudoIdentity(value, name) {
	if (typeof value !== "string" || !/^[1-9][0-9]*$/.test(value)) {
		throw new Error(`Sudo release builds require a non-root ${name}.`);
	}
	const id = Number(value);
	if (!Number.isSafeInteger(id) || id >= 0xffffffff) {
		throw new Error(`Sudo release builds require a valid ${name}.`);
	}
	return id;
}

// The caller creates the isolated network namespace before starting this entrypoint.
// Restore its file ownership before any build writes or subprocesses can run.
export function restoreSudoBuildIdentity(credentials = process, environment = process.env) {
	if (credentials.getuid?.() !== 0) return;
	if (environment.SUDO_UID === undefined && environment.SUDO_GID === undefined) return;
	const uid = sudoIdentity(environment.SUDO_UID, "SUDO_UID");
	const gid = sudoIdentity(environment.SUDO_GID, "SUDO_GID");
	credentials.setgroups([]);
	credentials.setgid(gid);
	credentials.setuid(uid);
	if (
		credentials.getuid() !== uid || credentials.geteuid() !== uid ||
		credentials.getgid() !== gid || credentials.getegid() !== gid ||
		credentials.getgroups().some((group) => group !== gid)
	) {
		throw new Error("Sudo release build did not restore the invoking identity.");
	}
}
