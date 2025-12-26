import * as core from "@actions/core";
import { exec } from "@actions/exec";
import { which } from "@actions/io";

export const install = async () => {
	core.startGroup("Install Binix");

	try {
		const flakeRef = core.getInput("flake-ref") || "github:krezh/binix";

		core.info(`Installing binix from ${flakeRef}`);
		await exec("nix", [
			"profile",
			"install",
			`${flakeRef}#binix-client`,
			"--extra-experimental-features",
			"nix-command flakes",
		]);
	} catch (e) {
		core.setFailed(`Failed to install binix: ${e}`);
	}

	core.endGroup();
};

export const isInstalled = async (): Promise<boolean> => {
	const binixPath = await which("binix", false);
	return binixPath.length > 0;
};
