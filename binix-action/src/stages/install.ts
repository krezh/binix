import * as core from "@actions/core";
import { exec } from "@actions/exec";
import { findInPath } from "@actions/io";

export const install = async () => {
	core.startGroup("Install Binix");

	core.info("Installing Binix");

	const inputsFrom = core.getInput("inputs-from");

	try {
		if (inputsFrom) {
			await exec("nix", ["profile", "install", "--inputs-from", inputsFrom, "binix#binix-client"]);
		} else {
			await exec("nix", ["profile", "install", "github:krezh/binix#binix-client"]);
		}
	} catch (e) {
		core.setFailed(`Action failed with error: ${e}`);
	}

	core.endGroup();
};

export const isInstalled = async () => {
	return (await findInPath("binix")).length > 0;
};
