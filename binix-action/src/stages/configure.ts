import * as core from "@actions/core";
import { exec } from "@actions/exec";
import { saveStorePaths } from "../utils";

export const configure = async () => {
	core.startGroup("Configure Binix");

	try {
		const endpoint = core.getInput("endpoint", { required: true });
		const cache = core.getInput("cache", { required: true });
		const token = core.getInput("token");
		const serverName = core.getInput("server-name") || "default";
		const skipUse = core.getInput("skip-use");

		core.info("Logging in to Binix server");

		const loginArgs = ["login", "--set-default", serverName, endpoint];
		if (token) {
			loginArgs.push(token);
		}

		await exec("binix", loginArgs);

		if (skipUse === "true") {
			core.info("Not adding Binix cache to substituters as skip-use is set to true");
		} else {
			core.info("Adding Binix cache to substituters");
			await exec("binix", ["use", cache]);
		}

		core.info("Collecting store paths before build");
		await saveStorePaths();
	} catch (e) {
		core.setFailed(`Action failed with error: ${e}`);
	}

	core.endGroup();
};
