import * as core from "@actions/core";
import { exec } from "@actions/exec";
import { saveStorePaths, getStorePaths } from "../utils";

export const push = async () => {
	core.startGroup("Push to Binix");

	try {
		const skipPush = core.getInput("skip-push");

		if (skipPush === "true") {
			core.info("Pushing to cache is disabled by skip-push");
		} else {
			const cache = core.getInput("cache", { required: true });
			const jobs = core.getInput("jobs") || "5";
			const chunkConcurrency = core.getInput("chunk-concurrency") || "4";

			core.info("Pushing to Binix cache");

			const oldPaths = await getStorePaths();
			await saveStorePaths();
			const newPaths = await getStorePaths();

			let pushPaths = newPaths
				.filter((p) => !oldPaths.includes(p))
				.filter(
					(p) =>
						!p.endsWith(".drv") &&
						!p.endsWith(".drv.chroot") &&
						!p.endsWith(".check") &&
						!p.endsWith(".lock")
				);

			const includePathsInput = core.getMultilineInput("include-paths");
			if (includePathsInput.length > 0) {
				const includePatterns = includePathsInput.map((v) => new RegExp(v));
				pushPaths = pushPaths.filter((p) =>
					includePatterns.some((pattern) => pattern.test(p))
				);
			}

			const excludePathsInput = core.getMultilineInput("exclude-paths");
			if (excludePathsInput.length > 0) {
				const excludePatterns = excludePathsInput.map((v) => new RegExp(v));
				pushPaths = pushPaths.filter(
					(p) => !excludePatterns.some((pattern) => pattern.test(p))
				);
			}

			if (pushPaths.length === 0) {
				core.info("No new store paths to push");
			} else {
				core.info(`Pushing ${pushPaths.length} store paths`);

				await exec(
					"binix",
					[
						"push",
						"--stdin",
						"-j",
						jobs,
						"--chunk-concurrency",
						chunkConcurrency,
						cache,
					],
					{
						input: Buffer.from(pushPaths.join("\n")),
					}
				);
			}
		}
	} catch (e) {
		core.warning(`Action encountered error: ${e}`);
		core.info("Not considering errors during push a failure.");
	}

	core.endGroup();
};
