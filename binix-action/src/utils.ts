import { exec } from "@actions/exec";
import { readFile } from "node:fs/promises";

const STORE_PATHS_FILE = "/tmp/binix-store-paths.json";

export const saveStorePaths = async () => {
	await exec("nix", ["path-info", "--all", "--json"], {
		outStream: require("fs").createWriteStream(STORE_PATHS_FILE),
	});
};

export const getStorePaths = async (): Promise<string[]> => {
	try {
		const content = await readFile(STORE_PATHS_FILE, "utf-8");
		const parsed = JSON.parse(content);

		// Handle both Nix 2.18+ format (array of objects) and older format (object)
		if (Array.isArray(parsed)) {
			return parsed.map((entry: { path: string }) => entry.path);
		} else {
			return Object.keys(parsed);
		}
	} catch {
		return [];
	}
};
