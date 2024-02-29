import swc from "@rollup/plugin-swc";
import json from "@rollup/plugin-json";

export default {
	input: "src/index.ts",
	output: {
		file: "dist/kafka-rewind.js",
		format: "esm",
	},
	plugins: [json(), swc()],
	external: [
		"commander",
		"inquirer",
		"chalk",
		"kafkajs",
		"ora",
		"@aws-sdk/credential-providers",
		"@aws-sdk/client-lambda",
		"@aws-sdk/client-secrets-manager",
	],
};
