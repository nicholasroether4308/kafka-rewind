import swc from "@rollup/plugin-swc";

export default {
	input: "src/index.ts",
	output: {
		file: "dist/kafka-rewind.js",
		format: "esm",
	},
	plugins: [swc()],
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
