module.exports = {
	root: true,
	extends: [
		"eslint:recommended",
		"plugin:@typescript-eslint/recommended-type-checked",
		"plugin:@typescript-eslint/stylistic-type-checked",
		"prettier",
	],
	parser: "@typescript-eslint/parser",
	parserOptions: {
		project: true,
		tsconfigRootDor: __dirname,
	},
	plugins: ["@typescript-eslint"],
};

