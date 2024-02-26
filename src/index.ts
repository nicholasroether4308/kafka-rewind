import { LambdaClient, ListEventSourceMappingsCommand } from "@aws-sdk/client-lambda";
import * as log from "./log.js";
import { Aws } from "./aws.js";

async function main() {
	log.setLogLevel(log.LogLevel.DEBUG);

	const functionName = process.argv[process.argv.length - 1];

	const aws = new Aws();
	const result = await aws.getKafkaTriggers(functionName);
	log.debug(JSON.stringify(result, null, 3));
}

void main();
