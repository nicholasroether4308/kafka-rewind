import * as log from "./log.js";
import { Aws } from "./aws.js";

async function main() {
	log.setLogLevel(log.LogLevel.DEBUG);

	const functionName = process.argv[process.argv.length - 1];

	const aws = new Aws();
	const triggers = await aws.getKafkaTriggers(functionName);
	log.debug(JSON.stringify(triggers, null, 3));

	const creds = await aws.obtainCredentials(triggers[0].credentials[0]);
	log.debug(JSON.stringify(creds, null, 3));
}

void main();
