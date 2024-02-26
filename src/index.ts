import * as log from "./log.js";
import { AwsInterface } from "./aws.js";
import { KafkaInterface } from "./kafka.js";

async function main() {
	log.setLogLevel(log.LogLevel.DEBUG);

	const functionName = process.argv[process.argv.length - 1];

	const aws = new AwsInterface();
	const triggers = await aws.getKafkaTriggers(functionName);
	log.debug(JSON.stringify(triggers, null, 3));

	const creds = await aws.obtainCredentials(triggers[0].credentials[0]);
	log.debug(JSON.stringify(creds, null, 3));

	if (!creds) process.exit(1);

	const kafka = new KafkaInterface(triggers[0].brokers, creds);
	await kafka.getMetadata(triggers[0].topics);
}

void main();
