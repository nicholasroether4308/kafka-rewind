import * as log from "./log.js";
import { AwsConnection } from "./aws.js";
import { KafkaConnection } from "./kafka.js";

async function main() {
	log.setLogLevel(log.LogLevel.DEBUG);

	const functionName = process.argv[process.argv.length - 1];

	const aws = new AwsConnection();
	await aws.connect();

	const triggers = await aws.getKafkaTriggers(functionName);
	log.debug(JSON.stringify(triggers, null, 3));

	const creds = await aws.obtainCredentials(triggers[0].credentials[0]);
	log.debug(JSON.stringify(creds, null, 3));

	if (!creds) process.exit(1);
	await aws.disconnect();

	const kafka = new KafkaConnection(triggers[0].brokers, creds);
	await kafka.connect();
	await kafka.setOffset(triggers[0].groupId, triggers[0].topics[0], Date.parse("2023-11-25"));
	await kafka.disconnect();
}

void main();
