import * as log from "./log.js";
import { AwsConnection } from "./aws.js";
import { program } from "commander";
import { KafkaConnection } from "./kafka.js";

program
	.name("kafka-rewind")
	.version("1.0.0")
	.option("-v, --verbose", "Print debug output")
	.option("-P, --profile <NAME>", "specify the AWS profile to use")
	.option("-t, --topic <NAME>", "specify the topic name to rewind")
	.argument("<FUNCTION_NAME>", "the name of the function to rewind")
	.argument("<DATE>", "the date to rewind to");

async function main(): Promise<number> {
	log.setLogLevel(log.LogLevel.DEBUG);

	const functionName = process.argv[process.argv.length - 1];

	const aws = await AwsConnection.connect();
	if (!aws) return 1;

	const triggers = await aws.getKafkaTriggers(functionName);
	if (!triggers) return 1;
	log.debug(JSON.stringify(triggers, null, 3));

	const creds = await aws.obtainCredentials(triggers[0].credentials[0]);
	if (!creds) return 1;
	log.debug(JSON.stringify(creds, null, 3));

	const result = await aws.setKafkaTriggerEnabled(triggers[0].uuid, false);
	if (!result) return 1;

	const kafka = await KafkaConnection.connect(triggers[0].brokers, creds);
	await kafka.setOffset(triggers[0].groupId, triggers[0].topics[0], Date.parse("2024-02-26"));
	await kafka.disconnect();

	await aws.setKafkaTriggerEnabled(triggers[0].uuid, true);

	await aws.disconnect();
	return 0;
}

void main().then((res) => process.exit(res));
