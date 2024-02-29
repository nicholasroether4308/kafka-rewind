#!/usr/bin/env node

import * as log from "./log.ts";
import { AwsConnection } from "./aws.ts";
import { program } from "commander";
import { KafkaConnection } from "./kafka.ts";
import inquirer from "inquirer";
import { KafkaCredentials, KafkaTrigger } from "./common.ts";

const VERSION = "1.0.0";

program
	.name("kafka-rewind")
	.version(VERSION)
	.option("-v, --verbose", "Print debug output")
	.option("-P, --profile <NAME>", "specify the AWS profile to use")
	.option(
		"-t, --topic <NAMES>",
		"specify the topic name(s) to rewind. Can be a comma-separated list.",
	)
	.argument("<FUNCTION_NAME>", "the name of the function to rewind")
	.argument("<DATE>", "the date to rewind to");

interface Parameters {
	verbose: boolean;
	profile?: string;
	topics?: string[];
	functionName: string;
	date: number;
}

function getParameters(): Parameters | null {
	program.parse();
	const options = program.opts();

	const verbose: boolean = options.verbose ?? false;
	const profile: string | undefined = options.profile;
	let topics: string[] | undefined = options.topic?.split(",") ?? null;
	const functionName: string = program.args[0];
	let date: number;
	try {
		date = Date.parse(program.args[1]);
		if (isNaN(date)) throw new Error("");
	} catch (e) {
		log.error(`"${program.args[1]}" is not a valid date`, e);
		return null;
	}

	return { verbose, profile, topics, functionName, date };
}

async function selectTopics(
	paramTopics: string[] | undefined,
	availableTopics: string[],
): Promise<string[] | null> {
	if (paramTopics) {
		for (const topic of paramTopics) {
			if (!availableTopics.includes(topic)) {
				log.error(`This function does not subscribe to the topic ${topic}`);
				return null;
			}
		}
		return paramTopics;
	} else {
		if (availableTopics.length == 1) {
			log.info(`The function consumes a single topic: ${availableTopics[0]}`);
			return availableTopics;
		} else {
			return await inquirer
				.prompt({ type: "checkbox", choices: availableTopics, name: "triggers" })
				.then((res) => res.triggers);
		}
	}
}

async function setKafkaOffset(
	trigger: KafkaTrigger,
	credentials: KafkaCredentials,
	topics: string[],
	date: number,
): Promise<boolean> {
	const kafka = await KafkaConnection.connect(trigger.brokers, credentials);
	if (!kafka) return false;

	if (!(await kafka.waitForEmptyGroup(trigger.groupId))) return false;

	let result = true;
	for (const topic of topics) {
		log.info(`Applying offset for topic ${topic}`);
		result = await kafka.setOffset(trigger.groupId, topic, date);
		if (!result) break;
	}

	await kafka.disconnect();
	return result;
}

async function setTriggerOffset(
	aws: AwsConnection,
	trigger: KafkaTrigger,
	topics: string[],
	date: number,
): Promise<boolean> {
	log.info(`Setting offset for trigger ${trigger.uuid}...`);
	if (trigger.credentials.length == 0) {
		log.error(`Trigger ${trigger.uuid} has no supported credentials`);
		return false;
	}
	log.debug(`Using auth method ${trigger.credentials[0].method} for trigger ${trigger.uuid}`);

	const credentials = await aws.obtainCredentials(trigger.credentials[0]);
	if (!credentials) return false;

	if (!(await aws.setKafkaTriggerEnabled(trigger.uuid, false))) return false;

	const result = await setKafkaOffset(trigger, credentials, topics, date);

	await aws.setKafkaTriggerEnabled(trigger.uuid, true);
	return result;
}

async function main(): Promise<number> {
	const params = getParameters();
	if (!params) return 1;

	if (params.verbose) log.setLogLevel(log.LogLevel.DEBUG);

	const aws = await AwsConnection.connect(params.profile);
	if (!aws) return 1;

	const triggers = await aws.getKafkaTriggers(params.functionName);
	if (!triggers) return 1;
	if (triggers.length == 0) {
		log.info(`The function ${params.functionName} has no Kafka triggers defined.`);
		return 0;
	}

	const availableTopics = triggers.flatMap((trigger) => trigger.topics);
	const topics = await selectTopics(params.topics, availableTopics);
	if (!topics) return 1;

	const affectedTriggers = triggers.filter((trigger) =>
		trigger.topics.some((topic) => topics.includes(topic)),
	);
	if (affectedTriggers.length == 0) {
		log.info("No triggers are affected by this change");
		return 0;
	}
	const result = await inquirer.prompt({
		type: "confirm",
		message: `Setting offset ${new Date(params.date).toISOString()} for ${topics.length} topic(s) across ${affectedTriggers.length} Kafka trigger(s). OK?`,
		name: "ok",
	});
	if (!result.ok) {
		log.info("Aborting.");
		return 0;
	}

	for (const trigger of triggers) {
		if (!(await setTriggerOffset(aws, trigger, topics, params.date))) {
			return 1;
		}
	}

	await aws.disconnect();
	log.info("Success!");
	return 0;
}

void main().then((res) => process.exit(res));
