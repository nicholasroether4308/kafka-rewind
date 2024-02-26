import { Admin, Kafka, LogEntry, SASLOptions, logLevel } from "kafkajs";
import { KafkaAuthMethod, KafkaCredentials } from "./common.js";
import * as log from "./log.js";

function kafkaLogger(entry: LogEntry) {
	const message = `KafkaJS: [${entry.namespace}] ${entry.log.message}`;
	switch (entry.level) {
		case logLevel.DEBUG:
			log.debug(message);
			break;
		case logLevel.WARN:
			log.warn(message);
			break;
		case logLevel.ERROR:
			log.error(message);
			break;
	}
}

function getMechanism(method: KafkaAuthMethod): "plain" | "scram-sha-256" | "scram-sha-512" {
	switch (method) {
		case KafkaAuthMethod.BASIC:
			return "plain";
		case KafkaAuthMethod.SCRAM_256:
			return "scram-sha-256";
		case KafkaAuthMethod.SCRAM_512:
			return "scram-sha-512";
	}
}

export class KafkaInterface {
	private readonly kafka: Admin;

	constructor(brokers: string[], credentials: KafkaCredentials) {
		this.kafka = new Kafka({
			clientId: "kafka-rewind",
			brokers,
			sasl: <SASLOptions>{
				mechanism: getMechanism(credentials.method),
				username: credentials.username,
				password: credentials.password,
			},
			logLevel: logLevel.DEBUG,
			logCreator: () => kafkaLogger,
		}).admin();
	}

	public async getMetadata(topics: string[]) {
		const result = await this.kafka.fetchTopicMetadata({ topics });
		log.debug(JSON.stringify(result, null, 3));
	}
}
