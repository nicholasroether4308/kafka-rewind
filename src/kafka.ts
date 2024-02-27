import {
	Admin,
	Kafka,
	LogEntry,
	OffsetsByTopicPartition,
	PartitionOffset,
	SASLOptions,
	logLevel,
} from "kafkajs";
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

export class KafkaConnection {
	private readonly kafka: Admin;

	constructor(brokers: string[], credentials: KafkaCredentials) {
		log.debug("Using brokers list %j for kafka", brokers);
		const sasl: SASLOptions = {
			mechanism: getMechanism(credentials.method),
			username: credentials.username,
			password: credentials.password,
		};
		log.debug("Using credentials %j for kafka", sasl);
		this.kafka = new Kafka({
			clientId: "kafka-rewind",
			brokers,
			ssl: true,
			sasl,
			authenticationTimeout: 10_000,
			connectionTimeout: 10_000,
			logLevel: logLevel.DEBUG,
			logCreator: () => kafkaLogger,
		}).admin();
	}

	public async connect() {
		try {
			await this.kafka.connect();
		} catch (e) {
			log.panic("Failed to connect to kafka", e);
		}
	}

	public async disconnect() {
		await this.kafka.disconnect();
	}

	public async setOffset(groupId: string, topic: string, timestamp: number) {
		const partitionOffsets = await this.getOffsetsForTimestamp(topic, timestamp);
		try {
			await this.kafka.setOffsets({ groupId, topic, partitions: partitionOffsets });
		} catch (e) {
			log.panic(`Failed to set offsets to ${timestamp} for topic ${topic}`, e);
		}
	}

	async getOffsetsForTimestamp(topic: string, timestamp: number): Promise<PartitionOffset[]> {
		try {
			return await this.kafka.fetchTopicOffsetsByTimestamp(topic, timestamp);
		} catch (e) {
			log.panic(`Failed to get partition offsets for timestamp ${timestamp}`);
		}
	}
}
