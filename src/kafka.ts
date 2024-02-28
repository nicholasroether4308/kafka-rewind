import { Admin, Kafka, LogEntry, PartitionOffset, SASLOptions, logLevel } from "kafkajs";
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
	private constructor(private readonly kafka: Admin) {}

	public static async connect(
		brokers: string[],
		credentials: KafkaCredentials,
	): Promise<KafkaConnection | null> {
		log.debug("Using brokers list %j for kafka", brokers);
		const sasl: SASLOptions = {
			mechanism: getMechanism(credentials.method),
			username: credentials.username,
			password: credentials.password,
		};
		log.debug("Using credentials %j for kafka", sasl);
		const kafka = new Kafka({
			clientId: "kafka-rewind",
			brokers,
			ssl: true,
			sasl,
			authenticationTimeout: 10_000,
			connectionTimeout: 10_000,
			logLevel: logLevel.DEBUG,
			logCreator: () => kafkaLogger,
		}).admin();
		try {
			await kafka.connect();
			return new KafkaConnection(kafka);
		} catch (e) {
			log.error("Failed to connect to kafka", e);
			return null;
		}
	}

	public async disconnect() {
		await this.kafka.disconnect();
	}

	public async setOffset(groupId: string, topic: string, timestamp: number): Promise<boolean> {
		const partitionOffsets = await this.getOffsetsForTimestamp(topic, timestamp);
		if (!partitionOffsets) return false;
		try {
			await this.kafka.setOffsets({ groupId, topic, partitions: partitionOffsets });
			return true;
		} catch (e) {
			log.error(`Failed to set offsets to ${timestamp} for topic ${topic}`, e);
			return false;
		}
	}

	async getOffsetsForTimestamp(
		topic: string,
		timestamp: number,
	): Promise<PartitionOffset[] | null> {
		try {
			return await this.kafka.fetchTopicOffsetsByTimestamp(topic, timestamp);
		} catch (e) {
			log.error(`Failed to get partition offsets for timestamp ${timestamp}`);
			return null;
		}
	}
}
