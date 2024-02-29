import { Admin, Kafka, LogEntry, PartitionOffset, SASLOptions, logLevel } from "kafkajs";
import { KafkaAuthMethod, KafkaCredentials } from "./common.ts";
import * as log from "./log.ts";
import ora from "ora";

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

	public async waitForEmptyGroup(groupId: string): Promise<boolean> {
		const POLLING_INTERVAL = 5000;
		const NUM_TRIES = 100;

		const spinner = ora("Waiting for Kafka client shutdown...").start();

		for (let i = 0; i < NUM_TRIES; i++) {
			await new Promise((res) => setTimeout(res, POLLING_INTERVAL));
			const hasConsumers = await this.groupHasConsumers(groupId);
			if (hasConsumers == null) {
				spinner.stop();
				return false;
			}
			if (hasConsumers == false) {
				spinner.stop();
				return true;
			}
		}

		spinner.stop();
		log.error("Waiting for empty consumer group timed out");
		return false;
	}

	async groupHasConsumers(groupId: string): Promise<boolean | null> {
		log.debug("Checking consumer group status");
		try {
			const result = await this.kafka.describeGroups([groupId]);
			return result.groups[0].members.length > 0;
		} catch (e) {
			log.error(`Failed to get consumer group metadata`);
			return null;
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
