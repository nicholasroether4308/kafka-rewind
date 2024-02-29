import { fromIni } from "@aws-sdk/credential-providers";
import { AwsCredentialIdentityProvider } from "@smithy/types";
import * as log from "./log.ts";
import {
	KafkaCredentialsLocation,
	KafkaAuthMethod,
	KafkaTrigger,
	KafkaCredentials,
} from "./common.ts";
import {
	EventSourceMappingConfiguration,
	GetEventSourceMappingCommand,
	LambdaClient,
	ListEventSourceMappingsCommand,
	SelfManagedEventSource,
	SourceAccessConfiguration,
	SourceAccessType,
	UpdateEventSourceMappingCommand,
} from "@aws-sdk/client-lambda";
import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import ora from "ora";

function getCredentials(profile?: string): AwsCredentialIdentityProvider | null {
	try {
		return fromIni({ profile });
	} catch (e) {
		log.error(`Failed to get credentials for profile "${profile}"`, e);
		return null;
	}
}

function getKafkaCredentials(
	accessConfigs: SourceAccessConfiguration[],
): KafkaCredentialsLocation[] {
	const credentialList: KafkaCredentialsLocation[] = [];
	for (const accessConfig of accessConfigs) {
		if (!accessConfig.URI) {
			log.warn("Found source access configuration without defined URI! Ignoring...");
			continue;
		}
		if (!accessConfig.Type) {
			log.warn("Found source access configuration without defined type! Ignoring...");
			continue;
		}
		const credentials: Partial<KafkaCredentialsLocation> = { uri: accessConfig.URI };
		switch (accessConfig.Type) {
			case SourceAccessType.BASIC_AUTH:
				credentials.method = KafkaAuthMethod.BASIC;
				break;
			case SourceAccessType.SASL_SCRAM_256_AUTH:
				credentials.method = KafkaAuthMethod.SCRAM_256;
				break;
			case SourceAccessType.SASL_SCRAM_512_AUTH:
				credentials.method = KafkaAuthMethod.SCRAM_512;
				break;
			default:
				log.warn(
					`Found source access configuration of unsupported type ${accessConfig.Type}! Ignoring...`,
				);
				continue;
		}
		log.debug(`Discovered credentials for method "${credentials.method}"`);
		credentialList.push(credentials as KafkaCredentialsLocation);
	}
	return credentialList;
}

function getKafkaBrokers(eventSource: SelfManagedEventSource): string[] {
	return eventSource.Endpoints?.KAFKA_BOOTSTRAP_SERVERS ?? [];
}

export class AwsConnection {
	private constructor(
		private readonly lambda: LambdaClient,
		private readonly secrets: SecretsManagerClient,
	) {}

	public static async connect(profile?: string): Promise<AwsConnection | null> {
		const credentials = getCredentials(profile);
		if (!credentials) return null;
		const lambda = new LambdaClient({ credentials });
		const secrets = new SecretsManagerClient({ credentials });
		return new AwsConnection(lambda, secrets);
	}

	public async disconnect() {
		this.lambda.destroy();
		this.secrets.destroy();
	}

	private async getEventSourceMappings(
		functionName: string,
	): Promise<EventSourceMappingConfiguration[] | null> {
		try {
			const result = await this.lambda.send(
				new ListEventSourceMappingsCommand({
					FunctionName: functionName,
				}),
			);
			if (!result.EventSourceMappings) {
				log.error(`Lambda ${functionName} has no defined event source mappings`);
				return null;
			}
			return result.EventSourceMappings;
		} catch (e) {
			log.error(`Failed to get event source mappings for lambda ${functionName}`, e);
			return null;
		}
	}

	public async getKafkaTriggers(functionName: string): Promise<KafkaTrigger[] | null> {
		log.info(`Examining Kafka triggers for lambda ${functionName}`);
		const sourceMappings = await this.getEventSourceMappings(functionName);
		if (!sourceMappings) return null;
		const triggers: KafkaTrigger[] = [];
		for (const sourceMapping of sourceMappings) {
			if (!sourceMapping.UUID) {
				log.warn("Found event source mapping without defined UUID. It will be ignored.");
				continue;
			}
			if (!sourceMapping.SourceAccessConfigurations) {
				log.warn(
					`Event source mapping ${sourceMapping.UUID} has no defined access configuration and will be ignored`,
				);
				continue;
			}
			if (!sourceMapping.SelfManagedEventSource) {
				log.warn(
					`Event source ${sourceMapping.UUID} is not self-managed. MSK sources are not yet supported. It will be ignored.`,
				);
				continue;
			}
			if (!sourceMapping.SelfManagedKafkaEventSourceConfig?.ConsumerGroupId) {
				log.warn(
					`Event source ${sourceMapping.UUID} has no defined consumer group id. It will be ignored.`,
				);
				continue;
			}
			log.debug(`Discovered source mapping ${sourceMapping.UUID}`);
			triggers.push({
				uuid: sourceMapping.UUID,
				groupId: sourceMapping.SelfManagedKafkaEventSourceConfig.ConsumerGroupId,
				brokers: getKafkaBrokers(sourceMapping.SelfManagedEventSource),
				topics: sourceMapping.Topics ?? [],
				credentials: getKafkaCredentials(sourceMapping.SourceAccessConfigurations),
			});
		}
		log.info(`${triggers.length} trigger(s) found for function`);
		return triggers;
	}

	public async obtainCredentials(
		location: KafkaCredentialsLocation,
	): Promise<KafkaCredentials | null> {
		log.info(`Obtaining Kafka credentials for method "${location.method}"`);
		try {
			const value = await this.secrets.send(
				new GetSecretValueCommand({ SecretId: location.uri }),
			);
			if (!value.SecretString) {
				log.error("The credential secret value is empty!");
				return null;
			}
			const credentials = JSON.parse(value.SecretString) as unknown;
			if (!credentials || typeof credentials != "object") {
				log.error(`The credential secret value is ${credentials}!`);
				return null;
			}

			if (!("username" in credentials) || typeof credentials.username != "string") {
				log.error(`Missing expected string "username" in credential secret`);
				return null;
			}
			if (!("password" in credentials) || typeof credentials.password != "string") {
				log.error(`Missing expected string "password" in credential secret`);
				return null;
			}

			return {
				method: location.method,
				username: credentials.username,
				password: credentials.password,
			};
		} catch (e) {
			log.error("Failed to obtain credentials", e);
			return null;
		}
	}

	private async kafkaTriggerUpdateIsComplete(
		uuid: string,
		enabled: boolean,
	): Promise<boolean | null> {
		log.debug("Checking Kafka trigger status...");
		try {
			const result = await this.lambda.send(new GetEventSourceMappingCommand({ UUID: uuid }));
			switch (result.State) {
				case "Enabled":
					return enabled;
				case "Disabled":
					return !enabled;
				default:
					return false;
			}
		} catch (e) {
			log.error(`Failed to get Kafka trigger status`, e);
			return null;
		}
	}

	private async waitForKafkaTriggerUpdateComplete(
		uuid: string,
		enabled: boolean,
	): Promise<boolean> {
		const POLLING_INTERVAL = 2000;
		const NUM_TRIES = 50;

		const spinner = ora(
			`Waiting for kafka trigger ${enabled ? "startup" : "shutdown"}...`,
		).start();

		for (let i = 0; i < NUM_TRIES; i++) {
			await new Promise((res) => setTimeout(res, POLLING_INTERVAL));
			const result = await this.kafkaTriggerUpdateIsComplete(uuid, enabled);
			if (result == null) {
				spinner.stop();
				return false;
			}
			if (result == true) {
				spinner.stop();
				return true;
			}
		}

		spinner.stop();
		log.error("Kafka trigger update timed out!");
		return false;
	}

	public async setKafkaTriggerEnabled(uuid: string, enabled: boolean): Promise<boolean> {
		log.info(`${enabled ? "Enabling" : "Disabling"} Kafka trigger ${uuid}...`);
		try {
			await this.lambda.send(
				new UpdateEventSourceMappingCommand({ UUID: uuid, Enabled: enabled }),
			);
		} catch (e) {
			log.error(`Failed to ${enabled ? "enable" : "disable"} Kafka trigger ${uuid}`, e);
			return false;
		}

		const result = await this.waitForKafkaTriggerUpdateComplete(uuid, enabled);
		if (result) {
			log.info(`Successfully ${enabled ? "enabled" : "disabled"} Kafka trigger ${uuid}`);
		}
		return result;
	}
}
