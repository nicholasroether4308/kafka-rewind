import { fromIni } from "@aws-sdk/credential-providers";
import { AwsCredentialIdentityProvider } from "@smithy/types";
import * as log from "./log.js";
import {
	KafkaCredentialsLocation,
	KafkaAuthMethod,
	KafkaTrigger,
	KafkaCredentials,
} from "./common.js";
import {
	EventSourceMappingConfiguration,
	LambdaClient,
	ListEventSourceMappingsCommand,
	SelfManagedEventSource,
	SourceAccessConfiguration,
	SourceAccessType,
} from "@aws-sdk/client-lambda";
import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";

function getCredentials(profile?: string): AwsCredentialIdentityProvider {
	try {
		return fromIni({ profile });
	} catch (e) {
		log.panic(`Failed to get credentials for profile "${profile}"`, e);
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

export class Aws {
	private readonly lambda: LambdaClient;
	private readonly secrets: SecretsManagerClient;

	constructor(profile?: string) {
		const credentials = getCredentials(profile);
		this.lambda = new LambdaClient({ credentials });
		this.secrets = new SecretsManagerClient({ credentials });
	}

	private async getEventSourceMappings(
		functionName: string,
	): Promise<EventSourceMappingConfiguration[]> {
		try {
			const result = await this.lambda.send(
				new ListEventSourceMappingsCommand({
					FunctionName: functionName,
				}),
			);
			if (!result.EventSourceMappings) {
				log.panic(`Lambda ${functionName} has no defined event source mappings`);
			}
			return result.EventSourceMappings;
		} catch (e) {
			log.panic(`Failed to get event source mappings for lambda ${functionName}`, e);
		}
	}

	public async getKafkaTriggers(functionName: string): Promise<KafkaTrigger[]> {
		log.info(`Examining Kafka triggers for lambda ${functionName}`);
		const sourceMappings = await this.getEventSourceMappings(functionName);
		const triggers: KafkaTrigger[] = [];
		for (const sourceMapping of sourceMappings) {
			if (!sourceMapping.UUID) {
				log.warn("Found event source mapping without defined UUID! Ignoring...");
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
			log.debug(`Discovered source mapping ${sourceMapping.UUID}`);
			triggers.push({
				uuid: sourceMapping.UUID,
				brokers: getKafkaBrokers(sourceMapping.SelfManagedEventSource),
				credentials: getKafkaCredentials(sourceMapping.SourceAccessConfigurations),
			});
		}
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
}
