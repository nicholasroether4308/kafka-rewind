import { fromIni } from "@aws-sdk/credential-providers";
import { AwsCredentialIdentityProvider } from "@smithy/types";
import * as log from "./log.js";
import { KafkaCredentials, KafkaAuthMethod, KafkaTrigger } from "./common.js";
import {
	EventSourceMappingConfiguration,
	LambdaClient,
	ListEventSourceMappingsCommand,
	SourceAccessConfiguration,
	SourceAccessType,
} from "@aws-sdk/client-lambda";

function getCredentials(profile?: string): AwsCredentialIdentityProvider {
	try {
		return fromIni({ profile });
	} catch (e) {
		log.panic(`Failed to get credentials for profile "${profile}"`, e);
	}
}

function getKafkaCredentials(accessConfigs: SourceAccessConfiguration[]): KafkaCredentials[] {
	const credentialList: KafkaCredentials[] = [];
	for (const accessConfig of accessConfigs) {
		if (!accessConfig.URI) {
			log.warn("Found source access configuration without defined URI! Ignoring...");
			continue;
		}
		if (!accessConfig.Type) {
			log.warn("Found source access configuration without defined type! Ignoring...");
			continue;
		}
		const credentials: Partial<KafkaCredentials> = { uri: accessConfig.URI };
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
		credentialList.push(credentials as KafkaCredentials);
	}
	return credentialList;
}

export class Aws {
	private readonly lambda: LambdaClient;

	constructor(profile?: string) {
		const credentials = getCredentials(profile);
		this.lambda = new LambdaClient({ credentials });
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
			log.debug(`Discovered source mapping ${sourceMapping.UUID}`);
			triggers.push({
				uuid: sourceMapping.UUID,
				credentials: getKafkaCredentials(sourceMapping.SourceAccessConfigurations),
			});
		}
		return triggers;
	}
}
