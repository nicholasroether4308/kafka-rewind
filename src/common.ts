export const enum KafkaAuthMethod {
	BASIC = "basic",
	SCRAM_256 = "scram-256",
	SCRAM_512 = "scram-512",
}

export interface KafkaCredentialsLocation {
	method: KafkaAuthMethod;
	uri: string;
}

export interface KafkaTrigger {
	uuid: string;
	brokers: string[];
	topics: string[];
	credentials: KafkaCredentialsLocation[];
}

export interface KafkaCredentials {
	method: KafkaAuthMethod;
	username: string;
	password: string;
}
