import chalk, { chalkStderr } from "chalk";

let globalLogLevel = LogLevel.INFO;

export const enum LogLevel {
	DEBUG = 0,
	INFO = 1,
	WARNING = 2,
	ERROR = 3,
	QUIET = 4,
}

export function debug(message: string, ...args: unknown[]) {
	if (globalLogLevel > LogLevel.DEBUG) return;
	console.debug(
		chalk(
			chalk.white.bold("DEBUG"),
			chalk.blackBright("-"),
			chalk.white(message),
		),
		...args,
	);
}

export function info(message: string, ...args: unknown[]) {
	if (globalLogLevel > LogLevel.INFO) return;
	console.info(
		chalk(
			chalk.blueBright.bold("INFO"),
			chalk.blackBright("-"),
			chalk.whiteBright(message),
		),
		...args,
	);
}

export function warn(message: string, body?: unknown) {
	if (globalLogLevel > LogLevel.WARNING) return;
	console.warn(
		chalkStderr(
			chalkStderr.yellowBright.bold("INFO"),
			chalkStderr.blackBright("-"),
			chalkStderr.yellowBright(message),
		),
	);
	if (body) console.warn(chalkStderr.yellow(body));
}

export function error(message: string, body?: unknown) {
	if (globalLogLevel > LogLevel.ERROR) return;
	console.error(
		chalkStderr(
			chalkStderr.redBright.bold("INFO"),
			chalkStderr.blackBright("-"),
			chalkStderr.redBright(message),
		),
	);
	if (body) console.warn(chalkStderr.red(body));
}

export function panic(message: string, body?: unknown): never {
	error(message, body);
	process.exit(1);
}

export function setLogLevel(logLevel: LogLevel) {
	globalLogLevel = logLevel;
}
