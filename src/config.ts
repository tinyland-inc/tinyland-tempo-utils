

























export interface TempoUtilsLogger {
	info: (msg: string, meta?: Record<string, unknown>) => void;
	warn: (msg: string, meta?: Record<string, unknown>) => void;
	error: (msg: string, meta?: Record<string, unknown>) => void;
	debug: (msg: string, meta?: Record<string, unknown>) => void;
}




export interface QueryPerformanceTracker {
	recordQueryExecution: (execution: {
		queryHash: string;
		query: string;
		executionTimeMs: number;
		resultCount: number;
		startTime: Date;
		endTime: Date;
		success: boolean;
		errorMessage?: string;
	}) => void;
	hashQuery: (query: string) => string;
}




export interface TempoUtilsConfig {
	
	logger?: TempoUtilsLogger;
	
	tempoBaseUrl?: string;
	
	tempoApiKey?: string;
	
	queryPerformanceTracker?: QueryPerformanceTracker;
}


const noopLogger: TempoUtilsLogger = {
	info: () => {},
	warn: () => {},
	error: () => {},
	debug: () => {},
};

let config: TempoUtilsConfig = {};

















export function configureTempoUtils(c: TempoUtilsConfig): void {
	config = { ...config, ...c };
}






export function getTempoUtilsConfig(): TempoUtilsConfig {
	return config;
}





export function resetTempoUtilsConfig(): void {
	config = {};
}






export function getLogger(): TempoUtilsLogger {
	return config.logger ?? noopLogger;
}













export function getTempoBaseUrl(
	defaultUrl: string = 'http://blahaj-tempo.blahaj-ops.svc.cluster.local:3200'
): string {
	return (
		config.tempoBaseUrl ??
		process.env.TEMPO_ENDPOINT ??
		process.env.TEMPO_URL ??
		defaultUrl
	);
}






export function getQueryPerformanceTracker(): QueryPerformanceTracker | undefined {
	return config.queryPerformanceTracker;
}
