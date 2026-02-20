/**
 * Configuration injection for tinyland-tempo-utils
 *
 * Provides a way to inject external dependencies (logger, performance tracker,
 * Tempo connection details) without coupling to specific implementations.
 *
 * All config values are optional - sensible no-op defaults are used when
 * no configuration is provided.
 *
 * @module config
 *
 * @example
 * ```typescript
 * import { configureTempoUtils } from '@tummycrypt/tinyland-tempo-utils';
 *
 * configureTempoUtils({
 *   logger: myStructuredLogger,
 *   tempoBaseUrl: 'http://tempo:3200',
 *   queryPerformanceTracker: myPerformanceService,
 * });
 * ```
 */

/**
 * Logger interface for structured logging
 */
export interface TempoUtilsLogger {
	info: (msg: string, meta?: Record<string, unknown>) => void;
	warn: (msg: string, meta?: Record<string, unknown>) => void;
	error: (msg: string, meta?: Record<string, unknown>) => void;
	debug: (msg: string, meta?: Record<string, unknown>) => void;
}

/**
 * Query performance tracker interface for recording execution metrics
 */
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

/**
 * Configuration options for tinyland-tempo-utils
 */
export interface TempoUtilsConfig {
	/** Structured logger. Defaults to silent no-op logger. */
	logger?: TempoUtilsLogger;
	/** Base URL for Tempo HTTP API. Defaults to env vars or k8s service DNS. */
	tempoBaseUrl?: string;
	/** API key for Tempo authentication (if required). */
	tempoApiKey?: string;
	/** Query performance tracker for recording execution metrics. */
	queryPerformanceTracker?: QueryPerformanceTracker;
}

/** Silent no-op logger used when no logger is configured */
const noopLogger: TempoUtilsLogger = {
	info: () => {},
	warn: () => {},
	error: () => {},
	debug: () => {},
};

let config: TempoUtilsConfig = {};

/**
 * Configure tempo-utils with external dependencies.
 *
 * Call this once at application startup before using any query functions.
 * Merges with existing configuration (does not replace).
 *
 * @param c - Configuration options to merge
 *
 * @example
 * ```typescript
 * configureTempoUtils({
 *   logger: pinoLogger,
 *   tempoBaseUrl: 'http://tempo:3200',
 * });
 * ```
 */
export function configureTempoUtils(c: TempoUtilsConfig): void {
	config = { ...config, ...c };
}

/**
 * Get current configuration.
 *
 * @returns Current merged configuration
 */
export function getTempoUtilsConfig(): TempoUtilsConfig {
	return config;
}

/**
 * Reset all configuration to empty defaults.
 * Primarily useful for testing.
 */
export function resetTempoUtilsConfig(): void {
	config = {};
}

/**
 * Get the configured logger, falling back to silent no-op.
 *
 * @returns Logger instance
 */
export function getLogger(): TempoUtilsLogger {
	return config.logger ?? noopLogger;
}

/**
 * Get the configured Tempo base URL.
 *
 * Resolution order:
 * 1. Configured via `configureTempoUtils({ tempoBaseUrl })`
 * 2. `TEMPO_ENDPOINT` environment variable
 * 3. `TEMPO_URL` environment variable
 * 4. Default Kubernetes service DNS
 *
 * @param defaultUrl - Default URL if nothing else is configured
 * @returns Resolved Tempo base URL
 */
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

/**
 * Get the configured query performance tracker, or undefined.
 *
 * @returns Query performance tracker or undefined
 */
export function getQueryPerformanceTracker(): QueryPerformanceTracker | undefined {
	return config.queryPerformanceTracker;
}
