/**
 * @tinyland-inc/tinyland-tempo-utils
 *
 * Tempo query utilities and child span analysis for Grafana Tempo.
 * Provides TraceQL query builder, batch query execution, and backward-compatible
 * geo data extraction from parent and child spans.
 *
 * Usage:
 * ```typescript
 * import {
 *   configureTempoUtils,
 *   queryTraceQL,
 *   SpanReader,
 * } from '@tinyland-inc/tinyland-tempo-utils';
 *
 * // Configure once at startup
 * configureTempoUtils({
 *   logger: myLogger,
 *   tempoBaseUrl: 'http://tempo:3200',
 * });
 *
 * // Query traces
 * const result = await queryTraceQL(
 *   '{ span.http.status_code >= 500 }',
 *   new Date(Date.now() - 3600000),
 *   new Date()
 * );
 *
 * // Read geo data from spans
 * const reader = new SpanReader();
 * const geo = await reader.readGeo(trace);
 * ```
 *
 * @module @tinyland-inc/tinyland-tempo-utils
 */

// Configuration
export {
	configureTempoUtils,
	getTempoUtilsConfig,
	resetTempoUtilsConfig,
	getLogger,
	getTempoBaseUrl,
	getQueryPerformanceTracker,
} from './config.js';

export type {
	TempoUtilsConfig,
	TempoUtilsLogger,
	QueryPerformanceTracker,
} from './config.js';

// Tempo Query Client
export {
	queryTraceQL,
	queryTracesByFingerprint,
	queryTracesBySession,
	queryTracesByStatusCode,
	queryTraceQLBatch,
} from './tempoQuery.js';

export type {
	TraceQLSpan,
	TraceQLSpanSet,
	TraceQLTrace,
	TraceQLResult,
	BatchQueryItemResult,
	BatchQueryResult,
} from './tempoQuery.js';

// Child Span Reader
export {
	SpanReader,
	ChildSpanReader,
} from './ChildSpanReader.js';

export type {
	GeoLocation,
	TempoTrace,
	TempoSpan,
	SpanAttribute,
	SpanReaderOptions,
} from './ChildSpanReader.js';
