/**
 * Tempo TraceQL HTTP Query Client
 *
 * Provides type-safe HTTP client for querying Tempo's TraceQL API.
 * Uses config injection for logger and performance tracking dependencies.
 *
 * @module tempoQuery
 *
 * @example
 * // Query all traces for a specific fingerprint ID
 * const result = await queryTraceQL(
 *   '{ resource.fingerprint_id = "abc123" }',
 *   new Date(Date.now() - 3600000), // 1 hour ago
 *   new Date()
 * );
 *
 * @example
 * // Query traces with specific attributes
 * const result = await queryTraceQL(
 *   '{ span.http.method = "POST" && span.http.status_code >= 500 }',
 *   new Date(Date.now() - 86400000), // 24 hours ago
 *   new Date(),
 *   100 // limit to 100 traces
 * );
 *
 * @example
 * // Query traces by user session
 * const result = await queryTraceQL(
 *   '{ resource.session_id = "sess_xyz" }',
 *   new Date(Date.now() - 7200000), // 2 hours ago
 *   new Date()
 * );
 */

import { getLogger, getTempoBaseUrl, getQueryPerformanceTracker } from './config.js';

/**
 * Default query timeout in milliseconds (30 seconds)
 * TraceQL queries can be expensive for large time ranges
 */
const DEFAULT_TIMEOUT_MS = 30000;

/**
 * TraceQL span attributes and metadata
 */
export interface TraceQLSpan {
	/** Unique span identifier (hex string) */
	spanID: string;
	/** Human-readable span name (e.g., "HTTP POST /api/trpc/observability.logA11yViolation") */
	name: string;
	/** Start time in nanoseconds since Unix epoch */
	startTimeUnixNano: string;
	/** Duration in nanoseconds */
	durationNanos: string;
	/** Span attributes (e.g., http.method, http.status_code, fingerprint_id) */
	attributes: Record<string, unknown>;
}

/**
 * TraceQL span set (group of related spans in a trace)
 */
export interface TraceQLSpanSet {
	/** Spans matching the TraceQL query within this set */
	spans: TraceQLSpan[];
	/** Number of spans matched by the query in this set */
	matched: number;
}

/**
 * TraceQL trace (top-level trace containing span sets)
 */
export interface TraceQLTrace {
	/** Unique trace identifier (hex string) */
	traceID: string;
	/** Root service name (e.g., "stonewall-sveltekit") */
	rootServiceName: string;
	/** Root span name (e.g., "HTTP GET /admin/security") */
	rootTraceName: string;
	/** Trace start time in nanoseconds since Unix epoch */
	startTimeUnixNano: string;
	/** Total trace duration in milliseconds */
	durationMs: number;
	/** Span sets within this trace matching the query */
	spanSets: TraceQLSpanSet[];
}

/**
 * TraceQL query result with traces and metrics
 */
export interface TraceQLResult {
	/** Traces matching the query */
	traces: TraceQLTrace[];
	/** Query execution metrics */
	metrics: {
		/** Number of traces inspected during query execution */
		inspectedTraces: number;
		/** Number of spans inspected during query execution */
		inspectedSpans: number;
		/** Bytes of data inspected during query execution */
		inspectedBytes: number;
	};
}

/**
 * Tempo API error response structure
 */
interface TempoErrorResponse {
	error?: string;
	message?: string;
	statusCode?: number;
}

/**
 * Query Tempo's TraceQL API
 *
 * Sends a TraceQL query to Tempo and returns matching traces with full span details.
 * Implements proper error handling, timeout, and logging following project patterns.
 *
 * @param query - TraceQL query string (e.g., '{ resource.fingerprint_id = "abc123" }')
 * @param start - Start time for query range (inclusive)
 * @param end - End time for query range (inclusive)
 * @param limit - Maximum number of traces to return (default: 20, max: 1000)
 * @returns Promise resolving to TraceQL result with traces and metrics
 * @throws Error if HTTP request fails, timeout occurs, or Tempo returns error
 *
 * @example
 * const traces = await queryTraceQL(
 *   '{ span.http.status_code >= 500 }',
 *   new Date(Date.now() - 3600000),
 *   new Date()
 * );
 * console.log(`Found ${traces.traces.length} traces with errors`);
 */
export async function queryTraceQL(
	query: string,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const logger = getLogger();
	const tempoUrl = getTempoBaseUrl();
	const perfTracker = getQueryPerformanceTracker();
	const url = `${tempoUrl}/api/search`;

	// Validate limit (Tempo enforces max 1000)
	if (limit < 1 || limit > 1000) {
		throw new Error(`Invalid limit: ${limit}. Must be between 1 and 1000.`);
	}

	// Convert dates to Unix nanoseconds (Tempo expects nanosecond precision)
	const startNanos = start.getTime() * 1_000_000;
	const endNanos = end.getTime() * 1_000_000;

	const requestBody = {
		query,
		start: startNanos,
		end: endNanos,
		limit
	};

	const abortController = new AbortController();
	const timeoutId = setTimeout(() => abortController.abort(), DEFAULT_TIMEOUT_MS);

	const startTime = new Date();
	const queryHash = perfTracker?.hashQuery(query) ?? '';

	try {
		logger.debug('Querying Tempo TraceQL', {
			url,
			query,
			startTime: start.toISOString(),
			endTime: end.toISOString(),
			limit: limit.toString(),
			timeRange: `${Math.floor((end.getTime() - start.getTime()) / 1000)}s`
		});

		const response = await fetch(url, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json'
			},
			body: JSON.stringify(requestBody),
			signal: abortController.signal
		});

		clearTimeout(timeoutId);

		if (!response.ok) {
			// Attempt to parse error body for better error messages
			const errorBody = await response.text().catch(() => 'Unable to read error body');
			let errorMessage = `Tempo query failed: ${response.status} ${response.statusText}`;

			try {
				const errorJson: TempoErrorResponse = JSON.parse(errorBody);
				if (errorJson.error || errorJson.message) {
					errorMessage = `Tempo query failed: ${errorJson.error || errorJson.message}`;
				}
			} catch {
				// If error body is not JSON, include raw text
				if (errorBody && errorBody.length < 200) {
					errorMessage += ` - ${errorBody}`;
				}
			}

			logger.warn('Tempo query failed', {
				url,
				query,
				status: response.status.toString(),
				statusText: response.statusText,
				errorBody
			});

			throw new Error(errorMessage);
		}

		const result = await response.json() as TraceQLResult;
		const endTime = new Date();
		const executionTimeMs = endTime.getTime() - startTime.getTime();

		logger.debug('Tempo query succeeded', {
			url,
			query,
			tracesFound: result.traces.length.toString(),
			inspectedTraces: result.metrics.inspectedTraces.toString(),
			inspectedSpans: result.metrics.inspectedSpans.toString(),
			inspectedBytes: result.metrics.inspectedBytes.toString(),
			executionTimeMs: executionTimeMs.toString()
		});

		// Record performance metrics (if tracker configured)
		perfTracker?.recordQueryExecution({
			queryHash,
			query,
			executionTimeMs,
			resultCount: result.traces.length,
			startTime,
			endTime,
			success: true
		});

		return result;

	} catch (error) {
		clearTimeout(timeoutId);

		const endTime = new Date();
		const executionTimeMs = endTime.getTime() - startTime.getTime();
		const errorMessage = error instanceof Error ? error.message : String(error);

		// Record failed execution (if tracker configured)
		perfTracker?.recordQueryExecution({
			queryHash,
			query,
			executionTimeMs,
			resultCount: 0,
			startTime,
			endTime,
			success: false,
			errorMessage
		});

		if (error instanceof Error) {
			// Handle timeout specifically
			if (error.name === 'AbortError') {
				logger.error('Tempo query timeout', {
					url,
					query,
					timeoutMs: DEFAULT_TIMEOUT_MS.toString(),
					error: 'Query exceeded 30 second timeout'
				});
				throw new Error(`Tempo query timeout: Query exceeded ${DEFAULT_TIMEOUT_MS / 1000}s timeout`);
			}

			// Handle network errors
			if (error.message.includes('fetch')) {
				logger.error('Tempo fetch error', {
					url,
					query,
					error: error.message
				});
				throw new Error(`Tempo fetch error: ${error.message}`);
			}
		}

		// Re-throw with context for unexpected errors
		logger.error('Tempo query error', {
			url,
			query,
			error: error instanceof Error ? error.message : String(error)
		});
		throw error;
	}
}

/**
 * Query traces by fingerprint ID
 *
 * Convenience method for querying traces associated with a specific browser fingerprint.
 * Common use case: tracking user journey through the application.
 *
 * @param fingerprintId - Browser fingerprint ID (from FingerprintJS or similar)
 * @param start - Start time for query range
 * @param end - End time for query range
 * @param limit - Maximum traces to return (default: 20)
 * @returns Promise resolving to TraceQL result
 *
 * @example
 * const userTraces = await queryTracesByFingerprint(
 *   'fp_abc123xyz',
 *   new Date(Date.now() - 3600000), // last hour
 *   new Date()
 * );
 */
export async function queryTracesByFingerprint(
	fingerprintId: string,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const query = `{ resource.fingerprint_id = "${fingerprintId}" }`;
	return queryTraceQL(query, start, end, limit);
}

/**
 * Query traces by session ID
 *
 * Convenience method for querying traces within a specific user session.
 * Common use case: debugging authentication flows, analyzing session behavior.
 *
 * @param sessionId - Session identifier (from session cookie)
 * @param start - Start time for query range
 * @param end - End time for query range
 * @param limit - Maximum traces to return (default: 20)
 * @returns Promise resolving to TraceQL result
 *
 * @example
 * const sessionTraces = await queryTracesBySession(
 *   'sess_xyz789',
 *   new Date(Date.now() - 7200000), // last 2 hours
 *   new Date()
 * );
 */
export async function queryTracesBySession(
	sessionId: string,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const query = `{ resource.session_id = "${sessionId}" }`;
	return queryTraceQL(query, start, end, limit);
}

/**
 * Query traces by HTTP status code range
 *
 * Convenience method for finding errors or specific HTTP response patterns.
 * Common use case: debugging 500 errors, analyzing 404s, tracking redirects.
 *
 * @param minStatus - Minimum HTTP status code (inclusive)
 * @param maxStatus - Maximum HTTP status code (inclusive, optional)
 * @param start - Start time for query range
 * @param end - End time for query range
 * @param limit - Maximum traces to return (default: 20)
 * @returns Promise resolving to TraceQL result
 *
 * @example
 * // Find all 5xx errors
 * const errors = await queryTracesByStatusCode(
 *   500, 599,
 *   new Date(Date.now() - 86400000), // last 24 hours
 *   new Date()
 * );
 *
 * @example
 * // Find specific 404s
 * const notFound = await queryTracesByStatusCode(
 *   404, 404,
 *   new Date(Date.now() - 3600000), // last hour
 *   new Date()
 * );
 */
export async function queryTracesByStatusCode(
	minStatus: number,
	maxStatus: number | null = null,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const query = maxStatus !== null
		? `{ span.http.status_code >= ${minStatus} && span.http.status_code <= ${maxStatus} }`
		: `{ span.http.status_code = ${minStatus} }`;
	return queryTraceQL(query, start, end, limit);
}

/**
 * Batch query result - individual query result in batch execution
 */
export interface BatchQueryItemResult {
	/** Whether the query succeeded */
	success: boolean;
	/** Query result data (only if success = true) */
	data?: TraceQLResult;
	/** Error message (only if success = false) */
	error?: string;
	/** Individual query execution time in milliseconds */
	executionTimeMs: number;
}

/**
 * Batch query result - aggregated results from parallel execution
 */
export interface BatchQueryResult {
	/** Array of results matching input query order */
	results: BatchQueryItemResult[];
	/** Total batch execution time in milliseconds (parallel execution) */
	totalExecutionTimeMs: number;
}

/**
 * Execute multiple TraceQL queries in parallel
 *
 * Optimizes dashboard load times by running queries concurrently instead of sequentially.
 * Handles partial failures gracefully - some queries can fail while others succeed.
 *
 * Performance benefits:
 * - 5 sequential queries @ 200ms each = 1000ms total
 * - 5 parallel queries @ 200ms each = ~200ms total (5x faster)
 *
 * @param queries - Array of 1-10 query configurations
 * @returns Promise resolving to batch result with all query results in order
 *
 * @example
 * const batchResult = await queryTraceQLBatch([
 *   { query: '{ span.http.method = "GET" }', start: hourAgo, end: now, limit: 50 },
 *   { query: '{ span.http.status_code >= 500 }', start: hourAgo, end: now, limit: 50 },
 *   { query: '{ resource.fingerprint_id = "fp_123" }', start: hourAgo, end: now, limit: 100 }
 * ]);
 *
 * // Check results
 * batchResult.results.forEach((result, idx) => {
 *   if (result.success) {
 *     console.log(`Query ${idx}: ${result.data.traces.length} traces`);
 *   } else {
 *     console.error(`Query ${idx} failed: ${result.error}`);
 *   }
 * });
 */
export async function queryTraceQLBatch(
	queries: Array<{
		query: string;
		start: Date;
		end: Date;
		limit?: number;
	}>
): Promise<BatchQueryResult> {
	const logger = getLogger();
	const batchStartMs = Date.now();

	logger.debug('Executing TraceQL batch query', {
		batchSize: queries.length.toString(),
		timeRange: `${queries[0]?.start.toISOString()} to ${queries[0]?.end.toISOString()}`
	});

	// Execute all queries in parallel
	const queryPromises = queries.map(async (queryConfig, idx) => {
		const queryStartMs = Date.now();

		try {
			const result = await queryTraceQL(
				queryConfig.query,
				queryConfig.start,
				queryConfig.end,
				queryConfig.limit
			);

			const executionTimeMs = Date.now() - queryStartMs;

			return {
				success: true,
				data: result,
				executionTimeMs
			} as BatchQueryItemResult;
		} catch (error) {
			const executionTimeMs = Date.now() - queryStartMs;
			const errorMessage = error instanceof Error ? error.message : String(error);

			logger.warn('Batch query item failed', {
				queryIndex: idx.toString(),
				query: queryConfig.query,
				error: errorMessage
			});

			return {
				success: false,
				error: errorMessage,
				executionTimeMs
			} as BatchQueryItemResult;
		}
	});

	// Wait for all queries to complete (parallel execution)
	const results = await Promise.all(queryPromises);
	const totalExecutionTimeMs = Date.now() - batchStartMs;

	const successCount = results.filter(r => r.success).length;
	const failureCount = results.length - successCount;

	logger.info('TraceQL batch query completed', {
		batchSize: queries.length.toString(),
		successCount: successCount.toString(),
		failureCount: failureCount.toString(),
		totalExecutionTimeMs: totalExecutionTimeMs.toString(),
		avgQueryTimeMs: (results.reduce((sum, r) => sum + r.executionTimeMs, 0) / results.length).toString()
	});

	return {
		results,
		totalExecutionTimeMs
	};
}
