

































import { getLogger, getTempoBaseUrl, getQueryPerformanceTracker } from './config.js';





const DEFAULT_TIMEOUT_MS = 30000;




export interface TraceQLSpan {
	
	spanID: string;
	
	name: string;
	
	startTimeUnixNano: string;
	
	durationNanos: string;
	
	attributes: Record<string, unknown>;
}




export interface TraceQLSpanSet {
	
	spans: TraceQLSpan[];
	
	matched: number;
}




export interface TraceQLTrace {
	
	traceID: string;
	
	rootServiceName: string;
	
	rootTraceName: string;
	
	startTimeUnixNano: string;
	
	durationMs: number;
	
	spanSets: TraceQLSpanSet[];
}




export interface TraceQLResult {
	
	traces: TraceQLTrace[];
	
	metrics: {
		
		inspectedTraces: number;
		
		inspectedSpans: number;
		
		inspectedBytes: number;
	};
}




interface TempoErrorResponse {
	error?: string;
	message?: string;
	statusCode?: number;
}






















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

	
	if (limit < 1 || limit > 1000) {
		throw new Error(`Invalid limit: ${limit}. Must be between 1 and 1000.`);
	}

	
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
			
			const errorBody = await response.text().catch(() => 'Unable to read error body');
			let errorMessage = `Tempo query failed: ${response.status} ${response.statusText}`;

			try {
				const errorJson: TempoErrorResponse = JSON.parse(errorBody);
				if (errorJson.error || errorJson.message) {
					errorMessage = `Tempo query failed: ${errorJson.error || errorJson.message}`;
				}
			} catch {
				
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
			
			if (error.name === 'AbortError') {
				logger.error('Tempo query timeout', {
					url,
					query,
					timeoutMs: DEFAULT_TIMEOUT_MS.toString(),
					error: 'Query exceeded 30 second timeout'
				});
				throw new Error(`Tempo query timeout: Query exceeded ${DEFAULT_TIMEOUT_MS / 1000}s timeout`);
			}

			
			if (error.message.includes('fetch')) {
				logger.error('Tempo fetch error', {
					url,
					query,
					error: error.message
				});
				throw new Error(`Tempo fetch error: ${error.message}`);
			}
		}

		
		logger.error('Tempo query error', {
			url,
			query,
			error: error instanceof Error ? error.message : String(error)
		});
		throw error;
	}
}




















export async function queryTracesByFingerprint(
	fingerprintId: string,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const query = `{ resource.fingerprint_id = "${fingerprintId}" }`;
	return queryTraceQL(query, start, end, limit);
}




















export async function queryTracesBySession(
	sessionId: string,
	start: Date,
	end: Date,
	limit: number = 20
): Promise<TraceQLResult> {
	const query = `{ resource.session_id = "${sessionId}" }`;
	return queryTraceQL(query, start, end, limit);
}






























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




export interface BatchQueryItemResult {
	
	success: boolean;
	
	data?: TraceQLResult;
	
	error?: string;
	
	executionTimeMs: number;
}




export interface BatchQueryResult {
	
	results: BatchQueryItemResult[];
	
	totalExecutionTimeMs: number;
}






























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
