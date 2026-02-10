/**
 * Tests for Tempo TraceQL query client
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
	queryTraceQL,
	queryTracesByFingerprint,
	queryTracesBySession,
	queryTracesByStatusCode,
	queryTraceQLBatch,
} from '../src/tempoQuery.js';
import type { TraceQLResult } from '../src/tempoQuery.js';
import {
	configureTempoUtils,
	resetTempoUtilsConfig,
} from '../src/config.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockResult(traceCount: number = 1): TraceQLResult {
	return {
		traces: Array.from({ length: traceCount }, (_, i) => ({
			traceID: `trace-${i}`,
			rootServiceName: 'test-service',
			rootTraceName: 'HTTP GET /test',
			startTimeUnixNano: String(Date.now() * 1_000_000),
			durationMs: 100 + i,
			spanSets: [{
				spans: [{
					spanID: `span-${i}`,
					name: `test-span-${i}`,
					startTimeUnixNano: String(Date.now() * 1_000_000),
					durationNanos: String((100 + i) * 1_000_000),
					attributes: {}
				}],
				matched: 1
			}]
		})),
		metrics: {
			inspectedTraces: traceCount * 10,
			inspectedSpans: traceCount * 50,
			inspectedBytes: traceCount * 1024
		}
	};
}

function createMockLogger() {
	return {
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		debug: vi.fn(),
	};
}

function createMockPerfTracker() {
	return {
		recordQueryExecution: vi.fn(),
		hashQuery: vi.fn().mockReturnValue('mock-hash-abc123'),
	};
}

const oneHourAgo = new Date(Date.now() - 3600000);
const now = new Date();

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('tempoQuery', () => {
	let mockLogger: ReturnType<typeof createMockLogger>;
	let mockPerfTracker: ReturnType<typeof createMockPerfTracker>;

	beforeEach(() => {
		mockLogger = createMockLogger();
		mockPerfTracker = createMockPerfTracker();
		configureTempoUtils({
			logger: mockLogger,
			tempoBaseUrl: 'http://test-tempo:3200',
			queryPerformanceTracker: mockPerfTracker,
		});
	});

	afterEach(() => {
		resetTempoUtilsConfig();
		vi.restoreAllMocks();
	});

	// -----------------------------------------------------------------------
	// queryTraceQL
	// -----------------------------------------------------------------------

	describe('queryTraceQL', () => {
		it('should send POST request with correct body', async () => {
			const mockResult = createMockResult(2);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTraceQL('{ span.http.method = "GET" }', oneHourAgo, now, 50);

			expect(fetchSpy).toHaveBeenCalledOnce();
			const [url, opts] = fetchSpy.mock.calls[0];
			expect(url).toBe('http://test-tempo:3200/api/search');
			expect(opts?.method).toBe('POST');
			expect(opts?.headers).toEqual({ 'Content-Type': 'application/json' });

			const body = JSON.parse(opts?.body as string);
			expect(body.query).toBe('{ span.http.method = "GET" }');
			expect(body.limit).toBe(50);
			expect(body.start).toBe(oneHourAgo.getTime() * 1_000_000);
			expect(body.end).toBe(now.getTime() * 1_000_000);
		});

		it('should return parsed TraceQL result', async () => {
			const mockResult = createMockResult(3);
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			const result = await queryTraceQL('{ span.http.method = "GET" }', oneHourAgo, now);

			expect(result.traces).toHaveLength(3);
			expect(result.traces[0].traceID).toBe('trace-0');
			expect(result.metrics.inspectedTraces).toBe(30);
		});

		it('should use default limit of 20', async () => {
			const mockResult = createMockResult(1);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTraceQL('{ span.test = "1" }', oneHourAgo, now);

			const body = JSON.parse(fetchSpy.mock.calls[0][1]?.body as string);
			expect(body.limit).toBe(20);
		});

		it('should throw on limit below 1', async () => {
			await expect(
				queryTraceQL('{ span.test = "1" }', oneHourAgo, now, 0)
			).rejects.toThrow('Invalid limit: 0. Must be between 1 and 1000.');
		});

		it('should throw on limit above 1000', async () => {
			await expect(
				queryTraceQL('{ span.test = "1" }', oneHourAgo, now, 1001)
			).rejects.toThrow('Invalid limit: 1001. Must be between 1 and 1000.');
		});

		it('should throw on HTTP error with JSON error body', async () => {
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(
					JSON.stringify({ error: 'invalid TraceQL query' }),
					{ status: 400, statusText: 'Bad Request' }
				)
			);

			await expect(
				queryTraceQL('{ invalid }', oneHourAgo, now)
			).rejects.toThrow('Tempo query failed: invalid TraceQL query');
		});

		it('should throw on HTTP error with plain text body', async () => {
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response('Service Unavailable', { status: 503, statusText: 'Service Unavailable' })
			);

			await expect(
				queryTraceQL('{ span.test = "1" }', oneHourAgo, now)
			).rejects.toThrow('Tempo query failed: 503 Service Unavailable - Service Unavailable');
		});

		it('should throw timeout error on AbortError', async () => {
			const abortError = new DOMException('The operation was aborted', 'AbortError');
			vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(abortError);

			await expect(
				queryTraceQL('{ span.test = "1" }', oneHourAgo, now)
			).rejects.toThrow('Tempo query timeout: Query exceeded 30s timeout');
		});

		it('should throw fetch error on network failure', async () => {
			vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(
				new TypeError('fetch failed: ECONNREFUSED')
			);

			await expect(
				queryTraceQL('{ span.test = "1" }', oneHourAgo, now)
			).rejects.toThrow('Tempo fetch error: fetch failed: ECONNREFUSED');
		});

		it('should record performance metrics on success', async () => {
			const mockResult = createMockResult(5);
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTraceQL('{ span.test = "1" }', oneHourAgo, now);

			expect(mockPerfTracker.hashQuery).toHaveBeenCalledWith('{ span.test = "1" }');
			expect(mockPerfTracker.recordQueryExecution).toHaveBeenCalledWith(
				expect.objectContaining({
					queryHash: 'mock-hash-abc123',
					query: '{ span.test = "1" }',
					resultCount: 5,
					success: true,
				})
			);
		});

		it('should record performance metrics on failure', async () => {
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response('error', { status: 500, statusText: 'Internal Server Error' })
			);

			await expect(
				queryTraceQL('{ span.fail = "1" }', oneHourAgo, now)
			).rejects.toThrow();

			expect(mockPerfTracker.recordQueryExecution).toHaveBeenCalledWith(
				expect.objectContaining({
					query: '{ span.fail = "1" }',
					resultCount: 0,
					success: false,
					errorMessage: expect.any(String),
				})
			);
		});

		it('should work without performance tracker configured', async () => {
			resetTempoUtilsConfig();
			configureTempoUtils({
				tempoBaseUrl: 'http://test-tempo:3200',
			});

			const mockResult = createMockResult(1);
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			const result = await queryTraceQL('{ span.test = "1" }', oneHourAgo, now);
			expect(result.traces).toHaveLength(1);
		});

		it('should log debug messages during query execution', async () => {
			const mockResult = createMockResult(1);
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTraceQL('{ span.test = "1" }', oneHourAgo, now);

			expect(mockLogger.debug).toHaveBeenCalledWith(
				'Querying Tempo TraceQL',
				expect.objectContaining({ query: '{ span.test = "1" }' })
			);
			expect(mockLogger.debug).toHaveBeenCalledWith(
				'Tempo query succeeded',
				expect.objectContaining({ tracesFound: '1' })
			);
		});
	});

	// -----------------------------------------------------------------------
	// Convenience query builders
	// -----------------------------------------------------------------------

	describe('queryTracesByFingerprint', () => {
		it('should construct correct TraceQL query', async () => {
			const mockResult = createMockResult(1);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTracesByFingerprint('fp_abc123', oneHourAgo, now);

			const body = JSON.parse(fetchSpy.mock.calls[0][1]?.body as string);
			expect(body.query).toBe('{ resource.fingerprint_id = "fp_abc123" }');
		});
	});

	describe('queryTracesBySession', () => {
		it('should construct correct TraceQL query', async () => {
			const mockResult = createMockResult(1);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTracesBySession('sess_xyz789', oneHourAgo, now);

			const body = JSON.parse(fetchSpy.mock.calls[0][1]?.body as string);
			expect(body.query).toBe('{ resource.session_id = "sess_xyz789" }');
		});
	});

	describe('queryTracesByStatusCode', () => {
		it('should construct range query with min and max', async () => {
			const mockResult = createMockResult(1);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTracesByStatusCode(500, 599, oneHourAgo, now);

			const body = JSON.parse(fetchSpy.mock.calls[0][1]?.body as string);
			expect(body.query).toBe('{ span.http.status_code >= 500 && span.http.status_code <= 599 }');
		});

		it('should construct exact match query without max', async () => {
			const mockResult = createMockResult(1);
			const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(mockResult), { status: 200 })
			);

			await queryTracesByStatusCode(404, null, oneHourAgo, now);

			const body = JSON.parse(fetchSpy.mock.calls[0][1]?.body as string);
			expect(body.query).toBe('{ span.http.status_code = 404 }');
		});
	});

	// -----------------------------------------------------------------------
	// queryTraceQLBatch
	// -----------------------------------------------------------------------

	describe('queryTraceQLBatch', () => {
		it('should execute multiple queries in parallel', async () => {
			const mockResult1 = createMockResult(2);
			const mockResult2 = createMockResult(3);

			vi.spyOn(globalThis, 'fetch')
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult1), { status: 200 }))
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult2), { status: 200 }));

			const batchResult = await queryTraceQLBatch([
				{ query: '{ span.http.method = "GET" }', start: oneHourAgo, end: now },
				{ query: '{ span.http.method = "POST" }', start: oneHourAgo, end: now },
			]);

			expect(batchResult.results).toHaveLength(2);
			expect(batchResult.results[0].success).toBe(true);
			expect(batchResult.results[0].data?.traces).toHaveLength(2);
			expect(batchResult.results[1].success).toBe(true);
			expect(batchResult.results[1].data?.traces).toHaveLength(3);
			expect(batchResult.totalExecutionTimeMs).toBeGreaterThanOrEqual(0);
		});

		it('should handle partial failures gracefully', async () => {
			const mockResult = createMockResult(1);

			vi.spyOn(globalThis, 'fetch')
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult), { status: 200 }))
				.mockResolvedValueOnce(new Response('error', { status: 500, statusText: 'Internal Server Error' }));

			const batchResult = await queryTraceQLBatch([
				{ query: '{ span.ok = "1" }', start: oneHourAgo, end: now },
				{ query: '{ span.fail = "1" }', start: oneHourAgo, end: now },
			]);

			expect(batchResult.results[0].success).toBe(true);
			expect(batchResult.results[1].success).toBe(false);
			expect(batchResult.results[1].error).toContain('Tempo query failed');
		});

		it('should log batch completion with stats', async () => {
			const mockResult = createMockResult(1);

			vi.spyOn(globalThis, 'fetch')
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult), { status: 200 }));

			await queryTraceQLBatch([
				{ query: '{ span.test = "1" }', start: oneHourAgo, end: now },
			]);

			expect(mockLogger.info).toHaveBeenCalledWith(
				'TraceQL batch query completed',
				expect.objectContaining({
					batchSize: '1',
					successCount: '1',
					failureCount: '0',
				})
			);
		});

		it('should record execution time per query', async () => {
			const mockResult = createMockResult(1);

			vi.spyOn(globalThis, 'fetch')
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult), { status: 200 }))
				.mockResolvedValueOnce(new Response(JSON.stringify(mockResult), { status: 200 }));

			const batchResult = await queryTraceQLBatch([
				{ query: '{ q1 }', start: oneHourAgo, end: now, limit: 10 },
				{ query: '{ q2 }', start: oneHourAgo, end: now, limit: 10 },
			]);

			expect(batchResult.results[0].executionTimeMs).toBeGreaterThanOrEqual(0);
			expect(batchResult.results[1].executionTimeMs).toBeGreaterThanOrEqual(0);
		});
	});
});
