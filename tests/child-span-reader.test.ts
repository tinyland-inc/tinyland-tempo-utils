/**
 * Tests for Child Span Reader
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { SpanReader, ChildSpanReader } from '../src/ChildSpanReader.js';
import type {
	TempoTrace,
	GeoLocation,
	SpanAttribute,
} from '../src/ChildSpanReader.js';
import {
	configureTempoUtils,
	resetTempoUtilsConfig,
} from '../src/config.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockLogger() {
	return {
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		debug: vi.fn(),
	};
}

function makeAttr(key: string, value: string): SpanAttribute {
	return { key, value: { stringValue: value } };
}

function makeDoubleAttr(key: string, value: number): SpanAttribute {
	return { key, value: { doubleValue: value } };
}

function makeIntAttr(key: string, value: number): SpanAttribute {
	return { key, value: { intValue: value } };
}

function makeBoolAttr(key: string, value: boolean): SpanAttribute {
	return { key, value: { boolValue: value } };
}

function createTraceWithParentGeo(overrides: Partial<{
	traceID: string;
	latitude: string;
	longitude: string;
	country: string;
	city: string;
	countryCode: string;
	timezone: string;
}> = {}): TempoTrace {
	const {
		traceID = 'trace-001',
		latitude = '42.4440',
		longitude = '-76.5019',
		country = 'United States',
		city = 'Ithaca',
		countryCode = 'US',
		timezone = 'America/New_York',
	} = overrides;

	return {
		traceID,
		rootServiceName: 'test-service',
		rootTraceName: 'fingerprint.enrichment',
		spanSet: {
			spans: [{
				spanID: 'span-001',
				name: 'fingerprint.enrichment',
				startTimeUnixNano: String(Date.now() * 1_000_000),
				durationNanos: '50000000',
				attributes: [
					makeAttr('geo.latitude', latitude),
					makeAttr('geo.longitude', longitude),
					makeAttr('geo.country', country),
					makeAttr('geo.city', city),
					makeAttr('geo.country_code', countryCode),
					makeAttr('geo.timezone', timezone),
				]
			}],
			matched: 1
		}
	};
}

function createTraceWithoutGeo(traceID: string = 'trace-no-geo'): TempoTrace {
	return {
		traceID,
		rootServiceName: 'test-service',
		rootTraceName: 'fingerprint.enrichment',
		spanSet: {
			spans: [{
				spanID: 'span-002',
				name: 'fingerprint.enrichment',
				startTimeUnixNano: String(Date.now() * 1_000_000),
				durationNanos: '50000000',
				attributes: [
					makeAttr('http.method', 'POST'),
					makeAttr('http.url', '/api/fingerprint'),
				]
			}],
			matched: 1
		}
	};
}

function createEmptyTrace(traceID: string = 'trace-empty'): TempoTrace {
	return {
		traceID,
		rootServiceName: 'test-service',
		spanSet: {
			spans: [],
			matched: 0
		}
	};
}

function createOTLPTraceWithChildGeo() {
	return {
		batches: [{
			scopeSpans: [{
				spans: [
					{
						spanId: 'parent-span-1',
						traceId: 'trace-old-001',
						name: 'fingerprint.enrichment',
						startTimeUnixNano: String(Date.now() * 1_000_000),
						endTimeUnixNano: String((Date.now() + 50) * 1_000_000),
						attributes: [makeAttr('http.method', 'POST')],
					},
					{
						spanId: 'child-span-1',
						traceId: 'trace-old-001',
						name: 'fingerprint.geoip_lookup',
						startTimeUnixNano: String(Date.now() * 1_000_000),
						endTimeUnixNano: String((Date.now() + 20) * 1_000_000),
						attributes: [
							makeAttr('geo.latitude', '40.7128'),
							makeAttr('geo.longitude', '-74.0060'),
							makeAttr('geo.country', 'United States'),
							makeAttr('geo.city', 'New York'),
							makeAttr('geo.country_code', 'US'),
						],
					}
				]
			}]
		}]
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('ChildSpanReader', () => {
	let mockLogger: ReturnType<typeof createMockLogger>;

	beforeEach(() => {
		mockLogger = createMockLogger();
		configureTempoUtils({
			logger: mockLogger,
			tempoBaseUrl: 'http://test-tempo:3200',
		});
	});

	afterEach(() => {
		resetTempoUtilsConfig();
		vi.restoreAllMocks();
	});

	// -----------------------------------------------------------------------
	// Constructor and backward compatibility
	// -----------------------------------------------------------------------

	describe('constructor', () => {
		it('should create instance with default options', () => {
			const reader = new SpanReader();
			expect(reader).toBeInstanceOf(SpanReader);
			expect(reader.getCacheStats()).toEqual({ hits: 0, misses: 0, size: 0 });
		});

		it('should accept custom options', () => {
			const reader = new SpanReader({
				cacheEnabled: false,
				cacheTTL: 60000,
				maxConcurrency: 5,
			});
			expect(reader).toBeInstanceOf(SpanReader);
		});

		it('should accept custom tempo URL via options', () => {
			const reader = new SpanReader({
				tempoUrl: 'http://custom-tempo:3200',
			});
			expect(reader).toBeInstanceOf(SpanReader);
		});

		it('should export ChildSpanReader as alias', () => {
			expect(ChildSpanReader).toBe(SpanReader);
		});
	});

	// -----------------------------------------------------------------------
	// readGeo - parent span path
	// -----------------------------------------------------------------------

	describe('readGeo - parent span', () => {
		it('should read geo data from parent span', async () => {
			const reader = new SpanReader();
			const trace = createTraceWithParentGeo();

			const geo = await reader.readGeo(trace);

			expect(geo).not.toBeNull();
			expect(geo!.country).toBe('United States');
			expect(geo!.city).toBe('Ithaca');
			expect(geo!.latitude).toBe(42.444);
			expect(geo!.longitude).toBe(-76.5019);
			expect(geo!.countryCode).toBe('US');
			expect(geo!.timezone).toBe('America/New_York');
			expect(geo!.source).toBe('parent-span');
		});

		it('should return null for trace with no spans', async () => {
			const reader = new SpanReader();
			const trace = createEmptyTrace();

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});

		it('should return null for trace without geo attributes', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithoutGeo();

			// Mock fetch to return no child span either
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify({ batches: [] }), { status: 200 })
			);

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});

		it('should return null for invalid latitude', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithParentGeo({ latitude: '999', longitude: '-76.5' });

			// Will fall through to child span lookup
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify({ batches: [] }), { status: 200 })
			);

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});

		it('should return null for invalid longitude', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithParentGeo({ latitude: '42.0', longitude: '999' });

			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify({ batches: [] }), { status: 200 })
			);

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});

		it('should use "Unknown" for missing country', async () => {
			const reader = new SpanReader();
			const trace: TempoTrace = {
				traceID: 'trace-no-country',
				spanSet: {
					spans: [{
						spanID: 'span-1',
						startTimeUnixNano: String(Date.now() * 1_000_000),
						durationNanos: '50000000',
						attributes: [
							makeAttr('geo.latitude', '42.444'),
							makeAttr('geo.longitude', '-76.5019'),
						]
					}],
					matched: 1
				}
			};

			const geo = await reader.readGeo(trace);
			expect(geo).not.toBeNull();
			expect(geo!.country).toBe('Unknown');
			expect(geo!.city).toBeNull();
		});
	});

	// -----------------------------------------------------------------------
	// readGeo - child span fallback
	// -----------------------------------------------------------------------

	describe('readGeo - child span fallback', () => {
		it('should fall back to child span when parent has no geo', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithoutGeo('trace-old-001');

			const childTraceData = createOTLPTraceWithChildGeo();
			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response(JSON.stringify(childTraceData), { status: 200 })
			);

			const geo = await reader.readGeo(trace);

			expect(geo).not.toBeNull();
			expect(geo!.city).toBe('New York');
			expect(geo!.latitude).toBe(40.7128);
			expect(geo!.longitude).toBe(-74.006);
			expect(geo!.source).toBe('child-span');
		});

		it('should return null when fetch fails for full trace', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithoutGeo();

			vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
				new Response('Not Found', { status: 404, statusText: 'Not Found' })
			);

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});

		it('should return null on network error during full trace fetch', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithoutGeo();

			vi.spyOn(globalThis, 'fetch').mockRejectedValueOnce(
				new TypeError('fetch failed')
			);

			const geo = await reader.readGeo(trace);
			expect(geo).toBeNull();
		});
	});

	// -----------------------------------------------------------------------
	// Cache behavior
	// -----------------------------------------------------------------------

	describe('cache', () => {
		it('should cache results on first read', async () => {
			const reader = new SpanReader();
			const trace = createTraceWithParentGeo();

			await reader.readGeo(trace);
			const stats = reader.getCacheStats();

			expect(stats.misses).toBe(1);
			expect(stats.size).toBe(1);
		});

		it('should return cached result on second read', async () => {
			const reader = new SpanReader();
			const trace = createTraceWithParentGeo();

			const geo1 = await reader.readGeo(trace);
			const geo2 = await reader.readGeo(trace);

			expect(geo1).toEqual(geo2);

			const stats = reader.getCacheStats();
			expect(stats.hits).toBe(1);
			expect(stats.misses).toBe(1);
		});

		it('should skip cache when disabled', async () => {
			const reader = new SpanReader({ cacheEnabled: false });
			const trace = createTraceWithParentGeo();

			await reader.readGeo(trace);
			await reader.readGeo(trace);

			const stats = reader.getCacheStats();
			expect(stats.hits).toBe(0);
			expect(stats.misses).toBe(0);
			expect(stats.size).toBe(0);
		});

		it('should expire cached entries after TTL', async () => {
			const reader = new SpanReader({ cacheTTL: 50 }); // 50ms TTL
			const trace = createTraceWithParentGeo();

			await reader.readGeo(trace);

			// Wait for TTL to expire
			await new Promise(resolve => setTimeout(resolve, 60));

			await reader.readGeo(trace);

			const stats = reader.getCacheStats();
			expect(stats.misses).toBe(2); // Both were misses
		});

		it('should clear cache', () => {
			const reader = new SpanReader();
			// Manually trigger internal state by calling readGeo
			reader.clearCache();

			const stats = reader.getCacheStats();
			expect(stats.size).toBe(0);
		});
	});

	// -----------------------------------------------------------------------
	// needsChildSpan
	// -----------------------------------------------------------------------

	describe('needsChildSpan', () => {
		it('should return false for trace with parent geo', () => {
			const reader = new SpanReader();
			const trace = createTraceWithParentGeo();

			expect(reader.needsChildSpan(trace)).toBe(false);
		});

		it('should return true for trace without parent geo', () => {
			const reader = new SpanReader();
			const trace = createTraceWithoutGeo();

			expect(reader.needsChildSpan(trace)).toBe(true);
		});

		it('should return true for trace with no spans', () => {
			const reader = new SpanReader();
			const trace = createEmptyTrace();

			expect(reader.needsChildSpan(trace)).toBe(true);
		});
	});

	// -----------------------------------------------------------------------
	// readGeoBulk
	// -----------------------------------------------------------------------

	describe('readGeoBulk', () => {
		it('should process multiple traces', async () => {
			const reader = new SpanReader();
			const traces = [
				createTraceWithParentGeo({ traceID: 'bulk-1' }),
				createTraceWithParentGeo({ traceID: 'bulk-2', city: 'Buffalo' }),
				createTraceWithParentGeo({ traceID: 'bulk-3', city: 'Rochester' }),
			];

			const results = await reader.readGeoBulk(traces);

			expect(results.size).toBe(3);
			expect(results.get('bulk-1')!.city).toBe('Ithaca');
			expect(results.get('bulk-2')!.city).toBe('Buffalo');
			expect(results.get('bulk-3')!.city).toBe('Rochester');
		});

		it('should handle empty traces array', async () => {
			const reader = new SpanReader();
			const results = await reader.readGeoBulk([]);

			expect(results.size).toBe(0);
		});

		it('should respect maxConcurrency batching', async () => {
			const reader = new SpanReader({ maxConcurrency: 2 });
			const traces = [
				createTraceWithParentGeo({ traceID: 'batch-1' }),
				createTraceWithParentGeo({ traceID: 'batch-2' }),
				createTraceWithParentGeo({ traceID: 'batch-3' }),
			];

			const results = await reader.readGeoBulk(traces);
			expect(results.size).toBe(3);
		});
	});

	// -----------------------------------------------------------------------
	// parseSpanAttributes
	// -----------------------------------------------------------------------

	describe('parseSpanAttributes', () => {
		it('should parse string values', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes([
				makeAttr('key1', 'value1'),
				makeAttr('key2', 'value2'),
			]);

			expect(attrs['key1']).toBe('value1');
			expect(attrs['key2']).toBe('value2');
		});

		it('should parse double values', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes([
				makeDoubleAttr('latitude', 42.444),
			]);

			expect(attrs['latitude']).toBe('42.444');
		});

		it('should parse integer values', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes([
				makeIntAttr('count', 42),
			]);

			expect(attrs['count']).toBe('42');
		});

		it('should parse boolean values', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes([
				makeBoolAttr('enabled', true),
			]);

			expect(attrs['enabled']).toBe('true');
		});

		it('should handle empty attributes array', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes([]);

			expect(Object.keys(attrs)).toHaveLength(0);
		});

		it('should handle null/undefined attributes', () => {
			const reader = new SpanReader();
			const attrs = reader.parseSpanAttributes(null as unknown as SpanAttribute[]);

			expect(Object.keys(attrs)).toHaveLength(0);
		});
	});

	// -----------------------------------------------------------------------
	// parseCoordinate
	// -----------------------------------------------------------------------

	describe('parseCoordinate', () => {
		it('should parse valid coordinate string', () => {
			const reader = new SpanReader();
			expect(reader.parseCoordinate('42.444')).toBe(42.444);
			expect(reader.parseCoordinate('-76.5019')).toBe(-76.5019);
			expect(reader.parseCoordinate('0')).toBe(0);
		});

		it('should return null for undefined', () => {
			const reader = new SpanReader();
			expect(reader.parseCoordinate(undefined)).toBeNull();
		});

		it('should return null for empty string', () => {
			const reader = new SpanReader();
			expect(reader.parseCoordinate('')).toBeNull();
		});

		it('should return null for non-numeric string', () => {
			const reader = new SpanReader();
			expect(reader.parseCoordinate('abc')).toBeNull();
			expect(reader.parseCoordinate('not-a-number')).toBeNull();
		});
	});

	// -----------------------------------------------------------------------
	// validateCoordinates
	// -----------------------------------------------------------------------

	describe('validateCoordinates', () => {
		it('should accept valid coordinates', () => {
			const reader = new SpanReader();
			expect(reader.validateCoordinates(42.444, -76.5019)).toBe(true);
			expect(reader.validateCoordinates(0, 0)).toBe(true);
			expect(reader.validateCoordinates(-90, -180)).toBe(true);
			expect(reader.validateCoordinates(90, 180)).toBe(true);
		});

		it('should reject out-of-range latitude', () => {
			const reader = new SpanReader();
			expect(reader.validateCoordinates(91, 0)).toBe(false);
			expect(reader.validateCoordinates(-91, 0)).toBe(false);
		});

		it('should reject out-of-range longitude', () => {
			const reader = new SpanReader();
			expect(reader.validateCoordinates(0, 181)).toBe(false);
			expect(reader.validateCoordinates(0, -181)).toBe(false);
		});
	});

	// -----------------------------------------------------------------------
	// Config injection lifecycle
	// -----------------------------------------------------------------------

	describe('config injection', () => {
		it('should work with no config (silent defaults)', async () => {
			resetTempoUtilsConfig();
			const reader = new SpanReader({ tempoUrl: 'http://localhost:3200' });
			const trace = createTraceWithParentGeo();

			const geo = await reader.readGeo(trace);
			expect(geo).not.toBeNull();
			expect(geo!.city).toBe('Ithaca');
		});

		it('should use injected logger', async () => {
			const reader = new SpanReader();
			const trace = createTraceWithParentGeo();

			await reader.readGeo(trace);

			// Logger should have been called during initialization and readGeo
			expect(mockLogger.debug).toHaveBeenCalled();
		});

		it('should respect config reset between tests', () => {
			resetTempoUtilsConfig();
			const reader = new SpanReader();
			// Should not throw - uses silent defaults
			expect(reader).toBeInstanceOf(SpanReader);
		});
	});
});
