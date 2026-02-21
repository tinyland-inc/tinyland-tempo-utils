/**
 * Child Span Reader
 *
 * Backward compatibility utility for reading geographic coordinates from fingerprint traces
 * during the migration from child-span geo storage to parent-span geo storage.
 *
 * **Background**:
 * - **Old traces** (before Nov 15, 2025): Geo data in child span `fingerprint.geoip_lookup` only
 * - **New traces** (after Nov 15, 2025): Geo data in both parent span `fingerprint.enrichment` AND child span
 *
 * **Fallback Logic**:
 * 1. Try parent span attributes (new traces) - O(1) lookup
 * 2. If null, try child span attributes (old traces) - O(n) scan
 * 3. If no child span, return null
 *
 * **Performance**:
 * - Caches child span lookups (5-minute TTL by default)
 * - Bounded concurrency (10 parallel child span fetches)
 * - O(1) parent check, O(n) child scan worst case
 *
 * **Removal Plan**:
 * - Remove after Tempo retention period (30 days after Phase 3 deployment)
 * - Verify no traces with child spans remain: `{ name="fingerprint.geoip_lookup" }` -> 0 results
 *
 * @module ChildSpanReader
 */

import { getLogger, getTempoBaseUrl } from './config.js';

/**
 * Geographic location from Tempo span
 */
export interface GeoLocation {
  country: string;
  countryCode?: string;
  city: string | null;
  latitude: number | null;
  longitude: number | null;
  timezone?: string | null;
  source: 'parent-span' | 'child-span'; // Track where data came from
}

/**
 * Tempo trace structure (from /api/search response)
 */
export interface TempoTrace {
  traceID: string;
  rootServiceName?: string;
  rootTraceName?: string;
  startTimeUnixNano?: string;
  durationMs?: number;
  spanSet?: {
    spans: TempoSpan[];
    matched: number;
  };
}

/**
 * Tempo span with attributes (from search response)
 */
export interface TempoSpan {
  spanID: string;
  name?: string;
  startTimeUnixNano: string;
  durationNanos: string;
  attributes: SpanAttribute[];
}

/**
 * Span attribute (key-value pair)
 */
export interface SpanAttribute {
  key: string;
  value: {
    stringValue?: string;
    intValue?: string | number;
    doubleValue?: number;
    boolValue?: boolean;
  };
}

/**
 * Full OTLP trace response from /api/traces/{traceID}
 */
interface OTLPTraceResponse {
  batches: Array<{
    scopeSpans: Array<{
      spans: Array<{
        spanId: string;
        traceId: string;
        name: string;
        startTimeUnixNano: string;
        endTimeUnixNano: string;
        attributes: SpanAttribute[];
      }>;
    }>;
  }>;
}

/**
 * Cache entry for child span lookups
 */
interface CacheEntry {
  geoLocation: GeoLocation | null;
  fetchedAt: number; // Unix timestamp (ms)
}

/**
 * Cache statistics for monitoring
 */
interface CacheStats {
  hits: number;
  misses: number;
  size: number;
}

/**
 * Options for SpanReader
 */
export interface SpanReaderOptions {
  cacheEnabled?: boolean; // Default: true
  cacheTTL?: number; // Default: 300000 (5 minutes)
  maxConcurrency?: number; // Default: 10
  tempoUrl?: string; // Override for Tempo URL (takes precedence over config)
}

/**
 * Child Span Reader - Backward compatibility utility for reading geo data from Tempo traces
 *
 * Handles the transition from child span geo storage to parent span geo storage.
 *
 * @example
 * ```typescript
 * const reader = new SpanReader();
 * const geo = await reader.readGeo(trace); // Tries parent first, then child
 * console.log(`Location: ${geo?.city}, ${geo?.country} (source: ${geo?.source})`);
 * ```
 */
export class SpanReader {
  private cache: Map<string, CacheEntry>;
  private cacheStats: CacheStats;
  private tempoUrl: string;
  private cacheEnabled: boolean;
  private cacheTTL: number;
  private maxConcurrency: number;

  constructor(options: SpanReaderOptions = {}) {
    this.cache = new Map();
    this.cacheStats = { hits: 0, misses: 0, size: 0 };

    this.tempoUrl = options.tempoUrl ?? getTempoBaseUrl('http://stonewall-tempo:3200');

    this.cacheEnabled = options.cacheEnabled ?? true;
    this.cacheTTL = options.cacheTTL ?? 300000; // 5 minutes
    this.maxConcurrency = options.maxConcurrency ?? 10;

    const logger = getLogger();
    logger.debug('ChildSpanReader initialized', {
      cacheEnabled: this.cacheEnabled,
      cacheTTL: this.cacheTTL,
      maxConcurrency: this.maxConcurrency
    });
  }

  /**
   * Read geographic location from trace (parent -> child fallback)
   *
   * @param trace - Tempo trace with spanSet
   * @returns GeoLocation or null if not found
   *
   * @example
   * ```typescript
   * const reader = new SpanReader();
   * const traces = await tempoService.searchTraces('{ ... }', start, end);
   * for (const trace of traces) {
   *   const geo = await reader.readGeo(trace);
   *   if (geo) console.log(`${geo.city}, ${geo.country} from ${geo.source}`);
   * }
   * ```
   */
  async readGeo(trace: TempoTrace): Promise<GeoLocation | null> {
    const logger = getLogger();

    // Step 1: Check cache first (if enabled)
    if (this.cacheEnabled) {
      const cached = this.getCached(trace.traceID);
      if (cached !== undefined) {
        this.cacheStats.hits++;
        logger.debug('Cache hit for geo data', { traceID: trace.traceID });
        return cached;
      }
      this.cacheStats.misses++;
    }

    // Step 2: Try parent span attributes (new traces)
    const parentGeo = this.readGeoFromParentSpan(trace);
    if (parentGeo) {
      if (this.cacheEnabled) {
        this.setCached(trace.traceID, parentGeo);
      }
      return parentGeo;
    }

    // Step 3: Fallback to child span (old traces)
    const childGeo = await this.readGeoFromChildSpan(trace);
    if (this.cacheEnabled) {
      this.setCached(trace.traceID, childGeo);
    }
    return childGeo;
  }

  /**
   * Bulk read geo locations with bounded concurrency
   *
   * @param traces - Array of Tempo traces
   * @returns Map of traceId -> GeoLocation
   *
   * @example
   * ```typescript
   * const reader = new SpanReader();
   * const traces = await tempoService.searchTraces('{ ... }', start, end);
   * const geoMap = await reader.readGeoBulk(traces);
   * for (const [traceID, geo] of geoMap.entries()) {
   *   if (geo) console.log(`${traceID}: ${geo.city}`);
   * }
   * ```
   */
  async readGeoBulk(traces: TempoTrace[]): Promise<Map<string, GeoLocation | null>> {
    const logger = getLogger();
    const results = new Map<string, GeoLocation | null>();
    const BATCH_SIZE = this.maxConcurrency;

    logger.debug('Bulk reading geo data', {
      traceCount: traces.length,
      batchSize: BATCH_SIZE
    });

    // Process in batches to avoid overwhelming Tempo
    for (let i = 0; i < traces.length; i += BATCH_SIZE) {
      const batch = traces.slice(i, i + BATCH_SIZE);

      // Process batch in parallel
      const batchResults = await Promise.all(
        batch.map(async (trace) => ({
          traceID: trace.traceID,
          geo: await this.readGeo(trace)
        }))
      );

      for (const { traceID, geo } of batchResults) {
        results.set(traceID, geo);
      }
    }

    logger.debug('Bulk read complete', {
      totalTraces: traces.length,
      tracesWithGeo: Array.from(results.values()).filter(g => g !== null).length
    });

    return results;
  }

  /**
   * Check if trace needs child span fallback
   *
   * @param trace - Tempo trace
   * @returns true if parent span missing geo data
   */
  needsChildSpan(trace: TempoTrace): boolean {
    const parentGeo = this.readGeoFromParentSpan(trace);
    return parentGeo === null;
  }

  /**
   * Clear cache (for testing or periodic cleanup)
   */
  clearCache(): void {
    const logger = getLogger();
    const previousSize = this.cache.size;
    this.cache.clear();
    this.cacheStats.size = 0;
    logger.debug('Cache cleared', { previousSize });
  }

  /**
   * Get cache statistics (for monitoring)
   */
  getCacheStats(): CacheStats {
    return {
      ...this.cacheStats,
      size: this.cache.size
    };
  }

  /**
   * Read geo from parent span (new traces)
   *
   * @private
   * @param trace - Tempo trace
   * @returns GeoLocation or null if parent missing geo
   */
  private readGeoFromParentSpan(trace: TempoTrace): GeoLocation | null {
    const logger = getLogger();

    // Extract parent span (first span in spanSet)
    const parentSpan = trace.spanSet?.spans?.[0];
    if (!parentSpan) {
      logger.debug('No spans in trace', { traceID: trace.traceID });
      return null;
    }

    // Parse attributes
    const attrs = this.parseSpanAttributes(parentSpan.attributes);

    // Validate coordinates
    const latitude = this.parseCoordinate(attrs['geo.latitude']);
    const longitude = this.parseCoordinate(attrs['geo.longitude']);

    if (latitude === null || longitude === null) {
      logger.debug('Parent span missing geo coordinates', {
        traceID: trace.traceID,
        hasLatitude: attrs['geo.latitude'] !== undefined,
        hasLongitude: attrs['geo.longitude'] !== undefined
      });
      return null;
    }

    // Validate coordinate ranges
    if (!this.validateCoordinates(latitude, longitude)) {
      logger.warn('Invalid coordinates in parent span', {
        traceID: trace.traceID,
        latitude,
        longitude
      });
      return null;
    }

    return {
      country: attrs['geo.country'] || 'Unknown',
      countryCode: attrs['geo.country_code'],
      city: attrs['geo.city'] || null,
      latitude,
      longitude,
      timezone: attrs['geo.timezone'],
      source: 'parent-span'
    };
  }

  /**
   * Read geo from child span (old traces)
   *
   * Fetches full OTLP trace and finds child span with name="fingerprint.geoip_lookup"
   *
   * @private
   * @param trace - Tempo trace
   * @returns GeoLocation or null if child span not found
   */
  private async readGeoFromChildSpan(trace: TempoTrace): Promise<GeoLocation | null> {
    const logger = getLogger();

    try {
      // Fetch full OTLP trace
      const fullTrace = await this.fetchFullTrace(trace.traceID);
      if (!fullTrace) {
        logger.debug('Failed to fetch full trace', { traceID: trace.traceID });
        return null;
      }

      // Find child span with name "fingerprint.geoip_lookup"
      for (const batch of fullTrace.batches || []) {
        for (const scopeSpan of batch.scopeSpans || []) {
          for (const span of scopeSpan.spans || []) {
            // Match child span by name
            if (span.name === 'fingerprint.geoip_lookup') {
              const attrs = this.parseSpanAttributes(span.attributes);

              const latitude = this.parseCoordinate(attrs['geo.latitude']);
              const longitude = this.parseCoordinate(attrs['geo.longitude']);

              // Validate coordinates
              if (latitude !== null && longitude !== null && this.validateCoordinates(latitude, longitude)) {
                logger.debug('Found geo data in child span', {
                  traceID: trace.traceID,
                  spanName: span.name
                });

                return {
                  country: attrs['geo.country'] || 'Unknown',
                  countryCode: attrs['geo.country_code'],
                  city: attrs['geo.city'] || null,
                  latitude,
                  longitude,
                  timezone: attrs['geo.timezone'],
                  source: 'child-span'
                };
              }
            }
          }
        }
      }

      logger.debug('No valid child span found', { traceID: trace.traceID });
      return null;
    } catch (error) {
      logger.warn('Error reading geo from child span', {
        traceID: trace.traceID,
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
  }

  /**
   * Fetch full OTLP trace from Tempo
   *
   * @private
   * @param traceID - Trace ID
   * @returns Full OTLP trace or null if fetch fails
   */
  private async fetchFullTrace(traceID: string): Promise<OTLPTraceResponse | null> {
    const logger = getLogger();
    const url = `${this.tempoUrl}/api/traces/${traceID}`;

    try {
      const response = await fetch(url, {
        headers: { 'Accept': 'application/json' }
      });

      if (!response.ok) {
        logger.debug('Failed to fetch full trace', {
          traceID,
          status: response.status,
          statusText: response.statusText
        });
        return null;
      }

      return await response.json() as OTLPTraceResponse;
    } catch (error) {
      logger.warn('Error fetching full trace', {
        traceID,
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
  }

  /**
   * Parse span attributes to key-value map
   *
   * @private
   * @param attributes - Span attributes
   * @returns Key-value map
   */
  parseSpanAttributes(attributes: SpanAttribute[]): Record<string, string> {
    const attrs: Record<string, string> = {};

    for (const attr of attributes || []) {
      // Convert all attribute types to strings
      if (attr.value.stringValue !== undefined) {
        attrs[attr.key] = attr.value.stringValue;
      } else if (attr.value.intValue !== undefined) {
        attrs[attr.key] = String(attr.value.intValue);
      } else if (attr.value.doubleValue !== undefined) {
        attrs[attr.key] = String(attr.value.doubleValue);
      } else if (attr.value.boolValue !== undefined) {
        attrs[attr.key] = String(attr.value.boolValue);
      }
    }

    return attrs;
  }

  /**
   * Parse coordinate string to number
   *
   * @param value - Coordinate value (string or undefined)
   * @returns Parsed coordinate or null if invalid
   */
  parseCoordinate(value: string | undefined): number | null {
    if (!value) return null;

    const parsed = parseFloat(value);
    if (isNaN(parsed)) return null;

    return parsed;
  }

  /**
   * Validate coordinate ranges
   *
   * @param latitude - Latitude (-90 to 90)
   * @param longitude - Longitude (-180 to 180)
   * @returns true if coordinates are valid
   */
  validateCoordinates(latitude: number, longitude: number): boolean {
    return (
      latitude >= -90 && latitude <= 90 &&
      longitude >= -180 && longitude <= 180
    );
  }

  /**
   * Get cached geo location
   *
   * @private
   * @param traceID - Trace ID
   * @returns Cached geo location or undefined if not cached or expired
   */
  private getCached(traceID: string): GeoLocation | null | undefined {
    const entry = this.cache.get(traceID);
    if (!entry) return undefined;

    // Check TTL
    const now = Date.now();
    if (now - entry.fetchedAt > this.cacheTTL) {
      this.cache.delete(traceID);
      this.cacheStats.size = this.cache.size;
      return undefined;
    }

    return entry.geoLocation;
  }

  /**
   * Set cached geo location
   *
   * @private
   * @param traceID - Trace ID
   * @param geoLocation - Geo location to cache
   */
  private setCached(traceID: string, geoLocation: GeoLocation | null): void {
    this.cache.set(traceID, {
      geoLocation,
      fetchedAt: Date.now()
    });
    this.cacheStats.size = this.cache.size;
  }
}

// Backward compatibility alias
export const ChildSpanReader = SpanReader;
