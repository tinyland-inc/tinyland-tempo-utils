


























import { getLogger, getTempoBaseUrl } from './config.js';




export interface GeoLocation {
  country: string;
  countryCode?: string;
  city: string | null;
  latitude: number | null;
  longitude: number | null;
  timezone?: string | null;
  source: 'parent-span' | 'child-span'; 
}




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




export interface TempoSpan {
  spanID: string;
  name?: string;
  startTimeUnixNano: string;
  durationNanos: string;
  attributes: SpanAttribute[];
}




export interface SpanAttribute {
  key: string;
  value: {
    stringValue?: string;
    intValue?: string | number;
    doubleValue?: number;
    boolValue?: boolean;
  };
}




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




interface CacheEntry {
  geoLocation: GeoLocation | null;
  fetchedAt: number; 
}




interface CacheStats {
  hits: number;
  misses: number;
  size: number;
}




export interface SpanReaderOptions {
  cacheEnabled?: boolean; 
  cacheTTL?: number; 
  maxConcurrency?: number; 
  tempoUrl?: string; 
}













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
    this.cacheTTL = options.cacheTTL ?? 300000; 
    this.maxConcurrency = options.maxConcurrency ?? 10;

    const logger = getLogger();
    logger.debug('ChildSpanReader initialized', {
      cacheEnabled: this.cacheEnabled,
      cacheTTL: this.cacheTTL,
      maxConcurrency: this.maxConcurrency
    });
  }

  















  async readGeo(trace: TempoTrace): Promise<GeoLocation | null> {
    const logger = getLogger();

    
    if (this.cacheEnabled) {
      const cached = this.getCached(trace.traceID);
      if (cached !== undefined) {
        this.cacheStats.hits++;
        logger.debug('Cache hit for geo data', { traceID: trace.traceID });
        return cached;
      }
      this.cacheStats.misses++;
    }

    
    const parentGeo = this.readGeoFromParentSpan(trace);
    if (parentGeo) {
      if (this.cacheEnabled) {
        this.setCached(trace.traceID, parentGeo);
      }
      return parentGeo;
    }

    
    const childGeo = await this.readGeoFromChildSpan(trace);
    if (this.cacheEnabled) {
      this.setCached(trace.traceID, childGeo);
    }
    return childGeo;
  }

  















  async readGeoBulk(traces: TempoTrace[]): Promise<Map<string, GeoLocation | null>> {
    const logger = getLogger();
    const results = new Map<string, GeoLocation | null>();
    const BATCH_SIZE = this.maxConcurrency;

    logger.debug('Bulk reading geo data', {
      traceCount: traces.length,
      batchSize: BATCH_SIZE
    });

    
    for (let i = 0; i < traces.length; i += BATCH_SIZE) {
      const batch = traces.slice(i, i + BATCH_SIZE);

      
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

  





  needsChildSpan(trace: TempoTrace): boolean {
    const parentGeo = this.readGeoFromParentSpan(trace);
    return parentGeo === null;
  }

  


  clearCache(): void {
    const logger = getLogger();
    const previousSize = this.cache.size;
    this.cache.clear();
    this.cacheStats.size = 0;
    logger.debug('Cache cleared', { previousSize });
  }

  


  getCacheStats(): CacheStats {
    return {
      ...this.cacheStats,
      size: this.cache.size
    };
  }

  






  private readGeoFromParentSpan(trace: TempoTrace): GeoLocation | null {
    const logger = getLogger();

    
    const parentSpan = trace.spanSet?.spans?.[0];
    if (!parentSpan) {
      logger.debug('No spans in trace', { traceID: trace.traceID });
      return null;
    }

    
    const attrs = this.parseSpanAttributes(parentSpan.attributes);

    
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

  








  private async readGeoFromChildSpan(trace: TempoTrace): Promise<GeoLocation | null> {
    const logger = getLogger();

    try {
      
      const fullTrace = await this.fetchFullTrace(trace.traceID);
      if (!fullTrace) {
        logger.debug('Failed to fetch full trace', { traceID: trace.traceID });
        return null;
      }

      
      for (const batch of fullTrace.batches || []) {
        for (const scopeSpan of batch.scopeSpans || []) {
          for (const span of scopeSpan.spans || []) {
            
            if (span.name === 'fingerprint.geoip_lookup') {
              const attrs = this.parseSpanAttributes(span.attributes);

              const latitude = this.parseCoordinate(attrs['geo.latitude']);
              const longitude = this.parseCoordinate(attrs['geo.longitude']);

              
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

  






  parseSpanAttributes(attributes: SpanAttribute[]): Record<string, string> {
    const attrs: Record<string, string> = {};

    for (const attr of attributes || []) {
      
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

  





  parseCoordinate(value: string | undefined): number | null {
    if (!value) return null;

    const parsed = parseFloat(value);
    if (isNaN(parsed)) return null;

    return parsed;
  }

  






  validateCoordinates(latitude: number, longitude: number): boolean {
    return (
      latitude >= -90 && latitude <= 90 &&
      longitude >= -180 && longitude <= 180
    );
  }

  






  private getCached(traceID: string): GeoLocation | null | undefined {
    const entry = this.cache.get(traceID);
    if (!entry) return undefined;

    
    const now = Date.now();
    if (now - entry.fetchedAt > this.cacheTTL) {
      this.cache.delete(traceID);
      this.cacheStats.size = this.cache.size;
      return undefined;
    }

    return entry.geoLocation;
  }

  






  private setCached(traceID: string, geoLocation: GeoLocation | null): void {
    this.cache.set(traceID, {
      geoLocation,
      fetchedAt: Date.now()
    });
    this.cacheStats.size = this.cache.size;
  }
}


export const ChildSpanReader = SpanReader;
