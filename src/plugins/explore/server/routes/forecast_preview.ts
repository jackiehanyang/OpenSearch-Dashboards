/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { schema } from '@osd/config-schema';
import type { Logger, IRouter } from '../../../../core/server';

const DEFAULT_BUCKET_MILLIS = 60_000;
const DEFAULT_SHINGLE_SIZE = 8;
const DEFAULT_HORIZON_DURATION_MILLIS = 60 * 60_000; // 1 hour ahead by default
const MAX_BUCKETS_TARGET = 10_000;
const TARGET_BUCKETS = 5_000;
const BUCKET_CANDIDATES_MILLIS = [
  60_000, // 1m
  5 * 60_000, // 5m
  15 * 60_000, // 15m
  60 * 60_000, // 1h
  6 * 60 * 60_000, // 6h
  24 * 60 * 60_000, // 1d
];

function createTempIndexName() {
  const suffix = Math.random().toString(16).slice(2, 10);
  return `.explore-forecast-preview-${Date.now()}-${suffix}`;
}

function createTempForecasterName() {
  const suffix = Math.random().toString(16).slice(2, 10);
  return `explore_forecast_preview_${Date.now()}_${suffix}`;
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function bucketize(points: Array<{ ts: number; value: number }>, bucketMillis: number) {
  const buckets = new Map<number, { sum: number; count: number }>();
  for (const p of points) {
    const ts = Number(p.ts);
    const value = Number(p.value);
    if (!Number.isFinite(ts) || !Number.isFinite(value)) continue;
    const b = Math.floor(ts / bucketMillis) * bucketMillis;
    const cur = buckets.get(b);
    if (cur) {
      cur.sum += value;
      cur.count += 1;
    } else {
      buckets.set(b, { sum: value, count: 1 });
    }
  }
  return Array.from(buckets.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([timestamp, agg]) => ({
      timestamp,
      value: agg.count > 0 ? agg.sum / agg.count : null,
    }))
    .filter((d) => d.value !== null) as Array<{ timestamp: number; value: number }>;
}

function pickBucketMillis(rangeMillis: number) {
  if (!Number.isFinite(rangeMillis) || rangeMillis <= 0) return DEFAULT_BUCKET_MILLIS;
  const raw = Math.ceil(rangeMillis / TARGET_BUCKETS);
  for (const c of BUCKET_CANDIDATES_MILLIS) {
    if (c >= raw) return c;
  }
  return BUCKET_CANDIDATES_MILLIS[BUCKET_CANDIDATES_MILLIS.length - 1];
}

function bucketMillisToPeriod(
  bucketMillis: number
): { interval: number; unit: 'Minutes' | 'Hours' | 'Days' } {
  const MIN = 60_000;
  const HOUR = 60 * MIN;
  const DAY = 24 * HOUR;
  if (bucketMillis % DAY === 0)
    return { interval: Math.max(1, Math.floor(bucketMillis / DAY)), unit: 'Days' };
  if (bucketMillis % HOUR === 0)
    return { interval: Math.max(1, Math.floor(bucketMillis / HOUR)), unit: 'Hours' };
  return { interval: Math.max(1, Math.floor(bucketMillis / MIN)), unit: 'Minutes' };
}

async function transportRequest(client: any, opts: { method: string; path: string; body?: any }) {
  const body = opts.body === undefined ? undefined : JSON.stringify(opts.body);
  return await client.transport.request({
    method: opts.method,
    path: opts.path,
    ...(body !== undefined ? { body } : {}),
  });
}

function looksLikeNoHandler(e: any) {
  const b = e?.body;
  const s = typeof b === 'string' ? b : b ? JSON.stringify(b) : '';
  const msg = e?.message ?? '';
  const combined = `${s}\n${msg}`;
  return combined.includes('no handler found for uri') || combined.includes('no handler found');
}

export function registerForecastPreviewRoutes({
  router,
  logger,
}: {
  router: IRouter;
  logger: Logger;
}) {
  router.post(
    {
      path: '/api/explore/forecast_preview',
      validate: {
        body: schema.object({
          points: schema.arrayOf(
            schema.object({
              ts: schema.number(),
              value: schema.number(),
            })
          ),
          startTime: schema.maybe(schema.number()),
          endTime: schema.maybe(schema.number()),
          bucketSizeSeconds: schema.maybe(schema.number()),
          shingleSize: schema.maybe(schema.number()),
          horizon: schema.maybe(schema.number()),
        }),
      },
    },
    async (context, request, response) => {
      const client = context.core.opensearch.client.asCurrentUser;
      const {
        points,
        startTime,
        endTime,
        bucketSizeSeconds,
        shingleSize = DEFAULT_SHINGLE_SIZE,
        horizon,
      } = request.body as any;

      if (!points?.length) {
        return response.ok({ body: { ok: false, message: 'points must be non-empty' } });
      }

      const ptsSorted = [...points].sort((a, b) => a.ts - b.ts);
      const ptsStart = ptsSorted[0]?.ts;
      const ptsEnd = ptsSorted[ptsSorted.length - 1]?.ts;
      const periodStart = Number.isFinite(startTime) ? startTime : ptsStart;
      const periodEnd = Number.isFinite(endTime) ? endTime : ptsEnd;
      const rangeMillis =
        Number.isFinite(periodStart) && Number.isFinite(periodEnd) && periodEnd > periodStart
          ? periodEnd - periodStart
          : 0;

      let effectiveBucketMillis = pickBucketMillis(rangeMillis);
      if (Number.isFinite(bucketSizeSeconds) && bucketSizeSeconds > 0) {
        effectiveBucketMillis = Math.max(
          DEFAULT_BUCKET_MILLIS,
          Math.floor(bucketSizeSeconds * 1000)
        );
      }

      let docs = bucketize(points, effectiveBucketMillis);
      while (docs.length > MAX_BUCKETS_TARGET) {
        const next = BUCKET_CANDIDATES_MILLIS.find((c) => c > effectiveBucketMillis);
        if (!next) break;
        effectiveBucketMillis = next;
        docs = bucketize(points, effectiveBucketMillis);
      }
      if (!docs.length) {
        return response.ok({
          body: { ok: false, message: 'No valid points to preview after bucketing' },
        });
      }

      const period = bucketMillisToPeriod(effectiveBucketMillis);
      const horizonBuckets =
        Number.isFinite(horizon) && horizon > 0
          ? Math.floor(horizon)
          : Math.max(1, Math.floor(DEFAULT_HORIZON_DURATION_MILLIS / effectiveBucketMillis));

      const tempIndex = createTempIndexName();
      let forecasterId: string | undefined;

      try {
        // Create hidden temp index
        await client.indices.create({
          index: tempIndex,
          body: {
            settings: {
              index: {
                hidden: true,
                number_of_shards: 1,
                number_of_replicas: 0,
                refresh_interval: -1,
              },
            },
            mappings: {
              dynamic: false,
              properties: {
                timestamp: { type: 'date', format: 'epoch_millis' },
                value: { type: 'double' },
              },
            },
          },
        });

        const bulkBody: any[] = [];
        for (const d of docs) {
          bulkBody.push({ index: { _index: tempIndex } });
          bulkBody.push({ timestamp: d.timestamp, value: d.value });
        }
        await client.bulk({ body: bulkBody, refresh: true });

        // Create a temporary forecaster config (single-stream)
        const createBody = {
          name: createTempForecasterName(),
          description: 'Forecast preview from Explore Metrics',
          time_field: 'timestamp',
          indices: [tempIndex],
          filter_query: { match_all: {} },
          feature_attributes: [
            {
              feature_name: 'value_avg',
              feature_enabled: true,
              aggregation_query: {
                value_avg: { avg: { field: 'value' } },
              },
            },
          ],
          forecast_interval: { period },
          window_delay: { period: { interval: 0, unit: 'Minutes' } },
          shingle_size: shingleSize,
          horizon: horizonBuckets,
        };

        let createResp: any;
        try {
          createResp = await transportRequest(client, {
            method: 'POST',
            path: '/_plugins/_forecast/forecasters',
            body: createBody,
          });
        } catch (e: any) {
          if (looksLikeNoHandler(e)) {
            return response.ok({
              body: {
                ok: false,
                message:
                  'Forecasting plugin endpoint not found on this OpenSearch cluster. Install/enable the Forecast plugin to use preview.',
                details: e?.body ?? e?.message,
              },
            });
          }
          throw e;
        }

        forecasterId = createResp?.body?._id ?? createResp?._id;
        if (!forecasterId) {
          return response.ok({
            body: {
              ok: false,
              message: 'Failed to create temporary forecaster',
              details: createResp?.body ?? createResp,
            },
          });
        }

        // Run one analysis (run-once)
        const runOnceResp = await transportRequest(client, {
          method: 'POST',
          path: `/_plugins/_forecast/forecasters/${encodeURIComponent(forecasterId)}/_run_once`,
          body: {}, // API accepts empty body in UI usage
        });

        const taskId =
          runOnceResp?.body?.task_id ??
          runOnceResp?.body?.taskId ??
          runOnceResp?.body?.id ??
          runOnceResp?.body ??
          undefined;

        if (!taskId || typeof taskId !== 'string') {
          return response.ok({
            body: {
              ok: false,
              message: 'Forecast run-once did not return a task id',
              details: runOnceResp?.body ?? runOnceResp,
            },
          });
        }

        // Poll forecast result index for results with this task_id.
        const deadline = Date.now() + 30_000;
        let hits: any[] = [];
        while (Date.now() < deadline) {
          const searchResp = await transportRequest(client, {
            method: 'POST',
            path: `/opensearch-forecast-results*/_search`,
            body: {
              size: 10000,
              sort: [{ forecast_data_end_time: 'asc' }],
              query: {
                bool: {
                  filter: [
                    { term: { task_id: taskId } },
                    { exists: { field: 'forecast_data_end_time' } },
                  ],
                },
              },
            },
          });
          hits = searchResp?.body?.hits?.hits ?? [];
          if (hits.length) break;
          await sleep(1500);
        }

        const forecastPoints = hits
          .map((h: any) => {
            const s = h?._source ?? {};
            const t = s.forecast_data_end_time ?? s.forecast_data_start_time;
            const v = s.forecast_value;
            const lo = s.forecast_lower_bound;
            const hi = s.forecast_upper_bound;
            const toNum = (x: any) => (x != null && x !== 'NaN' ? Number(x) : NaN);
            const tt = Number(t);
            const vv = toNum(v);
            const ll = toNum(lo);
            const hh = toNum(hi);
            if (!Number.isFinite(tt) || !Number.isFinite(vv)) return null;
            return {
              t: tt,
              v: vv,
              lo: Number.isFinite(ll) ? ll : undefined,
              hi: Number.isFinite(hh) ? hh : undefined,
            };
          })
          .filter(Boolean);

        // If the forecast plugin returns nothing within timeout, still return ok with empty points
        return response.ok({
          body: {
            ok: true,
            response: {
              taskId,
              periodStart,
              periodEnd,
              points: forecastPoints,
            },
            meta: {
              bucketSizeMillis: effectiveBucketMillis,
              pointsIn: points.length,
              pointsBucketed: docs.length,
              forecasterId,
              forecastInterval: period,
              horizon: horizonBuckets,
            },
          },
        });
      } catch (e: any) {
        logger.error(`Explore Forecast preview failed: ${e?.message ?? e}`);
        return response.ok({
          body: {
            ok: false,
            message:
              e?.body?.error?.reason ??
              e?.body?.error?.root_cause?.[0]?.reason ??
              e?.body?.error?.type ??
              (typeof e?.body === 'string' ? e.body : undefined) ??
              e?.message ??
              'Failed to preview forecast',
            details: e?.body?.error ?? e?.body ?? undefined,
          },
        });
      } finally {
        // Best-effort cleanup
        if (forecasterId) {
          try {
            await transportRequest(client, {
              method: 'DELETE',
              path: `/_plugins/_forecast/forecasters/${encodeURIComponent(forecasterId)}`,
            });
          } catch (e: any) {
            logger.debug(
              `Explore Forecast preview cleanup failed for forecaster ${forecasterId}: ${
                e?.message ?? e
              }`
            );
          }
        }
        try {
          await client.indices.delete({ index: tempIndex });
        } catch (e: any) {
          logger.debug(
            `Explore Forecast preview cleanup failed for ${tempIndex}: ${e?.message ?? e}`
          );
        }
      }
    }
  );
}
