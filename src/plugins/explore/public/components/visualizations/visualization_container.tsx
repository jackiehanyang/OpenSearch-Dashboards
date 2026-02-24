/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import './visualization_container.scss';
import { EuiPanel } from '@elastic/eui';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import moment from 'moment';
import { useDispatch } from 'react-redux';
import { useObservable } from 'react-use';
import dateMath from '@elastic/datemath';

import './visualization_container.scss';
import { AxisColumnMappings } from './types';
import { useTabResults } from '../../application/utils/hooks/use_tab_results';
import { useSearchContext } from '../query_panel/utils/use_search_context';
import { getVisualizationBuilder } from './visualization_builder';
import { TimeRange } from '../../../../data/common';
import { useOpenSearchDashboards } from '../../../../opensearch_dashboards_react/public';
import { ExploreServices } from '../../types';
import {
  clearQueryStatusMap,
  clearResults,
  setDateRange,
} from '../../application/utils/state_management/slices';
import { executeQueries } from '../../application/utils/state_management/actions/query_actions';

export interface UpdateVisualizationProps {
  mappings: AxisColumnMappings;
}
// TODO: add back notifications
// const VISUALIZATION_TOAST_MSG = {
//   useRule: i18n.translate('explore.visualize.toast.useRule', {
//     defaultMessage: 'Cannot apply previous configured visualization, use rule matched',
//   }),
//   reset: i18n.translate('explore.visualize.toast.reset', {
//     defaultMessage: 'Cannot apply previous configured visualization, reset',
//   }),
//   metricReset: i18n.translate('explore.visualize.toast.metricReset', {
//     defaultMessage: 'Cannot apply metric type visualization, reset',
//   }),
//   switchReset: i18n.translate('explore.visualize.toast.switchReset', {
//     defaultMessage: 'Cannot apply configured visualization to the current chart type, reset',
//   }),
// };

export const VisualizationContainer = React.memo(() => {
  const { services } = useOpenSearchDashboards<ExploreServices>();
  const { results } = useTabResults();
  const searchContext = useSearchContext();
  const dispatch = useDispatch();

  const visualizationBuilder = getVisualizationBuilder();
  const visData = useObservable(visualizationBuilder.data$);
  const chartConfig = useObservable(visualizationBuilder.visConfig$);
  const [adPreview, setAdPreview] = useState<any | null>(null);
  const [adPreviewError, setAdPreviewError] = useState<string>('');
  const lastPreviewKeyRef = React.useRef<string>('');
  const [forecastPreview, setForecastPreview] = useState<any | null>(null);
  const [forecastPreviewError, setForecastPreviewError] = useState<string>('');
  const lastForecastKeyRef = React.useRef<string>('');
  const [forecastClampToZero, setForecastClampToZero] = useState<boolean>(false);

  useEffect(() => {
    if (results) {
      const rows = results.hits?.hits || [];
      const fieldSchema = results.fieldSchema || [];
      visualizationBuilder.handleData(rows, fieldSchema);
    }
  }, [visualizationBuilder, results]);

  useEffect(() => {
    visualizationBuilder.init();
    return () => {
      // reset visualization builder
      visualizationBuilder.reset();
    };
  }, [visualizationBuilder]);

  const onSelectTimeRange = useCallback(
    (timeRange?: TimeRange) => {
      if (timeRange) {
        dispatch(
          setDateRange({
            from: moment(timeRange.from).toISOString(),
            to: moment(timeRange.to).toISOString(),
          })
        );
        dispatch(clearResults());
        dispatch(clearQueryStatusMap());
        dispatch(executeQueries({ services }));
      }
    },
    [services, dispatch]
  );

  const allVisColumns = useMemo(() => {
    return [
      ...(visData?.numericalColumns ?? []),
      ...(visData?.categoricalColumns ?? []),
      ...(visData?.dateColumns ?? []),
    ];
  }, [visData?.numericalColumns, visData?.categoricalColumns, visData?.dateColumns]);

  const resolveKey = useCallback(
    (preferredName: string | undefined, fallbackMatchers: RegExp[], candidateRows: any[]) => {
      if (preferredName) {
        const col = allVisColumns.find((c) => c.name === preferredName);
        return col?.column ?? preferredName;
      }
      if (allVisColumns.length) {
        const matchCol = allVisColumns.find((c) => fallbackMatchers.some((re) => re.test(c.name)));
        if (matchCol) return matchCol.column;
      }
      const first = candidateRows?.[0];
      if (!first) return undefined;
      const keys = Object.keys(first);
      return keys.find((k) => fallbackMatchers.some((re) => re.test(k))) || undefined;
    },
    [allVisColumns]
  );

  const parseTimeRangeToEpoch = useCallback((tr?: { from?: string; to?: string }) => {
    const from = tr?.from ? dateMath.parse(tr.from)?.valueOf() : undefined;
    const to = tr?.to ? dateMath.parse(tr.to, { roundUp: true })?.valueOf() : undefined;
    return { from, to };
  }, []);

  // Auto-run AD preview when Visualization tab has data/timeRange.
  useEffect(() => {
    let cancelled = false;
    async function run() {
      const tr = searchContext?.timeRange;
      const { from, to } = parseTimeRangeToEpoch(tr);

      // Candidate rows: prefer normalized vis rows; fallback to Prometheus instant hits.
      const candidateRows: Array<Record<string, any>> = visData?.transformedData?.length
        ? (visData.transformedData as any[])
        : Array.isArray((results as any)?.instantHits?.hits)
        ? (results as any).instantHits.hits.map((h: any) => h?._source).filter(Boolean)
        : Array.isArray((results as any)?.hits?.hits)
        ? (results as any).hits.hits.map((h: any) => h?._source).filter(Boolean)
        : [];

      const axesMapping = chartConfig?.axesMapping ?? {};
      const effectiveTimeKey = resolveKey(
        axesMapping.x,
        [/^time$/i, /timestamp/i, /@timestamp/i],
        candidateRows
      );
      const effectiveValueKey = resolveKey(
        axesMapping.y,
        [/^value$/i, /^value #/i, /val/i],
        candidateRows
      );
      const effectiveSeriesKey = resolveKey(
        axesMapping.color,
        [/^series$/i, /^metric$/i, /label/i],
        candidateRows
      );

      if (!effectiveTimeKey || !effectiveValueKey || candidateRows.length === 0) {
        return;
      }

      // Pick the first series value (if series column exists) to keep the preview deterministic.
      const seriesValue = effectiveSeriesKey
        ? String(
            candidateRows.find((r) => r?.[effectiveSeriesKey] != null)?.[effectiveSeriesKey] ?? ''
          )
        : '';

      const points: Array<{ ts: number; value: number }> = [];
      for (const row of candidateRows) {
        if (effectiveSeriesKey && seriesValue && String(row?.[effectiveSeriesKey]) !== seriesValue)
          continue;
        const rawTs = row?.[effectiveTimeKey];
        const rawV = row?.[effectiveValueKey];
        const ts =
          typeof rawTs === 'number' ? rawTs : rawTs ? new Date(String(rawTs)).getTime() : NaN;
        const value = typeof rawV === 'number' ? rawV : rawV != null ? Number(rawV) : NaN;
        if (Number.isFinite(ts) && Number.isFinite(value)) points.push({ ts, value });
      }
      points.sort((a, b) => a.ts - b.ts);
      if (points.length === 0) return;

      const previewKey = `${tr?.from ?? ''}|${tr?.to ?? ''}|${points.length}|${seriesValue}`;
      if (lastPreviewKeyRef.current === previewKey) return;
      lastPreviewKeyRef.current = previewKey;

      setAdPreviewError('');

      try {
        const resp = await services.http.post('/api/explore/anomaly_preview', {
          body: JSON.stringify({
            points,
            startTime: from,
            endTime: to,
            shingleSize: 8,
          }),
        });
        if (cancelled) return;
        if ((resp as any)?.ok === true) {
          setAdPreview(resp);
        } else {
          setAdPreview(null);
          setAdPreviewError((resp as any)?.message ?? 'AD preview failed');
        }
      } catch (e: any) {
        if (cancelled) return;
        setAdPreview(null);
        setAdPreviewError(e?.body?.message ?? e?.message ?? 'AD preview failed');
      }
    }
    run();
    return () => {
      cancelled = true;
    };
  }, [
    chartConfig?.axesMapping,
    parseTimeRangeToEpoch,
    resolveKey,
    results,
    searchContext?.timeRange,
    services.http,
    visData?.transformedData,
  ]);

  // Auto-run Forecast preview when Visualization tab has data/timeRange.
  useEffect(() => {
    let cancelled = false;
    async function run() {
      const tr = searchContext?.timeRange;
      const { from, to } = parseTimeRangeToEpoch(tr);

      const candidateRows: Array<Record<string, any>> = visData?.transformedData?.length
        ? (visData.transformedData as any[])
        : Array.isArray((results as any)?.instantHits?.hits)
        ? (results as any).instantHits.hits.map((h: any) => h?._source).filter(Boolean)
        : Array.isArray((results as any)?.hits?.hits)
        ? (results as any).hits.hits.map((h: any) => h?._source).filter(Boolean)
        : [];

      const axesMapping = chartConfig?.axesMapping ?? {};
      const effectiveTimeKey = resolveKey(
        axesMapping.x,
        [/^time$/i, /timestamp/i, /@timestamp/i],
        candidateRows
      );
      const effectiveValueKey = resolveKey(
        axesMapping.y,
        [/^value$/i, /^value #/i, /val/i],
        candidateRows
      );
      const effectiveSeriesKey = resolveKey(
        axesMapping.color,
        [/^series$/i, /^metric$/i, /label/i],
        candidateRows
      );

      if (!effectiveTimeKey || !effectiveValueKey || candidateRows.length === 0) {
        return;
      }

      const seriesValue = effectiveSeriesKey
        ? String(
            candidateRows.find((r) => r?.[effectiveSeriesKey] != null)?.[effectiveSeriesKey] ?? ''
          )
        : '';

      const points: Array<{ ts: number; value: number }> = [];
      for (const row of candidateRows) {
        if (effectiveSeriesKey && seriesValue && String(row?.[effectiveSeriesKey]) !== seriesValue)
          continue;
        const rawTs = row?.[effectiveTimeKey];
        const rawV = row?.[effectiveValueKey];
        const ts =
          typeof rawTs === 'number' ? rawTs : rawTs ? new Date(String(rawTs)).getTime() : NaN;
        const value = typeof rawV === 'number' ? rawV : rawV != null ? Number(rawV) : NaN;
        if (Number.isFinite(ts) && Number.isFinite(value)) points.push({ ts, value });
      }
      points.sort((a, b) => a.ts - b.ts);
      if (points.length === 0) return;

      // Decide clamp policy from the actual metric points we are forecasting.
      // If the observed series is non-negative, clamp forecast + bounds to >= 0 for UX sanity.
      const minObserved = points.reduce(
        (m, p) => (p.value < m ? p.value : m),
        Number.POSITIVE_INFINITY
      );
      setForecastClampToZero(Number.isFinite(minObserved) && minObserved >= 0);

      const forecastKey = `${tr?.from ?? ''}|${tr?.to ?? ''}|${points.length}|${seriesValue}`;
      if (lastForecastKeyRef.current === forecastKey) return;
      lastForecastKeyRef.current = forecastKey;

      setForecastPreviewError('');
      try {
        const resp = await services.http.post('/api/explore/forecast_preview', {
          body: JSON.stringify({
            points,
            startTime: from,
            endTime: to,
            shingleSize: 8,
          }),
        });
        if (cancelled) return;
        if ((resp as any)?.ok === true) {
          setForecastPreview(resp);
        } else {
          setForecastPreview(null);
          setForecastPreviewError((resp as any)?.message ?? 'Forecast preview failed');
        }
      } catch (e: any) {
        if (cancelled) return;
        setForecastPreview(null);
        setForecastPreviewError(e?.body?.message ?? e?.message ?? 'Forecast preview failed');
      }
    }
    run();
    return () => {
      cancelled = true;
    };
  }, [
    chartConfig?.axesMapping,
    parseTimeRangeToEpoch,
    resolveKey,
    results,
    searchContext?.timeRange,
    services.http,
    visData?.transformedData,
  ]);

  const augmentEchartsSpec = useCallback(
    (spec: any, ctx: { timeRange: TimeRange }) => {
      if (!spec) return spec;

      // Only augment simple single-grid charts for now (skip faceted/multi-grid).
      if (Array.isArray(spec.grid)) {
        return spec;
      }

      const xAxes = Array.isArray(spec.xAxis) ? spec.xAxis : [spec.xAxis ?? { type: 'time' }];
      const yAxes = Array.isArray(spec.yAxis) ? spec.yAxis : [spec.yAxis ?? { type: 'value' }];
      const baseGrid = spec.grid && typeof spec.grid === 'object' ? spec.grid : {};

      const left = baseGrid.left ?? 40;
      const right = baseGrid.right ?? 30;

      // Clamp policy is computed from the same points we send to forecast_preview.
      const clampForecastToZero = forecastClampToZero;

      const augmented: any = {
        ...spec,
        grid: {
          ...baseGrid,
          left,
          right,
        },
        xAxis: xAxes,
        yAxis: [...yAxes],
        tooltip: spec.tooltip,
        series: [...(Array.isArray(spec.series) ? spec.series : [])],
      };

      // -------------------------
      // Forecast overlay (always independent from AD preview)
      // -------------------------
      if (forecastPreview?.ok === true && Array.isArray(forecastPreview?.response?.points)) {
        augmented.series.push(
          ...((() => {
            const pts = (forecastPreview.response.points as any[])
              .map((p) => {
                const t = p?.t;
                const v = p?.v;
                const lo = p?.lo;
                const hi = p?.hi;
                if (!Number.isFinite(t) || !Number.isFinite(v)) return null;
                const clamp = (x: any) => {
                  const n = typeof x === 'number' ? x : x != null ? Number(x) : NaN;
                  if (!Number.isFinite(n)) return undefined;
                  return clampForecastToZero ? Math.max(0, n) : n;
                };
                const vv = clamp(v);
                const ll = clamp(lo);
                const hh = clamp(hi);
                if (vv === undefined) return null;
                // Normalize bounds if both exist
                const lower = ll !== undefined && hh !== undefined ? Math.min(ll, hh) : ll;
                const upper = ll !== undefined && hh !== undefined ? Math.max(ll, hh) : hh;
                return { t, v: vv, lo: lower, hi: upper };
              })
              .filter(Boolean);
            if (!pts.length) return [];

            const dataForecast = pts.map((p) => [p.t, p.v]);
            const dataLower = pts.map((p) => [p.t, Number.isFinite(p.lo) ? p.lo : p.v]);
            const dataUpperMinusLower = pts.map((p) => {
              const lo = Number.isFinite(p.lo) ? p.lo : p.v;
              const hi = Number.isFinite(p.hi) ? p.hi : p.v;
              return [p.t, Math.max(0, hi - lo)];
            });

            return [
              {
                name: 'Forecast lower',
                type: 'line',
                xAxisIndex: 0,
                yAxisIndex: 0,
                data: dataLower,
                stack: 'forecastBand',
                symbol: 'none',
                lineStyle: { opacity: 0 },
                tooltip: { show: false },
                z: 2,
              },
              {
                name: 'Forecast band',
                type: 'line',
                xAxisIndex: 0,
                yAxisIndex: 0,
                data: dataUpperMinusLower,
                stack: 'forecastBand',
                symbol: 'none',
                lineStyle: { opacity: 0 },
                areaStyle: { color: 'rgba(250, 164, 58, 0.18)' },
                tooltip: { show: false },
                z: 2,
              },
              {
                name: 'Forecast',
                type: 'line',
                xAxisIndex: 0,
                yAxisIndex: 0,
                data: dataForecast,
                symbol: 'none',
                lineStyle: { width: 2, type: 'dashed', color: '#FAA43A' },
                z: 3,
              },
            ];
          })() as any[])
        );
      }

      // -------------------------
      // AD overlay (only if AD preview succeeded)
      // -------------------------
      if (adPreview?.ok === true) {
        const anomalies = adPreview?.response?.anomaly_result ?? [];
        const bucketMs: number = Number(adPreview?.meta?.bucketSizeMillis) || 60_000;

        const overlayAxisIndex = augmented.yAxis.length;
        const overlayYAxis = {
          type: 'value',
          min: 0,
          max: 1,
          axisLabel: { show: false },
          axisTick: { show: false },
          axisLine: { show: false },
          splitLine: { show: false },
        };
        augmented.yAxis = [...augmented.yAxis, overlayYAxis];

        const markerData = (Array.isArray(anomalies) ? anomalies : [])
          .map((a: any) => {
            const t = a.data_end_time ?? a.data_start_time;
            const g = Number(a.anomaly_grade ?? 0);
            const c = Number(a.confidence ?? 0);
            if (t == null || !Number.isFinite(g) || g <= 0) return null;
            return {
              value: [t, 0],
              anomaly_grade: g,
              confidence: Number.isFinite(c) ? c : 0,
            };
          })
          .filter(Boolean);

        const highlightTimes = Array.from(
          new Set(
            (markerData as any[]).map((d) => Math.floor(Number(d.value?.[0]) / bucketMs) * bucketMs)
          )
        )
          .filter((t) => Number.isFinite(t))
          .sort((a, b) => a - b);

        const anomalyByBucket = new Map<number, { anomalyGrade: number; confidence: number }>();
        for (const a of Array.isArray(anomalies) ? anomalies : []) {
          const t = a.data_end_time ?? a.data_start_time;
          const g = Number(a.anomaly_grade ?? 0);
          const c = Number(a.confidence ?? 0);
          if (!Number.isFinite(t) || !Number.isFinite(g) || g <= 0) continue;
          const bucket = Math.floor(Number(t) / bucketMs) * bucketMs;
          const existing = anomalyByBucket.get(bucket);
          if (!existing || g > existing.anomalyGrade) {
            anomalyByBucket.set(bucket, {
              anomalyGrade: g,
              confidence: Number.isFinite(c) ? c : 0,
            });
          }
        }

        const baseTooltip = augmented.tooltip ?? spec.tooltip ?? {};
        const baseFormatter =
          typeof baseTooltip?.formatter === 'function' ? baseTooltip.formatter : undefined;

        augmented.tooltip = {
          ...baseTooltip,
          trigger: 'axis',
          formatter: (params: any) => {
            try {
              if (baseFormatter) {
                const base = baseFormatter(params);
                const axisVal = params?.[0]?.axisValue ?? params?.[0]?.value?.[0];
                const axisMs =
                  typeof axisVal === 'number'
                    ? axisVal
                    : axisVal
                    ? Date.parse(String(axisVal))
                    : NaN;
                const bucket = Number.isFinite(axisMs)
                  ? Math.floor(axisMs / bucketMs) * bucketMs
                  : NaN;
                const hit = Number.isFinite(bucket) ? anomalyByBucket.get(bucket) : undefined;
                if (!hit) return base;
                return `${base}<br/><span style="color:#D36086">▲</span> Anomaly grade: ${hit.anomalyGrade.toFixed(
                  2
                )}<br/><span style="color:#54B399">●</span> Confidence: ${hit.confidence.toFixed(
                  2
                )}`;
              }

              const list = Array.isArray(params) ? params : [params];
              const axisLabel = list?.[0]?.axisValueLabel ?? list?.[0]?.axisValue ?? '';
              const lines = [String(axisLabel)];
              for (const p of list) {
                if (p?.seriesName === 'Anomalies (preview)') continue;
                const v = Array.isArray(p?.data)
                  ? p.data[1]
                  : Array.isArray(p?.value)
                  ? p.value[1]
                  : p?.value;
                lines.push(`${p.marker ?? ''}${p.seriesName}: ${v}`);
              }
              const axisVal = list?.[0]?.axisValue ?? list?.[0]?.value?.[0];
              const axisMs =
                typeof axisVal === 'number' ? axisVal : axisVal ? Date.parse(String(axisVal)) : NaN;
              const bucket = Number.isFinite(axisMs)
                ? Math.floor(axisMs / bucketMs) * bucketMs
                : NaN;
              const hit = Number.isFinite(bucket) ? anomalyByBucket.get(bucket) : undefined;
              if (hit) {
                lines.push(
                  `<span style="color:#D36086">▲</span> Anomaly grade: ${hit.anomalyGrade.toFixed(
                    2
                  )}`
                );
                lines.push(
                  `<span style="color:#54B399">●</span> Confidence: ${hit.confidence.toFixed(2)}`
                );
              }
              return lines.join('<br/>');
            } catch {
              return '';
            }
          },
        };

        if (highlightTimes.length) {
          augmented.series.push({
            name: 'Anomaly highlights (preview)',
            type: 'line',
            xAxisIndex: 0,
            yAxisIndex: 0,
            data: [],
            markLine: {
              silent: true,
              symbol: 'none',
              lineStyle: {
                color: 'rgba(211, 96, 134, 0.25)',
                width: 1,
              },
              data: highlightTimes.map((t) => ({ xAxis: t })),
            },
            tooltip: { show: false },
            z: 5,
          });
        }

        augmented.series.push({
          name: 'Anomalies (preview)',
          type: 'scatter',
          xAxisIndex: 0,
          yAxisIndex: overlayAxisIndex,
          symbol: 'triangle',
          symbolOffset: [0, '50%'],
          itemStyle: { color: '#D36086' },
          z: 10,
          data: markerData as any[],
          symbolSize: (_val: any, params: any) => {
            const g = params?.data?.anomaly_grade ?? 0;
            return 6 + Math.min(14, Math.max(0, g * 12));
          },
          tooltip: {
            formatter: (p: any) => {
              const g = p?.data?.anomaly_grade ?? 0;
              const c = p?.data?.confidence ?? 0;
              return `Anomaly grade: ${Number(g).toFixed?.(2) ?? g}<br/>Confidence: ${
                Number(c).toFixed?.(2) ?? c
              }`;
            },
          },
        });
      }

      return augmented;
    },
    [adPreview, forecastClampToZero, forecastPreview]
  );

  return (
    <div className="exploreVisContainer">
      <EuiPanel
        hasBorder={false}
        hasShadow={false}
        data-test-subj="exploreVisualizationLoader"
        className="exploreVisPanel"
        paddingSize="none"
      >
        <div className="exploreVisPanel__inner">
          {visualizationBuilder.renderVisualization({
            searchContext,
            onSelectTimeRange,
            augmentEchartsSpec,
          })}
        </div>
      </EuiPanel>
    </div>
  );
});
