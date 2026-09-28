import type { Knex as KnexType } from "knex";

/**
 * 15-minute rollup of monitoring_data for the day-bar aggregation.
 *
 * Invariant: every bucket with bucket_ts < watermark is materialised in
 * monitoring_data_rollup from the raw rows that existed when it was last
 * derived; everything at or above the watermark is read from raw rows. Reads
 * split at the watermark, so they are correct whether or not the rollup has
 * caught up — an empty rollup is simply the old all-raw query.
 *
 * Every write to monitoring_data goes through MonitoringRepository, which calls
 * rebuildRollupRange() for any write below the watermark, so a historical edit
 * (incident/maintenance overlay, confirmation backfill, late push, delete) is
 * re-derived in place instead of going stale. If that re-derivation fails, the
 * watermark is lowered below the edit instead — reads then fall back to raw for
 * that span (slower, never wrong) and the scheduler re-materialises it.
 *
 * Raw rows removed by a retention sweep (Kener's own dailyCleanup or an external
 * one) do NOT remove rollup rows: the rollup keeps ROLLUP_RETENTION_DAYS of day
 * bars even when raw history is shorter. Re-derivation is therefore clipped to
 * the oldest surviving raw row, so it can never overwrite a retained bucket with
 * the empty result of a trimmed range.
 */

export const ROLLUP_BUCKET_SECONDS = 900;
/** A bucket is materialised only once it closed this long ago (late writes land raw). */
export const ROLLUP_SETTLE_SECONDS = 300;
/** Each advance re-derives this much history below the watermark, as a backstop. */
export const ROLLUP_RECHECK_SECONDS = 3600;
export const ROLLUP_CHUNK_SECONDS = 6 * 3600;
export const ROLLUP_MAX_CHUNKS_PER_RUN = 8;
/** Monitor bars show at most 90 days; keep a little slack for timezone offsets. */
export const ROLLUP_RETENTION_DAYS = 92;

const STATE_ID = 1;

export interface GroupedStatusSums {
  monitor_tag: string;
  ts: number;
  countOfUp: number;
  countOfDown: number;
  countOfDegraded: number;
  countOfMaintenance: number;
  latencySum: number;
  latencyCount: number;
  latencyMin: number | null;
  latencyMax: number | null;
}

const isSQLite = (knex: KnexType): boolean => {
  const client = (knex.client as any).config.client;
  return client === "better-sqlite3" || client === "sqlite3";
};

// SQLite (better-sqlite3) returns rows directly, MySQL [rows, fields], PostgreSQL { rows }.
export const rowsOf = (result: any): any[] => {
  if (Array.isArray(result)) {
    return Array.isArray(result[0]) ? result[0] : result;
  }
  return result?.rows || [];
};

const nullableNumber = (value: unknown): number | null =>
  value === null || value === undefined ? null : Number(value);

export const floorBucket = (ts: number): number => Math.floor(ts / ROLLUP_BUCKET_SECONDS) * ROLLUP_BUCKET_SECONDS;
export const ceilBucket = (ts: number): number => Math.ceil(ts / ROLLUP_BUCKET_SECONDS) * ROLLUP_BUCKET_SECONDS;

/**
 * The rollup can serve a request only when every requested interval is a whole
 * number of buckets starting on a bucket boundary. Monitor bars always are
 * (a viewer's local midnight is a multiple of 15 min from UTC midnight); any
 * other shape falls back to raw rows.
 */
export const canUseRollup = (startTimestamp: number, intervalInSeconds: number): boolean =>
  Number.isInteger(startTimestamp) &&
  Number.isInteger(intervalInSeconds) &&
  intervalInSeconds > 0 &&
  startTimestamp % ROLLUP_BUCKET_SECONDS === 0 &&
  intervalInSeconds % ROLLUP_BUCKET_SECONDS === 0;

// In-process hint for the insert path, so a live insert (always above the
// watermark) costs no extra query. `undefined` = not loaded yet.
let cachedWatermark: number | null | undefined;

export const resetRollupWatermarkCache = (): void => {
  cachedWatermark = undefined;
};

export const getRollupWatermark = async (knex: KnexType): Promise<number | null> => {
  const row = await knex("monitoring_data_rollup_state").where("id", STATE_ID).first();
  const watermark = nullableNumber(row?.watermark);
  cachedWatermark = watermark;
  return watermark;
};

export const getCachedRollupWatermark = async (knex: KnexType): Promise<number | null> => {
  if (cachedWatermark === undefined) {
    return await getRollupWatermark(knex);
  }
  return cachedWatermark;
};

const setRollupWatermark = async (knex: KnexType, watermark: number | null): Promise<void> => {
  const updated_at = Math.floor(Date.now() / 1000);
  await knex("monitoring_data_rollup_state")
    .insert({ id: STATE_ID, watermark, updated_at })
    .onConflict("id")
    .merge({ watermark, updated_at });
  cachedWatermark = watermark;
};

const tagClause = (tags: string[] | undefined): { sql: string; bindings: string[] } =>
  tags && tags.length > 0
    ? { sql: ` AND monitor_tag IN (${tags.map(() => "?").join(", ")})`, bindings: tags }
    : { sql: "", bindings: [] };

/** Replace buckets [from, to) — optionally only for `tags` — with a fresh derivation from raw rows. */
const materialise = async (knex: KnexType, from: number, to: number, tags?: string[]): Promise<void> => {
  if (from >= to) return;
  const bucketExpr = isSQLite(knex)
    ? `CAST(timestamp / ${ROLLUP_BUCKET_SECONDS} AS INT) * ${ROLLUP_BUCKET_SECONDS}`
    : `FLOOR(timestamp / ${ROLLUP_BUCKET_SECONDS}) * ${ROLLUP_BUCKET_SECONDS}`;
  const tc = tagClause(tags);

  await knex.transaction(async (trx: KnexType.Transaction) => {
    const del = trx("monitoring_data_rollup").where("bucket_ts", ">=", from).where("bucket_ts", "<", to);
    if (tags && tags.length > 0) del.whereIn("monitor_tag", tags);
    await del.del();

    await trx.raw(
      `
      INSERT INTO monitoring_data_rollup (
        monitor_tag, bucket_ts, count_of_up, count_of_down, count_of_degraded, count_of_maintenance,
        latency_sum, latency_count, latency_min, latency_max
      )
      SELECT
        monitor_tag,
        ${bucketExpr} AS bucket_ts,
        SUM(CASE WHEN status = 'UP' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'DOWN' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'DEGRADED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'MAINTENANCE' THEN 1 ELSE 0 END),
        SUM(latency),
        COUNT(latency),
        MIN(latency),
        MAX(latency)
      FROM monitoring_data
      WHERE timestamp >= ? AND timestamp < ?${tc.sql}
      GROUP BY monitor_tag, ${bucketExpr}
      `,
      [from, to, ...tc.bindings],
    );
  });
};

const oldestRawTimestamp = async (knex: KnexType): Promise<number | null> => {
  const row = await knex("monitoring_data").min("timestamp as min_ts").first();
  return nullableNumber((row as any)?.min_ts);
};

/**
 * Re-derive already-materialised buckets overlapping raw timestamps [from, to)
 * after a write to them. No-op above the watermark (those spans are read raw).
 * Never throws: on failure it lowers the watermark so reads fall back to raw.
 */
export const rebuildRollupRange = async (knex: KnexType, from: number, to: number, tags?: string[]): Promise<void> => {
  let watermark: number | null = null;
  try {
    watermark = await getCachedRollupWatermark(knex);
    if (watermark === null) return;
    const start = floorBucket(from);
    const end = Math.min(ceilBucket(to), watermark);
    if (start >= end) return;

    // Clip to the bucket holding the oldest surviving raw row: below it the raw
    // history was trimmed and the rollup is the only copy left. (Re-deriving
    // that one edge bucket can drop at most its trimmed part — under 15 min.)
    const oldest = await oldestRawTimestamp(knex);
    if (oldest === null) return;
    await materialise(knex, Math.max(start, floorBucket(oldest)), end, tags);
  } catch (err) {
    console.error("monitoring_data_rollup: in-place rebuild failed; lowering watermark", err);
    try {
      if (watermark !== null) {
        await setRollupWatermark(knex, Math.min(watermark, floorBucket(from)));
      }
    } catch (innerErr) {
      console.error("monitoring_data_rollup: failed to lower watermark", innerErr);
      cachedWatermark = undefined;
    }
  }
};

export interface AdvanceRollupResult {
  watermark: number | null;
  previousWatermark: number | null;
  chunks: number;
  prunedRows: number;
}

/**
 * Scheduler entry point: re-derive the last hour below the watermark, then move
 * the watermark forward in bounded chunks toward (now - settle). The first run
 * backfills from the oldest raw row, a few chunks per run.
 */
export const advanceRollup = async (knex: KnexType, nowTs: number): Promise<AdvanceRollupResult> => {
  const target = floorBucket(nowTs - ROLLUP_SETTLE_SECONDS);
  const retentionFloor = floorBucket(nowTs - ROLLUP_RETENTION_DAYS * 86400);
  const previousWatermark = await getRollupWatermark(knex);
  let watermark = previousWatermark;

  if (watermark === null) {
    const oldest = await oldestRawTimestamp(knex);
    watermark = oldest === null ? target : Math.max(floorBucket(oldest), retentionFloor);
    // Fresh start: nothing at or above the new watermark may be trusted.
    await knex("monitoring_data_rollup").where("bucket_ts", ">=", watermark).del();
    await setRollupWatermark(knex, watermark);
  }

  if (watermark > target) {
    // Clock went backwards (or target shrank): don't claim buckets we can't vouch for.
    watermark = target;
    await setRollupWatermark(knex, watermark);
  }

  await rebuildRollupRange(knex, watermark - ROLLUP_RECHECK_SECONDS, watermark);

  let chunks = 0;
  while (watermark < target && chunks < ROLLUP_MAX_CHUNKS_PER_RUN) {
    const next = Math.min(watermark + ROLLUP_CHUNK_SECONDS, target);
    await materialise(knex, watermark, next);
    await setRollupWatermark(knex, next);
    watermark = next;
    chunks++;
  }

  const prunedRows = await knex("monitoring_data_rollup").where("bucket_ts", "<", retentionFloor).del();

  return { watermark, previousWatermark, chunks, prunedRows };
};

/**
 * Mirror an explicit raw delete: drop every rollup bucket overlapping the
 * deleted span (so a deliberate delete also removes retained history), then
 * re-derive the partially-covered edge buckets from whatever raw rows remain.
 * `end` is inclusive, matching deleteMonitorDataByTag.
 */
export const deleteRollupRange = async (knex: KnexType, tag?: string, start?: number, end?: number): Promise<void> => {
  try {
    const query = knex("monitoring_data_rollup");
    if (tag) query.where("monitor_tag", tag);
    if (start !== undefined) query.where("bucket_ts", ">=", floorBucket(start));
    if (end !== undefined) query.where("bucket_ts", "<=", end);
    await query.del();
  } catch (err) {
    console.error("monitoring_data_rollup: delete mirror failed; lowering watermark", err);
    cachedWatermark = undefined;
    const watermark = await getCachedRollupWatermark(knex).catch(() => null);
    if (watermark !== null) {
      await setRollupWatermark(knex, Math.min(watermark, floorBucket(start ?? 0))).catch(() => {});
    }
    return;
  }
  const tags = tag ? [tag] : undefined;
  if (start !== undefined) await rebuildRollupRange(knex, start, start + 1, tags);
  if (end !== undefined) await rebuildRollupRange(knex, end, end + 1, tags);
};

/**
 * Status counts per (monitor_tag, interval) with latency as sum/count, so raw
 * and rollup spans merge exactly. `interval` buckets start at `startTimestamp`.
 */
export const aggregateRaw = async (
  knex: KnexType,
  tags: string[],
  startTimestamp: number,
  intervalInSeconds: number,
  from: number,
  to: number,
): Promise<GroupedStatusSums[]> => {
  if (from >= to || tags.length === 0) return [];
  const tsExpression = isSQLite(knex)
    ? `CAST((timestamp - ?) / ? AS INT) * ? + ?`
    : `FLOOR((timestamp - ?) / ?) * ? + ?`;
  const result = await knex.raw(
    `
    SELECT
      monitor_tag,
      ${tsExpression} AS ts,
      SUM(CASE WHEN status = 'UP' THEN 1 ELSE 0 END) AS count_of_up,
      SUM(CASE WHEN status = 'DOWN' THEN 1 ELSE 0 END) AS count_of_down,
      SUM(CASE WHEN status = 'DEGRADED' THEN 1 ELSE 0 END) AS count_of_degraded,
      SUM(CASE WHEN status = 'MAINTENANCE' THEN 1 ELSE 0 END) AS count_of_maintenance,
      SUM(latency) AS latency_sum,
      COUNT(latency) AS latency_count,
      MIN(latency) AS latency_min,
      MAX(latency) AS latency_max
    FROM monitoring_data
    WHERE monitor_tag IN (${tags.map(() => "?").join(", ")}) AND timestamp >= ? AND timestamp < ?
    GROUP BY monitor_tag, ts
    `,
    [startTimestamp, intervalInSeconds, intervalInSeconds, startTimestamp, ...tags, from, to],
  );
  return rowsOf(result).map(toSums);
};

export const aggregateRollup = async (
  knex: KnexType,
  tags: string[],
  startTimestamp: number,
  intervalInSeconds: number,
  from: number,
  to: number,
): Promise<GroupedStatusSums[]> => {
  if (from >= to || tags.length === 0) return [];
  const tsExpression = isSQLite(knex)
    ? `CAST((bucket_ts - ?) / ? AS INT) * ? + ?`
    : `FLOOR((bucket_ts - ?) / ?) * ? + ?`;
  const result = await knex.raw(
    `
    SELECT
      monitor_tag,
      ${tsExpression} AS ts,
      SUM(count_of_up) AS count_of_up,
      SUM(count_of_down) AS count_of_down,
      SUM(count_of_degraded) AS count_of_degraded,
      SUM(count_of_maintenance) AS count_of_maintenance,
      SUM(latency_sum) AS latency_sum,
      SUM(latency_count) AS latency_count,
      MIN(latency_min) AS latency_min,
      MAX(latency_max) AS latency_max
    FROM monitoring_data_rollup
    WHERE monitor_tag IN (${tags.map(() => "?").join(", ")}) AND bucket_ts >= ? AND bucket_ts < ?
    GROUP BY monitor_tag, ts
    `,
    [startTimestamp, intervalInSeconds, intervalInSeconds, startTimestamp, ...tags, from, to],
  );
  return rowsOf(result).map(toSums);
};

const toSums = (row: any): GroupedStatusSums => ({
  monitor_tag: row.monitor_tag,
  ts: Number(row.ts),
  countOfUp: Number(row.count_of_up) || 0,
  countOfDown: Number(row.count_of_down) || 0,
  countOfDegraded: Number(row.count_of_degraded) || 0,
  countOfMaintenance: Number(row.count_of_maintenance) || 0,
  latencySum: Number(row.latency_sum) || 0,
  latencyCount: Number(row.latency_count) || 0,
  latencyMin: nullableNumber(row.latency_min),
  latencyMax: nullableNumber(row.latency_max),
});

/** Merge partial sums for the same (monitor_tag, ts), ordered by monitor_tag then ts. */
export const mergeSums = (...parts: GroupedStatusSums[][]): GroupedStatusSums[] => {
  const byKey = new Map<string, GroupedStatusSums>();
  for (const part of parts) {
    for (const row of part) {
      const key = `${row.monitor_tag}\u0000${row.ts}`;
      const prev = byKey.get(key);
      if (!prev) {
        byKey.set(key, { ...row });
        continue;
      }
      prev.countOfUp += row.countOfUp;
      prev.countOfDown += row.countOfDown;
      prev.countOfDegraded += row.countOfDegraded;
      prev.countOfMaintenance += row.countOfMaintenance;
      prev.latencySum += row.latencySum;
      prev.latencyCount += row.latencyCount;
      prev.latencyMin =
        prev.latencyMin === null
          ? row.latencyMin
          : row.latencyMin === null
            ? prev.latencyMin
            : Math.min(prev.latencyMin, row.latencyMin);
      prev.latencyMax =
        prev.latencyMax === null
          ? row.latencyMax
          : row.latencyMax === null
            ? prev.latencyMax
            : Math.max(prev.latencyMax, row.latencyMax);
    }
  }
  return Array.from(byKey.values()).sort((a, b) =>
    a.monitor_tag < b.monitor_tag ? -1 : a.monitor_tag > b.monitor_tag ? 1 : a.ts - b.ts,
  );
};

/**
 * Grouped status sums over [startTimestamp, startTimestamp + points*interval),
 * served from the rollup below the watermark and from raw rows above it.
 */
export const aggregateWithRollup = async (
  knex: KnexType,
  tags: string[],
  startTimestamp: number,
  intervalInSeconds: number,
  numberOfPoints: number,
): Promise<GroupedStatusSums[]> => {
  const end = startTimestamp + numberOfPoints * intervalInSeconds;
  let split = startTimestamp;
  if (canUseRollup(startTimestamp, intervalInSeconds)) {
    const watermark = await getRollupWatermark(knex);
    if (watermark !== null) {
      split = Math.max(startTimestamp, Math.min(watermark, end));
    }
  }
  const [rolled, raw] = await Promise.all([
    aggregateRollup(knex, tags, startTimestamp, intervalInSeconds, startTimestamp, split),
    aggregateRaw(knex, tags, startTimestamp, intervalInSeconds, split, end),
  ]);
  return mergeSums(rolled, raw);
};
