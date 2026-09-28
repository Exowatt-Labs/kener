import type { Knex as KnexType } from "knex";

/**
 * 15-minute rollup of monitoring_data for the day-bar aggregation.
 *
 * State (one row): every bucket in [floor, watermark) is materialised in
 * monitoring_data_rollup; everything outside that span is read from raw rows.
 * Reads split on it, so they are correct whether or not the rollup has caught
 * up — an empty rollup is simply the old all-raw query.
 *
 * Concurrency: every mutation of the rollup or its state runs in a transaction
 * whose FIRST statement is a no-op UPDATE of the state row. That takes the row
 * lock on Postgres/MySQL (and the write lock on SQLite) before anything is
 * read, so the scheduler advancing the watermark and a request-path rebuild
 * can never interleave: a raw edit either commits before the scheduler's
 * INSERT…SELECT reads it, or its rebuild runs after the watermark moved and
 * re-derives the bucket.
 *
 * Every write to monitoring_data goes through MonitoringRepository, which calls
 * rebuildRollupRange() for any write that may land below the watermark, so a
 * historical edit (incident/maintenance overlay, confirmation backfill, late
 * push, delete) is re-derived in place instead of going stale. If that fails,
 * the watermark is lowered to the edit instead — reads fall back to raw for
 * that span (slower, never wrong) and the scheduler re-materialises it.
 *
 * Raw rows removed by a retention sweep (Kener's own dailyCleanup or an
 * external one) do NOT remove rollup rows: the rollup keeps
 * ROLLUP_RETENTION_DAYS of day bars even when raw history is shorter. Nothing
 * ever re-derives below the bucket holding the oldest surviving raw row, so a
 * trimmed range can never overwrite retained buckets with an empty result.
 */

export const ROLLUP_BUCKET_SECONDS = 900;
/** A bucket is materialised only once it closed this long ago (late writes land raw). */
export const ROLLUP_SETTLE_SECONDS = 300;
/** Each watermark advance first re-derives this much history below it, as a backstop. */
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

export interface RollupState {
  watermark: number | null;
  floor: number | null;
}

type Q = KnexType | KnexType.Transaction;

const isSQLite = (knex: Q): boolean => {
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

/**
 * A write at `timestamp` can only touch a materialised bucket if it is older
 * than the newest bucket the scheduler may have rolled up. Live inserts (the
 * current minute) skip the rollup entirely — no extra query, no cache.
 */
export const mayBeBelowWatermark = (timestamp: number, nowTs: number): boolean =>
  timestamp < floorBucket(nowTs - ROLLUP_SETTLE_SECONDS);

export const getRollupState = async (knex: Q): Promise<RollupState> => {
  const row = await knex("monitoring_data_rollup_state").where("id", STATE_ID).first();
  return { watermark: nullableNumber(row?.watermark), floor: nullableNumber(row?.floor) };
};

export const getRollupWatermark = async (knex: Q): Promise<number | null> => (await getRollupState(knex)).watermark;

const writeState = async (trx: Q, state: RollupState): Promise<void> => {
  const updated_at = Math.floor(Date.now() / 1000);
  await trx("monitoring_data_rollup_state")
    .insert({ id: STATE_ID, watermark: state.watermark, floor: state.floor, updated_at })
    .onConflict("id")
    .merge({ watermark: state.watermark, floor: state.floor, updated_at });
};

/**
 * Run `fn` holding the rollup lock (see the header). The no-op UPDATE is the
 * first statement so the lock is taken before any state or raw row is read.
 */
const withRollupLock = async <T>(knex: KnexType, fn: (trx: KnexType.Transaction) => Promise<T>): Promise<T> =>
  await knex.transaction(async (trx: KnexType.Transaction) => {
    await trx("monitoring_data_rollup_state")
      .where("id", STATE_ID)
      .update({ updated_at: trx.ref("updated_at") });
    return await fn(trx);
  });

const tagClause = (tags: string[] | undefined): { sql: string; bindings: string[] } =>
  tags && tags.length > 0
    ? { sql: ` AND monitor_tag IN (${tags.map(() => "?").join(", ")})`, bindings: tags }
    : { sql: "", bindings: [] };

/**
 * Replace buckets [from, to) — optionally only for `tags` — with a fresh
 * derivation from raw rows. Caller must hold the rollup lock.
 */
const materialise = async (trx: KnexType.Transaction, from: number, to: number, tags?: string[]): Promise<void> => {
  if (from >= to) return;
  const bucketExpr = isSQLite(trx)
    ? `CAST(timestamp / ${ROLLUP_BUCKET_SECONDS} AS INT) * ${ROLLUP_BUCKET_SECONDS}`
    : `FLOOR(timestamp / ${ROLLUP_BUCKET_SECONDS}) * ${ROLLUP_BUCKET_SECONDS}`;
  const tc = tagClause(tags);

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
};

export const oldestRawTimestamp = async (q: Q): Promise<number | null> => {
  const row = await q("monitoring_data").min("timestamp as min_ts").first();
  return nullableNumber((row as any)?.min_ts);
};

/**
 * Lowest bucket that may be re-derived from raw: never below the rollup floor,
 * and never below the bucket holding the oldest surviving raw row (below it the
 * rollup is the only copy left; re-deriving that one edge bucket can drop at
 * most its trimmed part — under 15 min).
 */
const rederiveFloor = (state: RollupState, oldestRaw: number | null): number | null => {
  if (oldestRaw === null || state.floor === null) return null;
  return Math.max(state.floor, floorBucket(oldestRaw));
};

/** Fallback when an in-place re-derivation fails: serve [from, …) raw until the scheduler catches up. */
const lowerWatermarkTo = async (knex: KnexType, from: number): Promise<void> => {
  try {
    await withRollupLock(knex, async (trx) => {
      const state = await getRollupState(trx);
      if (state.watermark === null) return;
      const floor = rederiveFloor(state, await oldestRawTimestamp(trx));
      if (floor === null) return;
      const lowered = Math.max(floorBucket(from), floor);
      if (lowered < state.watermark) await writeState(trx, { ...state, watermark: lowered });
    });
  } catch (err) {
    console.error("monitoring_data_rollup: failed to lower watermark", err);
  }
};

/**
 * Re-derive already-materialised buckets overlapping raw timestamps [from, to)
 * after a write to them. No-op for spans that are read raw anyway.
 * Never throws: on failure it lowers the watermark so reads fall back to raw.
 */
export const rebuildRollupRange = async (
  knex: KnexType,
  from: number,
  to: number,
  tags?: string[],
  oldestRawBeforeWrite?: number | null,
): Promise<void> => {
  try {
    await withRollupLock(knex, async (trx) => {
      const state = await getRollupState(trx);
      if (state.watermark === null) return;
      // A delete can itself remove the oldest raw rows; clipping at the
      // post-delete oldest would then skip the edge bucket it just changed.
      const oldestNow = await oldestRawTimestamp(trx);
      const oldest =
        oldestRawBeforeWrite === undefined || oldestRawBeforeWrite === null
          ? oldestNow
          : oldestNow === null
            ? oldestRawBeforeWrite
            : Math.min(oldestNow, oldestRawBeforeWrite);
      const floor = rederiveFloor(state, oldest);
      if (floor === null) return;
      const start = Math.max(floorBucket(from), floor);
      const end = Math.min(ceilBucket(to), state.watermark);
      await materialise(trx, start, end, tags);
    });
  } catch (err) {
    console.error("monitoring_data_rollup: in-place rebuild failed; lowering watermark", err);
    await lowerWatermarkTo(knex, from);
  }
};

export interface AdvanceRollupResult {
  watermark: number | null;
  previousWatermark: number | null;
  chunks: number;
  prunedRows: number;
}

/**
 * Scheduler entry point: when a bucket has settled, re-derive the last hour
 * below the watermark, then move the watermark forward in bounded chunks
 * toward (now - settle). The first run backfills from the oldest raw row, a
 * few chunks per run. Safe to run concurrently (every step takes the lock and
 * re-reads the state).
 */
export const advanceRollup = async (knex: KnexType, nowTs: number): Promise<AdvanceRollupResult> => {
  const target = floorBucket(nowTs - ROLLUP_SETTLE_SECONDS);
  const retentionFloor = floorBucket(nowTs - ROLLUP_RETENTION_DAYS * 86400);

  const previousWatermark = await withRollupLock(knex, async (trx) => {
    const state = await getRollupState(trx);
    if (state.watermark === null) {
      const oldest = await oldestRawTimestamp(trx);
      const start = oldest === null ? target : Math.min(target, Math.max(floorBucket(oldest), retentionFloor));
      // Fresh start: nothing at or above the new floor may be trusted.
      await trx("monitoring_data_rollup").where("bucket_ts", ">=", start).del();
      await writeState(trx, { watermark: start, floor: start });
    } else if (state.watermark > target) {
      // Clock went backwards: don't claim buckets we can't vouch for.
      await writeState(trx, { ...state, watermark: target });
    }
    return state.watermark;
  });

  const current = await getRollupWatermark(knex);
  if (current !== null && current < target) {
    await rebuildRollupRange(knex, current - ROLLUP_RECHECK_SECONDS, current);
  }

  let chunks = 0;
  let watermark = current;
  while (chunks < ROLLUP_MAX_CHUNKS_PER_RUN) {
    const advanced = await withRollupLock(knex, async (trx) => {
      const state = await getRollupState(trx);
      if (state.watermark === null || state.floor === null || state.watermark >= target) return null;
      let from = state.watermark;
      // Below the oldest surviving raw row only the rollup holds data: skip
      // ahead instead of re-deriving (and erasing) it.
      const oldest = await oldestRawTimestamp(trx);
      if (oldest !== null && from < floorBucket(oldest)) from = Math.min(floorBucket(oldest), target);
      const next = Math.min(from + ROLLUP_CHUNK_SECONDS, target);
      await materialise(trx, from, next);
      await writeState(trx, { ...state, watermark: next });
      return next;
    });
    if (advanced === null) break;
    watermark = advanced;
    chunks++;
  }

  const prunedRows = await withRollupLock(knex, async (trx) => {
    const state = await getRollupState(trx);
    const pruned = await trx("monitoring_data_rollup").where("bucket_ts", "<", retentionFloor).del();
    if (state.floor !== null && state.floor < retentionFloor) {
      await writeState(trx, { ...state, floor: Math.min(retentionFloor, state.watermark ?? retentionFloor) });
    }
    return pruned;
  });

  return { watermark, previousWatermark, chunks, prunedRows };
};

/**
 * Mirror an explicit raw delete (`end` inclusive, as deleteMonitorDataByTag):
 * drop the rollup buckets wholly inside the deleted span — so a deliberate
 * delete also removes retained history — then re-derive the partially-covered
 * edge buckets from the raw rows that remain.
 */
export const deleteRollupRange = async (
  knex: KnexType,
  tag?: string,
  start?: number,
  end?: number,
  oldestRawBeforeDelete?: number | null,
): Promise<void> => {
  try {
    await withRollupLock(knex, async (trx) => {
      const query = trx("monitoring_data_rollup");
      if (tag) query.where("monitor_tag", tag);
      if (start !== undefined) query.where("bucket_ts", ">=", ceilBucket(start));
      if (end !== undefined) query.where("bucket_ts", "<=", end + 1 - ROLLUP_BUCKET_SECONDS);
      await query.del();
    });
  } catch (err) {
    console.error("monitoring_data_rollup: delete mirror failed", err);
    // A ranged delete can fall back to raw for its span; an unbounded one
    // (monitor deletion) has nothing safe to lower to and is left as is.
    if (start !== undefined) await lowerWatermarkTo(knex, start);
    return;
  }
  const tags = tag ? [tag] : undefined;
  if (start !== undefined) await rebuildRollupRange(knex, start, start + 1, tags, oldestRawBeforeDelete);
  if (end !== undefined) await rebuildRollupRange(knex, end, end + 1, tags, oldestRawBeforeDelete);
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
 * Grouped status sums over [startTimestamp, startTimestamp + points*interval):
 * served from the rollup inside [floor, watermark) and from raw rows outside it.
 */
export const aggregateWithRollup = async (
  knex: KnexType,
  tags: string[],
  startTimestamp: number,
  intervalInSeconds: number,
  numberOfPoints: number,
): Promise<GroupedStatusSums[]> => {
  const end = startTimestamp + numberOfPoints * intervalInSeconds;
  let rollFrom = startTimestamp;
  let rollTo = startTimestamp;
  if (canUseRollup(startTimestamp, intervalInSeconds)) {
    const state = await getRollupState(knex);
    if (state.watermark !== null && state.floor !== null) {
      rollFrom = Math.min(Math.max(startTimestamp, state.floor), end);
      rollTo = Math.max(rollFrom, Math.min(state.watermark, end));
    }
  }
  const [before, rolled, after] = await Promise.all([
    aggregateRaw(knex, tags, startTimestamp, intervalInSeconds, startTimestamp, rollFrom),
    aggregateRollup(knex, tags, startTimestamp, intervalInSeconds, rollFrom, rollTo),
    aggregateRaw(knex, tags, startTimestamp, intervalInSeconds, rollTo, end),
  ]);
  return mergeSums(before, rolled, after);
};
