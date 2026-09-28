/**
 * Equivalence test for the monitor-bars rollup: every rollup-served aggregation
 * must equal the original all-raw SQL, across timezone offsets and after every
 * kind of write (late insert, overlay update, confirmation backfill, ranged
 * delete), and retained buckets must survive a raw retention sweep.
 *
 * Run: npx vite-node scripts/test-monitoring-rollup.ts
 */
import assert from "node:assert/strict";
import Knex from "knex";
import { MonitoringRepository } from "../src/lib/server/db/repositories/monitoring.js";
import { up as rollupMigration } from "../migrations/20260928120000_add_monitoring_data_rollup.js";
import {
  ROLLUP_SETTLE_SECONDS,
  floorBucket,
  getRollupWatermark,
  resetRollupWatermarkCache,
} from "../src/lib/server/db/repositories/monitoringRollup.js";

const DAY = 86400;
const TAGS = ["alpha", "beta", "gamma", "delta"];
const STATUSES = ["UP", "UP", "UP", "UP", "DOWN", "DEGRADED", "MAINTENANCE", "NO_DATA"];

// Deterministic PRNG so failures reproduce.
let seed = 42;
const rand = () => {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
};

const db = Knex({ client: "better-sqlite3", connection: { filename: ":memory:" }, useNullAsDefault: true });

// The pre-rollup query, verbatim in shape: the reference every result must match.
const referenceGrouped = async (tags: string[], start: number, interval: number, points: number) => {
  const end = start + points * interval;
  const rows = await db.raw(
    `SELECT monitor_tag, CAST((timestamp - ?) / ? AS INT) * ? + ? as ts,
       SUM(CASE WHEN status = 'UP' THEN 1 ELSE 0 END) AS count_of_up,
       SUM(CASE WHEN status = 'DOWN' THEN 1 ELSE 0 END) AS count_of_down,
       SUM(CASE WHEN status = 'DEGRADED' THEN 1 ELSE 0 END) AS count_of_degraded,
       SUM(CASE WHEN status = 'MAINTENANCE' THEN 1 ELSE 0 END) AS count_of_maintenance,
       AVG(latency) AS avg_latency, MAX(latency) AS max_latency, MIN(latency) AS min_latency
     FROM monitoring_data
     WHERE monitor_tag IN (${tags.map(() => "?").join(", ")}) AND timestamp >= ? AND timestamp < ?
     GROUP BY monitor_tag, ts ORDER BY monitor_tag ASC, ts ASC`,
    [start, interval, interval, start, ...tags, start, end],
  );
  return rows.map((row: any) => ({
    monitor_tag: row.monitor_tag,
    ts: Number(row.ts),
    countOfUp: Number(row.count_of_up) || 0,
    countOfDown: Number(row.count_of_down) || 0,
    countOfDegraded: Number(row.count_of_degraded) || 0,
    countOfMaintenance: Number(row.count_of_maintenance) || 0,
    avgLatency: Number(row.avg_latency) || 0,
    maxLatency: Number(row.max_latency) || 0,
    minLatency: Number(row.min_latency) || 0,
  }));
};

const approxEqual = (actual: any[], expected: any[], label: string) => {
  assert.equal(actual.length, expected.length, `${label}: row count`);
  for (let i = 0; i < expected.length; i++) {
    const a = actual[i];
    const e = expected[i];
    for (const key of Object.keys(e)) {
      if (typeof e[key] === "number" && key.endsWith("Latency")) {
        assert.ok(Math.abs(a[key] - e[key]) < 1e-6, `${label}: row ${i} ${key} ${a[key]} != ${e[key]}`);
      } else {
        assert.equal(a[key], e[key], `${label}: row ${i} ${key}`);
      }
    }
  }
};

const main = async () => {
  await db.schema.createTable("monitoring_data", (table) => {
    table.string("monitor_tag", 255).notNullable();
    table.integer("timestamp").notNullable();
    table.text("status");
    table.float("latency");
    table.text("type");
    table.text("error_message");
    table.text("raw_status");
    table.primary(["monitor_tag", "timestamp"]);
  });
  await rollupMigration(db);
  const repo = new MonitoringRepository(db);

  // 12 days of data: a REALTIME row per minute plus a SIGNAL row every 30 s, some latencies NULL.
  const now = 1_790_640_000 + 7 * 60; // arbitrary, deliberately not bucket-aligned
  const first = now - 12 * DAY;
  const rows: any[] = [];
  for (const tag of TAGS) {
    for (let ts = first; ts < now; ts += 30) {
      if (rand() < 0.03) continue; // gaps
      rows.push({
        monitor_tag: tag,
        timestamp: ts,
        status: STATUSES[Math.floor(rand() * STATUSES.length)],
        latency: rand() < 0.1 ? null : Math.round(rand() * 5000) / 10,
        type: ts % 60 === 0 ? "REALTIME" : "SIGNAL",
      });
    }
  }
  for (let i = 0; i < rows.length; i += 500) {
    await db("monitoring_data").insert(rows.slice(i, i + 500));
  }

  // Viewer day boundaries: UTC, US Central (-5h), India (+5:30), Nepal (+5:45).
  const offsets = [0, -5 * 3600, 5.5 * 3600, 5.75 * 3600];
  const check = async (label: string) => {
    for (const offset of offsets) {
      const endOfDay = Math.floor((now + offset) / DAY) * DAY + DAY - offset;
      for (const days of [7, 30, 90]) {
        const start = endOfDay - days * DAY;
        const expected = await referenceGrouped(TAGS, start, DAY, days);
        const actual = await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, start, DAY, days);
        approxEqual(actual, expected, `${label} offset=${offset} days=${days}`);
      }
    }
    // Unaligned request shape → raw fallback, still identical.
    const odd = now - 3 * DAY + 17;
    approxEqual(
      await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, odd, 3600, 72),
      await referenceGrouped(TAGS, odd, 3600, 72),
      `${label} unaligned`,
    );
    // Single-tag variant (monitor-bar endpoint) sums identically.
    const single = await repo.getStatusCountsByInterval("beta", endOfDayUtc() - 30 * DAY, DAY, 30);
    const ref = (await referenceGrouped(["beta"], endOfDayUtc() - 30 * DAY, DAY, 30)).map(({ monitor_tag, ...r }) => r);
    approxEqual(single, ref, `${label} single-tag`);
  };
  const endOfDayUtc = () => Math.floor(now / DAY) * DAY + DAY;

  // 1. No rollup yet: pure raw path.
  await check("before-rollup");
  assert.equal(await getRollupWatermark(db), null);

  // 2. Advance until caught up (backfill runs in bounded chunks).
  let runs = 0;
  let result;
  do {
    result = await repo.advanceMonitoringRollup(now);
    runs++;
  } while (result.chunks > 0 && runs < 50);
  assert.equal(result.watermark, floorBucket(now - ROLLUP_SETTLE_SECONDS), "watermark caught up");
  assert.ok(runs > 1, "backfill was chunked");
  const rolled = Number((await db("monitoring_data_rollup").count("* as n").first())?.n);
  assert.ok(rolled > 0 && rolled < rows.length / 20, `rollup is compact (${rolled} vs ${rows.length})`);
  await check("after-backfill");

  // 3. Historical writes below the watermark are re-derived in place.
  await repo.insertMonitoringData({
    monitor_tag: "alpha",
    timestamp: now - 5 * DAY + 13,
    status: "DOWN",
    latency: 999,
    type: "MANUAL",
  } as any);
  await check("after-late-insert");

  await repo.updateMonitoringData("gamma", now - 4 * DAY, now - 4 * DAY + 7200, "MAINTENANCE", "MAINTENANCE", 5);
  await check("after-overlay-update");

  const confirmTs = [now - 2 * DAY + 60, now - 2 * DAY + 120];
  await db("monitoring_data")
    .where("monitor_tag", "delta")
    .whereIn("timestamp", confirmTs)
    .update({ raw_status: "DOWN" });
  await repo.backfillConfirmedStatus("delta", confirmTs, 3);
  await check("after-confirmation-backfill");

  await repo.deleteMonitorDataByTag("beta", now - 6 * DAY + 100, now - 6 * DAY + 3 * 3600);
  await check("after-ranged-delete");

  // A fresh process (cold watermark cache) still sees a back-dated insert.
  resetRollupWatermarkCache();
  await repo.insertMonitoringData({
    monitor_tag: "gamma",
    timestamp: now - 3 * DAY + 45,
    status: "DEGRADED",
    latency: 1,
    type: "MANUAL",
  } as any);
  await check("after-cold-cache-insert");

  // 4. A retention sweep trims raw history; retained day bars must not change.
  const endOfDay = endOfDayUtc();
  const before = await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, endOfDay - 30 * DAY, DAY, 30);
  await db("monitoring_data")
    .where("timestamp", "<", now - 8 * DAY)
    .del();
  await repo.advanceMonitoringRollup(now);
  const after = await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, endOfDay - 30 * DAY, DAY, 30);
  approxEqual(after, before, "retained-after-retention");
  // …and a later historical write near the trimmed edge does not wipe older buckets.
  await repo.updateMonitoringData("alpha", now - 7 * DAY, now - 7 * DAY + 600, "UP", "MANUAL", 1);
  const afterEdit = await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, endOfDay - 30 * DAY, DAY, 30);
  const olderThanEdge = (r: any[]) => r.filter((x) => x.ts < now - 8 * DAY - DAY);
  approxEqual(olderThanEdge(afterEdit), olderThanEdge(before), "older-buckets-kept");

  // 5. Live progression: advancing later still matches raw for the untrimmed window.
  await db("monitoring_data").insert(
    TAGS.map((tag) => ({ monitor_tag: tag, timestamp: now + 600, status: "UP", latency: 3, type: "REALTIME" })),
  );
  await repo.advanceMonitoringRollup(now + 3600);
  const liveStart = floorBucket(now - 7 * DAY);
  approxEqual(
    await repo.getStatusCountsByIntervalGroupedByMonitor(TAGS, liveStart, 900, (7 * DAY + 3600) / 900),
    await referenceGrouped(TAGS, liveStart, 900, (7 * DAY + 3600) / 900),
    "live-progression",
  );

  console.log(
    `monitoring rollup: all checks passed (${rows.length} raw rows, ${rolled} rollup rows, ${runs} backfill runs)`,
  );
  await db.destroy();
};

main().catch(async (err) => {
  console.error(err);
  await db.destroy();
  process.exit(1);
});
