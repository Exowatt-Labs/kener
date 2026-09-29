/**
 * getLatestMonitoringDataAllActive must return exactly the newest row per
 * requested tag (deduped, missing tags skipped) — the same rows as the
 * MAX(timestamp) GROUP BY self-join it replaced.
 *
 * Run: npx vite-node scripts/test-latest-monitoring-data.ts
 */
import assert from "node:assert/strict";
import Knex from "knex";
import { MonitoringRepository } from "../src/lib/server/db/repositories/monitoring.js";

const db = Knex({ client: "better-sqlite3", connection: { filename: ":memory:" }, useNullAsDefault: true });

const main = async () => {
  await db.schema.createTable("monitoring_data", (table) => {
    table.string("monitor_tag", 255).notNullable();
    table.integer("timestamp").notNullable();
    table.text("status");
    table.float("latency");
    table.text("type");
    table.primary(["monitor_tag", "timestamp"]);
  });
  const rows: any[] = [];
  for (let t = 0; t < 25; t++) {
    for (let k = 0; k < 40; k++) {
      rows.push({
        monitor_tag: `tag-${t}`,
        timestamp: 1000 + ((k * 7919 + t * 31) % 4000),
        status: k % 3 ? "UP" : "DOWN",
        latency: k,
        type: "REALTIME",
      });
    }
  }
  const unique = new Map(rows.map((r) => [`${r.monitor_tag}:${r.timestamp}`, r]));
  const deduped = [...unique.values()];
  for (let i = 0; i < deduped.length; i += 200) await db("monitoring_data").insert(deduped.slice(i, i + 200));
  const repo = new MonitoringRepository(db);

  const tags = [...Array.from({ length: 25 }, (_, t) => `tag-${t}`), "tag-3", "missing"];
  const reference = await db("monitoring_data as md")
    .join(
      db("monitoring_data")
        .select("monitor_tag")
        .max("timestamp as max_timestamp")
        .whereIn("monitor_tag", tags)
        .groupBy("monitor_tag")
        .as("l"),
      function () {
        this.on("md.monitor_tag", "=", "l.monitor_tag").andOn("md.timestamp", "=", "l.max_timestamp");
      },
    )
    .select("md.*");
  const actual = await repo.getLatestMonitoringDataAllActive(tags);
  const key = (r: any) => `${r.monitor_tag}:${r.timestamp}:${r.status}`;
  assert.deepEqual(actual.map(key).sort(), reference.map(key).sort());
  assert.equal(actual.length, 25, "deduped, missing tag skipped");
  assert.deepEqual(await repo.getLatestMonitoringDataAllActive([]), []);
  console.log("latest monitoring data: all checks passed");
  await db.destroy();
};

main().catch(async (err) => {
  console.error(err);
  await db.destroy();
  process.exit(1);
});
