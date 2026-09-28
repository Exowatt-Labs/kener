import type { Knex } from "knex";

// Pre-aggregated 15-minute buckets of monitoring_data, read by the monitor-bars
// aggregation instead of re-scanning every raw row on each page load.
//
// Why: a status page with ~200 heartbeat monitors writes ~4.3k rows per monitor
// per day (REALTIME checks + SIGNAL heartbeat receipts). The day-bar query
// GROUP BYs every one of them — ~10M rows per 100-tag batch over 30 days, ~12 s
// on SQLite even when fully served from the covering index. The rollup holds
// ~96 rows per monitor per day, so the same request reads ~45x fewer rows.
//
// 15 minutes because every real-world UTC offset is a multiple of it, so a
// bucket never straddles a viewer's local-midnight day boundary.
//
// latency_sum / latency_count (not an average) so buckets merge exactly:
// AVG over the union = SUM(latency_sum) / SUM(latency_count), and both ignore
// NULL latency the way SQL AVG does.
export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable("monitoring_data_rollup"))) {
    await knex.schema.createTable("monitoring_data_rollup", (table) => {
      table.string("monitor_tag", 255).notNullable();
      table.integer("bucket_ts").notNullable();
      table.integer("count_of_up").notNullable().defaultTo(0);
      table.integer("count_of_down").notNullable().defaultTo(0);
      table.integer("count_of_degraded").notNullable().defaultTo(0);
      table.integer("count_of_maintenance").notNullable().defaultTo(0);
      table.double("latency_sum").nullable();
      table.integer("latency_count").notNullable().defaultTo(0);
      table.double("latency_min").nullable();
      table.double("latency_max").nullable();
      table.primary(["monitor_tag", "bucket_ts"]);
      table.index(["bucket_ts"], "idx_monitoring_data_rollup_bucket_ts");
    });
  }

  // Single-row bookkeeping: every bucket below `watermark` is materialised in
  // monitoring_data_rollup; everything at or above it is read from raw rows.
  if (!(await knex.schema.hasTable("monitoring_data_rollup_state"))) {
    await knex.schema.createTable("monitoring_data_rollup_state", (table) => {
      table.integer("id").primary();
      table.integer("watermark").nullable();
      table.integer("updated_at").notNullable().defaultTo(0);
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("monitoring_data_rollup_state");
  await knex.schema.dropTableIfExists("monitoring_data_rollup");
}
