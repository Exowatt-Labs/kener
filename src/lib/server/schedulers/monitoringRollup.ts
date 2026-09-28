import { Queue, Worker, Job, type JobSchedulerTemplateOptions } from "bullmq";
import q from "../queues/q.js";
import db from "../db/db.js";
import { GetMinuteStartNowTimestampUTC } from "../tool.js";

let monitoringRollupQueue: Queue | null = null;
let worker: Worker | null = null;
const queueName = "monitoringRollupQueue";
const jobNamePrefix = "monitoringRollupJob";

const getQueue = () => {
  if (!monitoringRollupQueue) {
    monitoringRollupQueue = q.createQueue(queueName);
  }
  return monitoringRollupQueue;
};

const addWorker = () => {
  if (worker) return worker;

  worker = q.createWorker(
    getQueue(),
    async (_job: Job) => {
      const result = await db.advanceMonitoringRollup(GetMinuteStartNowTimestampUTC());
      if (result.chunks > 0 && result.previousWatermark === null) {
        console.log(`monitoring_data_rollup: backfill started, watermark=${result.watermark}`);
      }
      return result;
    },
    { concurrency: 1 },
  );

  worker.on("failed", (_job: Job | undefined, err: Error) => {
    console.error("Monitoring rollup scheduler failed:", err);
  });

  return worker;
};

/**
 * Start the monitor-bars rollup scheduler.
 * Runs every minute: re-derives the last hour below the watermark and advances
 * it toward now (backfilling a new install a few chunks per run).
 */
export const start = async (options?: JobSchedulerTemplateOptions) => {
  if (!options) {
    options = {};
  }

  options.removeOnComplete = {
    age: 3600,
    count: 100,
  };
  options.removeOnFail = {
    age: 7 * 24 * 3600,
  };

  const queue = getQueue();
  addWorker();

  await queue.upsertJobScheduler(
    jobNamePrefix + "_every_minute",
    {
      pattern: "* * * * *",
    },
    {
      opts: options,
    },
  );

  console.log("Monitoring rollup scheduler started (runs every minute)");
};

export const shutdown = async () => {
  if (worker) {
    await worker.close();
    worker = null;
  }
};

export default {
  start,
  shutdown,
};
