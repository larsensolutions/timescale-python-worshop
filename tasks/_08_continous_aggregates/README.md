# Task 8 | 🔍 Continuous Aggregates

In this task, you will:

- ✅ Already have completed [Task 1](../_01_setup/README.md), [Task 2](../_02_schema_hypertable/README.md), [Task 3](../_03_ingest_insert/README.md) or [Task 4](../_04_ingest_copy/README.md) and [Task 5](../_05_compression/README.md).

- ✅ Have a `sensors` hypertable created in your TimescaleDB instance with columns `(id, time, value)`.

- ✅ Have ingested a good amount of data into the `sensors` hypertable.

- ✅ Have applied compression (chunks are compressed)

## 🧱 Objective

Create a **continuous aggregate (CAGG)** over `sensors` that pre-aggregates data into **1-day buckets**.  
CAGGs speed up queries by storing precomputed results.

Docs: 👉 [TimescaleDB Continuous Aggregates](https://docs.tigerdata.com/api/latest/continuous-aggregates/)

> **Erik’s note:** CAGGs are materialized views refreshed on a schedule. For true real-time, use stream processing. CAGGs are still great for many use cases—and the CAGG itself is a hypertable, so you can compress it too.

## ✅ Requirements

1. Create a continuous aggregate view **`sensors_summary_daily`** that buckets by **1 day** and computes **avg**, **max**, **min** per `id`.  
   Columns: `id`, `bucket` (the time bucket), `avg_value`, `max_value`, `min_value`.
2. Add a **refresh policy** to refresh the **last month** on a **1-hour** schedule.
3. **Enable compression** on the CAGG.

```sql
-- 1) Create CAGG
CREATE MATERIALIZED VIEW sensors_summary_daily
WITH (timescaledb.continuous) AS
SELECT
  id,
  time_bucket('1 day', time) AS bucket,
  AVG(value) AS avg_value,
  MAX(value) AS max_value,
  MIN(value) AS min_value
FROM sensors
GROUP BY id, bucket
WITH NO DATA;

-- 2) Refresh policy (last month, hourly)
SELECT add_continuous_aggregate_policy(
  'sensors_summary_daily',
  start_offset     => INTERVAL '1 month',
  end_offset       => INTERVAL '1 hour',
  schedule_interval=> INTERVAL '1 hour'
);

-- 3) Compression on the CAGG (tune to your schema/queries)
ALTER MATERIALIZED VIEW sensors_summary_daily
SET (timescaledb.compress = true,
     timescaledb.compress_segmentby = 'id',
     timescaledb.compress_orderby   = 'bucket');

SELECT add_compression_policy('sensors_summary_daily', INTERVAL '30 days');
```

## 🚀 Run the task:

```sh
# Exercise
python cli.py t8 init_cagg
python cli.py t8 plot_all

# Solution (same commands)
python cli.py s8 init_cagg
python cli.py s8 plot_all
```

## 🧠 Evaluation

Query and plot from `sensors_summary_daily`.  
Compare performance vs querying raw `sensors` for the same range/aggregation:

```sql
SELECT id,
       time_bucket('1 day', time) AS bucket,
       AVG(value) AS avg_value,
       MAX(value) AS max_value,
       MIN(value) AS min_value
FROM sensors
WHERE time >= NOW() - INTERVAL '100 days'
GROUP BY id, bucket
ORDER BY id, bucket;
```

## ⚠️ Common Pitfalls

> 💡 **Tip:** Most CAGG issues are small syntax or refresh mistakes.  
> Here are the most frequent ones:
>
> - ❌ Forgetting `WITH NO DATA` — the CAGG won’t initialize correctly.
> - ❌ Not running `REFRESH CONTINUOUS AGGREGATE` after creation if no policy yet.
> - ⚠️ Missing `GROUP BY` keys matching your `SELECT` aggregation.
> - ⚙️ Forgetting to schedule a refresh policy → your view never updates.
> - 🗜️ Adding compression before first refresh → may prevent initial population.
> - 🧩 Forgetting `ORDER BY` / `SEGMENT BY` columns in the compression settings.

## 💡 Solution

Reference solution available here 👉 [solutions/\_08_continous_aggregates/task.py](../../solutions/_08_continous_aggregates/task.py) file.
