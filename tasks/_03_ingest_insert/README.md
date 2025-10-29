# Task 3 | 📦 Ingest data using INSERT

In this task, you will:

- ✅ Already have completed [Task 1](../_01_setup/README.md) and [Task 2](../_02_schema_hypertable/README.md).

- ✅ Have a `sensors` hypertable created in your TimescaleDB instance with columns `(id, time, value)`.

- ☑️ Keep an eye on disk usage -> truncate the `sensors` table as needed before re-running this task.

## 🧱 Objective

Generate **synthetic time-series data** and ingest it into the `sensors` hypertable using SQL `INSERT` statements.  
You’ll also monitor ingestion performance (rows per second).

A working Python script is already provided to ingest data for two devices over the last 100 days.  
It’s intentionally unoptimized — your goal is to **improve ingestion performance** while still using `INSERT` statements.

After completing the default run, you should see roughly:

- ~**17 million rows** in the `sensors` table

- A hypertable size of about **2 GB**

You can tweak the generator parameters (data volume, noise, drift, batch size, sampling rate, etc.) in[generator.py](../../utils/generator.py). The generator produces sinusoidal signals with noise and drift (simulating irregular real-world sensor data).

If you prefer to ingest from a CSV file instead, see [disk.py](../../utils/disk.py) for helper functions and an example mapper.

## 🚀 Run the task

```sh
# Using the CLI
python cli.py t3

# Compare with the solution version to see the performance difference
python cli.py s3
```

## 🧠 Evaluation

While ingesting, a **monitor window** (PyPlot) will start automatically in a separate thread, showing rows-per-second performance.  
Let it run until ingestion completes or you stop it manually — each batch results are submitted to the workshop leaderboard on the big screen.

<img src="leaderboard.png" alt="Workshop leaderboard" width="1200">

## ✅ Verification

Check your ingestion progress with **pgAdmin** or the **CLI**.

### Row count

```sql
-- Accurate but slow for large datasets
SELECT COUNT(*) FROM sensors;

-- Preferred: approximate and fast
SELECT approximate_row_count('sensors');
```

or

```sh
python cli.py table-count
```

### Hypertable size

```sql
SELECT pg_size_pretty(hypertable_size('sensors'));
-- Expected ≈ 2 GB after this task
```

or

````sh
python cli.py table-size
``

### Reset before rerun


```sql
TRUNCATE sensors;
````

or

```sh
python cli.py truncate-sensors
```

Then you can re-run the ingestion task as many times as you like. You can ofcourse just insert several times, but it will consume more disk space each time.

## 💡 Solution

Reference solution 👉 [solutions/\_03_ingest_insert/task.py](../../solutions/_03_ingest_insert/task.py)
