-- COMPLETE SQL SOLUTION. NOT IDEMPOTENT. COPY queries from here and EXECUTE in pgAdmin. ---

/********************************
  Requires that they are available as extensions in the database (So the docker image in our case)
 ********************************/
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit; 
CREATE EXTENSION IF NOT EXISTS pg_buffercache;      -- This is required for caching query results
CREATE EXTENSION IF NOT EXISTS pg_prewarm;          -- This is required for prewarming the buffer cache, i.e load hypertable chunks into memory
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;  -- This is required for query performance insights

-- We enable chunk skipping for better query performance if we have additional columns to filter on that are not part of the time dimension or segmentby columns.
-- We are not using any other columns in this workshop, but you are free to do so in your own experiments.
SET timescaledb.enable_chunk_skipping = on;


/********************************
  Create the sensors hypertable schema
 ********************************/

CREATE TABLE sensors (
  id INTEGER NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL -- Can also allow NULLS if desired
)
WITH (
  timescaledb.hypertable,
  timescaledb.partition_column = 'time',     -- required time partition
  timescaledb.segmentby        = 'id',-- segment by device
  timescaledb.orderby          = 'time ASC'  -- columnstore order (Hypercore)
);

-- Explicitly create the index as well, although it should be created automatically with the hypertable above by timescale
CREATE INDEX ON sensors (id, "time" ASC);

/********************************
  ADD COMPRESSION POLICY
 ********************************/
CALL add_columnstore_policy('sensors', after => INTERVAL '7 days', initial_start => now(), if_not_exists => true);


/********************************
  SET CHUNK TIME INTERVAL. Default is 7 days, if you don't set it explicitly. But good to be explicit. and know about it.
  You might want to change it depending on your data ingestion rate and query patterns.
 ********************************/
SELECT set_chunk_time_interval('sensors', INTERVAL '7 days');


/********************************
  Create a continuous aggregate materialized view for daily summaries
 ********************************/
CREATE MATERIALIZED VIEW IF NOT EXISTS sensors_summary_daily
    WITH (timescaledb.continuous) AS
    SELECT id,
           time_bucket(INTERVAL '1 day', time) AS bucket,
           AVG(value) AS avg_value,
           MAX(value) AS max_value,
           MIN(value) AS min_value
    FROM sensors
    GROUP BY id, bucket;

--- Add a policy to refresh the continuous aggregate every hour
SELECT add_continuous_aggregate_policy(
                'sensors_summary_daily',
                start_offset => INTERVAL '1 month',
                end_offset   => INTERVAL '1 day',
                schedule_interval => INTERVAL '1 hour'
            );

-- Add compression policy to the continuous aggregate as well
ALTER MATERIALIZED VIEW sensors_summary_daily 
SET (timescaledb.compress, timescaledb.compress_orderby = 'bucket DESC', timescaledb.compress_segmentby = 'id');

-- Add compression policy to compress data older than 7 days, and then immediately start compressing existing data
CALL add_columnstore_policy('sensors_summary_daily', after => INTERVAL '7 days', initial_start => now(), if_not_exists => true);

/********************************
  Generate some data. 
  NOTE: This is set to 1 second interval for 4 devices from 2025-08-01 until now, > 7.5 million rows.
*********************************/


INSERT INTO sensors (id, time, value)
SELECT
  id,
  time,
  random() * 100 AS value
FROM generate_series('2025-08-01 00:00:00'::timestamp,
                     now(),
                     INTERVAL '1 hour') AS time,
     generate_series(1, 4) AS id;


-- Otherwise, grap the SQL from the solutions files in each step folder.