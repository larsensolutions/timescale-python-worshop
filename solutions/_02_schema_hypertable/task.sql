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

-- Explicitly create the index as well, although it should be created automatically with the hypertable above by timescale,
-- You might want other indexes as well depending on your query patterns and additional columns
CREATE INDEX ON sensors (id, "time" ASC);

/********************************
  SET CHUNK TIME INTERVAL. Default is 7 days, if you don't set it explicitly. But good to be explicit. and know about it.
  You might want to change it depending on your data ingestion rate and query patterns.
 ********************************/
SELECT set_chunk_time_interval('sensors', INTERVAL '7 days');