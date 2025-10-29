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
  Create the sensors hypertable schema here
 ********************************/
