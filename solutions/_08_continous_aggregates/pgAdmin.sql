-- If you just want to do it in pgAdmin, run this script there

-- Create the continuous aggregate view
CREATE MATERIALIZED VIEW IF NOT EXISTS sensors_summary_daily
WITH (timescaledb.continuous) AS
SELECT id,
   time_bucket(INTERVAL '1 day', time) AS bucket,     
   AVG(value) AS avg_value,
   MAX(value) AS max_value,
   MIN(value) AS min_value
FROM sensors
WHERE time >= NOW() - INTERVAL '100 days'
GROUP BY id, bucket;

DO $$
  BEGIN
    IF NOT EXISTS (
          SELECT 1
          FROM timescaledb_information.jobs j
          WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
          AND j.hypertable_name = 'sensors_summary_daily'
      ) THEN
          PERFORM add_continuous_aggregate_policy(
              'sensors_summary_daily',
              start_offset => INTERVAL '1 month',
              end_offset   => INTERVAL '1 day',
              schedule_interval => INTERVAL '1 hour'
          );
      END IF;
  END;
$$;


DO $$
  BEGIN
    IF NOT EXISTS (
            SELECT 1
            FROM timescaledb_information.jobs j
            JOIN timescaledb_information.job_stats s USING (job_id)
            WHERE j.proc_name = 'policy_compression'
                AND j.hypertable_name = 'sensors_summary_daily'
        ) THEN
            -- Add compression policy to the continuous aggregate
            ALTER MATERIALIZED VIEW sensors_summary_daily 
            SET (timescaledb.compress, timescaledb.compress_orderby = 'bucket DESC', timescaledb.compress_segmentby = 'id');
            -- Add compression policy to compress data older than 7 days, and then immediately start compressing existing data
            CALL add_columnstore_policy('sensors_summary_daily', after => INTERVAL '7 days', initial_start => now());
            END IF;
  END;
$$;