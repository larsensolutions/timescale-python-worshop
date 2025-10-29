-- You can enable compression on a hypertable using the add_compression_policy function.
-- Note, this will compress the chunks immediately, use initial_start to delay the compression start time, for instance:
-- initial_start => now() + '1 hours'::interval
-- This will start compressing chunks 1 hour from now.
CALL add_columnstore_policy('sensors', after => INTERVAL '7 days', initial_start => now(), if_not_exists => true);