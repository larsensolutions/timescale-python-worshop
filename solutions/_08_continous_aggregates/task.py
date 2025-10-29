# solutions/_08_continous_aggregates/task.py
import datetime as dt
from utils.decorators import time_execution, db_write_once, db_read_once
from utils.plots import plot_multiple


# The "time_execution" decorator will print the execution time of this function
# The "db_write_once" decorator is ment to provide the function with a database connection with error handling and rollback for an assumed write operation.
@time_execution()
@db_write_once(autocommit=True)  # makes each execute its own statement, no TX block
def init_cagg(cur):
    create_sql = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS sensors_summary_daily
    WITH (timescaledb.continuous) AS
    SELECT id,
           time_bucket(INTERVAL '1 day', time) AS bucket,
           AVG(value) AS avg_value,
           MAX(value) AS max_value,
           MIN(value) AS min_value
    FROM sensors
    GROUP BY id, bucket;
    """
    cur.execute(create_sql)  # must be a single statement here

    # policy must reference the SAME name:
    policy_sql = """
    SELECT add_continuous_aggregate_policy(
                'sensors_summary_daily',
                start_offset => INTERVAL '1 month',
                end_offset   => INTERVAL '1 day',
                schedule_interval => INTERVAL '1 hour',
                if_not_exists => TRUE
            );
    """
    cur.execute(policy_sql)

    compression_sql = """
        ALTER MATERIALIZED VIEW sensors_summary_daily
        SET (timescaledb.compress, timescaledb.compress_orderby = 'bucket DESC',
                timescaledb.compress_segmentby = 'id');
        CALL add_columnstore_policy('sensors_summary_daily', after => INTERVAL '7 days', initial_start => now(), if_not_exists => true);
    """
    cur.execute(compression_sql)


@time_execution()
@db_read_once
def all(cur):
    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)
    cur.execute(
        """
        SELECT
            bucket, avg_value, max_value, min_value
        FROM sensors_summary_daily
        WHERE id = %s AND bucket BETWEEN %s AND %s
        ORDER BY bucket ASC;
        """,
        (id, start, end),
    )
    rows = cur.fetchall()
    times = [r[0] for r in rows]
    avg_v = [r[1] for r in rows]
    max_v = [r[2] for r in rows]
    min_v = [r[3] for r in rows]

    series = [
        {
            "kind": "range",
            "x": times,
            "y1": min_v,
            "y2": max_v,
            "label": "Min–Max",
            "alpha": 0.2,
        },
        {"kind": "line", "x": times, "y": avg_v, "label": "Average", "linewidth": 2},
    ]

    return series


def plot_all():
    """Cli function to plot all continuous aggregate data"""
    series = all()
    plot_multiple(
        "Sensor Daily Summary",
        series,
        title="Sensor Daily Summary",
        xlabel="Date",
        ylabel="Value",
    )


# The run function to be called when executing this task using the CLI. Please refer to the main README.md for instructions.
def run():
    init_cagg()


# Needed if this file is executed directly
if __name__ == "__main__":
    run()
