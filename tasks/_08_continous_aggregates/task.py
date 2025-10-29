# tasks/_08_continous_aggregates/task.py
import datetime as dt
from utils.decorators import time_execution, db_write_once, db_read_once
from utils.plots import plot_multiple


# The "time_execution" decorator will print the execution time of this function
# The "db_write_once" decorator is ment to provide the function with a database connection with error handling and rollback for an assumed write operation.
@time_execution()
@db_write_once(autocommit=True)  # makes each execute its own statement, no TX block
def init_cagg(cur):
    create_sql = """
    TODO: Create a continuous aggregate view named sensors_summary_daily that buckets data by 1 day intervals,
    calculating the average , maximum, and minimum values for each sensor id.
    """
    cur.execute(create_sql)  # must be a single statement here

    # policy must reference the SAME name:
    policy_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM timescaledb_information.jobs j
            WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
            AND j.hypertable_name = 'sensors_summary_daily'
        ) THEN
            TODO: Add a continuous aggregate refresh policy for sensors_summary_daily
        END IF;
    END;
    $$;
    """
    cur.execute(policy_sql)

    compression_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
                SELECT 1
                FROM timescaledb_information.jobs j
                JOIN timescaledb_information.job_stats s USING (job_id)
                WHERE j.proc_name = 'policy_compression'
                    AND j.hypertable_name = 'sensors_summary_daily'
            ) THEN
                TODO: Enable compression on sensors_summary_daily and add a compression policy to compress data older than 7 days
                END IF;
    END;
    $$;
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
