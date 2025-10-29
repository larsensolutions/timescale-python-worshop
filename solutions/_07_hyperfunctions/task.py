# ./solutions/_07_hyperfunctions/task.py
import time
import datetime as dt
from utils.decorators import db_read_once, time_execution
from utils.plots import show_xy_plot, show_timescale_histogram


@time_execution(sync=True)
@db_read_once
def downsampled(cur):
    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)
    resolution = 300  # target number of points

    query = """
       SELECT
        (timevector).time AS time,
        (timevector).value AS value
    FROM unnest(
        (SELECT lttb(td.time, td.value, %s)
        FROM sensors AS td
        WHERE td.id = %s AND td.time BETWEEN %s AND %s)
    ) AS timevector
        """
    start_time = time.monotonic()
    cur.execute(query, (resolution, id, start, end))
    end_time = time.monotonic()
    sql_duration_seconds = end_time - start_time
    print(
        f"SQL Execution Time (with data transfer): {sql_duration_seconds:.4f} seconds"
    )
    results = cur.fetchall()

    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    print(f"🔢 Got {len(results)} rows for id={id} between {start} and {end}")
    return (timestamps, values)


@time_execution(sync=True)
@db_read_once
def average_all(cur):
    """
    You should observe that there are no significant performance differences.
    However. The regular SQL query from task 6, cannot skip chunks with no relevant data, while the hyperfunction can.
    You can also set the start time of the bucket explicity
    """
    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)

    query = """
       SELECT
            time_bucket('1 month', "time") AS month,
            AVG(value) AS avg_value
        FROM sensors
        WHERE id = %s AND time BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 1;
        """

    cur.execute(query, (id, start, end))

    results = cur.fetchall()

    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    print(f"Got {len(results)} rows for id={id} between {start} and {end}")

    return (timestamps, values)


@time_execution(sync=True)
@db_read_once
def histogram(cur):
    """
    Compute a histogram of sensor values for a given id and time range using
    TimescaleDB's histogram() hyperfunction.

    The histogram(value, min, max, nbuckets) returns nbuckets + 2 counts:
    - 1 for underflow (< min)
    - nbuckets evenly spaced buckets between [min, max)
    - 1 for overflow (> max)
    """

    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)

    # Adjust min, max, and nbuckets for your data range
    min_val = 9
    max_val = 10
    nbuckets = 10

    query = """
        SELECT
            histogram(value, %s, %s, %s) AS hist
        FROM sensors
        WHERE id = %s AND time BETWEEN %s AND %s;
    """

    cur.execute(query, (min_val, max_val, nbuckets, id, start, end))
    result = cur.fetchone()

    if not result or not result[0]:
        print(f"No data found for id={id} between {start} and {end}")
        return None

    counts = list(result[0])  # histogram() returns a PostgreSQL array
    print(f"Got histogram for id={id}: {counts}")

    return (counts, min_val, max_val, nbuckets)


def plot_downsampled_all():
    """Cli function to plot downsampled"""
    (timestamps, values) = downsampled()
    show_xy_plot("Sensor Downsampled Data", timestamps, values)


def plot_average_all():
    """Cli function to plot average_all"""
    (timestamps, values) = average_all()
    show_xy_plot("Sensor Monthly Average", timestamps, values)


def plot_histogram():
    """Cli function to plot histogram"""
    (counts, min_val, max_val, nbuckets) = histogram()
    show_timescale_histogram(
        "Sensor Data Histogram", counts, min_val, max_val, nbuckets
    )


def run():
    """Run the task functions. You can switch between different functions to test them."""
    #  (timestamps, values) = downsampled()
    (timestamps, values) = average_all()

    show_xy_plot("Sensor Data", timestamps, values)

    # (counts, min_val, max_val, nbuckets) = histogram()
    # show_timescale_histogram(
    #    "Sensor Data Histogram", counts, min_val, max_val, nbuckets
    # )


if __name__ == "__main__":
    run()
