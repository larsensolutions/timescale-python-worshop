# tasks/_06_query_basics/task.py
import datetime as dt
from utils.decorators import db_read_once, time_execution
from utils.plots import show_xy_plot


@time_execution(sync=True)
@db_read_once
def plot_all(cur):
    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)

    query = """
        SELECT id, time, value FROM sensors
        WHERE id = %s AND time BETWEEN %s AND %s
        """

    cur.execute(query, (id, start, end))

    results = cur.fetchall()

    timestamps = [row[1] for row in results]
    values = [row[2] for row in results]

    print(f"🔢 Got {len(results)} rows for id={id} between {start} and {end}")

    # We return the timestamps and values for plotting,
    # so that we can time the sql query separately from the plotting
    return (timestamps, values)


@time_execution(sync=True)
@db_read_once
def average_all(cur):
    id = 1
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)

    query = """
        SELECT date_trunc('month', "time") AS month, avg(value) AS avg_value
            FROM sensors
            WHERE id = %s AND time BETWEEN %s AND %s
            GROUP BY 1
            ORDER BY 1;
        """

    cur.execute(query, (id, start, end))

    results = cur.fetchall()

    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    print(f"🔢 Got {len(results)} rows for id={id} between {start} and {end}")

    # We return the timestamps and values for plotting,
    # so that we can time the sql query separately from the plotting
    return (timestamps, values)


@time_execution(sync=True)
@db_read_once
def custom(cur):
    """Placeholder for a custom query function. You can implement your own query here."""

    # Please return data needed to plot, e.g. (timestamps, values) and don't plot directly here, for time execution measurement consistency.
    return None


def plot_average_all():
    """Cli function to plot average_all"""
    (timestamps, values) = average_all()
    show_xy_plot("Sensor Monthy Average", timestamps, values)


def plot_raw_all():
    """Cli function to plot plot_all"""
    (timestamps, values) = plot_all()
    show_xy_plot("Sensor Raw Data", timestamps, values)


def plot_custom():
    """Cli function custom plot placeholder. You might want a different chart, you should add it in the utils/plts.py"""
    res = custom()
    if res is None:
        print("⚠️  No custom query implemented")
        return
    (timestamps, values) = res
    show_xy_plot("Sensor Custom Data", timestamps, values)


def run():
    """If you are executing the file directly => You can switch between different functions to test them."""
    # (timestamps, values) = plot_all()
    (timestamps, values) = average_all()
    show_xy_plot("Sensor Data", timestamps, values)


if __name__ == "__main__":
    run()
