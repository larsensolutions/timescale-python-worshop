# solutions/_09_ingest_kaggle_bonus/task.py
"""
Bonus Task Solution: Ingest Kaggle data using COPY with insert monitoring and plot downsampled data.
DATA SOURCE: https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption

You can make this faster also. The improvement here comes from
not connecting for each batch. Further improvements could be made
by adjusting transaction settings per batch, you could collect an spesific
amount of data before committing, etc. Force commit on time intervals, whatever
comes first. It really depends on your use case.
"""
import csv
import io
from utils.db import get_connection
from utils.decorators import time_execution, db_read_once
from datetime import timezone
import datetime as dt

from utils.plots import plot_multiple

COPY_SQL = "COPY sensors(id, time, value) FROM STDIN WITH (FORMAT csv)"


@time_execution(rank=False)
@db_read_once
def downsampled(cur, id=1, start=None, end=None, resolution=300):
    if start is None:
        start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    if end is None:
        end = dt.datetime.now(dt.timezone.utc)
    if resolution is None:
        resolution = 300

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

    cur.execute(query, (resolution, id, start, end))

    results = cur.fetchall()

    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    print(f"🔢 Got {len(results)} rows for id={id} between {start} and {end}")
    return (timestamps, values)


@time_execution(rank=False)
def ingest_copy_kaggle_batch_solution(batch, cur, conn):
    """
    Commits this batch only (for timing/demo).
    `batch`: List[Tuple[int, datetime, float]]
    """
    # Per-transaction speed tweak
    cur.execute("SET LOCAL synchronous_commit = OFF")

    with cur.copy(COPY_SQL) as cp:
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        for sensor_id, ts, value in batch:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            # order: id, time, value  (matches COPY columns)
            w.writerow((sensor_id, ts.isoformat(), value))
        cp.write(buf.getvalue())

    conn.commit()
    print(f"📦 Ingested ~{len(batch):,} rows.\n")


@time_execution(sync=True)
def ingest_copy_kaggle_solution():
    from utils.disk import read_csv_in_batches

    csv_path = "data/kaggle_power_consumption.csv"  # "data/sensors_sample_data.csv"
    batch_size = 15_000

    with get_connection() as conn, conn.cursor() as cur:
        for batch in read_csv_in_batches(csv_path, batch_size=batch_size):
            # print(batch)
            ingest_copy_kaggle_batch_solution(batch, cur, conn)


def get_downsampled():
    """Cli function to plot all Kaggle sensors downsampled"""
    start = dt.datetime(2017, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2017, 12, 30, tzinfo=dt.timezone.utc)
    series = []

    # Since we are not using any sensor metadata table for simplicity, we just hardcode the sensor names here
    for index, sensor_name in enumerate(
        [
            "Temperature",
            "Humidity",
            "WindSpeed",
            "GeneralDiffuseFlows",
            "DiffuseFlows",
            "PowerConsumption_Zone1",
            "PowerConsumption_Zone2",
            "PowerConsumption_Zone3",
        ]
    ):
        (timestamps, values) = downsampled(
            id=index + 1, start=start, end=end, resolution=300
        )
        serie = {
            "kind": "line",
            "x": timestamps,
            "y": values,
            "label": sensor_name,
            "linewidth": 2,
        }
        series.append(serie)

    return series


def plot_downsampled_all():
    """Cli function to plot all Kaggle sensors downsampled"""
    series = get_downsampled()
    plot_multiple(
        "Kaggle Sensors Downsampled",
        series,
        title="Kaggle Sensors Downsampled",
        xlabel="Date",
        ylabel="Value",
    )


def run():
    """Run the ingestion task."""
    # ingest_copy_kaggle_solution()
    series = get_downsampled()
    plot_multiple("Kaggle Downsampled All tags", series)


if __name__ == "__main__":
    run()
