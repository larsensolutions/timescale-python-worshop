# tasks/_03_ingest_insert/task.py
from psycopg.rows import tuple_row
import io  # Needed to handle the CSV string
import csv  # Needed to parse the CSV string into rows
import datetime as dt

from utils.generator import generate_csv_lines_batch
from utils.decorators import time_execution
from utils.db import get_connection


@time_execution()
def ingest_insert_commit(conn, cur, rows):
    """
    This function is only split from insert_ingest() for timing purposes.
    """
    cur.executemany(
        """
                INSERT INTO sensors (time, id, value)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
        rows,
    )
    conn.commit()
    print(f"📦 Ingested ~{cur.rowcount} rows.")


@time_execution(sync=True)
def ingest_insert():
    # Feel free to adjust parameters as needed. Right now, it generates sinusoid data for 2 devices over ~100 days, in 15,000-row batches, with some drift and jitter.
    # This should be apporx 17 million rows total, and use about 2GB of disk space.
    for csv_block in generate_csv_lines_batch(
        devices=2,
        step_sec=1,
        batch_size=15_000,
        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100),
        end=dt.datetime.now(dt.timezone.utc),
        drift_per_day=0.01,
        jitter_frac=0.3,
    ):
        rows = list(csv.reader(io.StringIO(csv_block)))  # [(time, id, value), ...]
        num_lines = len(csv_block.splitlines())
        print(f"\n🧬 Generated {num_lines} lines of sample data")
        with get_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
            ingest_insert_commit(conn, cur, rows)


def run():
    ingest_insert()


if __name__ == "__main__":
    run()
