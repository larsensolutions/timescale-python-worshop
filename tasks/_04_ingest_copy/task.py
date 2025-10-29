# tasks/_04_ingest_copy/task.py
import datetime as dt

from utils.db import get_connection
from utils.generator import generate_csv_lines_batch
from utils.decorators import time_execution


@time_execution()
def ingest_copy_commit(conn, cur, rows):
    """
    This function is only split from ingest_copy() for timing purposes.
    """
    with cur.copy("COPY sensors(time, id, value) FROM STDIN WITH (FORMAT csv)") as cp:
        cp.write(rows)  # rows is a single CSV string
        # or stream in chunks: for chunk in gen(): cp.write(chunk)
    conn.commit()
    print(f"📦 Ingested ~{len(rows.splitlines())} rows.")


@time_execution(sync=True)
def ingest_copy():
    for csv_block in generate_csv_lines_batch(
        devices=2,
        step_sec=1,
        batch_size=15_000,
        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100),
        end=dt.datetime.now(dt.timezone.utc),
        drift_per_day=0.01,
        jitter_frac=0.3,
    ):
        num_lines = len(csv_block.splitlines())
        print(f"\n🧬 Generated {num_lines} lines of sample data")
        with get_connection() as conn, conn.cursor() as cur:
            ingest_copy_commit(conn, cur, csv_block)


def run():
    """Run the ingestion task."""
    ingest_copy()


if __name__ == "__main__":
    run()
