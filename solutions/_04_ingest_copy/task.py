# solutions/_04_ingest_copy/task.py
"""
You can make this faster also. The improvement here comes from
not connecting for each batch. Further improvements could be made
by adjusting transaction settings per batch, you could collect an spesific
amount of data before committing, etc. Force commit on time intervals, whatever
comes first. It really depends on your use case.
"""

import datetime as dt
from utils.db import get_connection
from utils.generator import generate_csv_lines_batch
from utils.decorators import time_execution

COPY_SQL = "COPY sensors(time, id, value) FROM STDIN WITH (FORMAT csv)"


@time_execution()
def ingest_copy_commit_solution(cur, rows, conn):
    """
    This function is only split from ingest_copy_solution() for timing purposes.
    """
    # Optional speed tweak per batch (applies to this transaction only)
    cur.execute("SET LOCAL synchronous_commit = OFF")

    with cur.copy(COPY_SQL) as cp:
        cp.write(rows)

    conn.commit()

    nrows = rows.count("\n")
    print(f"📦 Ingested ~{nrows:,} rows.")


@time_execution(sync=True)
def ingest_copy_solution():
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=100)
    end = dt.datetime.now(dt.timezone.utc)

    # Single connection + cursor reused across batches
    with get_connection() as conn, conn.cursor() as cur:
        for csv_block in generate_csv_lines_batch(
            devices=2,
            step_sec=1,
            batch_size=15_000,
            start=start,
            end=end,
            drift_per_day=0.01,
            jitter_frac=0.3,
        ):
            num_lines = len(csv_block.splitlines())
            print(f"\n🧬 Generated {num_lines} lines of sample data")
            ingest_copy_commit_solution(cur, csv_block, conn)  # ⬅️ one batch


def run():
    """Run the ingestion task."""
    ingest_copy_solution()


if __name__ == "__main__":
    run()
