from collections import deque
from datetime import datetime, timedelta
from typing import Sequence, Tuple

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.dates as mdates

from utils.decorators import db_read_once
from utils.api import post_async


def run(
    target_tables: Sequence[str] = ("public.timeseries_raw",),
    window_sec: float = 60.0,
    poll_sec: float = 1.0,
):
    """Live inserts/sec monitor for one or more Timescale hypertables.
    Each table gets its own subplot; all share the same time axis.
    """

    if not target_tables:
        raise ValueError("target_tables must contain at least one table")

    # --- Local state ---
    times = deque()  # shared x-axis
    prev_ts = None  # last sample timestamp (shared)
    prev_ins = {t: 0.0 for t in target_tables}  # last cumulative inserts per table
    series = {t: deque() for t in target_tables}  # rates per table

    def _split_schema_table(qualified: str) -> Tuple[str, str]:
        parts = qualified.split(".", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])

    # --- DB helper ---
    @db_read_once
    def fetch_inserts(cur, target_table: str) -> float:
        schema, name = _split_schema_table(target_table)
        # Sum n_tup_ins across all chunks for this hypertable
        cur.execute(
            """
            WITH parent AS (
            SELECT c.oid AS relid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            ),
            children AS (
            SELECT i.inhrelid AS relid
            FROM pg_inherits i
            JOIN parent p ON p.relid = i.inhparent
            )
            SELECT COALESCE(
            (SELECT SUM(s.n_tup_ins)::bigint
                FROM pg_stat_all_tables s
                WHERE s.relid IN (SELECT relid FROM children)),
            (SELECT s.n_tup_ins::bigint
                FROM pg_stat_all_tables s
                WHERE s.relid = (SELECT relid FROM parent)),
            0
            ) AS n_tup_ins_total
            """,
            (schema, name),
        )
        return float(cur.fetchone()[0])

    # --- Figure / axes (one row per table) ---
    n = len(target_tables)
    fig, axes = plt.subplots(n, 1, figsize=(9, max(3.2, 2.8 * n)), sharex=True)
    try:
        fig.canvas.manager.set_window_title("🐸 Timescale Inserts/sec")
    except Exception:
        pass

    # Normalize axes to a list
    if n == 1:
        axes = [axes]

    lines = {}
    for ax, table in zip(axes, target_tables):
        (line,) = ax.plot([], [], label=f"Inserts/sec · {table}")
        lines[table] = line
        ax.set_ylabel("rows/s")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Time")
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    # --- Sampling ---
    def sample():
        nonlocal prev_ts, prev_ins
        now = datetime.now()

        # Pull current cumulative inserts for each table
        curr_ins = {}
        for table in target_tables:
            try:
                curr_ins[table] = fetch_inserts(table)
            except Exception as e:
                print(f"Fetch error for {table}: {e}")
                # Keep previous value to compute 0 rate this tick
                curr_ins[table] = prev_ins.get(table, 0.0)

        if prev_ts is None:
            prev_ts = now
            prev_ins = curr_ins
            times.append(now)
            for t in target_tables:
                series[t].append(0.0)
            return

        dt = (now - prev_ts).total_seconds() or 1.0
        for t in target_tables:
            rate = max(0.0, (curr_ins[t] - prev_ins[t]) / dt)
            payload = {
                "funcName": f"writes_per_{poll_sec}s",
                "value": rate,
                "uom": "rows/s",
            }
            post_async(payload)
            series[t].append(rate)
        prev_ts, prev_ins = now, curr_ins
        times.append(now)

        # Trim rolling window
        cutoff = now - timedelta(seconds=window_sec)
        while times and times[0] < cutoff:
            times.popleft()
            for t in target_tables:
                if series[t]:
                    series[t].popleft()

    # --- Animation update ---
    def update(_):
        try:
            sample()
        except Exception as e:
            print("Sample error:", e)

        if times:
            now = times[-1]
            # Update each subplot
            for ax, t in zip(axes, target_tables):
                lines[t].set_data(times, series[t])
                # Y autoscale with a bit of headroom
                ymax = max(series[t]) if series[t] else 1.0
                ax.set_ylim(0, max(1.0, ymax * 1.2))
            # Shared x-limits (apply to any axis since sharex=True)
            axes[0].set_xlim(now - timedelta(seconds=window_sec), now)

        fig.autofmt_xdate()
        return tuple(lines.values())

    ani = FuncAnimation(
        fig,
        update,
        interval=int(poll_sec * 1000),
        blit=False,
        cache_frame_data=False,
    )
    plt.tight_layout()
    plt.show()
    return ani


if __name__ == "__main__":
    run()
