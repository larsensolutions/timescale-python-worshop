import psutil
from collections import deque
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from datetime import datetime, timedelta
import matplotlib.dates as mdates


def run(window_sec: float = 60.0, interval_ms: int = 1000, disk_path: str = "/"):
    """Live CPU, RAM, Disk usage, and Disk I/O monitor (4 stacked subplots).

    Args:
        window_sec: rolling time window shown in the plots.
        interval_ms: update interval in milliseconds.
        disk_path: which mount path to sample for disk usage (%).
    """

    # --- Local state (no globals) ---
    times = deque()

    cpu_vals = deque()
    ram_vals = deque()
    disk_pct_vals = deque()

    read_kbps = deque()
    write_kbps = deque()
    _last_io = None  # (ts, read_bytes, write_bytes)

    # --- Figure / axes ---
    fig, axes = plt.subplots(4, 1, figsize=(9, 8), sharex=True)
    ax_cpu, ax_ram, ax_disk, ax_io = axes
    try:
        fig.canvas.manager.set_window_title("🐸 Machine Monitor")
    except Exception:
        pass  # some backends (e.g., notebooks) don't have a window title

    (line_cpu,) = ax_cpu.plot([], [], label="CPU %")
    (line_ram,) = ax_ram.plot([], [], label="RAM %")
    (line_disk,) = ax_disk.plot([], [], label=f"Disk % ({disk_path})")
    (line_read,) = ax_io.plot([], [], label="Read KB/s")
    (line_write,) = ax_io.plot([], [], label="Write KB/s")

    # Common axis setup
    for ax in (ax_cpu, ax_ram, ax_disk):
        ax.set_ylim(0, 100)
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)

    for ax in (ax_io,):
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)

    ax_cpu.set_ylabel("CPU %")
    ax_ram.set_ylabel("RAM %")
    ax_disk.set_ylabel("Disk %")
    ax_io.set_ylabel("KB/s")
    ax_io.set_xlabel("Time")

    # Time formatting
    ax_io.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    # --- Updater ---
    def update(_):
        nonlocal _last_io

        now = datetime.now()
        times.append(now)

        # CPU / RAM
        cpu_vals.append(psutil.cpu_percent(interval=None))
        ram_vals.append(psutil.virtual_memory().percent)

        # Disk usage %
        try:
            disk_pct_vals.append(psutil.disk_usage(disk_path).percent)
        except Exception:
            # Fallback if the path vanishes; keep last value or 0
            disk_pct_vals.append(disk_pct_vals[-1] if disk_pct_vals else 0.0)

        # Disk I/O rates (KB/s) from cumulative counters
        io = psutil.disk_io_counters(nowrap=False)
        if _last_io is None:
            read_kbps.append(0.0)
            write_kbps.append(0.0)
        else:
            last_ts, last_r, last_w = _last_io
            dt = max((now - last_ts).total_seconds(), 1e-9)
            dread = io.read_bytes - last_r
            dwrite = io.write_bytes - last_w
            # Guard against counter resets/rollover
            if dread < 0:
                dread = 0
            if dwrite < 0:
                dwrite = 0
            read_kbps.append(dread / dt / 1024.0)
            write_kbps.append(dwrite / dt / 1024.0)
        _last_io = (now, io.read_bytes, io.write_bytes)

        # Trim to rolling time window
        cutoff = now - timedelta(seconds=window_sec)

        def trim(deq):
            while times and times[0] < cutoff:
                times.popleft()
                deq.popleft()

        for series in (cpu_vals, ram_vals, disk_pct_vals, read_kbps, write_kbps):
            trim(series)

        # Update lines
        line_cpu.set_data(times, cpu_vals)
        line_ram.set_data(times, ram_vals)
        line_disk.set_data(times, disk_pct_vals)
        line_read.set_data(times, read_kbps)
        line_write.set_data(times, write_kbps)

        # Keep x-range to the last window_sec
        for ax in (ax_cpu, ax_ram, ax_disk, ax_io):
            ax.set_xlim(now - timedelta(seconds=window_sec), now)

        # Adaptive Y headroom
        def adapt(ax, series, min_top=100.0):
            if series:
                ymax = max(series)
                ax.set_ylim(0, max(min_top, ymax * 1.2))

        adapt(ax_cpu, cpu_vals, 100.0)
        adapt(ax_ram, ram_vals, 100.0)
        adapt(ax_disk, disk_pct_vals, 100.0)
        adapt(ax_io, [*read_kbps, *(write_kbps or [0.0])], 10.0)

        fig.autofmt_xdate()
        return line_cpu, line_ram, line_disk, line_read, line_write

    # Keep a reference to avoid GC
    ani = FuncAnimation(
        fig, update, interval=interval_ms, blit=False, cache_frame_data=False
    )
    plt.tight_layout()
    plt.show()
    return ani


if __name__ == "__main__":
    run()
