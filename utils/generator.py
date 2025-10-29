import datetime as dt
import random
import math
from typing import Literal, Iterator, Tuple


def generate_csv(
    n: int | None = None,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
    step_sec: int = 60,
    devices: int = 100,
    jitter_frac: float = 0.2,
) -> str:
    """
    Generate simulated time-series sensor data as a CSV string.

    Each line has the format:
        time,id,value

    The generator can either:
      • produce `n` samples (if `n` is given), or
      • produce samples between `start` and `end` (if both are provided).

    Parameters
    ----------
    n : int | None, optional
        Total number of samples to generate.
        If None, data is generated from `start` until `end`.

    start : datetime | None, optional
        The start timestamp for data generation.
        Defaults to 10 days before the current UTC time.

    end : datetime | None, optional
        The end timestamp for data generation.
        Defaults to the current UTC time.

    step_sec : int, optional
        The nominal sampling interval in seconds between data points.
        Default is 60 seconds.

    devices : int, optional
        The number of distinct sensor/device IDs to simulate.
        Device IDs are assigned in a round-robin fashion.
        Default is 100 devices.

    jitter_frac : float, optional
        The maximum ±fractional jitter applied to `step_sec`
        to simulate irregular sampling intervals.
        Example: 0.2 → each interval varies between 80% and 120% of `step_sec`.
        Default is 0.2 (±20%).

    Example usage:
    csv_data = generate_csv(
            start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3),
            end=dt.datetime.now(dt.timezone.utc),
            step_sec=120,
            jitter_frac=0.1,
        )

    Returns
    -------
    str
        A CSV-formatted string representing the generated sensor data.
    """
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    if start is None:
        start = now - dt.timedelta(days=10)
    if end is None:
        end = now

    lines = []
    t = start
    i = 0

    # Generate until we hit the limit (by count or by end time)
    while (n and i < n) or (not n and t <= end):
        did = i % devices
        val = 10 + random.random()
        lines.append(f"{t.isoformat()},{did},{val}\n")

        # Apply ±jitter to the nominal step interval
        jitter = random.uniform(-jitter_frac, jitter_frac)
        actual_step = step_sec * (1 + jitter)
        t += dt.timedelta(seconds=actual_step)
        i += 1

        if not n and t > end:
            break

    return "".join(lines)


def generate_csv_lines_batch(
    n: int | None = None,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
    step_sec: int = 60,
    devices: int = 100,
    jitter_frac: float = 0.2,
    *,
    # --- BATCHING CONTROL ---
    batch_size: int = 10000,  # Maximum number of rows to yield in one batch
    # value model controls
    baseline_mode: Literal["random", "by_id"] = "random",
    base_value: float = 10.0,  # central baseline
    base_spread: float = 2.0,  # random baseline ±spread (used in "random" mode)
    by_id_step: float = 0.1,  # per-id increment (used in "by_id" mode)
    daily_amp: float = 0.5,  # daily sinusoid amplitude
    noise_sigma: float = 0.05,  # Gaussian noise (std dev)
    drift_per_day: float = 0.0,  # linear drift per day (e.g., 0.02)
    seed: int | None = 42,  # set None for non-deterministic
) -> Iterator[str]:
    """
    Generate simulated time-series sensor data and YIELD CSV data in batches
    based on a maximum row count (`batch_size`).

    CSV format per line:
        time,id,value

    Generation modes:
      • Count-limited: set `n`.
      • Time-limited: set `start` and `end` (defaults: last 10 days to now).
    """
    if seed is not None:
        random.seed(seed)

    # --- Time Setup: Set defaults if not provided ---
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    if start is None:
        start = now - dt.timedelta(days=10)
    if end is None:
        end = now

    # --- Build per-device profiles ---
    profiles = []
    for did in range(devices):
        if baseline_mode == "by_id":
            baseline = base_value + did * by_id_step
        else:  # "random"
            baseline = base_value + random.uniform(-base_spread, base_spread)

        # Give each device a unique phase and small amplitude tweak
        phase = random.uniform(0, 2 * math.pi)
        amp = daily_amp * random.uniform(0.8, 1.2)
        profiles.append((baseline, amp, phase))

    # --- Generator Loop ---
    t = start
    i = 0  # Total line counter
    current_batch_lines = []

    while t <= end:

        # 1. Iterate through ALL devices at the current time t
        for did in range(devices):
            baseline, amp, phase = profiles[did]

            # Time features
            seconds_since_start = (t - start).total_seconds()
            days_since_start = seconds_since_start / 86400.0
            day_angle = 2 * math.pi * ((t.timestamp() % 86400) / 86400.0)  # 24h cycle

            # Value model: baseline + daily cycle + drift + noise
            value = (
                baseline
                + amp * math.sin(day_angle + phase)
                + drift_per_day * days_since_start
                + random.gauss(0.0, noise_sigma)
            )

            current_batch_lines.append(f"{t.isoformat()},{did},{value}\n")
            i += 1

            # 2. Check the overall row count limit (`n`)
            if n is not None and i >= n:
                # Yield any final lines and stop the function
                if current_batch_lines:
                    yield "".join(current_batch_lines)
                return

            # 3. Check the batch size limit: Yield the batch and reset
            if len(current_batch_lines) >= batch_size:
                yield "".join(current_batch_lines)
                current_batch_lines = []  # Reset the batch

        # 4. Advance time ONLY after sampling all devices

        # Irregular sampling: ±jitter around step_sec
        jitter = random.uniform(-jitter_frac, jitter_frac)
        actual_step = step_sec * (1 + jitter)
        t += dt.timedelta(seconds=actual_step)

    # Yield any final remaining lines if the loop finished due to t > end
    if current_batch_lines:
        yield "".join(current_batch_lines)


def simulate_temp_sensor(
    start: dt.datetime,
    end: dt.datetime,
    step_sec: int = 60,
    *,
    # Device and environment characteristics
    baseline_temp: float = 20.0,  # Average temperature in Celsius
    daily_amplitude: float = 5.0,  # Max swing from baseline (e.g., 20 +/- 5)
    noise_sigma: float = 0.1,  # Standard deviation of random sensor noise
    drift_per_day: float = 0.005,  # Slow, linear warming/cooling drift
    seed: int | None = 42,
) -> Iterator[Tuple[dt.datetime, float]]:
    """
    Generates time and value tuples for a single simulated temperature sensor.

    Yields: (dt.datetime, float)
    """
    if seed is not None:
        random.seed(seed)

    # 1. Device Profile (Fixed for a single sensor)
    # The phase is randomized here to simulate an arbitrary start point
    # in the 24-hour cycle relative to the 'start' time.
    initial_phase = random.uniform(0, 2 * math.pi)

    t = start

    while t <= end:
        # Time calculations
        seconds_since_start = (t - start).total_seconds()
        days_since_start = seconds_since_start / 86400.0

        # Angle of the 24h cycle (0 to 2*pi over 86400 seconds)
        seconds_in_day = t.timestamp() % 86400
        day_angle = 2 * math.pi * (seconds_in_day / 86400.0)

        # 2. Value Model: Baseline + Cycle + Drift + Noise
        value = (
            # Baseline: Average temperature
            baseline_temp
            # Daily Cycle: Sinusoidal variation
            + daily_amplitude * math.sin(day_angle + initial_phase)
            # Drift: Slow environmental change over time
            + drift_per_day * days_since_start
            # Noise: High-frequency sensor error/fluctuation
            + random.gauss(0.0, noise_sigma)
        )

        # 3. Yield the timestamp and the value
        yield t, value

        # 4. Advance time (no jitter for simplicity in this example)
        t += dt.timedelta(seconds=step_sec)
