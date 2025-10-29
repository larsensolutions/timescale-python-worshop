import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter

import plotly.express as px
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple


def show_xy_plot(name, timestamps, values):
    print(plt.style.available)
    if "seaborn-v0_8-dark" in plt.style.available:
        plt.style.use("seaborn-v0_8-dark")

    fig, ax = plt.subplots(figsize=(10, 4))
    fig.canvas.manager.set_window_title("🐸")
    ax.plot(timestamps, values, marker="o")
    # Format x-axis dates
    ax.xaxis.set_major_formatter(
        DateFormatter("%Y-%m-%d %H:%M")
    )  # Customize format here
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())  # Auto spacing

    plt.xticks(rotation=45)  # Rotate labels for readability

    plt.plot(timestamps, values, marker="o")
    plt.xlabel("Time")
    plt.ylabel("Value")
    plt.title(name)
    plt.grid(True)
    plt.tight_layout()

    plt.show()


def show_interactive_xy_plot(id_time_value):
    df = pd.DataFrame(id_time_value, columns=["id", "time", "value"])
    fig = px.line(df, x="time", y="value", title="Interactive Timeseries Plot")

    # Format time axis
    fig.update_layout(
        xaxis_tickformat="%Y-%m-%d %H:%M",  # Format datetime labels
        xaxis_title="Time",
        yaxis_title="Value",
    )

    fig.show()


def show_timescale_histogram(name, counts, min_val, max_val, nbuckets):
    """
    Plot a histogram that matches TimescaleDB's histogram(value, min, max, nbuckets) output.

    counts: sequence of length nbuckets+2 (underflow, nbuckets in-range, overflow)
    """
    assert (
        len(counts) == nbuckets + 2
    ), "counts must be nbuckets + 2 (underflow + in-range + overflow)"
    edges = np.linspace(min_val, max_val, nbuckets + 1)

    # Build bucket labels: <min, [e0,e1), [e1,e2), ..., [e_{n-1},e_n), >max
    labels = [f"< {min_val:g}"]
    labels += [f"[{edges[i]:g}, {edges[i + 1]:g})" for i in range(nbuckets)]
    labels += [f"> {max_val:g}"]

    x = np.arange(len(counts))

    fig, ax = plt.subplots(figsize=(10, 4))
    fig.canvas.manager.set_window_title("🐸")
    ax.bar(x, counts, edgecolor="black", alpha=0.8)
    ax.set_xticks(x, labels, rotation=45, ha="right")
    ax.set_xlabel("Value range (Timescale bins)")
    ax.set_ylabel("Count")
    ax.set_title(name)
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()


def plot_multiple(
    name: str,
    series: List[Dict[str, Any]],
    *,
    title: Optional[str] = None,
    xlabel: str = "Time",
    ylabel: str = "Value",
    figsize: Tuple[int, int] = (10, 5),
):
    """
    Plot multiple lines (and optional ranges) on one chart.
    series: List of series to plot. Each series is a dict with keys:
        - kind: "line" or "range"
        - x: list of x values (datetime)
        - y: list of y values (for "line" kind)
        - y1, y2: list of y values (for "range" kind)
        - label: label for the legend
        - linestyle, linewidth, marker, alpha, color: optional style parameters
    """
    if "seaborn-v0_8-dark" in plt.style.available:
        plt.style.use("seaborn-v0_8-dark")

    plt.figure(name, figsize=figsize)
    ax = plt.gca()

    # Format x-axis dates
    ax.xaxis.set_major_formatter(
        DateFormatter("%Y-%m-%d %H:%M")
    )  # Customize format here
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())  # Auto spacing

    plt.xticks(rotation=45)  # Rotate labels for readability

    for s in series:
        kind = s.get("kind", "line")
        if kind == "line":
            x, y = s["x"], s["y"]
            ax.plot(
                x,
                y,
                label=s.get("label"),
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", None),
                alpha=s.get("alpha", 1.0),
                color=s.get("color", None),
            )
        elif kind == "range":
            x, y1, y2 = s["x"], s["y1"], s["y2"]
            ax.fill_between(
                x,
                y1,
                y2,
                label=s.get("label"),
                alpha=s.get("alpha", 0.2),
                color=s.get("color", None),
                linewidth=0,
            )
        else:
            raise ValueError(f"Unknown kind: {kind}")

    ax.set_title(title or name)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best")
    plt.tight_layout()
    plt.show()
