from utils.banner import print_banner
import sys
import typer
import questionary
from rich import print
import multiprocessing
from typing import List, Optional

import utils.monitor_inserts as mts
from utils.decorators import time_execution

import asyncio


print("python:", sys.version)

app = typer.Typer(help="Timescale workshop CLI")


@app.command("welcome")
@time_execution(sync=True, rank=False)
def welcome():
    """Print welcome banner"""
    print_banner()


#################
# RUN TASKS     #
#################


@app.command("t1")
@time_execution(sync=True, rank=False)
def task_1():
    """Task 1: Verify database connection"""
    from tasks._01_setup import task

    task.run()


@app.command("t2")
@time_execution(sync=True, rank=False)
def task_2():
    """Task 2: Create hypertable with chunking"""
    from tasks._02_schema_hypertable import task

    task.run()


@app.command("t3")
@time_execution(sync=True, rank=False)
def task_3():
    """Task 3: Ingest data using INSERT with insert monitoring"""
    from tasks._03_ingest_insert import task

    monitor_process = multiprocessing.Process(target=run_monitoring, daemon=True)
    monitor_process.start()

    task.run()


@app.command("t4")
@time_execution(sync=True, rank=False)
def task_4():
    """Task 4: Ingest data using COPY with insert monitoring"""
    from tasks._04_ingest_copy import task

    monitor_process = multiprocessing.Process(target=run_monitoring, daemon=True)
    monitor_process.start()

    task.run()


@app.command("t5")
@time_execution(sync=True, rank=False)
def task_5():
    """Task 5: Apply compression to hypertable"""
    from tasks._05_compression import task

    task.run()


@app.command("t6")
@time_execution(sync=True, rank=False)
def task_6(
    action: str = typer.Argument(
        "run", help="Action: run, plot_raw_all, plot_average_all, plot_custom"
    )
):
    """Task 6: Basic SQL queries -> actions: run, plot_raw_all, plot_average_all"""
    from tasks._06_query_basics import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


@app.command("t7")
@time_execution(sync=True, rank=False)
def task_7(
    action: str = typer.Argument(
        "run",
        help="Action: run, plot_downsampled_all, plot_average_all, plot_histogram",
    )
):
    """Task 7: Hyperfunctions -> actions: run, plot_downsampled_all, plot_average_all, plot_histogram"""
    from tasks._07_hyperfunctions import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


@app.command("t8")
@time_execution(sync=True, rank=False)
def task_8(
    action: str = typer.Argument(
        "run",
        help="Action: run, init_cagg, plot_all",
    )
):
    """Task 8: Continuous aggregates -> actions: run, init_cagg, plot_all"""
    from tasks._08_continous_aggregates import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


#################
# RUN SOLUTIONS #
#################


@app.command("s1")
@time_execution(sync=True, rank=False)
def solution_1():
    """Solution 1: Verify database connection"""
    from solutions._01_setup import task

    task.run()


@app.command("s2")
@time_execution(sync=True, rank=False)
def solution_2():
    """Solution 2: Create hypertable with chunking"""
    from solutions._02_schema_hypertable import task

    task.run()


@app.command("s3")
@time_execution(sync=True)
def solution_3():
    """Solution 3: Ingest data using batch INSERT with insert monitoring"""
    from solutions._03_ingest_insert import task

    monitor_process = multiprocessing.Process(target=run_monitoring, daemon=True)
    monitor_process.start()

    task.run()


def run_monitoring():
    mts.run(target_tables=["public.sensors"])


@app.command("s4")
@time_execution(sync=True)
def solution_4():
    """Solution 4: Ingest data using batch COPY with insert monitoring"""
    from solutions._04_ingest_copy import task

    monitor_process = multiprocessing.Process(target=run_monitoring, daemon=True)
    monitor_process.start()

    task.run()


@app.command("s5")
@time_execution(sync=True)
def solution_5():
    """Solution 5: Apply and run compression to hypertable."""
    from solutions._05_compression import task

    task.run()


@app.command("s6")
@time_execution(sync=True, rank=False)
def solution_6(
    action: str = typer.Argument(
        "run", help="Action: run, plot_raw_all, plot_average_all"
    )
):
    """Solution 6: Fetch and plot data — basic SQL queries"""
    from solutions._06_query_basics import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


@app.command("s7")
@time_execution(sync=True, rank=False)
def solution_7(
    action: str = typer.Argument(
        "run",
        help="Action: run, plot_downsampled_all, plot_average_all, plot_histogram",
    )
):
    """Solution 7: Fetch and plot data — hyperfunctions"""
    from solutions._07_hyperfunctions import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


@app.command("s8")
@time_execution(sync=True, rank=False)
def solution_8(
    action: str = typer.Argument(
        "run",
        help="Action: run, init_cagg, plot_all",
    )
):
    """Solution 8: Continuous aggregates -> actions: init_cagg, plot_all"""
    from solutions._08_continous_aggregates import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


@app.command("bonus-kaggle")
@time_execution(sync=True, rank=False)
def solution_9(
    action: str = typer.Argument(
        "run",
        help="Action: run, ingest_copy_kaggle_solution, plot_downsampled_all",
    )
):
    """Solution 9 [Bonus]: Ingest Kaggle data using COPY with insert monitoring. Bonus task."""
    from solutions._09_ingest_kaggle_bonus import task

    if not hasattr(task, action):
        typer.echo(f"❌  Unknown action: {action}")
        raise typer.Exit(code=1)

    typer.echo(f"▶️  Executing: {action}() ...")
    getattr(task, action)()


####################
# OTHER COMMANDS   #
####################


@app.command("ws-stream")
@time_execution(sync=True, rank=False)
def ws_stream():
    """View the workshop live event stream."""
    from utils.subscriber import main as run_subscriber

    asyncio.run(run_subscriber())


@app.command("monitor-machine")
@time_execution(sync=True, rank=False)
def monitor_machine():
    """Run the system monitor."""
    import utils.monitor_machine as mm

    try:
        mm.run()
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")


@app.command("monitor-inserts")
@time_execution(sync=True, rank=False)
def monitor_inserts(
    tables: Optional[List[str]] = typer.Argument(None, help="Tables to monitor")
):
    """Run the insert monitor for the sensor table. Does support multiple tables. Usage: monitor-inserts sensors sensors_archive"""
    tables = tables or ["public.sensors"]
    try:
        mts.run(target_tables=tables)
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")


@app.command("truncate-sensors")
@time_execution(sync=True, rank=False)
def truncate_sensors():
    """Truncate the sensors table"""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE sensors;")
        conn.commit()
        count = cur.execute("SELECT approximate_row_count('sensors');").fetchone()[0]
        print(f"✅ Sensors table truncated, approx row count: {count}")


@app.command("table-count")
@time_execution(sync=True, rank=False)
def row_count():
    """Get approximate row count of sensors table"""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        count = cur.execute("SELECT approximate_row_count('sensors');").fetchone()[0]
        print(f"✅ Sensor table approx row count: {count}")


@app.command("table-size")
@time_execution(sync=True, rank=False)
def table_size():
    """Get the size of sensors table"""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        size = cur.execute(
            "SELECT pg_size_pretty(hypertable_size('sensors'));"
        ).fetchone()[0]
        print(f"🎂 Sensor table size: {size}")


@app.command("table-chunks")
@time_execution(sync=True, rank=False)
def table_chunks():
    """Get the chunk information of sensors table"""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        chunks = cur.execute(
            """SELECT
                tic.chunk_name, range_start, range_end, is_compressed, pg_size_pretty(total_bytes) AS total
                FROM chunks_detailed_size('sensors') cs LEFT JOIN timescaledb_information.chunks tic
                ON tic.chunk_name= cs.chunk_name
                ORDER BY range_start DESC"""
        ).fetchall()
        for i, chunk in enumerate(chunks, start=1):
            print(
                f"🍰 Chunk {i}: {chunk[0]}, Range: {chunk[1]} - {chunk[2]}, Compressed: {chunk[3]}, Size: {chunk[4]}"
            )


@app.command("cagg-chunks")
@time_execution(sync=True, rank=False)
def cagg_chunks():
    """Get the chunk information of continuous aggregate sensors_summary_daily table"""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        chunks = cur.execute(
            """SELECT
                tic.chunk_name, range_start, range_end, is_compressed, pg_size_pretty(total_bytes) AS total
                FROM chunks_detailed_size('sensors_summary_daily') cs LEFT JOIN timescaledb_information.chunks tic
                ON tic.chunk_name= cs.chunk_name
                ORDER BY range_start DESC"""
        ).fetchall()
        for i, chunk in enumerate(chunks, start=1):
            print(
                f"🍰 Chunk {i}: {chunk[0]}, Range: {chunk[1]} - {chunk[2]}, Compressed: {chunk[3]}, Size: {chunk[4]}"
            )


@app.command("table-drop")
@time_execution(sync=True, rank=False)
def table_drop():
    """Drop the sensors table to get a fresh start. Deletes all data, tables and cagg."""
    from utils.db import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        try:
            cur.execute("DROP TABLE IF EXISTS sensors CASCADE;")
            conn.commit()
            # PostgreSQL sets this after any command
            msg = cur.statusmessage  # e.g. "DROP TABLE"
            if msg.startswith("DROP TABLE"):
                print("✅ Sensors table dropped successfully.")
            else:
                print(f"⚠️ Unexpected status: {msg}")
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to drop table: {e}")


@app.command("read-csv")
@time_execution(sync=True, rank=False)
def read_csv():
    """Read sample data from a CSV file and print the rows. This is just reference code if you want to go for Kaggle data ingestion."""
    from utils.disk import read_csv_in_batches

    csv_path = "data/kaggle_power_consumption.csv"  # "data/sensors_sample_data.csv"
    batch_size = 10
    for batch in read_csv_in_batches(csv_path, batch_size=batch_size):
        print(batch)


#############################
# Interactive setup         #
#############################

ACTIONS = {
    "t6": ["run", "plot_raw_all", "plot_average_all", "plot_custom"],
    "t7": ["run", "plot_downsampled_all", "plot_average_all", "plot_histogram"],
    "t8": ["run", "init_cagg", "plot_all"],
    "s6": ["run", "plot_raw_all", "plot_average_all"],
    "s7": ["run", "plot_downsampled_all", "plot_average_all", "plot_histogram"],
    "s8": ["run", "init_cagg", "plot_all"],
    "bonus-kaggle": [
        "ingest_copy_kaggle_solution",
        "plot_downsampled_all",
    ],
}


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """
    Runs interactive mode when called without subcommands.
    Example:
        $ python mycli.py       # shows menu
        $ python mycli.py s7    # runs directly
    """
    if ctx.invoked_subcommand is None:
        # show all available commands
        commands = list(ctx.command.commands.keys())

        choice = questionary.select(
            "Choose a command to run:", choices=commands + ["(quit)"]
        ).ask()

        if not choice or choice == "(quit)":
            raise typer.Exit()

        if choice in ACTIONS:
            action = questionary.select(
                "Choose an action:",
                choices=ACTIONS[choice],
            ).ask()
            app(args=[choice, action], standalone_mode=False)
        else:
            app(args=[choice], standalone_mode=False)


if __name__ == "__main__":
    app()
