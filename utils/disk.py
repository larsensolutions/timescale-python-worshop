from pathlib import Path
import inspect
import csv
from datetime import datetime
from typing import Iterator, List, Tuple


def load_sql(filename: str) -> str:
    """
    Load <filename> relative to the *caller* module's file.
    """
    # caller = frame 1 (0 is this function)
    caller_file = inspect.stack()[1].filename
    base_dir = Path(caller_file).resolve().parent
    return (base_dir / filename).read_text(encoding="utf-8")


def save_to_file(filepath: str, data: str) -> None:
    """
    Save data to a file at the specified filepath.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(data)


def map_row(row: dict) -> Tuple[int, datetime, float]:
    """
    Map a CSV row (dict) into a typed tuple: (id, time, value). You will need to adjust this function
    according to the actual columns and types in your CSV file.
    """
    return (
        int(row["id"]),
        datetime.fromisoformat(row["time"]),
        float(row["value"]),
    )


def map_row_to_sensors(row: dict) -> List[Tuple[int, datetime, float]]:
    """
    Kaggle friendly mapper: as most datasets have one timestamp column and multiple sensor columns,
    this function maps a single CSV row into multiple (id, time, value) tuples.
    We are using column index (starting from 1) as device id.
    """
    time = datetime.strptime(row["Datetime"], "%m/%d/%Y %H:%M")

    sensor_rows: List[Tuple[int, datetime, float]] = []
    # enumerate over all columns except Datetime
    for i, (col, val) in enumerate(row.items()):
        if col == "Datetime":
            continue
        try:
            sensor_rows.append((i, time, float(val)))
        except ValueError:
            # skip empty or non-numeric values
            continue
    return sensor_rows


def read_csv_in_batches(path: str, batch_size: int) -> Iterator[List[dict]]:
    """
    Yields batches of rows from a CSV file as lists of dicts.

    :param path: Path to the CSV file
    :param batch_size: Number of rows per batch
    :yield: List[dict] for each batch

    Usage:
    for batch in read_csv_in_batches("data.csv", batch_size=1000):
        # process batch here
    """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            # batch.append(map_row(row)) # Use this line if you want mapped rows and skip the flat-mapping

            for row in reader:
                mapped = map_row_to_sensors(
                    row
                )  # Use this line for Kaggle style mapping

                for item in mapped:  # basic flat-map
                    batch.append(item)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
        if batch:
            yield batch
