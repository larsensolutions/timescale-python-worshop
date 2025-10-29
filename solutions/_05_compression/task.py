# solutions/_05_compression/task.py
from utils.decorators import time_execution, db_write_once
from utils.disk import load_sql


# The time_execution decorator helps measure how long the function takes to run
@time_execution(sync=True, rank=False)
@db_write_once()
def apply_compression_solution(cursor):
    # TODO: Use the connection to verify the database is reachable
    """Verify the database connection."""
    return cursor.execute(load_sql("task.sql"))


def run():
    """Run the compression task."""
    apply_compression_solution()


if __name__ == "__main__":
    run()
